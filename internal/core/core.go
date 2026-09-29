// Package core runs the agent. Every device has a pipeline of its own: the
// device's reader frames what the port sends, a bounded channel holds the
// frames, and a publisher sends each one to the device's rx topic. The core
// records the outcome of every delivery, and shuts down in the order that lets
// the broker tell a clean stop from a crash.
//
// The transport is an interface, so that backpressure, publish timeouts,
// delivery records and the shutdown order can be tested without a broker.
package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/skuhus/device-agent/internal/device"
	"github.com/skuhus/device-agent/internal/logging"
	"github.com/skuhus/device-agent/internal/wire"
)

// DefaultDrainTimeout bounds the shutdown drain. A reading is perishable, so
// spending a minute delivering a backlog nobody can act on is worse than
// dropping it and recording it: the operator who rescans is ahead of the one
// reading a stale pick.
const DefaultDrainTimeout = 5 * time.Second

// Transport is the broker connection as the core uses it.
type Transport interface {
	PublishRx(ctx context.Context, topic string, payload []byte, expiry time.Duration) error
	PublishOffline(ctx context.Context, topic string, payload []byte) error
	Close(ctx context.Context) error
}

// Deliveries records the outcome of every reading that reached a publisher.
// logging.Audit implements it.
type Deliveries interface {
	Append(record logging.AuditRecord) error
}

// Device is one configured device as the core runs it.
type Device struct {
	Reader device.Device
	// Wire is how the device appears in its messages: its id, its type, and the
	// message expiry every reading is published with.
	Wire   wire.Device
	Topics wire.DeviceTopics
}

// Options configures the core. Configuration rules are the configuration
// package's to enforce; New refuses only what would make Run fail or hang.
type Options struct {
	Devices   []Device
	Transport Transport
	Builder   *wire.Builder
	// AgentStatus is the agent's status topic, where the offline message goes.
	AgentStatus string

	PublishTimeout time.Duration
	// BufferSize is how many frames each device's channel holds. When it is
	// full the reader blocks, which is the intended backpressure: a stalled
	// broker shows as a stalled device rather than as a queue growing unseen.
	BufferSize   int
	DrainTimeout time.Duration

	// Station is named in every delivery record.
	Station     string
	LogPayloads bool
	Logger      *slog.Logger
	// Deliveries may be nil, in which case outcomes are only logged.
	Deliveries Deliveries
	// Now defaults to time.Now. It is a field so tests do not have to sleep.
	Now func() time.Time
}

// Core is one running agent.
type Core struct {
	opts Options
	log  *slog.Logger
	// drainDeadlineMS is zero while the devices run, and is set to the moment
	// the shutdown drain gives up. Written by Run, read by every publisher.
	drainDeadlineMS atomic.Int64
}

// pipeline is one device's channel and the sequence number of its frames.
type pipeline struct {
	device Device
	frames chan device.Frame
	seq    uint64
}

// New checks the options and builds the core. It opens nothing: the readers
// open their ports in Run, and the connection is the caller's.
func New(opts Options) (*Core, error) {
	switch {
	case opts.Transport == nil:
		return nil, errors.New("core: transport is required")
	case opts.Builder == nil:
		return nil, errors.New("core: message builder is required")
	case len(opts.Devices) == 0:
		return nil, errors.New("core: at least one device is required")
	case opts.AgentStatus == "":
		return nil, errors.New("core: the agent's status topic is required")
	case opts.PublishTimeout <= 0:
		return nil, fmt.Errorf("core: publish timeout must be positive, got %s", opts.PublishTimeout)
	case opts.BufferSize <= 0:
		return nil, fmt.Errorf("core: buffer size must be positive, got %d", opts.BufferSize)
	}
	for i, dev := range opts.Devices {
		if dev.Reader == nil {
			return nil, fmt.Errorf("core: device %d has no reader", i)
		}
		if dev.Topics.Rx() == "" {
			return nil, fmt.Errorf("core: device %s has no topics", dev.Wire.ID)
		}
	}
	if opts.DrainTimeout <= 0 {
		opts.DrainTimeout = DefaultDrainTimeout
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Core{opts: opts, log: opts.Logger}, nil
}

// Run reads from every device and publishes until ctx is cancelled.
//
// Shutdown order matters and is deliberate. The readers stop first, so nothing
// new arrives. The publishers drain what is already framed, for at most the
// drain timeout. The offline message goes out, and only then does the
// connection close. Closing first would make the broker publish the will
// instead, reporting a crash where there was an orderly stop.
func (core *Core) Run(ctx context.Context) error {
	pipelines := make([]*pipeline, 0, len(core.opts.Devices))
	for _, dev := range core.opts.Devices {
		pipelines = append(pipelines, &pipeline{device: dev, frames: make(chan device.Frame, core.opts.BufferSize)})
	}

	// The readers run on their own context, not ctx, so that Run decides when
	// they stop relative to everything else.
	readerCtx, stopReaders := context.WithCancel(context.Background())
	defer stopReaders()
	var readers, publishers sync.WaitGroup
	for _, line := range pipelines {
		readers.Add(1)
		go func(line *pipeline) {
			defer readers.Done()
			reader := line.device.Reader
			core.log.Info("device starting", "device_id", reader.ID(), "device_kind", reader.Kind(), "device_path", reader.Path(),
				"rx_topic", line.device.Topics.Rx(), "message_expiry", line.device.Wire.Expiry.String())
			if err := reader.Run(readerCtx, line.frames, core.report); err != nil && !errors.Is(err, context.Canceled) {
				core.log.Error("device stopped", "device_id", reader.ID(), "error", err.Error())
				return
			}
			core.log.Info("device stopped", "device_id", reader.ID())
		}(line)
		publishers.Add(1)
		go func(line *pipeline) {
			defer publishers.Done()
			core.publish(line)
		}(line)
	}

	<-ctx.Done()
	core.log.Info("shutting down", "buffered", buffered(pipelines))

	stopReaders()
	readers.Wait()
	core.drainDeadlineMS.Store(core.opts.Now().Add(core.opts.DrainTimeout).UnixMilli())
	for _, line := range pipelines {
		close(line.frames)
	}
	publishers.Wait()

	core.publishOffline()

	// Bounded, because Close waits for the connection manager to finish, and
	// that involves writing a DISCONNECT to a socket which may be attached to a
	// network that has gone away. An unbounded wait here turns "the WAN
	// dropped" into "the service will not stop", and the agent has already
	// said everything it had to say.
	closeCtx, cancelClose := context.WithTimeout(context.Background(), core.opts.PublishTimeout)
	defer cancelClose()
	if err := core.opts.Transport.Close(closeCtx); err != nil {
		core.log.Warn("broker disconnect failed", "error", err.Error())
	}
	core.log.Info("stopped")
	return nil
}

// publish drains one device's frames until its channel is closed. It is the
// device's only publisher, so its sequence numbers are in reading order.
//
// During the shutdown drain each frame still gets its full publish timeout,
// but the drain as a whole stops at the deadline Run sets. Without it, a full
// buffer against an unresponsive broker would hold the process open for
// buffer_size times publish_timeout.
func (core *Core) publish(line *pipeline) {
	var dropped int
	for frame := range line.frames {
		line.seq++
		rx := core.opts.Builder.Rx(line.device.Wire, line.seq, frame.Raw, frame.At)
		if deadline := core.drainDeadlineMS.Load(); deadline > 0 && core.opts.Now().UnixMilli() > deadline {
			core.log.Warn("rx dropped", "id", rx.ID, "device_id", rx.DeviceID, "seq", rx.Seq, "bytes", len(frame.Raw),
				"reason", "shutdown drain deadline passed")
			core.record(rx, len(frame.Raw), logging.OutcomeDropped, "shutdown drain deadline passed")
			dropped++
			continue
		}
		core.publishRx(line, rx, len(frame.Raw))
	}
	if dropped > 0 {
		core.log.Warn("rx dropped at shutdown", "device_id", line.device.Wire.ID, "count", dropped,
			"drain_timeout", core.opts.DrainTimeout.String())
	}
}

// publishRx sends one reading and records the outcome either way.
func (core *Core) publishRx(line *pipeline, rx wire.Rx, size int) {
	attrs := []any{"id", rx.ID, "device_id", rx.DeviceID, "seq", rx.Seq, "bytes", size, "text_valid", rx.TextValid}
	payload, err := json.Marshal(rx)
	if err != nil {
		// Encoding a struct of strings and numbers cannot fail in practice. It
		// is recorded rather than ignored, because a reading that never reached
		// the broker and left no trace is the one failure the record exists to
		// make impossible.
		core.log.Error("rx could not be encoded", append(attrs, "error", err.Error())...)
		core.record(rx, size, logging.OutcomeDropped, err.Error())
		return
	}

	// A fresh context, not the run context: at shutdown the run context is
	// already cancelled, and the frames still in hand deserve their full
	// publish budget.
	ctx, cancel := context.WithTimeout(context.Background(), core.opts.PublishTimeout)
	err = core.opts.Transport.PublishRx(ctx, line.device.Topics.Rx(), payload, line.device.Wire.Expiry)
	cancel()
	if err != nil {
		core.log.Error("rx publish failed", append(attrs, "error", err.Error())...)
		core.record(rx, size, logging.OutcomeFailed, err.Error())
		return
	}
	core.log.Info("rx published", attrs...)
	if core.opts.LogPayloads {
		core.log.Debug("rx payload", "id", rx.ID, "raw_b64", rx.RawB64)
	}
	core.record(rx, size, logging.OutcomePublished, "")
}

// record writes one delivery outcome. A reading the broker never took is
// written down whole, because nothing else holds it; this is not gated on
// log_payloads, because a setting that silently turns data loss back on is not
// a privacy control.
func (core *Core) record(rx wire.Rx, size int, outcome logging.Outcome, detail string) {
	if core.opts.Deliveries == nil {
		return
	}
	entry := logging.AuditRecord{
		EventID:  rx.ID,
		Station:  core.opts.Station,
		DeviceID: rx.DeviceID,
		Outcome:  outcome,
		Bytes:    size,
		Seq:      rx.Seq,
		Detail:   detail,
	}
	if outcome != logging.OutcomePublished {
		entry.RawB64, entry.Text = rx.RawB64, rx.Text
	}
	if err := core.opts.Deliveries.Append(entry); err != nil {
		core.log.Error("delivery record write failed", "id", rx.ID, "device_id", rx.DeviceID, "error", err.Error())
	}
}

// report receives every port event from every reader. The readers log each
// event in its own terms; this records it once more, uniformly, at DEBUG. It
// is called inline by a reader and must not block.
func (core *Core) report(event device.Event) {
	attrs := []any{"device_id", event.DeviceID, "event", string(event.Kind)}
	if event.ErrorClass != "" {
		attrs = append(attrs, "error_class", event.ErrorClass)
	}
	if event.Err != nil {
		attrs = append(attrs, "error", event.Err.Error())
	}
	if event.Kind == device.BytesDiscarded {
		attrs = append(attrs, "reason", event.Reason, "bytes", event.Bytes)
	}
	core.log.Debug("port event", attrs...)
}

// publishOffline sends the offline message. A failure is logged and not
// retried: the connection is about to close, and a consumer learns the agent
// is gone from the missing keepalives in any case.
func (core *Core) publishOffline() {
	payload, err := json.Marshal(core.opts.Builder.Offline(wire.OfflineShutdown, core.opts.Now()))
	if err != nil {
		core.log.Error("offline message could not be encoded", "error", err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), core.opts.PublishTimeout)
	defer cancel()
	if err := core.opts.Transport.PublishOffline(ctx, core.opts.AgentStatus, payload); err != nil {
		core.log.Warn("offline message publish failed", "topic", core.opts.AgentStatus, "error", err.Error())
		return
	}
	core.log.Info("offline message published", "topic", core.opts.AgentStatus, "reason", string(wire.OfflineShutdown))
}

func buffered(pipelines []*pipeline) int {
	total := 0
	for _, line := range pipelines {
		total += len(line.frames)
	}
	return total
}
