// Package core runs the agent. Every device has a pipeline of its own: the
// device's reader frames what the port sends, a bounded channel holds the
// frames, and a publisher sends each one to the device's rx topic. The reader's
// port events are counted, and published on the device's status topic by a
// second publisher. A keepalive reports every device's counters on the agent's
// status topic. The core records the outcome of every delivery, and shuts down
// in the order that lets the broker tell a clean stop from a crash.
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

// DefaultEventBufferSize is how many port events each device holds for its
// event publisher. The reader hands events over inline and must not block, so
// an event that finds the buffer full is logged and not published. Nothing is
// lost from the keepalive, which counted the event before it was queued.
const DefaultEventBufferSize = 64

// Transport is the broker connection as the core uses it. Each method publishes
// one kind of message with the QoS and expiry DESIGN-V2.md, "Publishing",
// assigns it.
type Transport interface {
	PublishRx(ctx context.Context, topic string, payload []byte, expiry time.Duration) error
	PublishEvent(ctx context.Context, topic string, payload []byte, expiry time.Duration) error
	PublishKeepalive(ctx context.Context, topic string, payload []byte, expiry time.Duration) error
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
	// message expiry every reading and event is published with.
	Wire   wire.Device
	Topics wire.DeviceTopics
}

// Options configures the core. Configuration rules are the configuration
// package's to enforce; New refuses only what would make Run fail or hang.
type Options struct {
	Devices   []Device
	Transport Transport
	Builder   *wire.Builder
	// AgentStatus is the agent's status topic, where the keepalive and the
	// offline message go.
	AgentStatus string

	PublishTimeout time.Duration
	// BufferSize is how many frames each device's channel holds. When it is
	// full the reader blocks, which is the intended backpressure: a stalled
	// broker shows as a stalled device rather than as a queue growing unseen.
	BufferSize   int
	DrainTimeout time.Duration
	// EventBufferSize defaults to DefaultEventBufferSize.
	EventBufferSize int

	// KeepaliveInterval is how often the keepalive goes out, and
	// MissedKeepalives how many of them a consumer waits for before it treats
	// the agent as gone. Every keepalive carries both (DESIGN-V2.md, "Agent
	// keepalive").
	KeepaliveInterval time.Duration
	MissedKeepalives  int
	// Connected receives a value each time the broker connection comes up, and
	// a keepalive goes out at once: a consumer learns that the agent is there
	// without waiting an interval, including after a will. It may be nil.
	Connected <-chan struct{}
	// Started is when the process started, which the keepalive's uptime counts
	// from. It defaults to when New is called.
	Started time.Time

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

// pipeline is one device's channels, the sequence number of its frames, and
// what the keepalive reports about it.
type pipeline struct {
	device Device
	frames chan device.Frame
	events chan device.Event
	// seq is written only by the device's rx publisher.
	seq uint64

	// mu guards open and counters, which the reader, the rx publisher and the
	// keepalive all reach.
	mu       sync.Mutex
	open     bool
	counters wire.DeviceCounters
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
	case opts.KeepaliveInterval <= 0:
		return nil, fmt.Errorf("core: keepalive interval must be positive, got %s", opts.KeepaliveInterval)
	case opts.MissedKeepalives < 1:
		return nil, fmt.Errorf("core: missed keepalives must be at least 1, got %d", opts.MissedKeepalives)
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
	if opts.EventBufferSize <= 0 {
		opts.EventBufferSize = DefaultEventBufferSize
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Started.IsZero() {
		opts.Started = opts.Now()
	}
	return &Core{opts: opts, log: opts.Logger}, nil
}

// Run reads from every device and publishes until ctx is cancelled.
//
// Shutdown order matters and is deliberate. The keepalive stops, so none
// follows the offline message. The readers stop, so nothing new arrives; each
// reports its port closed. The publishers drain what is already framed and
// queued, for at most the drain timeout. The offline message goes out, and only
// then does the connection close. Closing first would make the broker publish
// the will instead, reporting a crash where there was an orderly stop.
func (core *Core) Run(ctx context.Context) error {
	pipelines := make([]*pipeline, 0, len(core.opts.Devices))
	for _, dev := range core.opts.Devices {
		pipelines = append(pipelines, &pipeline{
			device: dev,
			frames: make(chan device.Frame, core.opts.BufferSize),
			events: make(chan device.Event, core.opts.EventBufferSize),
		})
	}

	// The readers run on their own context, not ctx, so that Run decides when
	// they stop relative to everything else.
	readerCtx, stopReaders := context.WithCancel(context.Background())
	defer stopReaders()
	var readers, publishers, keepalives sync.WaitGroup
	for _, line := range pipelines {
		readers.Add(1)
		go func(line *pipeline) {
			defer readers.Done()
			reader := line.device.Reader
			core.log.Info("device starting", "device_id", reader.ID(), "device_kind", reader.Kind(), "device_path", reader.Path(),
				"rx_topic", line.device.Topics.Rx(), "status_topic", line.device.Topics.Status(),
				"message_expiry", line.device.Wire.Expiry.String())
			report := func(event device.Event) { core.report(line, event) }
			if err := reader.Run(readerCtx, line.frames, report); err != nil && !errors.Is(err, context.Canceled) {
				core.log.Error("device reader failed", "device_id", reader.ID(), "error", err.Error())
				return
			}
			core.log.Info("device reader stopped", "device_id", reader.ID())
		}(line)
		publishers.Add(2)
		go func(line *pipeline) {
			defer publishers.Done()
			core.publish(line)
		}(line)
		go func(line *pipeline) {
			defer publishers.Done()
			core.publishEvents(line)
		}(line)
	}

	stopKeepalive := make(chan struct{})
	keepalives.Add(1)
	go func() {
		defer keepalives.Done()
		core.keepalive(stopKeepalive, pipelines)
	}()

	<-ctx.Done()
	core.log.Info("shutting down", "buffered", buffered(pipelines))

	close(stopKeepalive)
	keepalives.Wait()
	stopReaders()
	readers.Wait()
	core.drainDeadlineMS.Store(core.opts.Now().Add(core.opts.DrainTimeout).UnixMilli())
	for _, line := range pipelines {
		close(line.frames)
		close(line.events)
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
		line.countFrame()
		rx := core.opts.Builder.Rx(line.device.Wire, line.seq, frame.Raw, frame.At)
		if core.pastDrainDeadline() {
			core.log.Warn("rx dropped", "id", rx.ID, "device_id", rx.DeviceID, "seq", rx.Seq, "bytes", len(frame.Raw),
				"reason", "shutdown drain deadline passed")
			line.countPublishFailure()
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
		line.countPublishFailure()
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
		line.countPublishFailure()
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

// report receives every port event from one device's reader. It counts the
// event, logs it, and queues the ones consumers see for the device's event
// publisher. It is called inline by the reader and must not block, so an event
// that finds the queue full is logged and not published; the keepalive has
// counted it either way.
func (core *Core) report(line *pipeline, event device.Event) {
	core.count(line, event)
	if event.Kind == device.BytesRead {
		// Counted only. The reader logs every read at DEBUG already.
		return
	}
	attrs := eventAttrs(event)
	core.log.Debug("port event", attrs...)
	select {
	case line.events <- event:
	default:
		core.log.Warn("device event not published: queue full", append(attrs, "queue_size", cap(line.events))...)
	}
}

// count updates what the keepalive reports about the device.
func (core *Core) count(line *pipeline, event device.Event) {
	line.mu.Lock()
	defer line.mu.Unlock()
	switch event.Kind {
	case device.PortOpened:
		line.open = true
	case device.PortClosed, device.PortLost:
		line.open = false
	case device.PortOpenFailed:
		line.open = false
		class, known := errorClass(event.ErrorClass)
		if !known {
			core.log.Error("port event carries an error class the keepalive has no counter for; counted as unknown",
				eventAttrs(event)...)
		}
		countFailedOpen(&line.counters.FailedOpens, class)
	case device.BytesDiscarded:
		if !countDiscard(&line.counters.Discards, wire.DiscardReason(event.Reason)) {
			core.log.Error("port event carries a discard reason the keepalive has no counter for; not counted",
				eventAttrs(event)...)
		}
	case device.BytesRead:
		line.counters.RxBytes += uint64(event.Bytes)
	}
}

func (line *pipeline) countFrame() {
	line.mu.Lock()
	defer line.mu.Unlock()
	line.counters.RxFrames++
}

func (line *pipeline) countPublishFailure() {
	line.mu.Lock()
	defer line.mu.Unlock()
	line.counters.PublishFailures++
}

// state is the device as the keepalive reports it now.
func (line *pipeline) state() wire.DeviceState {
	line.mu.Lock()
	defer line.mu.Unlock()
	counters := line.counters
	counters.BufferDepth = len(line.frames)
	return wire.DeviceState{Device: line.device.Wire, Open: line.open, Counters: counters}
}

// publishEvents sends one device's port events, in the order they happened,
// until its queue is closed. An event is not retried, as a reading is not: the
// keepalive counts what it reported either way. Past the shutdown drain
// deadline the rest are logged and not published.
func (core *Core) publishEvents(line *pipeline) {
	for event := range line.events {
		message, ok := core.buildEvent(line, event)
		if !ok {
			continue
		}
		attrs := []any{"id", message.ID, "device_id", message.DeviceID, "code", string(message.Code),
			"device_open", message.DeviceOpen, "text", message.Text}
		if core.pastDrainDeadline() {
			core.log.Warn("device event dropped", append(attrs, "reason", "shutdown drain deadline passed")...)
			continue
		}
		payload, err := json.Marshal(message)
		if err != nil {
			core.log.Error("device event could not be encoded", append(attrs, "error", err.Error())...)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), core.opts.PublishTimeout)
		err = core.opts.Transport.PublishEvent(ctx, line.device.Topics.Status(), payload, line.device.Wire.Expiry)
		cancel()
		if err != nil {
			core.log.Warn("device event publish failed", append(attrs, "error", err.Error())...)
			continue
		}
		core.log.Info("device event published", attrs...)
	}
}

// buildEvent turns a port event into the message consumers see. The time is
// when the event happened, as a reading's is when it was read.
func (core *Core) buildEvent(line *pipeline, event device.Event) (wire.Event, bool) {
	dev, path, at := line.device.Wire, line.device.Reader.Path(), event.At
	builder := core.opts.Builder
	switch event.Kind {
	case device.PortOpened:
		return builder.PortOpened(dev, path, at), true
	case device.PortClosed:
		return builder.PortClosed(dev, path, at), true
	case device.PortLost:
		class, _ := errorClass(event.ErrorClass)
		return builder.PortLost(dev, path, class, errString(event.Err), at), true
	case device.PortOpenFailed:
		class, _ := errorClass(event.ErrorClass)
		return builder.PortOpenFailed(dev, path, class, errString(event.Err), at), true
	case device.BytesDiscarded:
		return builder.BytesDiscarded(dev, wire.DiscardReason(event.Reason), event.Bytes, at), true
	}
	core.log.Error("port event of a kind with no message; not published", eventAttrs(event)...)
	return wire.Event{}, false
}

// keepalive publishes the agent's keepalive every interval, and at once each
// time the connection comes up, until stop is closed.
func (core *Core) keepalive(stop <-chan struct{}, pipelines []*pipeline) {
	ticker := time.NewTicker(core.opts.KeepaliveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-core.opts.Connected:
			core.publishKeepalive(pipelines, "connected")
		case <-ticker.C:
			core.publishKeepalive(pipelines, "interval")
		}
	}
}

// publishKeepalive sends one keepalive with every device's state. A failure is
// logged and not retried: the next one is an interval away.
func (core *Core) publishKeepalive(pipelines []*pipeline, trigger string) {
	states := make([]wire.DeviceState, 0, len(pipelines))
	for _, line := range pipelines {
		states = append(states, line.state())
	}
	message := core.opts.Builder.Keepalive(core.opts.Started, core.opts.Now(), core.opts.KeepaliveInterval,
		core.opts.MissedKeepalives, states)
	attrs := []any{"id", message.ID, "trigger", trigger, "uptime_s", message.UptimeS, "gone_after_s", message.GoneAfterS}
	payload, err := json.Marshal(message)
	if err != nil {
		core.log.Error("keepalive could not be encoded", append(attrs, "error", err.Error())...)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), core.opts.PublishTimeout)
	defer cancel()
	goneAfter := time.Duration(message.GoneAfterS) * time.Second
	if err := core.opts.Transport.PublishKeepalive(ctx, core.opts.AgentStatus, payload, goneAfter); err != nil {
		core.log.Warn("keepalive publish failed", append(attrs, "topic", core.opts.AgentStatus, "error", err.Error())...)
		return
	}
	core.log.Debug("keepalive published", append(attrs, "payload", string(payload))...)
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

func (core *Core) pastDrainDeadline() bool {
	deadline := core.drainDeadlineMS.Load()
	return deadline > 0 && core.opts.Now().UnixMilli() > deadline
}

// errorClass maps a reader's error class to the wire's. A class the wire does
// not define is reported as unknown, and false says so.
func errorClass(class string) (wire.ErrorClass, bool) {
	switch known := wire.ErrorClass(class); known {
	case wire.ErrorAbsent, wire.ErrorBusy, wire.ErrorPermissionDenied, wire.ErrorReadOnly,
		wire.ErrorDisconnected, wire.ErrorPortError, wire.ErrorUnknown:
		return known, true
	}
	return wire.ErrorUnknown, false
}

func countFailedOpen(counts *wire.OpenFailureCounts, class wire.ErrorClass) {
	switch class {
	case wire.ErrorAbsent:
		counts.Absent++
	case wire.ErrorBusy:
		counts.Busy++
	case wire.ErrorPermissionDenied:
		counts.PermissionDenied++
	case wire.ErrorReadOnly:
		counts.ReadOnly++
	case wire.ErrorDisconnected:
		counts.Disconnected++
	case wire.ErrorPortError:
		counts.PortError++
	default:
		counts.Unknown++
	}
}

// countDiscard reports false for a reason with no counter.
func countDiscard(counts *wire.DiscardCounts, reason wire.DiscardReason) bool {
	switch reason {
	case wire.DiscardOversize:
		counts.Oversize++
	case wire.DiscardInterCharTimeout:
		counts.InterCharTimeout++
	case wire.DiscardResync:
		counts.Resync++
	case wire.DiscardEmptyFrame:
		counts.EmptyFrame++
	default:
		return false
	}
	return true
}

// eventAttrs are a port event's log attributes.
func eventAttrs(event device.Event) []any {
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
	return attrs
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func buffered(pipelines []*pipeline) int {
	total := 0
	for _, line := range pipelines {
		total += len(line.frames)
	}
	return total
}
