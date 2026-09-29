// Package agent is the supervisor from section 10: it owns the device
// goroutines, the bounded channel between them and the publisher, the status
// reporting and the heartbeat.
//
// The publisher is an interface rather than the MQTT client so the supervisor's
// behaviour - backpressure, publish timeouts, audit outcomes, what happens at
// shutdown - can be tested without a broker.
package agent

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
	"github.com/skuhus/device-agent/internal/event"
	"github.com/skuhus/device-agent/internal/logging"
	"github.com/skuhus/device-agent/internal/transport/mqtt"
)

// DefaultDrainTimeout bounds the shutdown drain. A scan is perishable, so
// spending a minute delivering a backlog nobody can act on is worse than
// dropping it and saying so: the operator who rescans is ahead of the one
// reading a stale pick.
//
// It is an internal constant rather than a configuration key, for the reason
// section 9 fixes the schema: this is a tuning value, not a per-station
// decision.
const DefaultDrainTimeout = 5 * time.Second

// Publisher is the transport as the supervisor uses it.
type Publisher interface {
	PublishScan(ctx context.Context, payload []byte, ttl time.Duration) error
	PublishStatus(ctx context.Context, payload []byte) error
	PublishHeartbeat(ctx context.Context, payload []byte) error
	Close(ctx context.Context) error
}

// Options configures the supervisor. Everything without a default is required.
type Options struct {
	Devices   []device.Device
	Publisher Publisher
	Builder   *event.Builder
	Presence  *Presence

	// Connected fires each time the broker connection comes up, including
	// reconnections. Section 5.5 publishes the retained status on every
	// connect, because the will may have replaced it in the meantime.
	Connected <-chan struct{}

	ScanTTL        time.Duration
	PublishTimeout time.Duration
	BufferSize     int

	// Identity is stamped into every status and heartbeat, and its Station is
	// what audit records name. The same value belongs in the Builder, so that
	// a scan and a heartbeat from one process cannot disagree about who sent
	// them.
	Identity event.Identity
	// LogPayloads allows scan contents into the log at DEBUG. Default false
	// keeps payloads out of the log at every level, matching the device layer.
	LogPayloads bool

	Logger *slog.Logger
	// Audit records the outcome of every scan that reached the publisher. May
	// be nil, in which case nothing is recorded.
	Audit *logging.Audit

	// Now defaults to time.Now. It is a field so tests do not have to sleep.
	Now func() time.Time

	HeartbeatInterval time.Duration
	DrainTimeout      time.Duration
}

// Agent is one running supervisor.
type Agent struct {
	opts  Options
	log   *slog.Logger
	start time.Time

	scanCount       atomic.Uint64
	publishFailures atomic.Uint64
	lastScanUnixMS  atomic.Int64
	// drainDeadlineMS is zero while the devices are running, and is set to the
	// moment the shutdown drain gives up. Read by the publisher goroutine,
	// written by Run.
	drainDeadlineMS atomic.Int64
	bufferDepth     func() int
}

// New validates the options and builds the supervisor. It opens nothing: the
// devices open themselves in Run, and the connection is the caller's.
func New(opts Options) (*Agent, error) {
	if opts.Publisher == nil {
		return nil, errors.New("agent: publisher is required")
	}
	if opts.Builder == nil {
		return nil, errors.New("agent: event builder is required")
	}
	if len(opts.Devices) == 0 {
		return nil, errors.New("agent: at least one device is required")
	}
	if opts.ScanTTL <= 0 {
		return nil, fmt.Errorf("agent: scan ttl must be positive, got %s", opts.ScanTTL)
	}
	if opts.PublishTimeout <= 0 {
		return nil, fmt.Errorf("agent: publish timeout must be positive, got %s", opts.PublishTimeout)
	}
	if opts.PublishTimeout > opts.ScanTTL {
		// The configuration layer rejects this too. Repeated here because a
		// caller that built Options by hand would otherwise report a scan as
		// failed after the broker had already discarded it.
		return nil, fmt.Errorf("agent: publish timeout %s is longer than the scan ttl %s", opts.PublishTimeout, opts.ScanTTL)
	}
	if opts.BufferSize <= 0 {
		return nil, fmt.Errorf("agent: buffer size must be positive, got %dev", opts.BufferSize)
	}

	if opts.Presence == nil {
		ids := make([]string, 0, len(opts.Devices))
		for _, dev := range opts.Devices {
			ids = append(ids, dev.ID())
		}
		opts.Presence = NewPresence(ids...)
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.HeartbeatInterval <= 0 {
		opts.HeartbeatInterval = mqtt.HeartbeatInterval
	}
	if opts.DrainTimeout <= 0 {
		opts.DrainTimeout = DefaultDrainTimeout
	}

	return &Agent{opts: opts, log: opts.Logger, start: opts.Now()}, nil
}

// Run reads from the devices and publishes until ctx is cancelled.
//
// Shutdown order matters and is deliberate: the devices stop first so nothing
// new arrives, the publisher drains what is already framed, the offline status
// goes out, and only then does the connection close. Closing first would make
// the broker deliver the will instead, reporting a crash where there was an
// orderly stop.
func (agent *Agent) Run(ctx context.Context) error {
	frames := make(chan device.Frame, agent.opts.BufferSize)
	agent.bufferDepth = func() int { return len(frames) }

	deviceCtx, stopDevices := context.WithCancel(context.Background())
	defer stopDevices()

	var devices sync.WaitGroup
	for _, dev := range agent.opts.Devices {
		devices.Add(1)
		go func(dev device.Device) {
			defer devices.Done()
			agent.log.Info("device starting", "device_id", dev.ID(), "device_kind", dev.Kind(), "device_path", dev.Path())
			if err := dev.Run(deviceCtx, frames); err != nil && !errors.Is(err, context.Canceled) {
				agent.log.Error("device stopped", "device_id", dev.ID(), "error", err.Error())
				return
			}
			agent.log.Info("device stopped", "device_id", dev.ID())
		}(dev)
	}

	var background sync.WaitGroup
	background.Add(2)
	go func() { defer background.Done(); agent.reportStatus(ctx) }()
	go func() { defer background.Done(); agent.beat(ctx) }()

	publisherDone := make(chan struct{})
	go func() {
		defer close(publisherDone)
		agent.publish(frames)
	}()

	<-ctx.Done()
	agent.log.Info("shutting down", "scans", agent.scanCount.Load(), "publish_failures", agent.publishFailures.Load(),
		"buffered", len(frames))

	stopDevices()
	devices.Wait()
	agent.drainDeadlineMS.Store(agent.opts.Now().Add(agent.opts.DrainTimeout).UnixMilli())
	close(frames)
	<-publisherDone
	background.Wait()

	agent.publishStatus(event.StateOffline, event.ReasonShutdown)

	// Bounded, because Close waits for the connection manager to finish and
	// that involves writing a DISCONNECT to a socket which may be attached to
	// a network that has gone away. An unbounded wait here turns "the WAN
	// dropped" into "the service will not stop", and the agent has already
	// said everything it had to say.
	closeCtx, cancelClose := context.WithTimeout(context.Background(), agent.opts.PublishTimeout)
	defer cancelClose()
	if err := agent.opts.Publisher.Close(closeCtx); err != nil {
		agent.log.Warn("broker disconnect failed", "error", err.Error())
	}
	agent.log.Info("stopped", "scans", agent.scanCount.Load(), "publish_failures", agent.publishFailures.Load())
	return nil
}

// publish drains frames until the channel is closed. It runs on its own
// goroutine and is the only writer of scans, which is what section 10 means by
// one writer per connection.
//
// During the shutdown drain each frame still gets its full publish timeout, but
// the drain as a whole stops at the deadline Run sets. Without that bound, a
// full buffer against an unresponsive broker would hold the process open for
// buffer_size times publish_timeout, which for the defaults is over two
// minutes of delivering scans whose sessions have ended.
func (agent *Agent) publish(frames <-chan device.Frame) {
	var dropped int
	for frame := range frames {
		if deadline := agent.drainDeadlineMS.Load(); deadline > 0 && agent.opts.Now().UnixMilli() > deadline {
			agent.dropped(frame, "shutdown drain deadline passed")
			dropped++
			continue
		}
		agent.publishScan(frame)
	}
	if dropped > 0 {
		agent.log.Warn("scans dropped at shutdown", "count", dropped, "drain_timeout", agent.opts.DrainTimeout.String())
	}
}

// publishScan builds one envelope and sends it, recording the outcome in the
// audit log either way.
func (agent *Agent) publishScan(frame device.Frame) {
	scan := agent.opts.Builder.Scan(frame.Raw, frame.At, frame.DeviceID)
	payload, err := json.Marshal(scan)
	if err != nil {
		// Marshalling a struct of strings and numbers cannot fail in practice.
		// It is recorded rather than ignored because a scan that never reached
		// the broker and left no trace is the one failure the audit log exists
		// to make impossible.
		agent.log.Error("scan envelope could not be encoded", "event_id", scan.EventID,
			"device_id", frame.DeviceID, "error", err.Error())
		agent.audit(scan, len(frame.Raw), logging.OutcomeDropped, err.Error())
		return
	}

	// A fresh context, not the run context: at shutdown the run context is
	// already cancelled, and the frames still in hand deserve their full
	// publish budget.
	ctx, cancel := context.WithTimeout(context.Background(), agent.opts.PublishTimeout)
	err = agent.opts.Publisher.PublishScan(ctx, payload, agent.opts.ScanTTL)
	cancel()

	attrs := []any{
		"event_id", scan.EventID,
		"device_id", frame.DeviceID,
		"seq", scan.Seq,
		"bytes", len(frame.Raw),
		"text_valid", scan.TextValid,
	}
	if err != nil {
		agent.publishFailures.Add(1)
		agent.log.Error("scan publish failed", append(attrs, "error", err.Error())...)
		agent.audit(scan, len(frame.Raw), logging.OutcomeFailed, err.Error())
		return
	}

	agent.scanCount.Add(1)
	agent.lastScanUnixMS.Store(frame.At.UnixMilli())
	agent.log.Info("scan published", attrs...)
	if agent.opts.LogPayloads {
		agent.log.Debug("scan payload", "event_id", scan.EventID, "raw_b64", scan.RawB64)
	}
	agent.audit(scan, len(frame.Raw), logging.OutcomePublished, "")
}

// dropped records a scan that never reached the publisher.
func (agent *Agent) dropped(frame device.Frame, reason string) {
	scan := agent.opts.Builder.Scan(frame.Raw, frame.At, frame.DeviceID)
	agent.log.Warn("scan dropped", "event_id", scan.EventID, "device_id", frame.DeviceID,
		"bytes", len(frame.Raw), "reason", reason)
	agent.audit(scan, len(frame.Raw), logging.OutcomeDropped, reason)
}

func (agent *Agent) audit(scan event.Scan, bytes int, outcome logging.Outcome, detail string) {
	if agent.opts.Audit == nil {
		return
	}
	record := logging.AuditRecord{
		EventID:  scan.EventID,
		Station:  agent.opts.Identity.Station,
		DeviceID: scan.DeviceID,
		Outcome:  outcome,
		Bytes:    bytes,
		Seq:      scan.Seq,
		Detail:   detail,
	}
	// A scan the broker never took is written down in full, because nothing
	// else holds it. Not gated on log_payloads: a setting that silently turns
	// data loss back on is not a privacy control.
	if outcome != logging.OutcomePublished {
		record.RawB64, record.Text = scan.RawB64, scan.Text
	}
	err := agent.opts.Audit.Append(record)
	if err != nil {
		agent.log.Error("audit log write failed", "event_id", scan.EventID, "error", err.Error())
	}
}

// reportStatus publishes the retained status on every connection and on every
// device transition. Section 5.5.
func (agent *Agent) reportStatus(ctx context.Context) {
	presence := agent.opts.Presence.Changed()
	// With no connection signal wired, the supervisor cannot know the state of
	// the connection and publishes regardless, leaving the publisher to report
	// the failure.
	connected := agent.opts.Connected == nil
	for {
		select {
		case <-ctx.Done():
			return
		case <-agent.opts.Connected:
			connected = true
			agent.publishStatus(event.StateOnline, event.ReasonStartup)
		case <-presence:
			if !connected {
				// A device that opens before the broker connects is the normal
				// startup order, not a fault. The status published on connect
				// carries the current presence, so publishing here would only
				// produce a warning about a connection that does not exist yet.
				agent.log.Debug("device transition before the first broker connection; the connect status will carry it")
				continue
			}
			agent.publishStatus(event.StateOnline, event.ReasonDevice)
		}
	}
}

// publishStatus sends one retained status. Failures are logged and not retried:
// the next connection or the next transition publishes again, and a retry loop
// here would compete with the publisher for the same connection.
func (agent *Agent) publishStatus(state, reason string) {
	status := event.NewStatus(agent.opts.Identity, state, reason, agent.opts.Presence.Any(), agent.start, agent.opts.Now())
	payload, err := json.Marshal(status)
	if err != nil {
		agent.log.Error("status envelope could not be encoded", "error", err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), agent.opts.PublishTimeout)
	defer cancel()
	if err := agent.opts.Publisher.PublishStatus(ctx, payload); err != nil {
		agent.log.Warn("status publish failed", "state", state, "reason", reason, "error", err.Error())
		return
	}
	agent.log.Info("status published", "state", state, "reason", reason, "device_open", status.DeviceOpen)
}

// beat publishes the heartbeat on a ticker. Section 5.6.
func (agent *Agent) beat(ctx context.Context) {
	ticker := time.NewTicker(agent.opts.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			agent.publishHeartbeat()
		}
	}
}

func (agent *Agent) publishHeartbeat() {
	state := event.HeartbeatState{
		DeviceOpen:      agent.opts.Presence.Any(),
		ScanCount:       agent.scanCount.Load(),
		PublishFailures: agent.publishFailures.Load(),
		BufferDepth:     agent.depth(),
	}
	if ms := agent.lastScanUnixMS.Load(); ms > 0 {
		state.LastScan = time.UnixMilli(ms)
	}
	beat := event.NewHeartbeat(agent.opts.Identity, agent.start, agent.opts.Now(), state)
	payload, err := json.Marshal(beat)
	if err != nil {
		agent.log.Error("heartbeat envelope could not be encoded", "error", err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), agent.opts.PublishTimeout)
	defer cancel()
	if err := agent.opts.Publisher.PublishHeartbeat(ctx, payload); err != nil {
		// At QoS 0 this is a connection problem rather than a rejection, and
		// the connection manager is already reporting that; DEBUG keeps a
		// disconnected station from filling the log at one line every 15s.
		agent.log.Debug("heartbeat publish failed", "error", err.Error())
		return
	}
	agent.log.Debug("heartbeat published", "scan_count", state.ScanCount,
		"publish_failures", state.PublishFailures, "buffer_depth", state.BufferDepth,
		"device_open", state.DeviceOpen)
}

func (agent *Agent) depth() int {
	if agent.bufferDepth == nil {
		return 0
	}
	return agent.bufferDepth()
}
