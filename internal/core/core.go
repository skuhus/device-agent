// Package core runs the agent. Every device has a pipeline of its own: the
// device's reader frames what the port sends, a bounded channel holds the
// frames, and a publisher sends each one to the device's rx topic. The reader's
// port events are counted, and published on the device's status topic by a
// second publisher. A keepalive reports every device's counters on the agent's
// status topic. Every reading's log record says what became of it, and the
// core shuts down in the order that lets the broker tell a clean stop from a
// crash.
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

// Transport is the broker connection as the core uses it. Each method publishes
// one kind of message with the QoS and expiry DESIGN-V2.md, "Publishing",
// assigns it.
type Transport interface {
	// AwaitConnection blocks until the connection is up or ctx ends.
	AwaitConnection(ctx context.Context) error
	PublishRx(ctx context.Context, topic string, payload []byte, expiry time.Duration) error
	PublishEvent(ctx context.Context, topic string, payload []byte, expiry time.Duration) error
	PublishKeepalive(ctx context.Context, topic string, payload []byte, expiry time.Duration) error
	PublishOffline(ctx context.Context, topic string, payload []byte) error
	Close(ctx context.Context) error
}

// Device is one configured device as the core runs it.
type Device struct {
	Reader device.Device
	// Wire is how the device appears in its messages: its id, its type, and the
	// message expiry every reading and event is published with.
	Wire   wire.Device
	Topics wire.DeviceTopics
	// TxOpenAttempts is how many times a tx that finds the port closed asks
	// the reader to open it, TxOpenInterval apart, before it fails.
	TxOpenAttempts int
	TxOpenInterval time.Duration
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
	// EventBufferSize is how many port events each device keeps while they
	// cannot be published. A full queue drops its oldest event, so that a long
	// broker outage ends with the most recent ones rather than a flood.
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

	// TxIn delivers the tx that arrive on the devices' tx topics. Nil means
	// no tx is taken.
	TxIn <-chan TxMessage

	// LogPayloads puts each published reading's data on its record, and each
	// accepted and written tx's. A reading the broker did not take, and a tx
	// that failed, carry their data regardless.
	LogPayloads bool
	Logger      *slog.Logger
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
	// stopping is set, and stoppingCh closed, when the agent starts to stop,
	// so that no tx is started after it. intakeMu holds stopping still while a
	// tx is queued, so that none is queued after the writers were told to end.
	intakeMu   sync.Mutex
	stopping   atomic.Bool
	stoppingCh chan struct{}
	// writeCtx ends when a tx being written at shutdown has had the drain
	// timeout: it stops after the chunk in hand (#19 Q1).
	writeCtx context.Context
}

// pipeline is one device's frames and events, the sequence number of its
// frames, and what the keepalive reports about it.
type pipeline struct {
	device Device
	frames chan device.Frame
	events *eventQueue
	tx     *txQueue
	// seq is written only by the device's rx publisher.
	seq uint64

	// opened receives a value when the port opens, for a tx waiting for it.
	opened chan struct{}
	// txMu guards txActive: the tx queued or being written, by id.
	txMu     sync.Mutex
	txActive map[string]*txJob

	// mu guards open, the last port failure and counters, which the reader,
	// the publishers, the tx writer and the keepalive all reach.
	mu           sync.Mutex
	open         bool
	failureClass wire.ErrorClass
	failure      string
	counters     wire.DeviceCounters
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
	case opts.EventBufferSize <= 0:
		return nil, fmt.Errorf("core: event buffer size must be positive, got %d", opts.EventBufferSize)
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
		if dev.Wire.Expiry <= 0 {
			// Every event would be older than its expiry, and none published.
			return nil, fmt.Errorf("core: device %s has no message expiry", dev.Wire.ID)
		}
		if dev.TxOpenAttempts < 0 || dev.TxOpenInterval < 0 {
			return nil, fmt.Errorf("core: device %s has negative tx open settings", dev.Wire.ID)
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
	if opts.Started.IsZero() {
		opts.Started = opts.Now()
	}
	return &Core{opts: opts, log: opts.Logger, stoppingCh: make(chan struct{})}, nil
}

// Run reads from every device and publishes until ctx is cancelled.
//
// Shutdown order matters and is deliberate. No tx is started: one queued or
// waiting for its port fails as agent_stopping, and so does one that arrives
// from then on. One being written goes on for at most the drain timeout, with
// its port still open, and is then stopped after the chunk in hand (#19 Q1).
// The keepalive stops, so none follows the offline message. The readers stop,
// so nothing new arrives; each reports its port closed. The publishers drain what is already framed and
// queued, for at most the drain timeout, including events still waiting for
// the connection. The offline message goes out, and only then does the
// connection close. Closing first would make the broker publish the will
// instead, reporting a crash where there was an orderly stop.
func (core *Core) Run(ctx context.Context) error {
	pipelines := make([]*pipeline, 0, len(core.opts.Devices))
	byTxTopic := make(map[string]*pipeline, len(core.opts.Devices))
	for _, dev := range core.opts.Devices {
		line := &pipeline{
			device:   dev,
			frames:   make(chan device.Frame, core.opts.BufferSize),
			events:   newEventQueue(core.opts.EventBufferSize),
			tx:       newTxQueue(),
			opened:   make(chan struct{}, 1),
			txActive: map[string]*txJob{},
		}
		pipelines = append(pipelines, line)
		byTxTopic[dev.Topics.Tx()] = line
	}

	// drained ends the event publishers' wait for the connection when the
	// shutdown drain runs out, so that a broker that is down cannot hold the
	// process open.
	drained, endDrain := context.WithCancel(context.Background())
	defer endDrain()

	// The readers run on their own context, not ctx, so that Run decides when
	// they stop relative to everything else.
	readerCtx, stopReaders := context.WithCancel(context.Background())
	defer stopReaders()
	writeCtx, stopWrites := context.WithCancel(context.Background())
	defer stopWrites()
	core.writeCtx = writeCtx
	var readers, publishers, keepalives, writers, intake sync.WaitGroup
	stopIntake := make(chan struct{})
	intake.Add(1)
	go func() {
		defer intake.Done()
		core.takeTxs(stopIntake, byTxTopic)
	}()
	for _, line := range pipelines {
		writers.Add(1)
		go func(line *pipeline) {
			defer writers.Done()
			core.writeTxs(line)
		}(line)
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
			core.publishEvents(line, drained)
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
	core.intakeMu.Lock()
	core.stopping.Store(true)
	core.intakeMu.Unlock()
	close(core.stoppingCh)
	for _, line := range pipelines {
		line.tx.close()
	}
	cutWrites := time.AfterFunc(core.opts.DrainTimeout, stopWrites)
	writers.Wait()
	cutWrites.Stop()

	close(stopKeepalive)
	keepalives.Wait()
	stopReaders()
	readers.Wait()
	// A tx that arrived while the writers finished has its result queued
	// before the status queues close.
	close(stopIntake)
	intake.Wait()
	core.drainDeadlineMS.Store(core.opts.Now().Add(core.opts.DrainTimeout).UnixMilli())
	stopDrain := time.AfterFunc(core.opts.DrainTimeout, endDrain)
	defer stopDrain.Stop()
	for _, line := range pipelines {
		close(line.frames)
		line.events.close()
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
	core.recordUntaken()
	core.log.Info("stopped")
	return nil
}

// Delivery outcomes, as the outcome attribute of a reading's record.
const (
	// outcomePublished means the broker acknowledged the publish.
	outcomePublished = "published"
	// outcomeFailed means the publish failed or was not acknowledged in time.
	outcomeFailed = "failed"
	// outcomeDropped means the reading was never offered to the broker.
	outcomeDropped = "dropped"
)

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
			line.countPublishFailure()
			logging.Record(core.log, slog.LevelWarn, "rx dropped", append(rxAttrs(rx, frame.Raw, outcomeDropped, true),
				"reason", "shutdown drain deadline passed")...)
			dropped++
			continue
		}
		core.publishRx(line, rx, frame.Raw)
	}
	if dropped > 0 {
		core.log.Warn("rx dropped at shutdown", "device_id", line.device.Wire.ID, "count", dropped,
			"drain_timeout", core.opts.DrainTimeout.String())
	}
}

// publishRx sends one reading. Its log record is the record of its delivery,
// and every reading gets one, whatever logging.level says. A reading the
// broker did not take carries its data whatever log_payloads says, because
// nothing else holds it: a setting that silently turned data loss back on
// would not be a privacy control. A published one carries it only with
// log_payloads.
func (core *Core) publishRx(line *pipeline, rx wire.Rx, raw []byte) {
	payload, err := json.Marshal(rx)
	if err != nil {
		// Encoding a struct of strings and numbers cannot fail in practice. It
		// is recorded rather than ignored, because a reading that never reached
		// the broker and left no trace is the one failure the record exists to
		// make impossible.
		line.countPublishFailure()
		logging.Record(core.log, slog.LevelError, "rx could not be encoded", append(rxAttrs(rx, raw, outcomeDropped, true), "error", err.Error())...)
		return
	}

	// A fresh context, not the run context: at shutdown the run context is
	// already cancelled, and the frames still in hand deserve their full
	// publish budget.
	ctx, cancel := context.WithTimeout(context.Background(), core.opts.PublishTimeout)
	err = core.opts.Transport.PublishRx(ctx, line.device.Topics.Rx(), payload, line.device.Wire.Expiry)
	cancel()
	if err != nil {
		line.countPublishFailure()
		logging.Record(core.log, slog.LevelError, "rx publish failed", append(rxAttrs(rx, raw, outcomeFailed, true), "error", err.Error())...)
		return
	}
	logging.Record(core.log, slog.LevelInfo, "rx published", rxAttrs(rx, raw, outcomePublished, core.opts.LogPayloads)...)
}

// rxAttrs are a reading's record: what identifies it, its outcome, and, with
// withData, the frame's data.
func rxAttrs(rx wire.Rx, frame []byte, outcome string, withData bool) []any {
	attrs := []any{"id", rx.ID, "device_id", rx.DeviceID, "seq", rx.Seq, "bytes", len(frame),
		"text_valid", rx.TextValid, "outcome", outcome}
	if withData {
		attrs = append(attrs, logging.Payload(frame)...)
	}
	return attrs
}

// report receives every port event from one device's reader. It counts the
// event, logs it, and queues the ones consumers see for the device's event
// publisher. It is called inline by the reader and never blocks: a full queue
// drops its oldest event, which is logged, and the keepalive has counted it
// either way.
func (core *Core) report(line *pipeline, event device.Event) {
	core.count(line, event)
	if event.Kind == device.BytesRead {
		// Counted only. The reader logs every read at DEBUG already.
		return
	}
	core.log.Debug("port event", eventAttrs(event)...)
	if dropped, full := line.events.push(statusItem{event: &event}); full {
		core.logQueueFull(dropped)
	}
}

// count updates what the keepalive reports about the device.
func (core *Core) count(line *pipeline, event device.Event) {
	line.mu.Lock()
	defer line.mu.Unlock()
	switch event.Kind {
	case device.PortOpened:
		line.open = true
		select {
		case line.opened <- struct{}{}:
		default:
		}
	case device.PortClosed:
		line.open = false
	case device.PortLost:
		line.open = false
		line.failureClass, _ = errorClass(event.ErrorClass)
		line.failure = errString(event.Err)
	case device.PortOpenFailed:
		line.open = false
		class, known := errorClass(event.ErrorClass)
		line.failureClass, line.failure = class, errString(event.Err)
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

// countTx counts a tx result: written, or failed.
func (line *pipeline) countTx(written bool) {
	line.mu.Lock()
	defer line.mu.Unlock()
	if written {
		line.counters.TxWritten++
	} else {
		line.counters.TxFailed++
	}
}

func (line *pipeline) isOpen() bool {
	line.mu.Lock()
	defer line.mu.Unlock()
	return line.open
}

// lastFailure is the class and text of the port's last failure to open, or of
// its loss, for a tx that could not be written because of it.
func (line *pipeline) lastFailure() (wire.ErrorClass, string) {
	line.mu.Lock()
	defer line.mu.Unlock()
	if line.failure == "" {
		return wire.ErrorUnknown, "the port is not open"
	}
	return line.failureClass, line.failure
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

// publishEvents sends one device's events, in the order they happened, until
// its queue is closed and empty. It waits for the broker connection before it
// takes each event, so that events that happen while the connection is down
// wait in the queue, which keeps the most recent (#13 Q1). drained ends the
// wait at shutdown.
func (core *Core) publishEvents(line *pipeline, drained context.Context) {
	for line.events.wait() {
		if err := core.opts.Transport.AwaitConnection(drained); err != nil {
			reason := "the broker connection was still down when the shutdown drain ended"
			if drained.Err() == nil {
				// Only the drain should end the wait; anything else means the
				// connection manager is gone, and no event can be published.
				core.log.Error("waiting for the broker connection failed; device events are no longer published",
					"device_id", line.device.Wire.ID, "error", err.Error())
				reason = "waiting for the broker connection failed: " + err.Error()
			}
			core.dropEvents(line, reason)
			return
		}
		item, ok := line.events.pop()
		switch {
		case !ok:
		case item.tx != nil:
			core.publishTxResult(line, item.tx)
		default:
			core.publishEvent(line, *item.event)
		}
	}
}

// publishEvent sends one event, with what remains of the device's message
// expiry: an event that waited for the connection is that much closer to
// saying nothing true. One whose expiry has passed is dropped. An event is not
// retried, as a reading is not: the keepalive counts what it reported either
// way.
func (core *Core) publishEvent(line *pipeline, event device.Event) {
	message, ok := core.buildEvent(line, event)
	if !ok {
		return
	}
	age := core.opts.Now().Sub(event.At)
	attrs := []any{"id", message.ID, "device_id", message.DeviceID, "code", string(message.Code),
		"device_open", message.DeviceOpen, "text", message.Text, "agent_ts", message.AgentTS, "age", age.String()}
	if core.pastDrainDeadline() {
		core.log.Warn("device event dropped", append(attrs, "reason", "shutdown drain deadline passed")...)
		return
	}
	remaining := line.device.Wire.Expiry - age
	if remaining <= 0 {
		core.log.Warn("device event dropped", append(attrs, "reason", "older than its message expiry",
			"message_expiry", line.device.Wire.Expiry.String())...)
		return
	}
	payload, err := json.Marshal(message)
	if err != nil {
		core.log.Error("device event could not be encoded", append(attrs, "error", err.Error())...)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), core.opts.PublishTimeout)
	err = core.opts.Transport.PublishEvent(ctx, line.device.Topics.Status(), payload, remaining)
	cancel()
	if err != nil {
		core.log.Warn("device event publish failed", append(attrs, "error", err.Error())...)
		return
	}
	core.log.Info("device event published", append(attrs, "message_expiry_left", remaining.String())...)
}

// dropEvents empties a device's queue, logging each event with why it was not
// published.
func (core *Core) dropEvents(line *pipeline, reason string) {
	for {
		item, ok := line.events.pop()
		if !ok {
			return
		}
		message := "device event dropped"
		if item.tx != nil {
			message = "tx result dropped"
		}
		core.log.Warn(message, append(item.attrs(), "reason", reason)...)
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

// logQueueFull records an event dropped from a full queue.
func (core *Core) logQueueFull(dropped statusItem) {
	core.log.Warn("device event dropped: queue full, the most recent are kept",
		append(dropped.attrs(), "queue_size", core.opts.EventBufferSize)...)
}

// statusItem is one message for a device's status topic: a port event, or a
// tx result.
type statusItem struct {
	event *device.Event
	tx    *txResultItem
}

// txResultItem is a tx result and when it was produced, which its expiry
// counts from.
type txResultItem struct {
	result wire.TxResult
	at     time.Time
}

// attrs are the log attributes of a status message that was not published.
func (item statusItem) attrs() []any {
	if item.tx != nil {
		result := item.tx.result
		return []any{"device_id", result.DeviceID, "tx_id", deref(result.TxID), "state", string(result.State),
			"code", string(result.Code), "at", item.tx.at.UTC().Format(wire.TimeFormat)}
	}
	return eventAttrs(*item.event)
}

// eventQueue holds one device's status messages until they can be published,
// oldest first. It keeps at most size events: pushing an event when size are
// held drops the oldest, so that after a long broker outage the most recent
// events are the ones left (#13 Q1). A tx result is never dropped for room. Its
// sender waits for it and resends without it, and a printer prints a resent job
// twice. Results cannot pile up during an outage either, since no tx arrives
// without the connection. The reader and the tx side push, and one goroutine
// takes.
type eventQueue struct {
	mu     sync.Mutex
	events []statusItem
	size   int
	closed bool
	// ready holds a value after every push and at close, so that wait need
	// not poll.
	ready chan struct{}
}

func newEventQueue(size int) *eventQueue {
	return &eventQueue{size: size, ready: make(chan struct{}, 1)}
}

// push adds a message. When an event found size events held, it returns the
// oldest, which it dropped to make room, and true.
func (queue *eventQueue) push(item statusItem) (statusItem, bool) {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	var dropped statusItem
	full := false
	if item.event != nil {
		held, oldest := 0, -1
		for i, queued := range queue.events {
			if queued.event != nil {
				held++
				if oldest < 0 {
					oldest = i
				}
			}
		}
		if full = held >= queue.size; full {
			dropped = queue.events[oldest]
			queue.events = append(queue.events[:oldest], queue.events[oldest+1:]...)
		}
	}
	queue.events = append(queue.events, item)
	queue.signal()
	return dropped, full
}

// close says that no event will be pushed again.
func (queue *eventQueue) close() {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	queue.closed = true
	queue.signal()
}

// wait blocks until the queue holds an event, and returns false instead once
// it is closed and empty.
func (queue *eventQueue) wait() bool {
	for {
		queue.mu.Lock()
		held, closed := len(queue.events), queue.closed
		queue.mu.Unlock()
		switch {
		case held > 0:
			return true
		case closed:
			return false
		}
		<-queue.ready
	}
}

// pop takes the oldest message, if there is one.
func (queue *eventQueue) pop() (statusItem, bool) {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if len(queue.events) == 0 {
		return statusItem{}, false
	}
	event := queue.events[0]
	queue.events = queue.events[1:]
	return event, true
}

// signal wakes wait. It is called with mu held.
func (queue *eventQueue) signal() {
	select {
	case queue.ready <- struct{}{}:
	default:
	}
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
	attrs := []any{"device_id", event.DeviceID, "event", string(event.Kind), "at", event.At.UTC().Format(wire.TimeFormat)}
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
