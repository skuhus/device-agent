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
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/skuhus/device-agent/internal/device"
	"github.com/skuhus/device-agent/internal/wire"
)

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
	// SubscribeAnswers is the broker's SUBACK reason code for each topic
	// filter subscribed to on the current connection, keyed by the filter as
	// it went into the SUBSCRIBE packet. A filter not answered is absent.
	SubscribeAnswers() map[string]byte
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
	// TxRememberedIDs is how many written tx ids the device remembers, so
	// that a resend of one is answered already_written rather than written
	// again; the oldest is forgotten first.
	TxRememberedIDs int
	// BroadcastGroups are the tx topics of the broadcast groups the device is
	// in. A tx on one reaches every device in the group, as if sent to each on
	// its own topic (DESIGN-V2.md, "Broadcast groups").
	BroadcastGroups []wire.TxRoute
}

// txRoutes are the topics that reach the device's tx: its own first, then its
// broadcast groups'.
func (dev Device) txRoutes() []wire.TxRoute {
	return append([]wire.TxRoute{dev.Topics.TxRoute()}, dev.BroadcastGroups...)
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
	BufferSize int
	// DrainTimeout bounds the shutdown: how long a tx being written goes on,
	// and how long what is buffered waits for the broker. A reading is
	// perishable, so spending a minute delivering a backlog nobody can act on
	// is worse than dropping it and recording it: the operator who rescans is
	// ahead of the one reading a stale pick.
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

	// TxIn delivers the tx that arrive on the devices' tx topics and their
	// broadcast groups'. Nil means no tx is taken.
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
	case opts.DrainTimeout <= 0:
		return nil, fmt.Errorf("core: drain timeout must be positive, got %s", opts.DrainTimeout)
	case opts.EventBufferSize <= 0:
		return nil, fmt.Errorf("core: event buffer size must be positive, got %d", opts.EventBufferSize)
	case opts.KeepaliveInterval <= 0:
		return nil, fmt.Errorf("core: keepalive interval must be positive, got %s", opts.KeepaliveInterval)
	case opts.MissedKeepalives < 1:
		return nil, fmt.Errorf("core: missed keepalives must be at least 1, got %d", opts.MissedKeepalives)
	}
	for index, dev := range opts.Devices {
		if dev.Reader == nil {
			return nil, fmt.Errorf("core: device %d has no reader", index)
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
		if dev.TxRememberedIDs < 1 {
			return nil, fmt.Errorf("core: device %s must remember at least 1 written tx id, got %d", dev.Wire.ID, dev.TxRememberedIDs)
		}
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
// so nothing new arrives; each reports its port closed. The publishers drain
// what is already framed and queued, for at most the drain timeout, including
// events still waiting for the connection. The offline message goes out, and only then does the
// connection close. Closing first would make the broker publish the will
// instead, reporting a crash where there was an orderly stop.
func (core *Core) Run(ctx context.Context) error {
	pipelines := make([]*pipeline, 0, len(core.opts.Devices))
	byTxTopic := make(map[string]*txTarget, len(core.opts.Devices))
	for _, dev := range core.opts.Devices {
		line := &pipeline{
			device:   dev,
			txRoutes: dev.txRoutes(),
			frames:   make(chan device.Frame, core.opts.BufferSize),
			status:   newStatusQueue(core.opts.EventBufferSize),
			tx:       newWaitingQueue[*txJob](),
			opened:   make(chan struct{}, 1),
			txActive: map[string]*txJob{},
			written:  newWrittenIDs(dev.TxRememberedIDs),
		}
		pipelines = append(pipelines, line)
		for _, route := range line.txRoutes {
			target, known := byTxTopic[route.Topic]
			if !known {
				target = &txTarget{route: route}
				byTxTopic[route.Topic] = target
			}
			target.lines = append(target.lines, line)
		}
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
		core.receiveTxs(stopIntake, byTxTopic)
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
			txTopics := make([]string, 0, len(line.txRoutes))
			for _, route := range line.txRoutes {
				txTopics = append(txTopics, route.Topic)
			}
			core.log.Info("device starting", "device_id", reader.ID(), "device_kind", reader.Kind(), "device_path", reader.Path(),
				"rx_topic", line.device.Topics.Rx(), "status_topic", line.device.Topics.Status(), "tx_topics", txTopics,
				"message_expiry", line.device.Wire.Expiry.String(),
				"tx_open_attempts", line.device.TxOpenAttempts, "tx_open_interval", line.device.TxOpenInterval.String(),
				"tx_remembered_ids", line.device.TxRememberedIDs)
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
			core.publishFrames(line)
		}(line)
		go func(line *pipeline) {
			defer publishers.Done()
			core.publishStatusMessages(line, drained)
		}(line)
	}

	stopKeepalive := make(chan struct{})
	keepalives.Add(1)
	go func() {
		defer keepalives.Done()
		core.sendKeepalives(stopKeepalive, pipelines)
	}()

	<-ctx.Done()
	core.log.Info("shutting down", "buffered", bufferedFrames(pipelines), "drain_timeout", core.opts.DrainTimeout.String())
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
		line.status.close()
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

func (core *Core) pastDrainDeadline() bool {
	deadline := core.drainDeadlineMS.Load()
	return deadline > 0 && core.opts.Now().UnixMilli() > deadline
}

func bufferedFrames(pipelines []*pipeline) int {
	total := 0
	for _, line := range pipelines {
		total += len(line.frames)
	}
	return total
}
