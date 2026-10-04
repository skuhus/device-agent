package core

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/skuhus/device-agent/internal/device"
	"github.com/skuhus/device-agent/internal/logging"
	"github.com/skuhus/device-agent/internal/wire"
)

// TxMessage is a tx as it arrived from the broker, on a device's tx topic.
type TxMessage struct {
	Topic   string
	Payload []byte
	// Expiry is what remained of the tx's MQTT message expiry when it
	// arrived, and HasExpiry whether it had one. No attempt to write it is
	// made once the expiry has passed (DESIGN-V2.md, "Writing: tx").
	Expiry    time.Duration
	HasExpiry bool
	Received  time.Time
}

// Outcomes of a tx, as the outcome attribute of its log records.
const (
	txOutcomeAccepted = "accepted"
	txOutcomeWritten  = "written"
	txOutcomeFailed   = "failed"
	txOutcomeRejected = "rejected"
	// txOutcomeNotWritten is a tx the agent stopped before writing. It gets no
	// result for now (#19 Q1).
	txOutcomeNotWritten = "not_written"
)

// txJob is one tx taken for a device's port.
type txJob struct {
	ref      wire.TxRef
	data     []byte
	received time.Time
	// deadline is when the tx's message expiry passes; zero means it has none.
	deadline time.Time

	// mu guards where the tx stands, which a resend of it reports.
	mu      sync.Mutex
	stage   wire.TxStage
	since   time.Time
	written int
}

func (job *txJob) setStage(stage wire.TxStage, at time.Time) {
	job.mu.Lock()
	defer job.mu.Unlock()
	if job.stage != stage {
		job.stage, job.since = stage, at
	}
}

func (job *txJob) progress(written int) {
	job.mu.Lock()
	defer job.mu.Unlock()
	job.written = written
}

func (job *txJob) standing() (wire.TxStage, time.Time, int) {
	job.mu.Lock()
	defer job.mu.Unlock()
	return job.stage, job.since, job.written
}

func (job *txJob) expired(now time.Time) bool {
	return !job.deadline.IsZero() && !now.Before(job.deadline)
}

// txQueue holds a device's tx in the order they arrived, for its one writer.
// It is not bounded: every tx carries a message expiry, and one that waited
// past it is failed rather than written.
type txQueue struct {
	mu     sync.Mutex
	jobs   []*txJob
	closed bool
	ready  chan struct{}
}

func newTxQueue() *txQueue { return &txQueue{ready: make(chan struct{}, 1)} }

func (queue *txQueue) push(job *txJob) {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	queue.jobs = append(queue.jobs, job)
	queue.signal()
}

func (queue *txQueue) close() {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	queue.closed = true
	queue.signal()
}

// next blocks until a job is queued, and returns false once the queue is
// closed and empty.
func (queue *txQueue) next() (*txJob, bool) {
	for {
		queue.mu.Lock()
		if len(queue.jobs) > 0 {
			job := queue.jobs[0]
			queue.jobs = queue.jobs[1:]
			queue.mu.Unlock()
			return job, true
		}
		closed := queue.closed
		queue.mu.Unlock()
		if closed {
			return nil, false
		}
		<-queue.ready
	}
}

func (queue *txQueue) signal() {
	select {
	case queue.ready <- struct{}{}:
	default:
	}
}

// takeTxs reads the tx messages until ctx ends and gives each to its device.
// What is still buffered when ctx ends is recorded as not taken.
func (core *Core) takeTxs(ctx context.Context, byTopic map[string]*pipeline) {
	if core.opts.TxIn == nil {
		return
	}
	for {
		select {
		case message := <-core.opts.TxIn:
			core.intake(byTopic, message)
		case <-ctx.Done():
			for {
				select {
				case message := <-core.opts.TxIn:
					ref, _, _ := wire.ReadTx(message.Payload)
					logging.Record(core.log, slog.LevelError, "tx not taken: the agent is stopping",
						append([]any{"topic", message.Topic, "tx_id", ref.ID, "sender", ref.Sender,
							"outcome", txOutcomeNotWritten}, logging.Payload(message.Payload)...)...)
				default:
					return
				}
			}
		}
	}
}

// intake reads one tx, and either fails it, rejects it because a tx with its
// id is still in hand, or queues it for the device's writer. Each gets its
// result at once; a queued one gets accepted, and later written or failed.
func (core *Core) intake(byTopic map[string]*pipeline, message TxMessage) {
	line, ok := byTopic[message.Topic]
	if !ok {
		core.log.Error("tx on a topic that is no device's; not written", "topic", message.Topic, "bytes", len(message.Payload))
		return
	}
	now := core.opts.Now()
	dev := line.device.Wire
	ref, data, problem := wire.ReadTx(message.Payload)
	if problem != nil {
		var result wire.TxResult
		if problem.Code == wire.TxCodeInvalidID {
			result = core.opts.Builder.TxInvalidID(dev, line.isOpen(), ref, now)
		} else {
			result = core.opts.Builder.TxInvalidMessage(dev, line.isOpen(), ref, problem.Text, now)
		}
		core.txResult(line, result, now, append([]any{"error", problem.Text}, logging.Payload(message.Payload)...))
		return
	}

	line.txMu.Lock()
	if earlier, inHand := line.txActive[ref.ID]; inHand {
		line.txMu.Unlock()
		stage, since, written := earlier.standing()
		result := core.opts.Builder.TxInProgress(dev, line.isOpen(), ref, stage, since, written, now)
		core.txResult(line, result, now, nil)
		return
	}
	job := &txJob{ref: ref, data: data, received: message.Received, stage: wire.TxQueued, since: now}
	if message.HasExpiry {
		job.deadline = message.Received.Add(message.Expiry)
	}
	line.txActive[ref.ID] = job
	line.txMu.Unlock()

	expiry := "none"
	if message.HasExpiry {
		expiry = message.Expiry.String()
	}
	core.txResult(line, core.opts.Builder.TxAccepted(dev, line.isOpen(), ref, now), now,
		append([]any{"bytes", len(data), "message_expiry", expiry}, core.payloadIf(data)...))
	line.tx.push(job)
}

// writeTxs writes one device's tx in the order they were taken, until its
// queue is closed and empty.
func (core *Core) writeTxs(line *pipeline) {
	for {
		job, ok := line.tx.next()
		if !ok {
			return
		}
		core.writeTx(line, job)
		line.txMu.Lock()
		delete(line.txActive, job.ref.ID)
		line.txMu.Unlock()
	}
}

// writeTx writes one tx through the device's port. A tx that finds the port
// closed asks the reader to open it, up to the device's open attempts, the
// open interval apart; once writing has started, nothing is retried
// (DESIGN-V2.md, "Writing: tx").
func (core *Core) writeTx(line *pipeline, job *txJob) {
	dev := line.device.Wire
	writer, writable := line.device.Reader.(device.Writer)
	attempts := 0
	started := core.opts.Now()
	for {
		now := core.opts.Now()
		if core.stopping.Load() {
			core.txNotWritten(line, job, "the agent is stopping")
			return
		}
		if job.expired(now) {
			core.txResult(line, core.opts.Builder.TxExpired(dev, line.isOpen(), job.ref, attempts, now), now,
				append([]any{"open_attempts", attempts}, logging.Payload(job.data)...))
			return
		}
		if !writable {
			core.txResult(line, core.opts.Builder.TxPortUnavailable(dev, line.isOpen(), job.ref, wire.ErrorUnknown,
				"this device cannot be written", attempts, now), now, logging.Payload(job.data))
			return
		}

		job.setStage(wire.TxWriting, now)
		written, err := writer.Write(job.data, job.progress)
		now = core.opts.Now()
		if err == nil {
			core.txResult(line, core.opts.Builder.TxWritten(dev, line.isOpen(), job.ref, written, attempts, now), now,
				append([]any{"bytes_written", written, "open_attempts", attempts, "took", now.Sub(started).String()},
					core.payloadIf(job.data)...))
			return
		}
		if errors.Is(err, device.ErrNotOpen) && written == 0 {
			job.setStage(wire.TxQueued, now)
			if attempts >= line.device.TxOpenAttempts {
				class, failure := line.lastFailure()
				core.txResult(line, core.opts.Builder.TxPortUnavailable(dev, line.isOpen(), job.ref, class, failure, attempts, now),
					now, append([]any{"open_attempts", attempts}, logging.Payload(job.data)...))
				return
			}
			attempts++
			core.awaitPort(line, writer, job)
			continue
		}
		if core.stopping.Load() && errors.Is(err, device.ErrNotOpen) {
			core.txNotWritten(line, job, "the agent stopped while it was being written")
			return
		}
		class, failure := classOf(err)
		if errors.Is(err, device.ErrNotOpen) {
			// The reader closed the port under the write: it was lost, and
			// the loss's class is the reason.
			class, failure = line.lastFailure()
		}
		core.txResult(line, core.opts.Builder.TxWriteFailed(dev, line.isOpen(), job.ref, class, failure, written, attempts, now),
			now, append([]any{"bytes_written", written, "open_attempts", attempts}, logging.Payload(job.data)...))
		return
	}
}

// awaitPort asks the reader to open the port now and waits up to the device's
// open interval for it to open, or for the tx's expiry or the agent's stop.
func (core *Core) awaitPort(line *pipeline, writer device.Writer, job *txJob) {
	// A signal left from an earlier open would end the wait at once.
	select {
	case <-line.opened:
	default:
	}
	writer.RetryOpen()
	wait := line.device.TxOpenInterval
	if !job.deadline.IsZero() {
		wait = min(wait, max(job.deadline.Sub(core.opts.Now()), 0))
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-line.opened:
	case <-timer.C:
	case <-core.stoppingCh:
	}
}

// txNotWritten records a tx the agent stopped before writing, with its data.
// Whether it should get a result is #19 Q1.
func (core *Core) txNotWritten(line *pipeline, job *txJob, reason string) {
	_, _, written := job.standing()
	logging.Record(core.log, slog.LevelError, "tx not written: "+reason,
		append([]any{"tx_id", job.ref.ID, "sender", job.ref.Sender, "device_id", line.device.Wire.ID,
			"bytes", len(job.data), "bytes_written", written, "outcome", txOutcomeNotWritten}, logging.Payload(job.data)...)...)
}

// txResult counts a result, records it in the log, and queues it for the
// device's status topic, where it waits for the connection as the device's
// events do.
func (core *Core) txResult(line *pipeline, result wire.TxResult, at time.Time, extra []any) {
	outcome, level, message := txOutcomeAccepted, slog.LevelInfo, "tx accepted"
	switch result.State {
	case wire.TxWritten:
		outcome, message = txOutcomeWritten, "tx written"
		line.countTx(true)
	case wire.TxFailed:
		outcome, level, message = txOutcomeFailed, slog.LevelError, "tx failed"
		line.countTx(false)
	case wire.TxRejected:
		outcome, level, message = txOutcomeRejected, slog.LevelWarn, "tx rejected"
	}
	attrs := []any{"tx_id", deref(result.TxID), "sender", deref(result.Sender), "device_id", result.DeviceID,
		"code", string(result.Code), "text", result.Text, "outcome", outcome}
	logging.Record(core.log, level, message, append(attrs, extra...)...)
	line.events.push(statusItem{tx: &txResultItem{result: result, at: at}})
}

// publishTxResult sends one tx result with what remains of the device's
// message expiry, as an event is sent.
func (core *Core) publishTxResult(line *pipeline, item *txResultItem) {
	result := item.result
	age := core.opts.Now().Sub(item.at)
	attrs := []any{"id", result.ID, "device_id", result.DeviceID, "tx_id", deref(result.TxID),
		"state", string(result.State), "code", string(result.Code), "age", age.String()}
	if core.pastDrainDeadline() {
		core.log.Warn("tx result dropped", append(attrs, "reason", "shutdown drain deadline passed")...)
		return
	}
	remaining := line.device.Wire.Expiry - age
	if remaining <= 0 {
		core.log.Warn("tx result dropped", append(attrs, "reason", "older than its message expiry",
			"message_expiry", line.device.Wire.Expiry.String())...)
		return
	}
	payload, err := json.Marshal(result)
	if err != nil {
		core.log.Error("tx result could not be encoded", append(attrs, "error", err.Error())...)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), core.opts.PublishTimeout)
	err = core.opts.Transport.PublishEvent(ctx, line.device.Topics.Status(), payload, remaining)
	cancel()
	if err != nil {
		core.log.Warn("tx result publish failed", append(attrs, "error", err.Error())...)
		return
	}
	core.log.Info("tx result published", append(attrs, "message_expiry_left", remaining.String())...)
}

// payloadIf is a tx's data for a log record that carries it only with
// log_payloads: an accepted or a written tx. A failed one always carries it.
func (core *Core) payloadIf(data []byte) []any {
	if !core.opts.LogPayloads {
		return nil
	}
	return logging.Payload(data)
}

// classOf is a write error's class, as the reader classes the same failure.
func classOf(err error) (wire.ErrorClass, string) {
	var portErr *device.PortError
	if errors.As(err, &portErr) {
		class, _ := errorClass(portErr.Class)
		return class, err.Error()
	}
	return wire.ErrorUnknown, err.Error()
}

func deref(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
