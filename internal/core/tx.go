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
	// txOutcomeAlreadyWritten is a resend of a tx written before: answered,
	// and not written again (#11 Q6a).
	txOutcomeAlreadyWritten = "already_written"
	// txOutcomeNotTaken is a tx that arrived as the connection closed, too
	// late for any result to be published.
	txOutcomeNotTaken = "not_taken"
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

// rememberedWritten is how many written tx ids each device remembers. A
// sender resends within its own timeout, seconds or minutes, and this is more
// jobs than a station prints in that time. A tuning value, not a setting, as
// the drain timeout is (PLAN-V2.md, T16).
const rememberedWritten = 1024

// writtenIDs remembers the most recently written tx ids, and when each was
// written, so that a resend is answered rather than printed twice (#11 Q6a).
// Only the running agent holds them: a restart forgets them. The caller holds
// the pipeline's txMu.
type writtenIDs struct {
	limit int
	at    map[string]time.Time
	// order holds the ids oldest first, so that the oldest is forgotten when
	// the limit is reached.
	order []string
}

func newWrittenIDs(limit int) *writtenIDs {
	return &writtenIDs{limit: limit, at: make(map[string]time.Time, limit)}
}

func (ids *writtenIDs) remember(id string, at time.Time) {
	if _, known := ids.at[id]; known {
		ids.at[id] = at
		return
	}
	if len(ids.order) >= ids.limit {
		delete(ids.at, ids.order[0])
		ids.order = ids.order[1:]
	}
	ids.at[id] = at
	ids.order = append(ids.order, id)
}

func (ids *writtenIDs) lookup(id string) (time.Time, bool) {
	at, known := ids.at[id]
	return at, known
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

// takeTxs gives each tx to its device until stop is closed, and then takes
// what is already waiting. While the agent stops, each still gets a result:
// agent_stopping.
func (core *Core) takeTxs(stop <-chan struct{}, byTopic map[string]*pipeline) {
	if core.opts.TxIn == nil {
		return
	}
	for {
		select {
		case message := <-core.opts.TxIn:
			core.intake(byTopic, message)
		case <-stop:
			for {
				select {
				case message := <-core.opts.TxIn:
					core.intake(byTopic, message)
				default:
					return
				}
			}
		}
	}
}

// recordUntaken logs each tx that arrived after the intake stopped. The
// connection is closed by then, so no result can be published; the sender
// treats the tx as not written, as for one lost in a reconnect.
func (core *Core) recordUntaken() {
	if core.opts.TxIn == nil {
		return
	}
	for {
		select {
		case message := <-core.opts.TxIn:
			ref, _, _ := wire.ReadTx(message.Payload)
			logging.Record(core.log, slog.LevelError, "tx not taken: the agent had stopped",
				append([]any{"topic", message.Topic, "tx_id", ref.ID, "sender", ref.Sender,
					"outcome", txOutcomeNotTaken}, logging.Payload(message.Payload)...)...)
		default:
			return
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
	if writtenAt, known := line.written.lookup(ref.ID); known {
		line.txMu.Unlock()
		result := core.opts.Builder.TxAlreadyWritten(dev, line.isOpen(), ref, writtenAt, now)
		core.txResult(line, result, now, append([]any{"written_at", writtenAt.UTC().Format(wire.TimeFormat)}, logging.Payload(data)...))
		return
	}
	core.intakeMu.Lock()
	if core.stopping.Load() {
		core.intakeMu.Unlock()
		line.txMu.Unlock()
		core.txResult(line, core.opts.Builder.TxAgentStopping(dev, line.isOpen(), ref, 0, now), now,
			append([]any{"bytes_written", 0}, logging.Payload(data)...))
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
	// accepted is queued before the writer can see the job, so that it goes
	// out before the job's written or failed.
	core.txResult(line, core.opts.Builder.TxAccepted(dev, line.isOpen(), ref, now), now,
		append([]any{"bytes", len(data), "message_expiry", expiry}, core.payloadIf(data)...))
	line.tx.push(job)
	core.intakeMu.Unlock()
}

// writeTxs writes one device's tx in the order they were taken, until its
// queue is closed and empty.
func (core *Core) writeTxs(line *pipeline) {
	for {
		job, ok := line.tx.next()
		if !ok {
			return
		}
		writtenAt, written := core.writeTx(line, job)
		line.txMu.Lock()
		delete(line.txActive, job.ref.ID)
		if written {
			line.written.remember(job.ref.ID, writtenAt)
		}
		line.txMu.Unlock()
	}
}

// writeTx writes one tx through the device's port. A tx that finds the port
// closed asks the reader to open it, up to the device's open attempts, the
// open interval apart; once writing has started, nothing is retried
// (DESIGN-V2.md, "Writing: tx"). It reports when the tx was written, and
// whether it was.
func (core *Core) writeTx(line *pipeline, job *txJob) (time.Time, bool) {
	dev := line.device.Wire
	writer, writable := line.device.Reader.(device.Writer)
	attempts := 0
	started := core.opts.Now()
	for {
		now := core.opts.Now()
		if core.stopping.Load() {
			core.txResult(line, core.opts.Builder.TxAgentStopping(dev, line.isOpen(), job.ref, 0, now), now,
				append([]any{"bytes_written", 0, "open_attempts", attempts}, logging.Payload(job.data)...))
			return time.Time{}, false
		}
		if job.expired(now) {
			core.txResult(line, core.opts.Builder.TxExpired(dev, line.isOpen(), job.ref, attempts, now), now,
				append([]any{"open_attempts", attempts}, logging.Payload(job.data)...))
			return time.Time{}, false
		}
		if !writable {
			core.txResult(line, core.opts.Builder.TxPortUnavailable(dev, line.isOpen(), job.ref, wire.ErrorUnknown,
				"this device cannot be written", attempts, now), now, logging.Payload(job.data))
			return time.Time{}, false
		}

		job.setStage(wire.TxWriting, now)
		written, err := writer.Write(core.writeCtx, job.data, job.progress)
		now = core.opts.Now()
		if err == nil {
			core.txResult(line, core.opts.Builder.TxWritten(dev, line.isOpen(), job.ref, written, attempts, now), now,
				append([]any{"bytes_written", written, "open_attempts", attempts, "took", now.Sub(started).String()},
					core.payloadIf(job.data)...))
			return now, true
		}
		if errors.Is(err, device.ErrNotOpen) && written == 0 {
			job.setStage(wire.TxQueued, now)
			if attempts >= line.device.TxOpenAttempts {
				class, failure := line.lastFailure()
				core.txResult(line, core.opts.Builder.TxPortUnavailable(dev, line.isOpen(), job.ref, class, failure, attempts, now),
					now, append([]any{"open_attempts", attempts}, logging.Payload(job.data)...))
				return time.Time{}, false
			}
			attempts++
			core.awaitPort(line, writer, job)
			continue
		}
		if core.writeCtx.Err() != nil && errors.Is(err, core.writeCtx.Err()) {
			core.txResult(line, core.opts.Builder.TxAgentStopping(dev, line.isOpen(), job.ref, written, now), now,
				append([]any{"bytes_written", written, "open_attempts", attempts}, logging.Payload(job.data)...))
			return time.Time{}, false
		}
		class, failure := classOf(err)
		if errors.Is(err, device.ErrNotOpen) {
			// The reader closed the port under the write: it was lost, and
			// the loss's class is the reason.
			class, failure = line.lastFailure()
		}
		core.txResult(line, core.opts.Builder.TxWriteFailed(dev, line.isOpen(), job.ref, class, failure, written, attempts, now),
			now, append([]any{"bytes_written", written, "open_attempts", attempts}, logging.Payload(job.data)...))
		return time.Time{}, false
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

// txResult counts a result, records it in the log, and queues it for the
// device's status topic, where it waits for the connection as the device's
// events do.
func (core *Core) txResult(line *pipeline, result wire.TxResult, at time.Time, extra []any) {
	outcome, level, message := txOutcomeAccepted, slog.LevelInfo, "tx accepted"
	switch {
	case result.Code == wire.TxCodeAlreadyWritten:
		// Answered, not written: tx_written counts writes.
		outcome, message = txOutcomeAlreadyWritten, "tx already written"
	case result.State == wire.TxWritten:
		outcome, message = txOutcomeWritten, "tx written"
		line.countTx(true)
	case result.State == wire.TxFailed:
		outcome, level, message = txOutcomeFailed, slog.LevelError, "tx failed"
		line.countTx(false)
	case result.State == wire.TxRejected:
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
