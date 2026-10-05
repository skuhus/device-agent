package core

import (
	"errors"
	"log/slog"
	"slices"
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

// txTarget is where a tx topic leads: the route it is, and the devices it
// reaches, one for a device's own topic and every device in the group for a
// broadcast group's.
type txTarget struct {
	route wire.TxRoute
	lines []*pipeline
}

// receiveTxs gives each tx to its devices until stop is closed, and then takes
// what is already waiting. While the agent stops, each still gets a result:
// agent_stopping.
func (core *Core) receiveTxs(stop <-chan struct{}, byTopic map[string]*txTarget) {
	if core.opts.TxIn == nil {
		return
	}
	for {
		select {
		case message := <-core.opts.TxIn:
			core.admitTx(byTopic, message)
		case <-stop:
			for {
				select {
				case message := <-core.opts.TxIn:
					core.admitTx(byTopic, message)
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

// admitTx hands one tx to every device its topic reaches: one for a device's
// own topic, each device in the group for a broadcast group's.
func (core *Core) admitTx(byTopic map[string]*txTarget, message TxMessage) {
	target, ok := byTopic[message.Topic]
	if !ok {
		core.log.Error("tx on a topic that is no device's; not written", "topic", message.Topic, "bytes", len(message.Payload))
		return
	}
	if target.route.Scope != wire.ScopeDevice {
		deviceIDs := make([]string, 0, len(target.lines))
		for _, line := range target.lines {
			deviceIDs = append(deviceIDs, line.device.Wire.ID)
		}
		core.log.Info("tx on a broadcast group's topic; each device in the group takes it", "topic", message.Topic,
			"scope", string(target.route.Scope), "group", target.route.Group, "device_ids", deviceIDs, "bytes", len(message.Payload))
	}
	for _, line := range target.lines {
		core.admitTxTo(line, message)
	}
}

// admitTxTo reads one tx for one device, and either fails it, rejects it
// because a tx with its id is still in hand, or queues it for the device's
// writer. Each gets its result at once; a queued one gets accepted, and later
// written or failed.
func (core *Core) admitTxTo(line *pipeline, message TxMessage) {
	now := core.opts.Now()
	dev := line.device.Wire
	topic := []any{"topic", message.Topic}
	ref, data, problem := wire.ReadTx(message.Payload)
	if problem != nil {
		var result wire.TxResult
		if problem.Code == wire.TxCodeInvalidID {
			result = core.opts.Builder.TxInvalidID(dev, line.isOpen(), ref, now)
		} else {
			result = core.opts.Builder.TxInvalidMessage(dev, line.isOpen(), ref, problem.Text, now)
		}
		core.recordTxResult(line, result, now, slices.Concat(topic, []any{"error", problem.Text}, logging.Payload(message.Payload)))
		return
	}

	line.txMu.Lock()
	if earlier, inHand := line.txActive[ref.ID]; inHand {
		line.txMu.Unlock()
		stage, since, written := earlier.standing()
		result := core.opts.Builder.TxInProgress(dev, line.isOpen(), ref, stage, since, written, now)
		core.recordTxResult(line, result, now, topic)
		return
	}
	if writtenAt, known := line.written.lookup(ref.ID); known {
		line.txMu.Unlock()
		result := core.opts.Builder.TxAlreadyWritten(dev, line.isOpen(), ref, writtenAt, now)
		core.recordTxResult(line, result, now,
			slices.Concat(topic, []any{"written_at", writtenAt.UTC().Format(wire.TimeFormat)}, logging.Payload(data)))
		return
	}
	core.intakeMu.Lock()
	if core.stopping.Load() {
		core.intakeMu.Unlock()
		line.txMu.Unlock()
		core.recordTxResult(line, core.opts.Builder.TxAgentStopping(dev, line.isOpen(), ref, 0, now), now,
			slices.Concat(topic, []any{"bytes_written", 0}, logging.Payload(data)))
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
	core.recordTxResult(line, core.opts.Builder.TxAccepted(dev, line.isOpen(), ref, now), now,
		slices.Concat(topic, []any{"bytes", len(data), "message_expiry", expiry}, core.payloadIfLogged(data)))
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
			core.recordTxResult(line, core.opts.Builder.TxAgentStopping(dev, line.isOpen(), job.ref, 0, now), now,
				append([]any{"bytes_written", 0, "open_attempts", attempts}, logging.Payload(job.data)...))
			return time.Time{}, false
		}
		if job.expired(now) {
			core.recordTxResult(line, core.opts.Builder.TxExpired(dev, line.isOpen(), job.ref, attempts, now), now,
				append([]any{"open_attempts", attempts}, logging.Payload(job.data)...))
			return time.Time{}, false
		}
		if !writable {
			core.recordTxResult(line, core.opts.Builder.TxPortUnavailable(dev, line.isOpen(), job.ref, wire.ErrorUnknown,
				"this device cannot be written", attempts, now), now, logging.Payload(job.data))
			return time.Time{}, false
		}

		job.setStage(wire.TxWriting, now)
		written, err := writer.Write(core.writeCtx, job.data, job.progress)
		now = core.opts.Now()
		if err == nil {
			core.recordTxResult(line, core.opts.Builder.TxWritten(dev, line.isOpen(), job.ref, written, attempts, now), now,
				append([]any{"bytes_written", written, "open_attempts", attempts, "took", now.Sub(started).String()},
					core.payloadIfLogged(job.data)...))
			return now, true
		}
		if errors.Is(err, device.ErrNotOpen) && written == 0 {
			job.setStage(wire.TxQueued, now)
			if attempts >= line.device.TxOpenAttempts {
				class, failure := line.lastFailure()
				core.recordTxResult(line, core.opts.Builder.TxPortUnavailable(dev, line.isOpen(), job.ref, class, failure, attempts, now),
					now, append([]any{"open_attempts", attempts}, logging.Payload(job.data)...))
				return time.Time{}, false
			}
			attempts++
			core.awaitPort(line, writer, job)
			continue
		}
		if core.writeCtx.Err() != nil && errors.Is(err, core.writeCtx.Err()) {
			core.recordTxResult(line, core.opts.Builder.TxAgentStopping(dev, line.isOpen(), job.ref, written, now), now,
				append([]any{"bytes_written", written, "open_attempts", attempts}, logging.Payload(job.data)...))
			return time.Time{}, false
		}
		class, failure := writeFailureClass(err)
		if errors.Is(err, device.ErrNotOpen) {
			// The reader closed the port under the write: it was lost, and
			// the loss's class is the reason.
			class, failure = line.lastFailure()
		}
		core.recordTxResult(line, core.opts.Builder.TxWriteFailed(dev, line.isOpen(), job.ref, class, failure, written, attempts, now),
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

// recordTxResult counts a result, records it in the log, and queues it for the
// device's status topic, where it waits for the connection as the device's
// events do.
func (core *Core) recordTxResult(line *pipeline, result wire.TxResult, at time.Time, extra []any) {
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
	attrs := []any{"tx_id", stringOrEmpty(result.TxID), "sender", stringOrEmpty(result.Sender), "device_id", result.DeviceID,
		"code", string(result.Code), "text", result.Text, "outcome", outcome}
	logging.Record(core.log, level, message, append(attrs, extra...)...)
	line.status.push(statusItem{tx: &txResultItem{result: result, at: at}})
}

// payloadIfLogged is a tx's data for a log record that carries it only with
// log_payloads: an accepted or a written tx. A failed one always carries it.
func (core *Core) payloadIfLogged(data []byte) []any {
	if !core.opts.LogPayloads {
		return nil
	}
	return logging.Payload(data)
}

// writeFailureClass is a write error's class and message, the class as the
// reader gives the same failure.
func writeFailureClass(err error) (wire.ErrorClass, string) {
	var portErr *device.PortError
	if errors.As(err, &portErr) {
		return portErr.Class, err.Error()
	}
	return wire.ErrorUnknown, err.Error()
}

func stringOrEmpty(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
