package core

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/skuhus/device-agent/internal/logging"
	"github.com/skuhus/device-agent/internal/wire"
)

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
