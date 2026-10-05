package core

import (
	"context"
	"encoding/json"
	"time"

	"github.com/skuhus/device-agent/internal/device"
	"github.com/skuhus/device-agent/internal/wire"
)

// report receives every port event from one device's reader. It counts the
// event, logs it, and queues the ones consumers see for the device's event
// publisher. It is called inline by the reader and never blocks: a full queue
// drops its oldest event, which is logged, and the keepalive has counted it
// either way.
func (core *Core) report(line *pipeline, event device.Event) {
	// A class or reason the wire does not define is a bug in a reader. It is
	// logged once, here; a class is then treated as unknown everywhere, and a
	// reason is published as it came but has no counter.
	if event.ErrorClass != "" && !event.ErrorClass.Known() {
		core.log.Error("port event carries an error class the keepalive has no counter for; counted as unknown",
			eventAttrs(event)...)
		event.ErrorClass = wire.ErrorUnknown
	}
	if event.Kind == device.BytesDiscarded && !event.Reason.Known() {
		core.log.Error("port event carries a discard reason the keepalive has no counter for; not counted",
			eventAttrs(event)...)
	}
	core.count(line, event)
	if event.Kind == device.BytesRead {
		// Counted only. The reader logs every read at DEBUG already.
		return
	}
	core.log.Debug("port event", eventAttrs(event)...)
	if dropped, full := line.events.push(statusItem{event: &event}); full {
		core.log.Warn("device event dropped: queue full, the most recent are kept",
			append(dropped.attrs(), "queue_size", core.opts.EventBufferSize)...)
	}
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
		return builder.PortLost(dev, path, event.ErrorClass, errString(event.Err), at), true
	case device.PortOpenFailed:
		return builder.PortOpenFailed(dev, path, event.ErrorClass, errString(event.Err), at), true
	case device.BytesDiscarded:
		return builder.BytesDiscarded(dev, event.Reason, event.Bytes, at), true
	}
	core.log.Error("port event of a kind with no message; not published", eventAttrs(event)...)
	return wire.Event{}, false
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

// eventAttrs are a port event's log attributes.
func eventAttrs(event device.Event) []any {
	attrs := []any{"device_id", event.DeviceID, "event", string(event.Kind), "at", event.At.UTC().Format(wire.TimeFormat)}
	if event.ErrorClass != "" {
		attrs = append(attrs, "error_class", string(event.ErrorClass))
	}
	if event.Err != nil {
		attrs = append(attrs, "error", event.Err.Error())
	}
	if event.Kind == device.BytesDiscarded {
		attrs = append(attrs, "reason", string(event.Reason), "bytes", event.Bytes)
	}
	return attrs
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
