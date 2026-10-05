package core

import (
	"sync"

	"github.com/skuhus/device-agent/internal/device"
	"github.com/skuhus/device-agent/internal/wire"
)

// pipeline is one device's frames and events, the sequence number of its
// frames, and what the keepalive reports about it.
type pipeline struct {
	device Device
	frames chan device.Frame
	events *statusQueue
	// tx holds the device's tx in the order they arrived, for its one writer.
	// It is not bounded: every tx carries a message expiry, and one that
	// waited past it is failed rather than written.
	tx *waitingQueue[*txJob]
	// seq is written only by the device's rx publisher.
	seq uint64

	// opened receives a value when the port opens, for a tx waiting for it.
	opened chan struct{}
	// txMu guards txActive, the tx queued or being written, by id, and
	// written, the ids most recently written. An id leaves the one and joins
	// the other under the lock, so a resend always finds it in one of them.
	txMu     sync.Mutex
	txActive map[string]*txJob
	written  *writtenIDs

	// mu guards open, the last port failure and counters, which the reader,
	// the publishers, the tx writer and the keepalive all reach.
	mu           sync.Mutex
	open         bool
	failureClass wire.ErrorClass
	failure      string
	counters     wire.DeviceCounters
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
		line.failureClass, line.failure = event.ErrorClass, event.ErrorText()
	case device.PortOpenFailed:
		line.open = false
		line.failureClass, line.failure = event.ErrorClass, event.ErrorText()
		line.counters.FailedOpens.Add(event.ErrorClass)
	case device.BytesDiscarded:
		line.counters.Discards.Add(event.Reason)
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
