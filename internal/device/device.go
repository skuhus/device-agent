// Package device defines what the core needs of an attached device, whatever
// its transport: a Device reads, frames what the port sends and reports what
// happens to the port, and a device that can be written is also a Writer,
// which writes through the port the reader holds open. internal/device/serial
// is the one transport.
package device

import (
	"context"
	"errors"
	"time"

	"github.com/skuhus/device-agent/internal/wire"
)

// EventKind names what happened to a device's port.
type EventKind string

// Port events.
const (
	PortOpened     EventKind = "opened"
	PortClosed     EventKind = "closed"
	PortLost       EventKind = "lost"
	PortOpenFailed EventKind = "open_failed"
	BytesDiscarded EventKind = "discarded"
	// BytesRead is reported for every read that returned bytes, framed or not.
	// It is counted, not published: an unmatched separator shows as bytes read
	// while no frame comes out (DESIGN-V2.md, #23 Q5).
	BytesRead EventKind = "read"
)

// Event is something that happened to a device's port. A reader reports every
// one, so that whatever runs it can log, publish and count them without
// knowing how the port works.
type Event struct {
	DeviceID string
	Kind     EventKind
	At       time.Time
	// ErrorClass and Err say why, for PortLost and PortOpenFailed.
	ErrorClass wire.ErrorClass
	Err        error
	// Reason says why bytes were thrown away, for BytesDiscarded. Bytes is how
	// many bytes were thrown away, for BytesDiscarded, or read, for BytesRead.
	Reason wire.DiscardReason
	Bytes  int
}

// ErrorText is the message of the event's error, or "" when it has none.
func (event Event) ErrorText() string {
	if event.Err == nil {
		return ""
	}
	return event.Err.Error()
}

// Frame is one complete separator-delimited unit read from a device.
//
// Raw holds bytes, not text. GS1-128 and Data Matrix payloads carry 0x1D group
// separators, and treating them as a string corrupts them.
type Frame struct {
	DeviceID string
	Raw      []byte
	At       time.Time
}

// Device is one attached device owned by the agent.
type Device interface {
	// ID is the configured device identifier.
	ID() string
	// Kind is the configured transport kind, for example "serial".
	Kind() string
	// Path is the configured device path.
	Path() string
	// Run reads until ctx is cancelled, sending each complete frame to sink and
	// every port event to report. report is called inline and must not block.
	//
	// Run owns reopening the device: a disconnect is an expected condition and
	// is retried with jittered backoff rather than returned. It returns only
	// when ctx is cancelled, or on an error that reopening cannot fix.
	//
	// Run blocks when sink is full. That is the intended backpressure: a human
	// cannot scan faster than the publisher drains, so blocking the reader
	// surfaces a stalled broker instead of hiding it behind a growing queue.
	Run(ctx context.Context, sink chan<- Frame, report func(Event)) error
}

// ErrNotOpen is what Write returns when the device's port is not open, or was
// closed before the write could finish.
var ErrNotOpen = errors.New("the port is not open")

// Writer is a device that bytes can be written to. Its reader keeps owning the
// port: Write goes through the port the reader has open, and does not open it.
type Writer interface {
	// Write writes data to the port, all of it and in order, and calls
	// progress with the total after each piece the operating system accepted.
	// It returns how many bytes were written; with ErrNotOpen and none written,
	// the port was not open, and the write can be tried again once it is. When
	// ctx ends it stops after the piece in hand and returns ctx's error.
	// Write is not safe for concurrent use: a device has one writer.
	Write(ctx context.Context, data []byte, progress func(written int)) (int, error)
	// RetryOpen asks the reader to try opening the port now, rather than when
	// its backoff ends. It does not wait for the attempt.
	RetryOpen()
}

// PortError is a failure of the port, with the class the reader gives the
// same failure in its events.
type PortError struct {
	Class wire.ErrorClass
	Err   error
}

func (portErr *PortError) Error() string { return portErr.Err.Error() }

func (portErr *PortError) Unwrap() error { return portErr.Err }
