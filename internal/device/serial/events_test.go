package serial

import (
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/skuhus/device-agent/internal/device"
	goserial "go.bug.st/serial"
)

// eventRecorder keeps every event a device reports.
type eventRecorder struct {
	mu     sync.Mutex
	events []device.Event
}

func (recorder *eventRecorder) report(event device.Event) {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	recorder.events = append(recorder.events, event)
}

func (recorder *eventRecorder) snapshot() []device.Event {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return append([]device.Event(nil), recorder.events...)
}

// waitFor returns the first event of kind, failing the test if none arrives.
func (recorder *eventRecorder) waitFor(t *testing.T, kind device.EventKind) device.Event {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, event := range recorder.snapshot() {
			if event.Kind == kind {
				return event
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no %s event within 3s; got %v", kind, recorder.kinds())
	return device.Event{}
}

func (recorder *eventRecorder) kinds() []device.EventKind {
	var kinds []device.EventKind
	for _, event := range recorder.snapshot() {
		kinds = append(kinds, event.Kind)
	}
	return kinds
}

// A port that cannot be opened is reported every time, with the class that
// tells a missing device from a permissions problem.
func TestReportsFailedOpenWithItsClass(t *testing.T) {
	recorder := &eventRecorder{}
	opts := serialOpts("scale-1", "/dev/fake", "\r")
	opts.Open = func(string, *goserial.Mode) (goserial.Port, error) {
		return nil, &os.PathError{Op: "open", Path: "/dev/fake", Err: syscall.ENOENT}
	}
	runDeviceReporting(t, opts, 1, recorder.report)

	event := recorder.waitFor(t, device.PortOpenFailed)
	if event.DeviceID != "scale-1" || event.ErrorClass != "absent" || event.Err == nil {
		t.Errorf("event = %+v, want scale-1, class absent, with the error", event)
	}
	// Retried with backoff, and reported on each attempt.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		failed := 0
		for _, kind := range recorder.kinds() {
			if kind == device.PortOpenFailed {
				failed++
			}
		}
		if failed >= 2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("events = %v, want a failed open reported for each attempt", recorder.kinds())
}

// A port that fails under the reader is reported lost, with its class, after
// having been reported opened.
func TestReportsOpenedThenLostWithItsClass(t *testing.T) {
	recorder := &eventRecorder{}
	opts := serialOpts("scanner-1", "/dev/fake", "\r")
	opts.Open = func(string, *goserial.Mode) (goserial.Port, error) {
		return &blockingPort{hold: 50 * time.Millisecond}, nil
	}
	runDeviceReporting(t, opts, 1, recorder.report)

	recorder.waitFor(t, device.PortOpened)
	lost := recorder.waitFor(t, device.PortLost)
	if lost.ErrorClass != "disconnected" || lost.Err == nil {
		t.Errorf("lost = %+v, want class disconnected, with the error", lost)
	}
	kinds := recorder.kinds()
	if kinds[0] != device.PortOpened {
		t.Errorf("events = %v, want opened first", kinds)
	}
}

// Stopping the reader closes the port on purpose, which is not a lost port: a
// clean shutdown must not look like a device going missing.
func TestReportsClosedNotLostOnStop(t *testing.T) {
	recorder := &eventRecorder{}
	opts := serialOpts("scanner-1", "/dev/fake", "\r")
	opts.Open = func(string, *goserial.Mode) (goserial.Port, error) {
		return &blockingPort{hold: time.Hour}, nil
	}
	_, cancel := runDeviceReporting(t, opts, 1, recorder.report)
	recorder.waitFor(t, device.PortOpened)
	cancel()
	recorder.waitFor(t, device.PortClosed)
	for _, kind := range recorder.kinds() {
		if kind == device.PortLost {
			t.Errorf("events = %v, want no lost port on a clean stop", recorder.kinds())
		}
	}
}

// Every byte read is reported, whether it ends up in a frame or is discarded:
// rx_bytes rising while no frame comes out is how an unmatched separator shows
// (#23 Q5). The data is longer than one read, so it arrives in several.
func TestReportsEveryByteRead(t *testing.T) {
	recorder := &eventRecorder{}
	opts := serialOpts("scanner-1", "/dev/fake", "\r")
	opts.MaxFrameBytes = 64
	data := []byte(strings.Repeat("A7393481008232\r", 8) + "no-separator-" + strings.Repeat("x", 60))
	port := &scriptedPort{blockingPort: blockingPort{hold: time.Hour}, data: append([]byte(nil), data...)}
	opts.Open = func(string, *goserial.Mode) (goserial.Port, error) { return port, nil }
	frames, _ := runDeviceReporting(t, opts, 16, recorder.report)

	deadline := time.Now().Add(3 * time.Second)
	var read, reads int
	for time.Now().Before(deadline) {
		read, reads = 0, 0
		for _, event := range recorder.snapshot() {
			if event.Kind == device.BytesRead {
				read += event.Bytes
				reads++
			}
		}
		if read >= len(data) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if read != len(data) {
		t.Fatalf("reported %d bytes read in %d reads, want %d", read, reads, len(data))
	}
	if reads < 2 {
		t.Errorf("reported %d reads, want the data split across several", reads)
	}
	if got := len(frames); got != 8 {
		t.Errorf("%d frames, want 8: the reads are counted, not only the frames", got)
	}
}

// Every discard is reported with its reason and byte count, so that it can be
// counted: an oversize frame, and a partial frame the device stopped sending.
func TestReportsEveryDiscard(t *testing.T) {
	recorder := &eventRecorder{}
	opts := serialOpts("scanner-1", "/dev/fake", "\r")
	opts.MaxFrameBytes = 8
	port := &scriptedPort{blockingPort: blockingPort{hold: time.Hour}, data: []byte("0123456789\rPARTIAL")}
	opts.Open = func(string, *goserial.Mode) (goserial.Port, error) { return port, nil }
	runDeviceReporting(t, opts, 4, recorder.report)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		reasons := map[string]int{}
		for _, event := range recorder.snapshot() {
			if event.Kind == device.BytesDiscarded {
				reasons[event.Reason] = event.Bytes
			}
		}
		if reasons["oversize"] > 0 && reasons["inter_char_timeout"] > 0 {
			if reasons["inter_char_timeout"] != len("PARTIAL") {
				t.Errorf("timeout discard of %d bytes, want %d", reasons["inter_char_timeout"], len("PARTIAL"))
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	var seen []string
	for _, event := range recorder.snapshot() {
		seen = append(seen, string(event.Kind)+":"+event.Reason)
	}
	t.Errorf("events = %s, want an oversize and an inter_char_timeout discard", strings.Join(seen, " "))
}
