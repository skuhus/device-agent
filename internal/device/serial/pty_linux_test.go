//go:build linux

package serial

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/skuhus/device-agent/internal/backoff"
	"github.com/skuhus/device-agent/internal/device"
	"github.com/skuhus/device-agent/internal/logging/logtest"
	goserial "go.bug.st/serial"
	"golang.org/x/sys/unix"
)

// newPTY returns a pseudo-terminal pair: a master that stands in for the
// scanner, and the slave path the agent opens as if it were /dev/ttyACM0.
//
// This is the harness the specification calls for. It uses /dev/ptmx directly
// rather than socat, so the tests need no external process and no scanner on
// the desk.
func newPTY(t *testing.T) (master *os.File, slavePath string) {
	t.Helper()
	m, err := os.OpenFile("/dev/ptmx", os.O_RDWR, 0)
	if err != nil {
		t.Skipf("no /dev/ptmx on this host: %v", err)
	}
	t.Cleanup(func() { m.Close() })

	if err := unix.IoctlSetPointerInt(int(m.Fd()), unix.TIOCSPTLCK, 0); err != nil {
		t.Fatalf("unlock pty: %v", err)
	}
	n, err := unix.IoctlGetInt(int(m.Fd()), unix.TIOCGPTN)
	if err != nil {
		t.Fatalf("get pty number: %v", err)
	}
	return m, fmt.Sprintf("/dev/pts/%d", n)
}

// runDevice starts a device against the given path and returns its frame
// channel. It fails the test if the device goroutine outlives cancellation.
func runDevice(t *testing.T, opts Options, sinkCap int) (chan device.Frame, context.CancelFunc) {
	t.Helper()
	return runDeviceReporting(t, opts, sinkCap, nil)
}

// runDeviceReporting is runDevice with a function that receives every port
// event.
func runDeviceReporting(t *testing.T, opts Options, sinkCap int, report func(device.Event)) (chan device.Frame, context.CancelFunc) {
	t.Helper()
	if opts.Logger == nil {
		// The agent's log at DEBUG, captured, so that every line the reader
		// writes is held to the log's rules when the test ends.
		opts.Logger, _ = logtest.New(t, "debug")
	}
	d, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	frames := make(chan device.Frame, sinkCap)
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		if err := d.Run(ctx, frames, report); !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Run returned %v, want a context error", err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-stopped:
		case <-time.After(5 * time.Second):
			t.Fatal("device goroutine did not stop within 5s of cancellation")
		}
	})
	return frames, cancel
}

// testTxChunkBytes is the tests' tx chunk: not the default 1024, so that a
// chunk taken from anywhere but the options shows.
const testTxChunkBytes = 700

// fastReopen is the reopen policy of tests that do not test it: v1's shape,
// scaled down so that a test does not wait on it.
var fastReopen = backoff.Policy{Interval: 5 * time.Millisecond, Grow: true, Max: 20 * time.Millisecond, Jitter: 0.3}

func serialOpts(id, path string, term string) Options {
	return Options{
		ID:               id,
		Path:             path,
		Baud:             9600,
		Terminator:       []byte(term),
		MaxFrameBytes:    4096,
		InterCharTimeout: 50 * time.Millisecond,
		TxChunkBytes:     testTxChunkBytes,
		Reopen:           fastReopen,
	}
}

func recvFrame(t *testing.T, frames <-chan device.Frame) device.Frame {
	t.Helper()
	select {
	case f := <-frames:
		return f
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for a frame")
		return device.Frame{}
	}
}

func expectNoFrame(t *testing.T, frames <-chan device.Frame, within time.Duration) {
	t.Helper()
	select {
	case f := <-frames:
		t.Fatalf("unexpected frame %q", f.Raw)
	case <-time.After(within):
	}
}

// waitForOpen gives the device time to open the slave before the master writes,
// so a test failure means the read path is broken rather than that the write
// raced the open.
func waitForOpen(t *testing.T, present <-chan bool) {
	t.Helper()
	select {
	case p := <-present:
		if !p {
			t.Fatal("first presence transition was absent, want present")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("device did not open within 3s")
	}
}

// presenceChan turns port events into the present/absent transitions these
// tests were written against: opened is present, lost is absent. A failed open
// is neither, because a port that never opened was never present.
func presenceChan() (chan bool, func(device.Event)) {
	ch := make(chan bool, 16)
	return ch, func(event device.Event) {
		var present bool
		switch event.Kind {
		case device.PortOpened:
			present = true
		case device.PortLost:
			present = false
		default:
			return
		}
		select {
		case ch <- present:
		default:
		}
	}
}

// TestPTYReplayCaptures replays each recorded stream through a real serial port
// and checks the frames the agent would publish.
func TestPTYReplayCaptures(t *testing.T) {
	cases := []struct {
		file       string
		terminator string
		want       [][]byte
	}{
		{"code128-cr.bin", "\r", [][]byte{[]byte("0123456789")}},
		{"gs1-128-cr.bin", "\r", [][]byte{[]byte("]C1\x1d0104912345123459\x1d17250101")}},
		{"burst-crlf.bin", "\r\n", [][]byte{[]byte("SKU-0001"), []byte("SKU-0002"), []byte("SKU-0003")}},
		{"non-utf8-cr.bin", "\r", [][]byte{{'A', 'B', 0xff, 0xfe, 'C', 'D'}}},
		// Real payloads from a Symbol 05e0:1701. The 2D entry is 35 bytes,
		// which is where a CR/CRLF mismatch first showed itself in the field.
		{"symbol-05e0-1701-crlf.bin", "\r\n", [][]byte{
			[]byte("A7393481008232"),
			[]byte("A42154587"),
			[]byte("D1058740077194"),
			[]byte("P00YWVIT75KHXTJNE7V1HZMNNLGWEJ6FZHM"),
			[]byte("D02526000011133396093"),
			[]byte("A001100133391"),
		}},
	}
	for _, tc := range cases {
		t.Run(tc.file, func(t *testing.T) {
			capture, err := os.ReadFile(filepath.Join("testdata", tc.file))
			if err != nil {
				t.Fatalf("read capture: %v", err)
			}
			master, slave := newPTY(t)
			present, onPresence := presenceChan()

			opts := serialOpts("replay", slave, tc.terminator)
			frames, _ := runDeviceReporting(t, opts, 8, onPresence)
			waitForOpen(t, present)

			if _, err := master.Write(capture); err != nil {
				t.Fatalf("replay write: %v", err)
			}
			for i, want := range tc.want {
				got := recvFrame(t, frames)
				if string(got.Raw) != string(want) {
					t.Errorf("frame %d = %q, want %q", i, got.Raw, want)
				}
				if got.DeviceID != "replay" {
					t.Errorf("frame %d device id = %q, want replay", i, got.DeviceID)
				}
				if got.At.IsZero() {
					t.Errorf("frame %d has no timestamp", i)
				}
			}
			expectNoFrame(t, frames, 200*time.Millisecond)
		})
	}
}

// A scan written one byte at a time, slower than a real scanner but faster than
// the inter-character timeout, must still arrive as a single frame.
func TestPTYFrameArrivesAcrossManyReads(t *testing.T) {
	master, slave := newPTY(t)
	present, onPresence := presenceChan()
	opts := serialOpts("drip", slave, "\r")
	opts.InterCharTimeout = 500 * time.Millisecond
	frames, _ := runDeviceReporting(t, opts, 4, onPresence)
	waitForOpen(t, present)

	for _, b := range []byte("SKU-9911\r") {
		if _, err := master.Write([]byte{b}); err != nil {
			t.Fatalf("write %q: %v", b, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	got := recvFrame(t, frames)
	if string(got.Raw) != "SKU-9911" {
		t.Errorf("frame = %q, want SKU-9911", got.Raw)
	}
}

// An unterminated frame is never emitted, however long the device waits, and
// the port recovers to deliver the next scan.
//
// What happens to a tail that arrives after the timeout depends on how long the
// device stayed silent, and is pinned deterministically by the framer tests.
// See DESIGN-V2.md, "Choosing inter_char_timeout".
func TestPTYInterCharTimeoutDropsStalledFrame(t *testing.T) {
	master, slave := newPTY(t)
	present, onPresence := presenceChan()
	opts := serialOpts("stall", slave, "\r")
	opts.InterCharTimeout = 50 * time.Millisecond
	frames, _ := runDeviceReporting(t, opts, 4, onPresence)
	waitForOpen(t, present)

	if _, err := master.Write([]byte("STALL")); err != nil {
		t.Fatalf("write: %v", err)
	}
	expectNoFrame(t, frames, 300*time.Millisecond)

	if _, err := master.Write([]byte("RECOVERED\r")); err != nil {
		t.Fatalf("write: %v", err)
	}
	got := recvFrame(t, frames)
	if string(got.Raw) != "RECOVERED" {
		t.Errorf("frame = %q, want RECOVERED", got.Raw)
	}
}

// When the sink is full the reader blocks. Nothing is dropped and nothing is
// reordered: the operator's scans queue behind the stalled publisher.
func TestPTYFullSinkBlocksReaderWithoutLoss(t *testing.T) {
	master, slave := newPTY(t)
	present, onPresence := presenceChan()
	opts := serialOpts("backpressure", slave, "\r")
	frames, _ := runDeviceReporting(t, opts, 0, onPresence)
	waitForOpen(t, present)

	want := []string{"ONE", "TWO", "THREE", "FOUR"}
	for _, s := range want {
		if _, err := master.Write([]byte(s + "\r")); err != nil {
			t.Fatalf("write %s: %v", s, err)
		}
	}
	time.Sleep(100 * time.Millisecond)
	for i, w := range want {
		got := recvFrame(t, frames)
		if string(got.Raw) != w {
			t.Errorf("frame %d = %q, want %q", i, got.Raw, w)
		}
	}
}

// Losing the device must not end Run: it closes, reports absence and retries.
func TestPTYDisconnectReportsAbsenceAndRetries(t *testing.T) {
	master, slave := newPTY(t)
	present, onPresence := presenceChan()
	opts := serialOpts("unplug", slave, "\r")
	frames, _ := runDeviceReporting(t, opts, 4, onPresence)
	waitForOpen(t, present)

	if _, err := master.Write([]byte("BEFORE\r")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got := recvFrame(t, frames); string(got.Raw) != "BEFORE" {
		t.Fatalf("frame = %q, want BEFORE", got.Raw)
	}

	master.Close()

	deadline := time.After(3 * time.Second)
	for {
		select {
		case p := <-present:
			if !p {
				return // absence reported, Run is in its reopen loop
			}
		case <-deadline:
			t.Fatal("no absence transition within 3s of the device going away")
		}
	}
}

// The reopen loop must survive a device that is not there at all, and must stop
// promptly when the context is cancelled rather than spinning.
func TestReopenLoopOnMissingDeviceStopsOnCancel(t *testing.T) {
	var mu sync.Mutex
	attempts := 0
	opts := serialOpts("missing", "/dev/does-not-exist", "\r")
	opts.Open = func(string, *goserial.Mode) (goserial.Port, error) {
		mu.Lock()
		attempts++
		mu.Unlock()
		return nil, syscall.ENOENT
	}
	_, cancel := runDevice(t, opts, 1)

	time.Sleep(150 * time.Millisecond)
	cancel()

	mu.Lock()
	defer mu.Unlock()
	if attempts < 2 {
		t.Errorf("open was attempted %d times, want repeated retries", attempts)
	}
	// Backoff starts at 5ms and doubles to a 20ms ceiling, so 150ms cannot
	// contain an unbounded spin.
	if attempts > 60 {
		t.Errorf("open was attempted %d times in 150ms, backoff is not being applied", attempts)
	}
}

// Restarting a device repeatedly must not leak goroutines. Each session starts
// a watcher that closes the port on cancellation, and each one has to exit.
func TestNoGoroutineLeakAcrossDeviceRestarts(t *testing.T) {
	before := goroutineCount(t, 0)

	for i := 0; i < 5; i++ {
		master, slave := newPTY(t)
		present, onPresence := presenceChan()
		opts := serialOpts("cycle", slave, "\r")

		d, err := New(opts)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		frames := make(chan device.Frame, 4)
		stopped := make(chan struct{})
		go func() {
			defer close(stopped)
			d.Run(ctx, frames, onPresence)
		}()
		waitForOpen(t, present)
		if _, err := master.Write([]byte("CYCLE\r")); err != nil {
			t.Fatalf("write: %v", err)
		}
		recvFrame(t, frames)
		cancel()
		select {
		case <-stopped:
		case <-time.After(3 * time.Second):
			t.Fatal("Run did not return within 3s of cancellation")
		}
		master.Close()
	}

	after := goroutineCount(t, before)
	if after > before {
		buf := make([]byte, 1<<16)
		buf = buf[:runtime.Stack(buf, true)]
		t.Errorf("goroutines went from %d to %d after 5 device cycles\n%s", before, after, buf)
	}
}

// goroutineCount waits for the count to settle at or below target, so a
// goroutine that is merely slow to exit is not reported as a leak.
func goroutineCount(t *testing.T, target int) int {
	t.Helper()
	n := runtime.NumGoroutine()
	if target == 0 {
		return n
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		n = runtime.NumGoroutine()
		if n <= target {
			return n
		}
		time.Sleep(20 * time.Millisecond)
	}
	return n
}
