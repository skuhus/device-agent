package serial

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/skuhus/device-agent/internal/backoff"
	"github.com/skuhus/device-agent/internal/device"
	goserial "go.bug.st/serial"
)

// writtenPort records what is written to it. Each Write takes at most perCall
// bytes, as write(2) may, and can fail once failAfter bytes are in. A Write
// that arrives after Close is recorded, since the descriptor it would reach on
// a real port could belong to anything by then.
type writtenPort struct {
	blockingPort
	perCall   int
	failAfter int
	delay     time.Duration

	mu         sync.Mutex
	got        []byte
	calls      int
	closed     bool
	afterClose int
}

func newWrittenPort() *writtenPort {
	return &writtenPort{blockingPort: blockingPort{hold: time.Hour}, perCall: 1 << 30, failAfter: -1}
}

func (port *writtenPort) Write(data []byte) (int, error) {
	time.Sleep(port.delay)
	port.mu.Lock()
	defer port.mu.Unlock()
	port.calls++
	if port.closed {
		port.afterClose++
		return 0, syscall.EBADF
	}
	accepted := min(len(data), port.perCall)
	if port.failAfter >= 0 && len(port.got)+accepted > port.failAfter {
		accepted = port.failAfter - len(port.got)
		port.got = append(port.got, data[:accepted]...)
		return accepted, syscall.EIO
	}
	port.got = append(port.got, data[:accepted]...)
	return accepted, nil
}

func (port *writtenPort) Close() error {
	port.mu.Lock()
	defer port.mu.Unlock()
	port.closed = true
	return nil
}

func (port *writtenPort) state() ([]byte, int, int) {
	port.mu.Lock()
	defer port.mu.Unlock()
	return bytes.Clone(port.got), port.calls, port.afterClose
}

// runOn runs a device whose port is the given one, until the test ends or the
// returned cancel is called, and waits for the port to open.
func runOn(t *testing.T, port goserial.Port) (*Device, context.CancelFunc) {
	t.Helper()
	opts := serialOpts("printer-1", "/dev/fake", "\r\n")
	opts.Open = func(string, *goserial.Mode) (goserial.Port, error) { return port, nil }
	dev, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	opened := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = dev.Run(ctx, make(chan device.Frame, 8), func(event device.Event) {
			if event.Kind == device.PortOpened {
				select {
				case opened <- struct{}{}:
				default:
				}
			}
		})
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	select {
	case <-opened:
	case <-time.After(3 * time.Second):
		t.Fatal("the port did not open")
	}
	return dev, cancel
}

// Every byte reaches the port, in order, however few each write(2) takes, and
// progress reports the running total.
func TestWriteReachesTheOpenPortWhole(t *testing.T) {
	port := newWrittenPort()
	port.perCall = 333
	dev, _ := runOn(t, port)
	data := bytes.Repeat([]byte("0123456789"), 500)

	var reported []int
	written, err := dev.Write(context.Background(), data, func(total int) { reported = append(reported, total) })
	if err != nil || written != len(data) {
		t.Fatalf("Write = %d, %v; want %d, nil", written, err, len(data))
	}
	got, calls, _ := port.state()
	if !bytes.Equal(got, data) {
		t.Errorf("the port got %d bytes, not the %d written in order", len(got), len(data))
	}
	if want := (len(data) + 332) / 333; calls < want {
		t.Errorf("%d write calls for %d bytes at 333 a call, want at least %d", calls, len(data), want)
	}
	if len(reported) == 0 || reported[len(reported)-1] != len(data) {
		t.Errorf("progress = %v, want it to end at %d", reported, len(data))
	}
	for index := 1; index < len(reported); index++ {
		if reported[index] <= reported[index-1] || reported[index]-reported[index-1] > testTxChunkBytes {
			t.Fatalf("progress step %d -> %d; want rising, at most a chunk at a time", reported[index-1], reported[index])
		}
	}
}

// The port takes writes by the time it is reported open: a tx woken by the
// report must find it, or it spends one of its attempts for nothing.
func TestThePortTakesWritesWhenReportedOpen(t *testing.T) {
	port := newWrittenPort()
	opts := serialOpts("printer-1", "/dev/fake", "\r\n")
	opts.Open = func(string, *goserial.Mode) (goserial.Port, error) { return port, nil }
	dev, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	writeOnOpen := make(chan error, 1)
	report := func(event device.Event) {
		if event.Kind != device.PortOpened {
			return
		}
		_, err := dev.Write(context.Background(), []byte("label"), nil)
		select {
		case writeOnOpen <- err:
		default:
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		_ = dev.Run(ctx, make(chan device.Frame, 8), report)
	}()
	t.Cleanup(func() {
		cancel()
		<-stopped
	})

	select {
	case err := <-writeOnOpen:
		if err != nil {
			t.Errorf("a write as the port was reported open = %v, want it written", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the port was never reported open")
	}
}

// A tx chunk of nothing would never write a byte, so New refuses it.
func TestNewRejectsAnEmptyTxChunk(t *testing.T) {
	opts := serialOpts("printer-1", "/dev/fake", "\r\n")
	opts.TxChunkBytes = 0
	if _, err := New(opts); err == nil || !strings.Contains(err.Error(), "device printer-1: tx chunk bytes must be at least 1, got 0") {
		t.Errorf("err = %v, want the tx chunk refused", err)
	}
}

// No port, no write: the caller learns the port is not open, and can retry.
func TestWriteWithoutAnOpenPort(t *testing.T) {
	dev, err := New(serialOpts("printer-1", "/dev/fake", "\r\n"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	written, err := dev.Write(context.Background(), []byte("label"), nil)
	if written != 0 || !errors.Is(err, device.ErrNotOpen) {
		t.Errorf("Write = %d, %v; want 0 and ErrNotOpen", written, err)
	}
}

// A write racing a close stops at a chunk boundary, says the port is not
// open, and never reaches the closed port, whose descriptor a real port might
// already have given to something else (DESIGN-V2.md, "Writing: tx").
func TestWriteRacingACloseNeverReachesTheClosedPort(t *testing.T) {
	port := newWrittenPort()
	port.delay = time.Millisecond
	port.perCall = 256
	dev, cancel := runOn(t, port)
	data := bytes.Repeat([]byte("x"), 256*1024)

	started := make(chan struct{})
	var once sync.Once
	result := make(chan error, 1)
	var written int
	go func() {
		var err error
		written, err = dev.Write(context.Background(), data, func(int) { once.Do(func() { close(started) }) })
		result <- err
	}()
	<-started
	cancel()
	err := <-result
	if !errors.Is(err, device.ErrNotOpen) {
		t.Fatalf("Write ended with %v, want ErrNotOpen", err)
	}
	got, _, afterClose := port.state()
	if afterClose != 0 {
		t.Errorf("%d writes reached the port after it was closed", afterClose)
	}
	if written != len(got) || written == 0 || written >= len(data) {
		t.Errorf("Write reported %d bytes, the port has %d, of %d: want the same part way", written, len(got), len(data))
	}
	if written%testTxChunkBytes != 0 {
		t.Errorf("stopped at %d bytes, not at a chunk boundary of %d", written, testTxChunkBytes)
	}
}

// A port that fails part way says how far it got, and the failure carries the
// class the reader would give it.
func TestWriteFailureCarriesItsClass(t *testing.T) {
	port := newWrittenPort()
	port.failAfter = 1500
	dev, _ := runOn(t, port)
	written, err := dev.Write(context.Background(), bytes.Repeat([]byte("y"), 4000), nil)
	var portErr *device.PortError
	if !errors.As(err, &portErr) || portErr.Class != "disconnected" || !errors.Is(err, syscall.EIO) {
		t.Fatalf("Write error = %v, want a PortError of class disconnected wrapping EIO", err)
	}
	if written != 1500 {
		t.Errorf("written = %d, want the 1500 the port took", written)
	}
}

// A tx waiting for the port has the reader try now, rather than at the end of
// a backoff that may be half a minute long.
func TestRetryOpenCutsTheBackoffShort(t *testing.T) {
	var mu sync.Mutex
	var attempts []time.Time
	opts := serialOpts("printer-1", "/dev/absent", "\r\n")
	opts.Reopen = backoff.Policy{Interval: time.Minute}
	opts.Open = func(string, *goserial.Mode) (goserial.Port, error) {
		mu.Lock()
		defer mu.Unlock()
		attempts = append(attempts, time.Now())
		return nil, syscall.ENOENT
	}
	dev, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = dev.Run(ctx, make(chan device.Frame, 1), nil)
	}()
	defer func() {
		cancel()
		<-done
	}()
	count := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(attempts)
	}
	deadline := time.Now().Add(3 * time.Second)
	for count() < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	dev.RetryOpen()
	for count() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(attempts) != 2 {
		t.Fatalf("%d open attempts, want the first and one more on RetryOpen, inside a minute's backoff", len(attempts))
	}
	if gap := attempts[1].Sub(attempts[0]); gap > time.Second {
		t.Errorf("the retry came %s after the first attempt, want well inside the minute's backoff", gap)
	}
}

// A write told to stop finishes the chunk in hand and starts no other, so that
// a stopping agent cuts a long job at a chunk boundary (#19 Q1).
func TestWriteStopsBetweenChunksWhenAsked(t *testing.T) {
	port := newWrittenPort()
	port.delay = time.Millisecond
	port.perCall = 256
	dev, _ := runOn(t, port)
	ctx, cancel := context.WithCancel(context.Background())
	var once sync.Once
	written, err := dev.Write(ctx, bytes.Repeat([]byte("z"), 64*1024), func(int) { once.Do(cancel) })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Write ended with %v, want context.Canceled", err)
	}
	got, _, _ := port.state()
	if written != testTxChunkBytes || len(got) != testTxChunkBytes {
		t.Errorf("written %d, the port has %d; want the one chunk in hand, %d", written, len(got), testTxChunkBytes)
	}
}
