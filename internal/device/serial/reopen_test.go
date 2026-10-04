package serial

import (
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/skuhus/device-agent/internal/backoff"
	goserial "go.bug.st/serial"
)

// flakyPort opens successfully, reports one read timeout, then fails. It is the
// shape a failing cable or a flapping USB hub produces: the port enumerates and
// can be opened, but the device does not stay up.
type flakyPort struct {
	mu    sync.Mutex
	reads int
}

func (p *flakyPort) Read(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reads++
	if p.reads == 1 {
		// A read timeout: no bytes, no error. This is what the library returns
		// when the inter-character timeout expires on an idle device.
		return 0, nil
	}
	return 0, syscall.EIO
}

func (p *flakyPort) Write([]byte) (int, error)          { return 0, nil }
func (p *flakyPort) Drain() error                       { return nil }
func (p *flakyPort) ResetInputBuffer() error            { return nil }
func (p *flakyPort) ResetOutputBuffer() error           { return nil }
func (p *flakyPort) SetDTR(bool) error                  { return nil }
func (p *flakyPort) SetRTS(bool) error                  { return nil }
func (p *flakyPort) SetMode(*goserial.Mode) error       { return nil }
func (p *flakyPort) SetReadTimeout(time.Duration) error { return nil }
func (p *flakyPort) Close() error                       { return nil }
func (p *flakyPort) Break(time.Duration) error          { return nil }
func (p *flakyPort) GetModemStatusBits() (*goserial.ModemStatusBits, error) {
	return &goserial.ModemStatusBits{}, nil
}

// A device that opens and then immediately fails must back off, not reopen
// several times a second forever. Section 4.4 of the specification: do not spin.
func TestFlappingDeviceBacksOff(t *testing.T) {
	const (
		initial = 10 * time.Millisecond
		ceiling = 500 * time.Millisecond
		window  = 1 * time.Second
	)

	var mu sync.Mutex
	opens := 0
	opts := serialOpts("flapping", "/dev/fake", "\r")
	opts.Reopen = backoff.Policy{Interval: initial, Grow: true, Max: ceiling, Jitter: 0.3}
	opts.Open = func(string, *goserial.Mode) (goserial.Port, error) {
		mu.Lock()
		opens++
		mu.Unlock()
		return &flakyPort{}, nil
	}
	runDevice(t, opts, 1)

	time.Sleep(window)

	mu.Lock()
	got := opens
	mu.Unlock()

	// With backoff applied the schedule is 10, 20, 40, 80, 160, 320, 500...
	// which fits about seven opens into a second. Without it the device
	// reopens every 10ms, which is about a hundred.
	const limit = 20
	if got > limit {
		t.Errorf("device was reopened %d times in %s, want at most %d; the backoff is being reset on every failed session",
			got, window, limit)
	}
	if got < 2 {
		t.Errorf("device was reopened %d times, want the retry loop to be running", got)
	}
	t.Logf("%d reopens in %s", got, window)
}

// A session that stayed up must start the wait over, so a device that has
// been working for a long time reconnects promptly rather than waiting the
// ceiling. A session counts as stable once it lasted the longest wait.
func TestStableSessionResetsBackoff(t *testing.T) {
	const (
		interval = 10 * time.Millisecond
		ceiling  = 100 * time.Millisecond
		// Each session lasts longer than the ceiling, so each one is stable.
		session = 150 * time.Millisecond
	)
	var mu sync.Mutex
	var opens []time.Time

	opts := serialOpts("stable", "/dev/fake", "\r")
	opts.Reopen = backoff.Policy{Interval: interval, Grow: true, Max: ceiling}
	opts.Open = func(string, *goserial.Mode) (goserial.Port, error) {
		mu.Lock()
		opens = append(opens, time.Now())
		mu.Unlock()
		return &blockingPort{hold: session}, nil
	}
	runDevice(t, opts, 1)

	time.Sleep(1200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	// Without the reset the waits are 10, 20, 40 and 80 ms, so the fourth gap
	// between opens is the session and 80 ms. With it, each is the session and
	// 10 ms.
	if len(opens) < 5 {
		t.Fatalf("got %d opens, want at least 5", len(opens))
	}
	const limit = session + 60*time.Millisecond
	for index := 1; index < len(opens); index++ {
		if gap := opens[index].Sub(opens[index-1]); gap > limit {
			t.Errorf("open %d came %s after the one before, over %s; the wait grew although every session was stable",
				index, gap, limit)
		}
	}
}

// A device that would reopen with no wait at all spins (section 4.4), so New
// refuses it rather than running it.
func TestNewRejectsAnUnusableReopenPolicy(t *testing.T) {
	opts := serialOpts("scanner-1", "/dev/fake", "\r")
	opts.Reopen = backoff.Policy{}
	_, err := New(opts)
	if err == nil || !strings.Contains(err.Error(), "device scanner-1: reopen: the interval must be positive, got 0s") {
		t.Errorf("err = %v, want the reopen interval refused", err)
	}
}

// blockingPort stays readable for hold, then fails, so a session has a
// measurable duration.
type blockingPort struct {
	hold  time.Duration
	start time.Time
}

func (p *blockingPort) Read(b []byte) (int, error) {
	if p.start.IsZero() {
		p.start = time.Now()
	}
	if time.Since(p.start) < p.hold {
		time.Sleep(10 * time.Millisecond)
		return 0, nil
	}
	return 0, syscall.EIO
}

func (p *blockingPort) Write([]byte) (int, error)          { return 0, nil }
func (p *blockingPort) Drain() error                       { return nil }
func (p *blockingPort) ResetInputBuffer() error            { return nil }
func (p *blockingPort) ResetOutputBuffer() error           { return nil }
func (p *blockingPort) SetDTR(bool) error                  { return nil }
func (p *blockingPort) SetRTS(bool) error                  { return nil }
func (p *blockingPort) SetMode(*goserial.Mode) error       { return nil }
func (p *blockingPort) SetReadTimeout(time.Duration) error { return nil }
func (p *blockingPort) Close() error                       { return nil }
func (p *blockingPort) Break(time.Duration) error          { return nil }
func (p *blockingPort) GetModemStatusBits() (*goserial.ModemStatusBits, error) {
	return &goserial.ModemStatusBits{}, nil
}
