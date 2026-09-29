package serial

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	goserial "go.bug.st/serial"
)

// recordingPort notes which modem lines were raised, and can refuse to support
// them the way a pseudo-terminal does.
type recordingPort struct {
	blockingPort
	refuse bool

	mu  sync.Mutex
	dtr bool
	rts bool
}

func (p *recordingPort) SetDTR(v bool) error {
	if p.refuse {
		return goserial.PortError{}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dtr = v
	return nil
}

func (p *recordingPort) SetRTS(v bool) error {
	if p.refuse {
		return goserial.PortError{}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rts = v
	return nil
}

func (p *recordingPort) state() (dtr, rts bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.dtr, p.rts
}

// The modem lines must be raised deliberately. go.bug.st/serial documents a nil
// InitialStatusBits as defaulting to DTR and RTS true, but its code leaves the
// lines untouched, so the state would come from the operating system. A USB CDC
// device that waits for DTR before transmitting would be silent on one host and
// fine on another.
func TestOpenAssertsDTRAndRTS(t *testing.T) {
	port := &recordingPort{blockingPort: blockingPort{hold: time.Hour}}
	opened := make(chan *goserial.Mode, 1)

	opts := serialOpts("modem", "/dev/fake", "\r")
	opts.Open = func(_ string, mode *goserial.Mode) (goserial.Port, error) {
		select {
		case opened <- mode:
		default:
		}
		return port, nil
	}
	runDevice(t, opts, 1)

	select {
	case mode := <-opened:
		// Setting this makes the library query modem bits during open and fail
		// the open on any port without modem control, including a pty.
		if mode.InitialStatusBits != nil {
			t.Error("InitialStatusBits is set; open will fail on ports without modem control")
		}
		if mode.BaudRate != 9600 || mode.DataBits != 8 {
			t.Errorf("mode = %d baud, %d data bits, want 9600 and 8", mode.BaudRate, mode.DataBits)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the device never opened the port")
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if dtr, rts := port.state(); dtr && rts {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	dtr, rts := port.state()
	t.Errorf("after open DTR=%t RTS=%t, want both raised", dtr, rts)
}

// A port that does not support modem control must still be usable. Pseudo
// terminals and some USB serial drivers return ENOTTY for these calls.
func TestOpenSucceedsWhenModemLinesAreUnsupported(t *testing.T) {
	port := &recordingPort{blockingPort: blockingPort{hold: time.Hour}, refuse: true}
	present, onPresence := presenceChan()

	opts := serialOpts("no-modem-control", "/dev/fake", "\r")
	opts.Open = func(string, *goserial.Mode) (goserial.Port, error) { return port, nil }
	runDeviceReporting(t, opts, 1, onPresence)

	select {
	case p := <-present:
		if !p {
			t.Fatal("device reported absent; refusing DTR must not fail the open")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("device never became present")
	}
}

// captureLogs runs a device against a port and returns everything it logged.
func captureLogs(t *testing.T, logPayloads bool, data string) string {
	t.Helper()
	var buf syncBuffer
	present, onPresence := presenceChan()

	opts := serialOpts("payloads", "/dev/fake", "\r")
	opts.LogPayloads = logPayloads
	opts.InterCharTimeout = 20 * time.Millisecond
	opts.Logger = slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	opts.Open = func(string, *goserial.Mode) (goserial.Port, error) {
		return &scriptedPort{data: []byte(data)}, nil
	}
	runDeviceReporting(t, opts, 4, onPresence)

	select {
	case <-present:
	case <-time.After(3 * time.Second):
		t.Fatal("device never opened")
	}
	time.Sleep(300 * time.Millisecond)
	return buf.String()
}

// Section 7: payload content reaches the log only when it is asked for. The
// default must report the length and nothing else.
func TestLogPayloadsGatesContent(t *testing.T) {
	const payload = "SKU-98765"
	wrongTerminator := payload + "\n" // the device sends LF where CR is configured

	off := captureLogs(t, false, wrongTerminator)
	if strings.Contains(off, hex.EncodeToString([]byte(payload))) {
		t.Errorf("payload content reached the log with log_payloads off:\n%s", off)
	}
	if !strings.Contains(off, "discarded partial frame") {
		t.Errorf("the discard itself must still be reported:\n%s", off)
	}

	on := captureLogs(t, true, wrongTerminator)
	if !strings.Contains(on, hex.EncodeToString([]byte(wrongTerminator))) {
		t.Errorf("log_payloads on, but the discarded bytes are not in the log as hex:\n%s", on)
	}
}

// scriptedPort returns a fixed payload once, then reports read timeouts.
type scriptedPort struct {
	blockingPort
	mu   sync.Mutex
	data []byte
}

func (p *scriptedPort) Read(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.data) > 0 {
		n := copy(b, p.data)
		p.data = p.data[n:]
		return n, nil
	}
	time.Sleep(10 * time.Millisecond)
	return 0, nil
}

// syncBuffer is a bytes.Buffer that survives the logger writing from the device
// goroutine while the test reads it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// classify is what an operator greps for when a station is not scanning, so the
// mapping from errno to name is worth pinning down. permission_denied in
// particular is the one that means "the agent's user is not in the group that
// owns the device", which is the most common first-deployment failure and the
// one a container hits when it is run without --group-add.
func TestClassify(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, "none"},
		{"unplugged mid-read", syscall.EIO, "disconnected"},
		{"node gone", syscall.ENODEV, "disconnected"},
		{"never appeared", syscall.ENOENT, "absent"},
		{"wrong group", syscall.EACCES, "permission_denied"},
		{"not permitted", syscall.EPERM, "permission_denied"},
		{"device node mounted read only", syscall.EROFS, "read_only"},
		{"held by another process", syscall.EBUSY, "busy"},
		{"wrapped", fmt.Errorf("open /dev/scanner: %w", syscall.EACCES), "permission_denied"},
		{"unrecognised", errors.New("something else"), "unknown"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := classify(tc.err); got != tc.want {
				t.Errorf("classify(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}
