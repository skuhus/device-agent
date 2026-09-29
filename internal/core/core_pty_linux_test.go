package core

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/skuhus/device-agent/internal/device"
	"github.com/skuhus/device-agent/internal/device/serial"
	"golang.org/x/sys/unix"
)

// newPTY opens a pseudo-terminal pair through /dev/ptmx and returns the master
// and the slave's path, the same harness internal/device/serial uses.
func newPTY(t *testing.T) (*os.File, string) {
	t.Helper()
	master, err := os.OpenFile("/dev/ptmx", os.O_RDWR, 0)
	if err != nil {
		t.Skipf("no /dev/ptmx on this host: %v", err)
	}
	t.Cleanup(func() { master.Close() })
	if err := unix.IoctlSetPointerInt(int(master.Fd()), unix.TIOCSPTLCK, 0); err != nil {
		t.Fatalf("unlock pty: %v", err)
	}
	number, err := unix.IoctlGetInt(int(master.Fd()), unix.TIOCGPTN)
	if err != nil {
		t.Fatalf("get pty number: %v", err)
	}
	return master, fmt.Sprintf("/dev/pts/%d", number)
}

// openWatch wraps a reader and closes opened when it first reports its port
// open, so the test writes only once someone is reading.
type openWatch struct {
	device.Device
	opened chan struct{}
}

func (watch *openWatch) Run(ctx context.Context, sink chan<- device.Frame, report func(device.Event)) error {
	var once bool
	return watch.Device.Run(ctx, sink, func(event device.Event) {
		report(event)
		if event.Kind == device.PortOpened && !once {
			once = true
			close(watch.opened)
		}
	})
}

// With the pseudo-terminal harness, every frame produces exactly one rx message
// on its device's topic, and two devices on one agent keep to their own: the
// real Symbol 05e0:1701 capture on one port, a CRLF burst on the other.
func TestEveryFrameIsOneRxMessageOnItsDeviceTopic(t *testing.T) {
	captures := []struct {
		id    string
		file  string
		want  []string
		topic string
	}{
		{"scanner-main", "symbol-05e0-1701-crlf.bin",
			[]string{"A7393481008232", "A42154587", "D1058740077194", "P00YWVIT75KHXTJNE7V1HZMNNLGWEJ6FZHM", "D02526000011133396093", "A001100133391"},
			"skuhus/acme/vasby/pack-03/scanner-main/rx"},
		{"scanner-side", "burst-crlf.bin",
			[]string{"SKU-0001", "SKU-0002", "SKU-0003"},
			"skuhus/acme/vasby/pack-03/scanner-side/rx"},
	}

	transport := &fakeTransport{}
	var devices []Device
	var masters []*os.File
	var watches []*openWatch
	for _, capture := range captures {
		master, slave := newPTY(t)
		reader, err := serial.New(serial.Options{
			ID: capture.id, Path: slave, Baud: 9600, Terminator: []byte("\r\n"),
			MaxFrameBytes: 4096, InterCharTimeout: 50 * time.Millisecond,
			BackoffInitial: 5 * time.Millisecond, BackoffMax: 20 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("serial.New: %v", err)
		}
		watch := &openWatch{Device: reader, opened: make(chan struct{})}
		devices = append(devices, coreDevice(t, watch, ""))
		masters = append(masters, master)
		watches = append(watches, watch)
	}

	running := newCore(t, testOptions(t, transport, devices...))
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- running.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case <-errc:
		case <-time.After(10 * time.Second):
			t.Fatal("Run did not return within 10s of cancellation")
		}
	}()

	for i, capture := range captures {
		select {
		case <-watches[i].opened:
		case <-time.After(3 * time.Second):
			t.Fatalf("%s did not open its port", capture.id)
		}
		body, err := os.ReadFile(filepath.Join("..", "device", "serial", "testdata", capture.file))
		if err != nil {
			t.Fatalf("read capture: %v", err)
		}
		if _, err := masters[i].Write(body); err != nil {
			t.Fatalf("write capture: %v", err)
		}
	}

	for _, capture := range captures {
		deadline := time.Now().Add(5 * time.Second)
		for len(transport.rxOn(capture.topic)) < len(capture.want) && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
	}
	// Anything more that was going to arrive has arrived by now.
	time.Sleep(200 * time.Millisecond)

	for _, capture := range captures {
		rx := transport.rxOn(capture.topic)
		if len(rx) != len(capture.want) {
			t.Errorf("%s: %d rx messages, want exactly %d", capture.topic, len(rx), len(capture.want))
			continue
		}
		for i, want := range capture.want {
			raw, _ := base64.StdEncoding.DecodeString(rx[i].RawB64)
			if string(raw) != want || rx[i].Seq != uint64(i+1) || rx[i].DeviceID != capture.id {
				t.Errorf("%s message %d = %q seq %d device %s, want %q seq %d", capture.topic, i, raw, rx[i].Seq, rx[i].DeviceID, want, i+1)
			}
		}
	}
}
