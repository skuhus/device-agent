package core

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/skuhus/device-agent/internal/device"
	"github.com/skuhus/device-agent/internal/device/serial"
	"github.com/skuhus/device-agent/internal/wire"
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

// runPTYCore starts a core over one serial reader on a pseudo-terminal and
// stops it when the test ends.
func runPTYCore(t *testing.T, opts Options) {
	t.Helper()
	running := newCore(t, opts)
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- running.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-errc:
		case <-time.After(10 * time.Second):
			t.Error("Run did not return within 10s of cancellation")
		}
	})
}

func ptyReader(t *testing.T, id, slave string, separator string) device.Device {
	t.Helper()
	reader, err := serial.New(serial.Options{
		ID: id, Path: slave, Baud: 9600, Terminator: []byte(separator),
		MaxFrameBytes: 4096, InterCharTimeout: 50 * time.Millisecond,
		BackoffInitial: 5 * time.Millisecond, BackoffMax: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("serial.New: %v", err)
	}
	return reader
}

func eventsWithCode(events []wire.Event, code wire.EventCode) []wire.Event {
	var out []wire.Event
	for _, event := range events {
		if event.Code == code {
			out = append(out, event)
		}
	}
	return out
}

// Closing the pseudo-terminal, as pulling a scanner's cable does, publishes a
// port_lost event with its error class on the device's status topic.
func TestClosingThePTYPublishesPortLost(t *testing.T) {
	master, slave := newPTY(t)
	transport := &fakeTransport{}
	runPTYCore(t, testOptions(t, transport, coreDevice(t, ptyReader(t, "scanner-main", slave, "\r\n"), "symbol-05e0-1701")))

	topic := "skuhus/acme/vasby/pack-03/scanner-main/status"
	waitUntil(t, "port_opened", func() bool { return len(eventsWithCode(transport.eventsOn(topic), wire.EventPortOpened)) > 0 })
	master.Close()
	waitUntil(t, "port_lost", func() bool { return len(eventsWithCode(transport.eventsOn(topic), wire.EventPortLost)) > 0 })

	lost := eventsWithCode(transport.eventsOn(topic), wire.EventPortLost)[0]
	if lost.DeviceOpen || lost.Detail["error_class"] != "disconnected" || lost.Detail["path"] != slave || lost.Detail["error"] == "" {
		t.Errorf("port_lost device_open %v detail %v, want closed, class disconnected, path %s, the error", lost.DeviceOpen, lost.Detail, slave)
	}
}

// An unmatched separator frames nothing: the configured separator never
// appears in what the device sends, because it is misconfigured or because the
// device sends none. Each scan ends in one inter-character timeout discard, and
// consecutive keepalives show the signature DESIGN-V2.md describes: timeout
// discards and bytes read rising, rx frames at zero. The scans are the real
// Symbol 05e0:1701 capture, sent one at a time as a person scans them.
func TestUnmatchedSeparatorShowsInConsecutiveKeepalives(t *testing.T) {
	t.Run("misconfigured", func(t *testing.T) {
		// The scanner ends each scan in CRLF; the separator is configured the
		// other way round.
		unmatchedSeparator(t, "\n\r", false)
	})
	t.Run("missing from the data", func(t *testing.T) {
		// The separator is configured as CRLF, and the scanner sends none, as
		// one set up without a suffix does.
		unmatchedSeparator(t, "\r\n", true)
	})
}

// unmatchedSeparator sends the capture's six scans to a reader configured with
// separator, each without its CRLF when strip is set, and checks two
// keepalives, one after three scans and one after all six.
func unmatchedSeparator(t *testing.T, separator string, strip bool) {
	master, slave := newPTY(t)
	transport := &fakeTransport{}
	connected := make(chan struct{}, 1)
	opts := testOptions(t, transport, coreDevice(t, ptyReader(t, "scanner-main", slave, separator), "symbol-05e0-1701"))
	opts.Connected = connected
	runPTYCore(t, opts)

	topic := "skuhus/acme/vasby/pack-03/scanner-main/status"
	waitUntil(t, "port_opened", func() bool { return len(eventsWithCode(transport.eventsOn(topic), wire.EventPortOpened)) > 0 })

	body, err := os.ReadFile(filepath.Join("..", "device", "serial", "testdata", "symbol-05e0-1701-crlf.bin"))
	if err != nil {
		t.Fatalf("read capture: %v", err)
	}
	var scans [][]byte
	for _, scan := range bytes.SplitAfter(body, []byte("\r\n")) {
		if strip {
			scan = bytes.TrimSuffix(scan, []byte("\r\n"))
		}
		if len(scan) > 0 {
			scans = append(scans, scan)
		}
	}
	if len(scans) != 6 {
		t.Fatalf("capture holds %d scans, want 6", len(scans))
	}

	timeouts := func() int {
		count := 0
		for _, event := range eventsWithCode(transport.eventsOn(topic), wire.EventBytesDiscarded) {
			if event.Detail["reason"] == string(wire.DiscardInterCharTimeout) {
				count++
			}
		}
		return count
	}
	sent := 0
	keepaliveAfter := func(batch [][]byte, keepalives int) wire.KeepaliveDevice {
		t.Helper()
		for _, scan := range batch {
			if _, err := master.Write(scan); err != nil {
				t.Fatalf("write scan: %v", err)
			}
			sent += len(scan)
			// Three inter-character timeouts: the scan is discarded, and the
			// resynchronisation after it ends before the next one.
			time.Sleep(150 * time.Millisecond)
		}
		waitUntil(t, "a timeout discard per scan", func() bool { return timeouts() == 3*keepalives })
		connected <- struct{}{}
		waitUntil(t, "a keepalive", func() bool { return transport.calls("keepalive") == keepalives })
		return transport.keepalives()[keepalives-1].Devices[0]
	}

	first := keepaliveAfter(scans[:3], 1)
	sentFirst := sent
	second := keepaliveAfter(scans[3:], 2)

	if first.RxFrames != 0 || second.RxFrames != 0 || len(transport.of("rx", "")) != 0 {
		t.Errorf("rx_frames %d then %d, %d rx messages, want no frame at all", first.RxFrames, second.RxFrames, len(transport.of("rx", "")))
	}
	if first.Discards.InterCharTimeout != 3 || second.Discards.InterCharTimeout != 6 {
		t.Errorf("inter_char_timeout discards %d then %d, want 3 then 6", first.Discards.InterCharTimeout, second.Discards.InterCharTimeout)
	}
	if first.RxBytes != uint64(sentFirst) || second.RxBytes != uint64(sent) {
		t.Errorf("rx_bytes %d then %d, want every byte sent: %d then %d", first.RxBytes, second.RxBytes, sentFirst, sent)
	}
	if other := second.Discards.Oversize + second.Discards.Resync + second.Discards.EmptyFrame; other != 0 {
		t.Errorf("discards = %+v, want only inter_char_timeout", second.Discards)
	}
}
