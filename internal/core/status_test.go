package core

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/skuhus/device-agent/internal/device"
	"github.com/skuhus/device-agent/internal/logging/logtest"
	"github.com/skuhus/device-agent/internal/wire"
)

// Every port event a reader reports, except a read, is published on the
// device's status topic in the order it happened, with the time it happened,
// whether the agent holds the port open after it, and what remains of the
// device's expiry: the clock reads five seconds after the first event. A read
// is only counted: queued, it would take a slot a real event needs.
func TestPortEventsArePublishedOnTheDeviceStatusTopic(t *testing.T) {
	log, logged := logtest.New(t, "debug")
	transport := &fakeTransport{}
	reader := newFakeReader("scale-1")
	at := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	openErr := &os.PathError{Op: "open", Path: "/dev/null", Err: syscall.ENOENT}
	readErr := fmt.Errorf("read /dev/null: %w", syscall.EIO)
	reader.events = []device.Event{
		{DeviceID: "scale-1", Kind: device.PortOpenFailed, At: at, ErrorClass: "absent", Err: openErr},
		{DeviceID: "scale-1", Kind: device.PortOpened, At: at.Add(1 * time.Second)},
		{DeviceID: "scale-1", Kind: device.BytesRead, At: at.Add(2 * time.Second), Bytes: 9},
		{DeviceID: "scale-1", Kind: device.BytesDiscarded, At: at.Add(3 * time.Second), Reason: "inter_char_timeout", Bytes: 9},
		{DeviceID: "scale-1", Kind: device.PortLost, At: at.Add(4 * time.Second), ErrorClass: "disconnected", Err: readErr},
	}
	opts := testOptions(t, transport, coreDevice(t, reader, "mettler-ics"))
	opts.Logger = log
	now := at.Add(5 * time.Second)
	opts.Now = func() time.Time { return now }
	runUntil(t, newCore(t, opts), func() { <-reader.sent })
	if strings.Contains(logged.String(), `"level":"ERROR"`) {
		t.Errorf("an error was logged for ordinary port events:\n%s", logged.String())
	}

	topic := "skuhus/acme/vasby/pack-03/scale-1/status"
	want := []struct {
		code   wire.EventCode
		open   bool
		detail map[string]any
		at     time.Time
	}{
		{wire.EventPortOpenFailed, false, map[string]any{"path": "/dev/null", "error_class": "absent", "error": "open /dev/null: no such file or directory"}, at},
		{wire.EventPortOpened, true, map[string]any{"path": "/dev/null"}, at.Add(1 * time.Second)},
		{wire.EventBytesDiscarded, true, map[string]any{"reason": "inter_char_timeout", "bytes": float64(9)}, at.Add(3 * time.Second)},
		{wire.EventPortLost, false, map[string]any{"path": "/dev/null", "error_class": "disconnected", "error": "read /dev/null: input/output error"}, at.Add(4 * time.Second)},
	}
	events := transport.eventsOn(topic)
	if len(events) != len(want) {
		t.Fatalf("%d events on %s, want %d: a read is counted, not published", len(events), topic, len(want))
	}
	for i, w := range want {
		got := events[i]
		if got.Kind != wire.KindEvent || got.Schema != wire.Schema || got.DeviceID != "scale-1" ||
			got.DeviceType == nil || *got.DeviceType != "mettler-ics" || got.MessageExpiryS != 30 {
			t.Errorf("event %d header = kind %q schema %d device %q type %v expiry %d", i, got.Kind, got.Schema, got.DeviceID, got.DeviceType, got.MessageExpiryS)
		}
		if got.Code != w.code || got.DeviceOpen != w.open || !reflect.DeepEqual(got.Detail, w.detail) {
			t.Errorf("event %d = %s open %v detail %v, want %s open %v detail %v", i, got.Code, got.DeviceOpen, got.Detail, w.code, w.open, w.detail)
		}
		if got.AgentTS != w.at.Format(wire.TimeFormat) {
			t.Errorf("event %d agent_ts = %s, want when it happened, %s", i, got.AgentTS, w.at.Format(wire.TimeFormat))
		}
	}
	for i, message := range transport.of("event", "") {
		wantExpiry := 30*time.Second - now.Sub(want[i].at)
		if message.topic != topic || message.expiry != wantExpiry {
			t.Errorf("event %d published on %s with expiry %s, want %s with %s, the device's 30s less its age", i, message.topic, message.expiry, topic, wantExpiry)
		}
	}
}

// The keepalive reports every device in configuration order, with whether its
// port is open and what it did since the process started: frames, every byte
// read, discards by reason, failed opens by class, and readings the broker did
// not take.
func TestKeepaliveCountsWhatEachDeviceDid(t *testing.T) {
	transport := &fakeTransport{failRx: map[string]bool{"skuhus/acme/vasby/pack-03/scale-1/rx": true}}
	scanner := newFakeReader("scanner-main", "A1", "A2", "A3")
	scanner.events = []device.Event{
		{DeviceID: "scanner-main", Kind: device.PortOpened},
		{DeviceID: "scanner-main", Kind: device.BytesRead, Bytes: 12},
		{DeviceID: "scanner-main", Kind: device.BytesDiscarded, Reason: "inter_char_timeout", Bytes: 5},
		{DeviceID: "scanner-main", Kind: device.BytesRead, Bytes: 30},
		{DeviceID: "scanner-main", Kind: device.BytesDiscarded, Reason: "oversize", Bytes: 20},
		{DeviceID: "scanner-main", Kind: device.PortLost, ErrorClass: "disconnected", Err: syscall.EIO},
	}
	scale := newFakeReader("scale-1", "1.250 kg", "0.500 kg")
	scale.events = []device.Event{
		{DeviceID: "scale-1", Kind: device.PortOpenFailed, ErrorClass: "busy", Err: syscall.EBUSY},
		{DeviceID: "scale-1", Kind: device.PortOpenFailed, ErrorClass: "busy", Err: syscall.EBUSY},
		{DeviceID: "scale-1", Kind: device.PortOpenFailed, ErrorClass: "absent", Err: syscall.ENOENT},
		{DeviceID: "scale-1", Kind: device.PortOpened},
	}
	connected := make(chan struct{}, 1)
	opts := testOptions(t, transport, coreDevice(t, scanner, "symbol-05e0-1701"), coreDevice(t, scale, ""))
	opts.Connected = connected
	opts.Started = time.Now().Add(-90 * time.Second)
	runUntil(t, newCore(t, opts), func() {
		<-scanner.sent
		<-scale.sent
		waitUntil(t, "every reading's publish", func() bool {
			return transport.calls("rx") == 3 && transport.calls("rx-failed") == 2
		})
		connected <- struct{}{}
		waitUntil(t, "a keepalive", func() bool { return transport.calls("keepalive") == 1 })
	})

	message := transport.of("keepalive", "")[0]
	if message.topic != "skuhus/acme/vasby/pack-03/agent/pack-03/status" || message.expiry != 3*time.Hour {
		t.Errorf("keepalive on %s with expiry %s, want the agent's status topic and gone_after, 3h", message.topic, message.expiry)
	}
	keepalive := transport.keepalives()[0]
	if keepalive.Kind != wire.KindKeepalive || keepalive.IntervalS != 3600 || keepalive.GoneAfterS != 10800 {
		t.Errorf("keepalive kind %q interval_s %d gone_after_s %d, want keepalive 3600 10800", keepalive.Kind, keepalive.IntervalS, keepalive.GoneAfterS)
	}
	if keepalive.UptimeS < 90 || keepalive.UptimeS > 100 {
		t.Errorf("uptime_s = %d, want about 90, counted from Started", keepalive.UptimeS)
	}
	if len(keepalive.Devices) != 2 || keepalive.Devices[0].DeviceID != "scanner-main" || keepalive.Devices[1].DeviceID != "scale-1" {
		t.Fatalf("devices = %+v, want scanner-main then scale-1", keepalive.Devices)
	}
	want := map[string]wire.DeviceCounters{
		"scanner-main": {RxFrames: 3, RxBytes: 42, Discards: wire.DiscardCounts{InterCharTimeout: 1, Oversize: 1}},
		"scale-1":      {RxFrames: 2, FailedOpens: wire.OpenFailureCounts{Busy: 2, Absent: 1}, PublishFailures: 2},
	}
	// The scanner's port was lost after it opened; the scale's opened after
	// failing. Each is reported as it stands after its last event.
	wantOpen := map[string]bool{"scanner-main": false, "scale-1": true}
	for _, entry := range keepalive.Devices {
		if entry.DeviceCounters != want[entry.DeviceID] {
			t.Errorf("%s counters = %+v, want %+v", entry.DeviceID, entry.DeviceCounters, want[entry.DeviceID])
		}
		if entry.DeviceOpen != wantOpen[entry.DeviceID] || entry.MessageExpiryS != 30 {
			t.Errorf("%s device_open %v message_expiry_s %d, want %v, 30", entry.DeviceID, entry.DeviceOpen, entry.MessageExpiryS, wantOpen[entry.DeviceID])
		}
	}
	if keepalive.Devices[1].DeviceType != nil {
		t.Errorf("scale-1 device_type = %q, want null when none is configured", *keepalive.Devices[1].DeviceType)
	}
}

// buffer_depth is the frames waiting now. With the broker holding the first
// reading, the other two wait in the device's channel.
func TestKeepaliveReportsFramesWaitingAsBufferDepth(t *testing.T) {
	gate := make(chan struct{})
	transport := &fakeTransport{gate: gate, gated: make(chan struct{}, 8)}
	reader := newFakeReader("scanner-main", "one", "two", "three")
	connected := make(chan struct{}, 1)
	opts := testOptions(t, transport, coreDevice(t, reader, ""))
	opts.Connected = connected
	runUntil(t, newCore(t, opts), func() {
		<-reader.sent
		<-transport.gated
		connected <- struct{}{}
		waitUntil(t, "a keepalive", func() bool { return transport.calls("keepalive") == 1 })
		close(gate)
	})
	entry := transport.keepalives()[0].Devices[0]
	if entry.RxFrames != 1 || entry.BufferDepth != 2 {
		t.Errorf("rx_frames %d buffer_depth %d, want 1 taken and 2 waiting", entry.RxFrames, entry.BufferDepth)
	}
}

// The keepalive goes out at the interval without being asked, each with an id
// of its own, and none follows the offline message.
func TestKeepaliveGoesOutEveryInterval(t *testing.T) {
	transport := &fakeTransport{}
	reader := newFakeReader("scanner-main")
	opts := testOptions(t, transport, coreDevice(t, reader, ""))
	opts.KeepaliveInterval = 20 * time.Millisecond
	started := time.Now()
	var elapsed time.Duration
	runUntil(t, newCore(t, opts), func() {
		<-reader.sent
		waitUntil(t, "three keepalives", func() bool { return transport.calls("keepalive") >= 3 })
		elapsed = time.Since(started)
	})
	// The third tick is three intervals in; sooner means they are not paced.
	if elapsed < 55*time.Millisecond {
		t.Errorf("three keepalives within %s, want no sooner than three 20ms intervals", elapsed)
	}
	ids := map[string]bool{}
	for _, keepalive := range transport.keepalives() {
		if ids[keepalive.ID] {
			t.Errorf("keepalive id %s used twice", keepalive.ID)
		}
		ids[keepalive.ID] = true
	}
	_, order := transport.snapshot()
	offline := slices.Index(order, "offline")
	if offline < 0 {
		t.Fatalf("no offline message: %v", order)
	}
	if slices.Contains(order[offline:], "keepalive") {
		t.Errorf("a keepalive followed the offline message: %v", order)
	}
}

// Each time the connection comes up a keepalive goes out at once, so a
// consumer that saw the will learns the agent is back within a moment.
func TestKeepaliveAtOnceOnEveryConnection(t *testing.T) {
	transport := &fakeTransport{}
	reader := newFakeReader("scanner-main")
	connected := make(chan struct{}, 1)
	opts := testOptions(t, transport, coreDevice(t, reader, ""))
	opts.Connected = connected
	runUntil(t, newCore(t, opts), func() {
		<-reader.sent
		for i := 1; i <= 2; i++ {
			connected <- struct{}{}
			waitUntil(t, fmt.Sprintf("keepalive %d", i), func() bool { return transport.calls("keepalive") == i })
		}
	})
	if got := transport.calls("keepalive"); got != 2 {
		t.Errorf("%d keepalives, want one per connection, 2, with the interval an hour away", got)
	}
}

// At shutdown the keepalive stops first, the readers report their ports
// closed, those events go out, and only then the offline message: a consumer
// sees the port closed on purpose before it sees the agent go, and no
// keepalive after it says the agent is still there. The offline publish takes
// ten keepalive intervals, so a keepalive still running would show up after it.
func TestShutdownPublishesPortClosedBeforeOffline(t *testing.T) {
	transport := &fakeTransport{offlineHold: 50 * time.Millisecond}
	reader := newFakeReader("scanner-main", "one", "two")
	reader.onStop = []device.Event{{DeviceID: "scanner-main", Kind: device.PortClosed, At: time.Now()}}
	opts := testOptions(t, transport, coreDevice(t, reader, ""))
	opts.KeepaliveInterval = 5 * time.Millisecond
	runUntil(t, newCore(t, opts), func() {
		<-reader.sent
		waitUntil(t, "both readings and two keepalives", func() bool {
			return transport.calls("rx") == 2 && transport.calls("keepalive") >= 2
		})
	})
	_, order := transport.snapshot()
	if tail := strings.Join(order[len(order)-3:], " "); tail != "event offline close" {
		t.Errorf("call order ends %q, want event offline close: %v", tail, order)
	}
	events := transport.eventsOn("skuhus/acme/vasby/pack-03/scanner-main/status")
	if len(events) != 1 || events[0].Code != wire.EventPortClosed || events[0].DeviceOpen {
		t.Errorf("events = %+v, want one port_closed with device_open false", events)
	}
}

// A reader hands its events over inline, so a full event queue must not block
// it: the reader goes on reading, the queue drops its oldest event, which is
// logged, and the keepalive still counts every one. The broker holds the first
// event the publisher took; of the rest, the two most recent are published.
func TestFullEventQueueDoesNotBlockTheReader(t *testing.T) {
	log, logged := logtest.New(t, "debug")
	eventGate := make(chan struct{})
	transport := &fakeTransport{eventGate: eventGate}
	reader := newFakeReader("scanner-main", "A1")
	for i := range 20 {
		reader.events = append(reader.events, device.Event{DeviceID: "scanner-main", Kind: device.BytesDiscarded, Reason: "inter_char_timeout", Bytes: i + 1})
	}
	connected := make(chan struct{}, 1)
	opts := testOptions(t, transport, coreDevice(t, reader, ""))
	opts.EventBufferSize = 2
	opts.Connected = connected
	opts.Logger = log
	runUntil(t, newCore(t, opts), func() {
		select {
		case <-reader.sent:
		case <-time.After(3 * time.Second):
			t.Fatal("the reader blocked on a full event queue")
		}
		connected <- struct{}{}
		waitUntil(t, "a keepalive", func() bool { return transport.calls("keepalive") == 1 })
		close(eventGate)
	})
	if got := transport.keepalives()[0].Devices[0].Discards.InterCharTimeout; got != 20 {
		t.Errorf("keepalive counts %d discards, want all 20", got)
	}
	dropped := strings.Count(logged.String(), `"msg":"device event dropped: queue full, the most recent are kept"`)
	events := transport.eventsOn("skuhus/acme/vasby/pack-03/scanner-main/status")
	if dropped == 0 || len(events)+dropped != 20 || len(events) > 3 {
		t.Fatalf("%d published and %d logged as dropped, want at most 3 published, 20 in all", len(events), dropped)
	}
	last := events[len(events)-2:]
	if last[0].Detail["bytes"] != float64(19) || last[1].Detail["bytes"] != float64(20) {
		t.Errorf("the last two published are %v and %v, want the two most recent, 19 and 20", last[0].Detail, last[1].Detail)
	}
}

// A class the wire has no counter for is a bug in a reader. It is counted as
// unknown, published as unknown, and logged at ERROR with the class it had.
func TestUnknownErrorClassIsCountedAsUnknownAndLogged(t *testing.T) {
	log, logged := logtest.New(t, "debug")
	transport := &fakeTransport{}
	reader := newFakeReader("scanner-main")
	reader.events = []device.Event{{DeviceID: "scanner-main", Kind: device.PortOpenFailed, ErrorClass: "gremlins", Err: errors.New("open /dev/null: gremlins")}}
	connected := make(chan struct{}, 1)
	opts := testOptions(t, transport, coreDevice(t, reader, ""))
	opts.Connected = connected
	opts.Logger = log
	runUntil(t, newCore(t, opts), func() {
		<-reader.sent
		connected <- struct{}{}
		waitUntil(t, "a keepalive", func() bool { return transport.calls("keepalive") == 1 })
	})
	if got := transport.keepalives()[0].Devices[0].FailedOpens; got != (wire.OpenFailureCounts{Unknown: 1}) {
		t.Errorf("failed_opens = %+v, want one unknown", got)
	}
	events := transport.eventsOn("skuhus/acme/vasby/pack-03/scanner-main/status")
	if len(events) != 1 || events[0].Detail["error_class"] != "unknown" {
		t.Errorf("events = %+v, want one with error_class unknown", events)
	}
	for _, want := range []string{`"level":"ERROR"`, "error class the keepalive has no counter for", `"error_class":"gremlins"`} {
		if !strings.Contains(logged.String(), want) {
			t.Errorf("log does not contain %s:\n%s", want, logged.String())
		}
	}
}

// A discard reason the wire has no counter for is a bug in a reader too. It is
// logged at ERROR and not counted, and the event is published with the reason
// it came with, since discards has no unknown counter.
func TestUnknownDiscardReasonIsLoggedAndNotCounted(t *testing.T) {
	log, logged := logtest.New(t, "debug")
	transport := &fakeTransport{}
	reader := newFakeReader("scanner-main")
	reader.events = []device.Event{{DeviceID: "scanner-main", Kind: device.BytesDiscarded, Reason: "gremlins", Bytes: 3}}
	connected := make(chan struct{}, 1)
	opts := testOptions(t, transport, coreDevice(t, reader, ""))
	opts.Connected = connected
	opts.Logger = log
	runUntil(t, newCore(t, opts), func() {
		<-reader.sent
		connected <- struct{}{}
		waitUntil(t, "a keepalive", func() bool { return transport.calls("keepalive") == 1 })
	})
	if got := transport.keepalives()[0].Devices[0].Discards; got != (wire.DiscardCounts{}) {
		t.Errorf("discards = %+v, want nothing counted", got)
	}
	events := transport.eventsOn("skuhus/acme/vasby/pack-03/scanner-main/status")
	if len(events) != 1 || events[0].Detail["reason"] != "gremlins" {
		t.Errorf("events = %+v, want one with the reason it came with", events)
	}
	requireRecord(t, logged.WithMessage(t, "port event carries a discard reason the keepalive has no counter for; not counted"),
		"level", "ERROR", "reason", "gremlins")
}

// While the broker connection is down, events wait. Once it is up they are
// published in order, each with the time it happened.
func TestEventsWaitForTheConnection(t *testing.T) {
	down := make(chan struct{})
	transport := &fakeTransport{down: down}
	reader := newFakeReader("scanner-main")
	reader.events = []device.Event{
		{DeviceID: "scanner-main", Kind: device.PortOpenFailed, ErrorClass: "absent", Err: syscall.ENOENT},
		{DeviceID: "scanner-main", Kind: device.PortOpened},
	}
	topic := "skuhus/acme/vasby/pack-03/scanner-main/status"
	runUntil(t, newCore(t, testOptions(t, transport, coreDevice(t, reader, ""))), func() {
		<-reader.sent
		time.Sleep(50 * time.Millisecond)
		if got := transport.calls("event"); got != 0 {
			t.Errorf("%d events published while the connection was down, want none", got)
		}
		close(down)
		waitUntil(t, "both events", func() bool { return transport.calls("event") == 2 })
	})
	events := transport.eventsOn(topic)
	if len(events) != 2 || events[0].Code != wire.EventPortOpenFailed || events[1].Code != wire.EventPortOpened {
		t.Fatalf("events = %+v, want port_open_failed then port_opened", events)
	}
	if !(events[0].AgentTS <= events[1].AgentTS) {
		t.Errorf("agent_ts %s then %s, want when each happened, in order", events[0].AgentTS, events[1].AgentTS)
	}
}

// A long outage does not end in a flood: each device keeps only its most
// recent events, and the ones dropped to make room are logged.
func TestOnlyTheMostRecentEventsWaitForTheConnection(t *testing.T) {
	log, logged := logtest.New(t, "debug")
	down := make(chan struct{})
	transport := &fakeTransport{down: down}
	reader := newFakeReader("scanner-main")
	for i := range 10 {
		reader.events = append(reader.events, device.Event{DeviceID: "scanner-main", Kind: device.BytesDiscarded, Reason: "inter_char_timeout", Bytes: i + 1})
	}
	opts := testOptions(t, transport, coreDevice(t, reader, ""))
	opts.EventBufferSize = 3
	opts.Logger = log
	runUntil(t, newCore(t, opts), func() {
		<-reader.sent
		close(down)
		waitUntil(t, "the kept events", func() bool { return transport.calls("event") == 3 })
	})
	var published []any
	for _, event := range transport.eventsOn("skuhus/acme/vasby/pack-03/scanner-main/status") {
		published = append(published, event.Detail["bytes"])
	}
	if !reflect.DeepEqual(published, []any{float64(8), float64(9), float64(10)}) {
		t.Errorf("published the events of %v bytes, want the three most recent: 8, 9, 10", published)
	}
	if dropped := strings.Count(logged.String(), `"msg":"device event dropped: queue full, the most recent are kept"`); dropped != 7 {
		t.Errorf("%d drops logged, want 7", dropped)
	}
}

// An event that waited longer than the device's message expiry says nothing
// true any more, and is dropped. One within it goes out with what remains.
func TestEventOlderThanItsExpiryIsDropped(t *testing.T) {
	log, logged := logtest.New(t, "debug")
	down := make(chan struct{})
	transport := &fakeTransport{down: down}
	now := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	reader := newFakeReader("scanner-main")
	reader.events = []device.Event{
		{DeviceID: "scanner-main", Kind: device.PortLost, At: now.Add(-40 * time.Second), ErrorClass: "disconnected", Err: syscall.EIO},
		{DeviceID: "scanner-main", Kind: device.PortOpened, At: now.Add(-10 * time.Second)},
	}
	opts := testOptions(t, transport, coreDevice(t, reader, ""))
	opts.Now = func() time.Time { return now }
	opts.Logger = log
	runUntil(t, newCore(t, opts), func() {
		<-reader.sent
		close(down)
		waitUntil(t, "the event within its expiry", func() bool { return transport.calls("event") == 1 })
	})
	messages := transport.of("event", "")
	events := transport.eventsOn("skuhus/acme/vasby/pack-03/scanner-main/status")
	if len(events) != 1 || events[0].Code != wire.EventPortOpened || messages[0].expiry != 20*time.Second {
		t.Errorf("published %+v with expiry %s, want only port_opened, with 20s of its 30s left", events, messages[0].expiry)
	}
	for _, want := range []string{`"reason":"older than its message expiry"`, `"code":"port_lost"`} {
		if !strings.Contains(logged.String(), want) {
			t.Errorf("log does not contain %s:\n%s", want, logged.String())
		}
	}
}

// A broker that stays down does not hold the process open: at shutdown the
// events still waiting are dropped when the drain runs out, and logged.
func TestShutdownDoesNotWaitForAConnectionThatNeverComes(t *testing.T) {
	log, logged := logtest.New(t, "debug")
	transport := &fakeTransport{down: make(chan struct{})}
	reader := newFakeReader("scanner-main")
	reader.events = []device.Event{{DeviceID: "scanner-main", Kind: device.PortOpened}}
	opts := testOptions(t, transport, coreDevice(t, reader, ""))
	opts.DrainTimeout = 200 * time.Millisecond
	opts.Logger = log
	running := newCore(t, opts)

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- running.Run(ctx) }()
	<-reader.sent
	started := time.Now()
	cancel()
	select {
	case <-errc:
		if elapsed := time.Since(started); elapsed > 2*time.Second {
			t.Errorf("Run took %s to return, want about the 200ms drain", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return while the connection stayed down")
	}
	if got := transport.calls("event"); got != 0 {
		t.Errorf("%d events published, want none with the connection down", got)
	}
	if !strings.Contains(logged.String(), "the broker connection was still down when the shutdown drain ended") {
		t.Errorf("the dropped events were not logged with why:\n%s", logged.String())
	}
}
