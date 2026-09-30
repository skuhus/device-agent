package core

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/skuhus/device-agent/internal/device"
	"github.com/skuhus/device-agent/internal/logging"
	"github.com/skuhus/device-agent/internal/wire"
)

// published is one message the fake transport accepted.
type published struct {
	kind    string // "rx", "event", "keepalive" or "offline"
	topic   string
	payload []byte
	expiry  time.Duration
}

// fakeTransport records what the core sent, and can be made to fail or to
// block, which is how the failure and drain paths are exercised without a
// broker.
type fakeTransport struct {
	mu       sync.Mutex
	messages []published
	// order records the sequence of calls, so shutdown ordering is checkable.
	order  []string
	closed bool
	// closeHadDeadline records whether the disconnect was given a bound. It is
	// the difference between a station that stops and one that hangs when the
	// network it was talking to has gone away.
	closeHadDeadline bool

	rxErr error
	// failRx fails every rx publish to the topics it names, so that one device
	// can fail while another succeeds.
	failRx map[string]bool
	// gate, when non-nil, blocks every rx publish until it is closed, and
	// gated receives a value as each publish starts waiting.
	gate  chan struct{}
	gated chan struct{}
	// eventGate, when non-nil, blocks every event publish until it is closed.
	eventGate chan struct{}
	// closeBlocks makes Close wait for its context, as a disconnect does on a
	// network that has gone away.
	closeBlocks bool
	// offlineHold keeps the offline publish in progress for a while after it
	// is recorded, so that anything still running shows up after it.
	offlineHold time.Duration
}

func (fake *fakeTransport) PublishRx(ctx context.Context, topic string, payload []byte, expiry time.Duration) error {
	if fake.gate != nil {
		if fake.gated != nil {
			fake.gated <- struct{}{}
		}
		select {
		case <-fake.gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.rxErr != nil || fake.failRx[topic] {
		fake.order = append(fake.order, "rx-failed")
		if fake.rxErr != nil {
			return fake.rxErr
		}
		return errors.New("publish to " + topic + " refused with reason 0x87: not authorized")
	}
	fake.messages = append(fake.messages, published{"rx", topic, payload, expiry})
	fake.order = append(fake.order, "rx")
	return nil
}

func (fake *fakeTransport) PublishEvent(ctx context.Context, topic string, payload []byte, expiry time.Duration) error {
	if fake.eventGate != nil {
		select {
		case <-fake.eventGate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.messages = append(fake.messages, published{"event", topic, payload, expiry})
	fake.order = append(fake.order, "event")
	return nil
}

func (fake *fakeTransport) PublishKeepalive(_ context.Context, topic string, payload []byte, expiry time.Duration) error {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.messages = append(fake.messages, published{"keepalive", topic, payload, expiry})
	fake.order = append(fake.order, "keepalive")
	return nil
}

func (fake *fakeTransport) PublishOffline(_ context.Context, topic string, payload []byte) error {
	fake.mu.Lock()
	fake.messages = append(fake.messages, published{"offline", topic, payload, 0})
	fake.order = append(fake.order, "offline")
	fake.mu.Unlock()
	time.Sleep(fake.offlineHold)
	return nil
}

func (fake *fakeTransport) Close(ctx context.Context) error {
	if fake.closeBlocks {
		<-ctx.Done()
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.closed = true
	_, fake.closeHadDeadline = ctx.Deadline()
	fake.order = append(fake.order, "close")
	return nil
}

func (fake *fakeTransport) snapshot() ([]published, []string) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return append([]published(nil), fake.messages...), append([]string(nil), fake.order...)
}

func (fake *fakeTransport) rxOn(topic string) []wire.Rx {
	return decodeAll[wire.Rx](fake.of("rx", topic))
}

func (fake *fakeTransport) eventsOn(topic string) []wire.Event {
	return decodeAll[wire.Event](fake.of("event", topic))
}

func (fake *fakeTransport) keepalives() []wire.Keepalive {
	return decodeAll[wire.Keepalive](fake.of("keepalive", ""))
}

// of returns the messages of one kind, on topic when it is not empty.
func (fake *fakeTransport) of(kind, topic string) []published {
	messages, _ := fake.snapshot()
	var out []published
	for _, message := range messages {
		if message.kind == kind && (topic == "" || message.topic == topic) {
			out = append(out, message)
		}
	}
	return out
}

// calls counts the calls of one kind in the order record.
func (fake *fakeTransport) calls(kind string) int {
	_, order := fake.snapshot()
	count := 0
	for _, call := range order {
		if call == kind {
			count++
		}
	}
	return count
}

func decodeAll[T any](messages []published) []T {
	out := make([]T, 0, len(messages))
	for _, message := range messages {
		var decoded T
		if err := json.Unmarshal(message.payload, &decoded); err != nil {
			panic(err)
		}
		out = append(out, decoded)
	}
	return out
}

// waitUntil polls cond for up to 3s and fails the test with what if it never
// holds.
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// fakeReader emits a fixed set of frames and then waits for cancellation,
// which is what a real scanner between scans looks like. It reports the port
// events it is given before the first frame, and onStop once cancelled, as the
// serial reader reports its port closed.
type fakeReader struct {
	id     string
	frames [][]byte
	events []device.Event
	onStop []device.Event
	// sent is closed once every frame has been handed to the sink.
	sent chan struct{}
}

func newFakeReader(id string, frames ...string) *fakeReader {
	reader := &fakeReader{id: id, sent: make(chan struct{})}
	for _, frame := range frames {
		reader.frames = append(reader.frames, []byte(frame))
	}
	return reader
}

func (reader *fakeReader) ID() string                  { return reader.id }
func (reader *fakeReader) Kind() string                { return "fake" }
func (reader *fakeReader) Path() string                { return "/dev/null" }
func (reader *fakeReader) Direction() device.Direction { return device.Inbound }

func (reader *fakeReader) Run(ctx context.Context, sink chan<- device.Frame, report func(device.Event)) error {
	for _, event := range reader.events {
		report(event)
	}
	for _, raw := range reader.frames {
		select {
		case sink <- device.Frame{DeviceID: reader.id, Raw: raw, At: time.Now()}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	close(reader.sent)
	<-ctx.Done()
	for _, event := range reader.onStop {
		report(event)
	}
	return ctx.Err()
}

var station = mustStation()

func mustStation() wire.StationTopics {
	station, err := wire.NewStationTopics("acme", "vasby", "pack-03")
	if err != nil {
		panic(err)
	}
	return station
}

// coreDevice wraps a reader as the core runs it, with a 30 s expiry.
func coreDevice(t *testing.T, reader device.Device, deviceType string) Device {
	t.Helper()
	topics, err := station.Device(reader.ID())
	if err != nil {
		t.Fatalf("topics for %s: %v", reader.ID(), err)
	}
	return Device{
		Reader: reader,
		Wire:   wire.Device{ID: reader.ID(), Type: deviceType, Expiry: 30 * time.Second},
		Topics: topics,
	}
}

func agentStatus(t *testing.T) string {
	t.Helper()
	agent, err := station.Agent("pack-03")
	if err != nil {
		t.Fatalf("agent topics: %v", err)
	}
	return agent.Status()
}

func testOptions(t *testing.T, transport Transport, devices ...Device) Options {
	t.Helper()
	return Options{
		Devices:        devices,
		Transport:      transport,
		Builder:        wire.NewBuilder(wire.Agent{Project: "acme", Site: "vasby", Station: "pack-03", InstanceID: "pack-03", AgentVersion: "test"}, nil),
		AgentStatus:    agentStatus(t),
		PublishTimeout: time.Second,
		BufferSize:     8,
		DrainTimeout:   2 * time.Second,
		// An hour, so that no keepalive goes out unless a test asks for one.
		KeepaliveInterval: time.Hour,
		MissedKeepalives:  3,
		Station:           "pack-03",
	}
}

// runUntil starts the core, waits for ready, then cancels and waits for Run to
// return, so every assertion sees a completed shutdown.
func runUntil(t *testing.T, running *Core, ready func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- running.Run(ctx) }()

	ready()
	cancel()

	select {
	case err := <-errc:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return within 10s of cancellation")
	}
}

func newCore(t *testing.T, opts Options) *Core {
	t.Helper()
	running, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return running
}

func openDeliveries(t *testing.T) (*logging.Audit, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "agent.log")
	deliveries, err := logging.OpenAudit(path, 1, 1)
	if err != nil {
		t.Fatalf("OpenAudit: %v", err)
	}
	t.Cleanup(func() { deliveries.Close() })
	return deliveries, path
}

func readRecords(t *testing.T, deliveries *logging.Audit, path string) []logging.AuditRecord {
	t.Helper()
	if err := deliveries.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read records: %v", err)
	}
	var records []logging.AuditRecord
	for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
		if line == "" {
			continue
		}
		var record logging.AuditRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("unmarshal %q: %v", line, err)
		}
		records = append(records, record)
	}
	return records
}

// Each frame becomes one rx message on the device's rx topic, published with
// the device's message expiry, built by internal/wire.
func TestPublishesRxOnTheDeviceTopic(t *testing.T) {
	transport := &fakeTransport{}
	reader := newFakeReader("scanner-main", "7310425012345", "A42154587")
	running := newCore(t, testOptions(t, transport, coreDevice(t, reader, "symbol-05e0-1701")))
	runUntil(t, running, func() { <-reader.sent })

	messages, _ := transport.snapshot()
	var topics []string
	for _, message := range messages {
		if message.kind == "rx" {
			topics = append(topics, message.topic)
			if message.expiry != 30*time.Second {
				t.Errorf("expiry = %s, want the device's 30s", message.expiry)
			}
		}
	}
	if len(topics) != 2 || topics[0] != "skuhus/acme/vasby/pack-03/scanner-main/rx" {
		t.Fatalf("rx topics = %v, want two on skuhus/acme/vasby/pack-03/scanner-main/rx", topics)
	}
	rx := transport.rxOn("skuhus/acme/vasby/pack-03/scanner-main/rx")
	for i, want := range []string{"7310425012345", "A42154587"} {
		got := rx[i]
		if got.Kind != wire.KindRx || got.Schema != wire.Schema || got.DeviceID != "scanner-main" || got.Seq != uint64(i+1) {
			t.Errorf("rx %d = kind %q schema %d device %q seq %d", i, got.Kind, got.Schema, got.DeviceID, got.Seq)
		}
		if got.DeviceType == nil || *got.DeviceType != "symbol-05e0-1701" {
			t.Errorf("rx %d device_type = %v, want symbol-05e0-1701", i, got.DeviceType)
		}
		raw, _ := base64.StdEncoding.DecodeString(got.RawB64)
		if string(raw) != want || got.Text == nil || *got.Text != want {
			t.Errorf("rx %d carries %q (text %v), want %q", i, raw, got.Text, want)
		}
	}
}

// Two devices on one agent publish to their own topics, each numbering its
// frames from 1.
func TestTwoDevicesPublishToTheirOwnTopics(t *testing.T) {
	transport := &fakeTransport{}
	scanner := newFakeReader("scanner-main", "A1", "A2", "A3")
	scale := newFakeReader("scale-1", "1.250 kg", "0.500 kg")
	running := newCore(t, testOptions(t, transport, coreDevice(t, scanner, ""), coreDevice(t, scale, "")))
	runUntil(t, running, func() { <-scanner.sent; <-scale.sent })

	for topic, want := range map[string][]string{
		"skuhus/acme/vasby/pack-03/scanner-main/rx": {"A1", "A2", "A3"},
		"skuhus/acme/vasby/pack-03/scale-1/rx":      {"1.250 kg", "0.500 kg"},
	} {
		rx := transport.rxOn(topic)
		if len(rx) != len(want) {
			t.Errorf("%s got %d messages, want %d", topic, len(rx), len(want))
			continue
		}
		for i, text := range want {
			if rx[i].Text == nil || *rx[i].Text != text || rx[i].Seq != uint64(i+1) {
				t.Errorf("%s message %d = %v seq %d, want %q seq %d", topic, i, rx[i].Text, rx[i].Seq, text, i+1)
			}
		}
	}
}

// A reading the broker never took exists nowhere else. It is recorded as
// failed, with the error and the whole payload, so that it can be recovered.
func TestFailedPublishIsRecordedWithItsPayload(t *testing.T) {
	deliveries, path := openDeliveries(t)
	transport := &fakeTransport{rxErr: errors.New("publish to x: broker connection is down")}
	reader := newFakeReader("scanner-main", "A42154587")
	opts := testOptions(t, transport, coreDevice(t, reader, ""))
	opts.Deliveries = deliveries
	runUntil(t, newCore(t, opts), func() { <-reader.sent })

	records := readRecords(t, deliveries, path)
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1: %+v", len(records), records)
	}
	record := records[0]
	if record.Outcome != logging.OutcomeFailed || !strings.Contains(record.Detail, "broker connection is down") {
		t.Errorf("outcome %q detail %q, want failed with the publish error", record.Outcome, record.Detail)
	}
	raw, err := base64.StdEncoding.DecodeString(record.RawB64)
	if err != nil || string(raw) != "A42154587" || record.Text == nil || *record.Text != "A42154587" {
		t.Errorf("recovered %q (%v), text %v, want A42154587", raw, err, record.Text)
	}
	if record.Bytes != len("A42154587") || record.Seq != 1 || record.DeviceID != "scanner-main" || record.Station != "pack-03" {
		t.Errorf("record = %+v", record)
	}
}

// A delivered reading is upstream, and a copy here would make the file a replay
// source. Its record names the id the rx message carried, so the two can be
// matched.
func TestPublishedRxIsRecordedWithoutItsPayload(t *testing.T) {
	deliveries, path := openDeliveries(t)
	transport := &fakeTransport{}
	reader := newFakeReader("scanner-main", "A42154587")
	opts := testOptions(t, transport, coreDevice(t, reader, ""))
	opts.Deliveries = deliveries
	runUntil(t, newCore(t, opts), func() { <-reader.sent })

	records := readRecords(t, deliveries, path)
	rx := transport.rxOn("skuhus/acme/vasby/pack-03/scanner-main/rx")
	if len(records) != 1 || len(rx) != 1 {
		t.Fatalf("records %d, rx %d, want 1 each", len(records), len(rx))
	}
	record := records[0]
	if record.Outcome != logging.OutcomePublished || record.RawB64 != "" || record.Text != nil {
		t.Errorf("record = %+v, want published without the payload", record)
	}
	if record.EventID != rx[0].ID || record.EventID == "" {
		t.Errorf("record id %q, rx id %q, want the same", record.EventID, rx[0].ID)
	}
}

// Shutdown order: the devices stop, the publishers drain what was framed, the
// offline message goes out on the agent's status topic, and only then does the
// connection close. Closing earlier would make the broker publish the will,
// reporting a crash where there was an orderly stop.
func TestShutdownDrainsThenPublishesOfflineThenCloses(t *testing.T) {
	transport := &fakeTransport{}
	reader := newFakeReader("scanner-main", "one", "two")
	runUntil(t, newCore(t, testOptions(t, transport, coreDevice(t, reader, ""))), func() { <-reader.sent })

	messages, order := transport.snapshot()
	if strings.Join(order, " ") != "rx rx offline close" {
		t.Fatalf("call order = %v, want rx rx offline close", order)
	}
	offline := messages[len(messages)-1]
	var body map[string]any
	if err := json.Unmarshal(offline.payload, &body); err != nil {
		t.Fatalf("offline payload: %v", err)
	}
	if offline.topic != "skuhus/acme/vasby/pack-03/agent/pack-03/status" || body["kind"] != "offline" || body["reason"] != "shutdown" {
		t.Errorf("offline = %s on %s, want kind offline, reason shutdown, on the agent's status topic", offline.payload, offline.topic)
	}
	// Disconnecting writes to the network. With the network gone, an unbounded
	// wait here is the difference between stopping and hanging until something
	// sends SIGKILL.
	if !transport.closeHadDeadline {
		t.Error("the disconnect was given an unbounded context")
	}
}

// A disconnect that never completes does not keep the agent from stopping: Run
// returns once the publish timeout has passed.
func TestDisconnectIsBounded(t *testing.T) {
	transport := &fakeTransport{closeBlocks: true}
	reader := newFakeReader("scanner-main")
	opts := testOptions(t, transport, coreDevice(t, reader, ""))
	opts.PublishTimeout = 200 * time.Millisecond
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
			t.Errorf("Run took %s to return, want about the 200ms publish timeout", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return while the disconnect hung")
	}
}

// A broker that has stopped acknowledging must not hold the process open for
// buffer_size times publish_timeout. Past the drain deadline the remaining
// readings are dropped, and recorded as dropped, with their payload.
func TestDrainDeadlineDropsTheRest(t *testing.T) {
	deliveries, path := openDeliveries(t)
	gate := make(chan struct{})
	transport := &fakeTransport{gate: gate}
	reader := newFakeReader("scanner-main", "one", "two", "three")
	opts := testOptions(t, transport, coreDevice(t, reader, ""))
	opts.Deliveries = deliveries
	opts.DrainTimeout = 10 * time.Millisecond
	opts.PublishTimeout = 50 * time.Millisecond
	running := newCore(t, opts)

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- running.Run(ctx) }()
	<-reader.sent
	cancel()
	// Let the drain deadline pass while the publisher is still blocked, then
	// release it: the frame in flight completes or times out, the rest are
	// past the deadline.
	time.Sleep(60 * time.Millisecond)
	close(gate)
	select {
	case <-errc:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return")
	}

	var dropped int
	for _, record := range readRecords(t, deliveries, path) {
		if record.Outcome == logging.OutcomeDropped {
			dropped++
			if !strings.Contains(record.Detail, "drain deadline") || record.RawB64 == "" {
				t.Errorf("dropped record = %+v, want the drain deadline named and the payload kept", record)
			}
		}
	}
	if dropped == 0 {
		t.Error("no reading recorded as dropped")
	}
}

// Every port event a reader reports reaches the core, which logs it.
func TestPortEventsReachTheCore(t *testing.T) {
	var logged bytes.Buffer
	transport := &fakeTransport{}
	reader := newFakeReader("scale-1")
	reader.events = []device.Event{{DeviceID: "scale-1", Kind: device.PortLost, ErrorClass: "disconnected", Err: syscall.EIO}}
	opts := testOptions(t, transport, coreDevice(t, reader, ""))
	opts.Logger = slog.New(slog.NewJSONHandler(&syncWriter{buffer: &logged}, &slog.HandlerOptions{Level: slog.LevelDebug}))
	runUntil(t, newCore(t, opts), func() { <-reader.sent })

	for _, want := range []string{`"msg":"port event"`, `"device_id":"scale-1"`, `"event":"lost"`, `"error_class":"disconnected"`} {
		if !strings.Contains(logged.String(), want) {
			t.Errorf("log does not contain %s:\n%s", want, logged.String())
		}
	}
}

func TestNewRejectsUnusableOptions(t *testing.T) {
	transport := &fakeTransport{}
	reader := newFakeReader("scanner-main")
	tests := []struct {
		name   string
		mutate func(*Options)
		want   string
	}{
		{"no transport", func(o *Options) { o.Transport = nil }, "transport is required"},
		{"no builder", func(o *Options) { o.Builder = nil }, "message builder is required"},
		{"no devices", func(o *Options) { o.Devices = nil }, "at least one device"},
		{"no agent status topic", func(o *Options) { o.AgentStatus = "" }, "status topic is required"},
		{"zero publish timeout", func(o *Options) { o.PublishTimeout = 0 }, "publish timeout must be positive"},
		{"zero buffer", func(o *Options) { o.BufferSize = 0 }, "buffer size must be positive"},
		{"zero keepalive interval", func(o *Options) { o.KeepaliveInterval = 0 }, "keepalive interval must be positive"},
		{"no missed keepalives", func(o *Options) { o.MissedKeepalives = 0 }, "missed keepalives must be at least 1"},
		{"device without a reader", func(o *Options) { o.Devices[0].Reader = nil }, "has no reader"},
		{"device without topics", func(o *Options) { o.Devices[0].Topics = wire.DeviceTopics{} }, "has no topics"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := testOptions(t, transport, coreDevice(t, reader, ""))
			tc.mutate(&opts)
			if _, err := New(opts); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

// syncWriter serialises writes to a buffer shared between the core's
// goroutines and the test.
type syncWriter struct {
	mu     sync.Mutex
	buffer *bytes.Buffer
}

func (writer *syncWriter) Write(p []byte) (int, error) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.buffer.Write(p)
}
