package agent

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/skuhus/device-agent/internal/device"
	"github.com/skuhus/device-agent/internal/event"
	"github.com/skuhus/device-agent/internal/logging"
)

// fakePublisher records what the supervisor sent, and can be made to fail or to
// block, which is how the failure and drain paths are exercised without a
// broker.
type fakePublisher struct {
	mu         sync.Mutex
	scans      [][]byte
	scanTTLs   []time.Duration
	statuses   []event.Status
	heartbeats []event.Heartbeat
	closed     bool
	// closeHadDeadline records whether the disconnect was given a bound. It is
	// the difference between a station that stops and one that hangs when the
	// network it was talking to has gone away.
	closeHadDeadline bool
	// order records the sequence of calls, so shutdown ordering is checkable.
	order []string

	scanErr error
	// gate, when non-nil, blocks every scan publish until it is closed.
	gate chan struct{}
}

func (fake *fakePublisher) PublishScan(ctx context.Context, payload []byte, ttl time.Duration) error {
	if fake.gate != nil {
		select {
		case <-fake.gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.scanErr != nil {
		fake.order = append(fake.order, "scan-failed")
		return fake.scanErr
	}
	fake.scans = append(fake.scans, payload)
	fake.scanTTLs = append(fake.scanTTLs, ttl)
	fake.order = append(fake.order, "scan")
	return nil
}

func (fake *fakePublisher) PublishStatus(_ context.Context, payload []byte) error {
	var s event.Status
	if err := json.Unmarshal(payload, &s); err != nil {
		return err
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.statuses = append(fake.statuses, s)
	fake.order = append(fake.order, "status-"+s.State+"-"+s.Reason)
	return nil
}

func (fake *fakePublisher) PublishHeartbeat(_ context.Context, payload []byte) error {
	var h event.Heartbeat
	if err := json.Unmarshal(payload, &h); err != nil {
		return err
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.heartbeats = append(fake.heartbeats, h)
	return nil
}

func (fake *fakePublisher) Close(ctx context.Context) error {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.closed = true
	_, fake.closeHadDeadline = ctx.Deadline()
	fake.order = append(fake.order, "close")
	return nil
}

func (fake *fakePublisher) snapshot() ([][]byte, []event.Status, []event.Heartbeat, []string) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return append([][]byte(nil), fake.scans...), append([]event.Status(nil), fake.statuses...),
		append([]event.Heartbeat(nil), fake.heartbeats...), append([]string(nil), fake.order...)
}

// fakeDevice emits a fixed set of frames and then waits for cancellation, which
// is what a real scanner between scans looks like.
type fakeDevice struct {
	id     string
	frames [][]byte
	// sent is closed once every frame has been handed to the sink.
	sent chan struct{}
}

func newFakeDevice(id string, frames ...string) *fakeDevice {
	fakeDev := &fakeDevice{id: id, sent: make(chan struct{})}
	for _, fake := range frames {
		fakeDev.frames = append(fakeDev.frames, []byte(fake))
	}
	return fakeDev
}

func (fakeDev *fakeDevice) ID() string                  { return fakeDev.id }
func (fakeDev *fakeDevice) Kind() string                { return "fake" }
func (fakeDev *fakeDevice) Path() string                { return "/dev/null" }
func (fakeDev *fakeDevice) Direction() device.Direction { return device.Inbound }

func (fakeDev *fakeDevice) Run(ctx context.Context, sink chan<- device.Frame) error {
	for _, raw := range fakeDev.frames {
		select {
		case sink <- device.Frame{DeviceID: fakeDev.id, Raw: raw, At: time.Now()}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	close(fakeDev.sent)
	<-ctx.Done()
	return ctx.Err()
}

func testOptions(t *testing.T, pub Publisher, devices ...device.Device) Options {
	t.Helper()
	return Options{
		Devices:           devices,
		Publisher:         pub,
		Builder:           event.NewBuilder(event.Identity{Station: "pack-03", AgentVersion: "test"}, nil),
		ScanTTL:           30 * time.Second,
		PublishTimeout:    time.Second,
		BufferSize:        8,
		Identity:          event.Identity{Project: "acme", Site: "vasby", Station: "pack-03", InstanceID: "pack-03-b", AgentVersion: "test"},
		HeartbeatInterval: time.Hour, // Off unless a test asks for it.
		DrainTimeout:      2 * time.Second,
	}
}

// runUntil starts the supervisor, waits for done, then cancels and waits for
// Run to return, so every assertion sees a completed shutdown.
func runUntil(t *testing.T, supervisor *Agent, ready func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- supervisor.Run(ctx) }()

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

func TestPublishesScanEnvelope(t *testing.T) {
	pub := &fakePublisher{}
	dev := newFakeDevice("scanner-main", "A42154587")
	supervisor, err := New(testOptions(t, pub, dev))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	runUntil(t, supervisor, func() { <-dev.sent })

	scans, _, _, _ := pub.snapshot()
	if len(scans) != 1 {
		t.Fatalf("published %d scans, want 1", len(scans))
	}
	var got event.Scan
	if err := json.Unmarshal(scans[0], &got); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if got.DeviceID != "scanner-main" {
		t.Errorf("device_id = %q, want scanner-main", got.DeviceID)
	}
	if got.Text == nil || *got.Text != "A42154587" {
		t.Errorf("text = %v, want A42154587", got.Text)
	}
	if got.Seq != 1 {
		t.Errorf("seq = %d, want 1", got.Seq)
	}
	if pub.scanTTLs[0] != 30*time.Second {
		t.Errorf("ttl = %s, want the configured scan_ttl of 30s", pub.scanTTLs[0])
	}
}

// The transport is handed the message expiry, but only the supervisor knows
// which TTL applies, so a changed configuration has to reach the publish call.
func TestScanTTLComesFromConfiguration(t *testing.T) {
	pub := &fakePublisher{}
	dev := newFakeDevice("scanner-main", "x")
	opts := testOptions(t, pub, dev)
	opts.ScanTTL = 5 * time.Second
	supervisor, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	runUntil(t, supervisor, func() { <-dev.sent })

	if len(pub.scanTTLs) != 1 || pub.scanTTLs[0] != 5*time.Second {
		t.Errorf("ttls = %v, want one entry of 5s", pub.scanTTLs)
	}
}

func TestPublishFailureIsCountedAuditedAndLogged(t *testing.T) {
	auditPath := filepath.Join(t.TempDir(), "audit.log")
	audit, err := logging.OpenAudit(auditPath, 1, 1)
	if err != nil {
		t.Fatalf("OpenAudit: %v", err)
	}
	defer audit.Close()

	pub := &fakePublisher{scanErr: errors.New("broker connection is down")}
	dev := newFakeDevice("scanner-main", "A42154587")
	opts := testOptions(t, pub, dev)
	opts.Audit = audit
	supervisor, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	runUntil(t, supervisor, func() { <-dev.sent })

	if got := supervisor.publishFailures.Load(); got != 1 {
		t.Errorf("publish failures = %d, want 1", got)
	}
	if got := supervisor.scanCount.Load(); got != 0 {
		t.Errorf("scan count = %d, want 0 after a failed publish", got)
	}

	if err := audit.Sync(); err != nil {
		t.Fatalf("audit sync: %v", err)
	}
	line := readAudit(t, auditPath)
	if line.Outcome != logging.OutcomeFailed {
		t.Errorf("audit outcome = %q, want failed", line.Outcome)
	}
	if line.Bytes != len("A42154587") {
		t.Errorf("audit bytes = %d, want %d", line.Bytes, len("A42154587"))
	}
	if !strings.Contains(line.Detail, "broker connection is down") {
		t.Errorf("audit detail = %q, want the publish error", line.Detail)
	}
}

// A scan the broker never took exists nowhere else. If the audit line records
// only its length, the station has lost it and said nothing.
func TestFailedScanIsRecoverableFromTheAuditLog(t *testing.T) {
	auditPath := filepath.Join(t.TempDir(), "audit.log")
	audit, err := logging.OpenAudit(auditPath, 1, 1)
	if err != nil {
		t.Fatalf("OpenAudit: %v", err)
	}
	defer audit.Close()

	pub := &fakePublisher{scanErr: errors.New("broker connection is down")}
	dev := newFakeDevice("scanner-main", "A42154587")
	opts := testOptions(t, pub, dev)
	opts.Audit = audit
	supervisor, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	runUntil(t, supervisor, func() { <-dev.sent })

	if err := audit.Sync(); err != nil {
		t.Fatalf("audit sync: %v", err)
	}
	line := readAudit(t, auditPath)
	decoded, err := base64.StdEncoding.DecodeString(line.RawB64)
	if err != nil {
		t.Fatalf("raw_b64 %q does not decode: %v", line.RawB64, err)
	}
	if string(decoded) != "A42154587" {
		t.Errorf("recovered %q from the audit log, want A42154587", decoded)
	}
	if line.Text == nil || *line.Text != "A42154587" {
		t.Errorf("text = %v, want A42154587", line.Text)
	}
}

// A delivered scan is upstream, and a copy here would make this file the replay
// source section 6 forbids.
func TestPublishedScanRecordsNoPayload(t *testing.T) {
	auditPath := filepath.Join(t.TempDir(), "audit.log")
	audit, err := logging.OpenAudit(auditPath, 1, 1)
	if err != nil {
		t.Fatalf("OpenAudit: %v", err)
	}
	defer audit.Close()

	pub := &fakePublisher{}
	dev := newFakeDevice("scanner-main", "A42154587")
	opts := testOptions(t, pub, dev)
	opts.Audit = audit
	supervisor, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	runUntil(t, supervisor, func() { <-dev.sent })

	if err := audit.Sync(); err != nil {
		t.Fatalf("audit sync: %v", err)
	}
	line := readAudit(t, auditPath)
	if line.RawB64 != "" || line.Text != nil {
		t.Errorf("a published scan carried its payload into the audit log: %+v", line)
	}
	if line.Bytes != len("A42154587") {
		t.Errorf("bytes = %d, want %d", line.Bytes, len("A42154587"))
	}
}

func TestSuccessfulPublishIsAudited(t *testing.T) {
	auditPath := filepath.Join(t.TempDir(), "audit.log")
	audit, err := logging.OpenAudit(auditPath, 1, 1)
	if err != nil {
		t.Fatalf("OpenAudit: %v", err)
	}
	defer audit.Close()

	pub := &fakePublisher{}
	dev := newFakeDevice("scanner-main", "A42154587")
	opts := testOptions(t, pub, dev)
	opts.Audit = audit
	supervisor, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	runUntil(t, supervisor, func() { <-dev.sent })

	if err := audit.Sync(); err != nil {
		t.Fatalf("audit sync: %v", err)
	}
	line := readAudit(t, auditPath)
	if line.Outcome != logging.OutcomePublished {
		t.Errorf("audit outcome = %q, want published", line.Outcome)
	}
	if line.Station != "pack-03" || line.DeviceID != "scanner-main" {
		t.Errorf("audit identity = %s/%s, want pack-03/scanner-main", line.Station, line.DeviceID)
	}
	if line.EventID == "" {
		t.Error("audit record has no event id, so it cannot be matched to a published scan")
	}
}

func readAudit(t *testing.T, path string) logging.AuditRecord {
	t.Helper()
	body, err := readFile(path)
	if err != nil {
		t.Fatalf("read audit: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(body), "\n")
	if len(lines) != 1 {
		t.Fatalf("audit has %d lines, want 1: %q", len(lines), body)
	}
	var r logging.AuditRecord
	if err := json.Unmarshal([]byte(lines[0]), &r); err != nil {
		t.Fatalf("unmarshal audit line: %v", err)
	}
	return r
}

func TestStatusPublishedOnConnectionUp(t *testing.T) {
	pub := &fakePublisher{}
	dev := newFakeDevice("scanner-main")
	connected := make(chan struct{}, 1)
	opts := testOptions(t, pub, dev)
	opts.Connected = connected
	supervisor, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	runUntil(t, supervisor, func() {
		connected <- struct{}{}
		waitFor(t, func() bool {
			_, statuses, _, _ := pub.snapshot()
			return len(statuses) > 0
		}, "status after connection up")
	})

	_, statuses, _, _ := pub.snapshot()
	if statuses[0].State != event.StateOnline || statuses[0].Reason != event.ReasonStartup {
		t.Errorf("first status = %+v, want online/startup", statuses[0])
	}
	if statuses[0].AgentVersion != "test" {
		t.Errorf("agent_version = %q, want test", statuses[0].AgentVersion)
	}
	// Without this a status copied out of its topic says nothing about who sent it.
	if statuses[0].Station != "pack-03" || statuses[0].Project != "acme" || statuses[0].Site != "vasby" {
		t.Errorf("status identity = %s/%s/%s, want acme/vasby/pack-03",
			statuses[0].Project, statuses[0].Site, statuses[0].Station)
	}
	// A second agent on this station would carry a different instance id, and
	// the broker connection it holds is named by the same value.
	if statuses[0].InstanceID != "pack-03-b" {
		t.Errorf("instance_id = %q, want pack-03-b", statuses[0].InstanceID)
	}
}

// Section 5.5 asks for a republish when the device appears or disappears, which
// is how a consumer learns a station is up but its scanner is unplugged.
func TestStatusRepublishedOnDeviceTransition(t *testing.T) {
	pub := &fakePublisher{}
	dev := newFakeDevice("scanner-main")
	presence := NewPresence("scanner-main")
	opts := testOptions(t, pub, dev)
	opts.Presence = presence
	supervisor, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	runUntil(t, supervisor, func() {
		presence.Set("scanner-main", true)
		waitFor(t, func() bool {
			_, statuses, _, _ := pub.snapshot()
			return len(statuses) > 0
		}, "status after device appeared")
	})

	_, statuses, _, _ := pub.snapshot()
	if statuses[0].Reason != event.ReasonDevice || !statuses[0].DeviceOpen {
		t.Errorf("status = %+v, want reason device with device_open true", statuses[0])
	}
}

// A scanner is usually open before the broker answers. Publishing a status for
// that transition would fail against a connection that does not exist, so it is
// left to the status published when the connection comes up, which carries the
// same presence.
func TestNoStatusForDeviceTransitionBeforeFirstConnection(t *testing.T) {
	pub := &fakePublisher{}
	dev := newFakeDevice("scanner-main")
	presence := NewPresence("scanner-main")
	connected := make(chan struct{}, 1)
	opts := testOptions(t, pub, dev)
	opts.Presence = presence
	opts.Connected = connected
	supervisor, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	runUntil(t, supervisor, func() {
		presence.Set("scanner-main", true)
		time.Sleep(50 * time.Millisecond)
		if _, statuses, _, _ := pub.snapshot(); len(statuses) != 0 {
			t.Errorf("published %d statuses before the first connection, want none", len(statuses))
		}
		connected <- struct{}{}
		waitFor(t, func() bool {
			_, statuses, _, _ := pub.snapshot()
			return len(statuses) > 0
		}, "status after connection up")
	})

	_, statuses, _, _ := pub.snapshot()
	if statuses[0].Reason != event.ReasonStartup || !statuses[0].DeviceOpen {
		t.Errorf("first status = %+v, want startup carrying device_open true", statuses[0])
	}
}

func TestHeartbeatReportsCounters(t *testing.T) {
	pub := &fakePublisher{}
	dev := newFakeDevice("scanner-main", "A42154587")
	presence := NewPresence("scanner-main")
	presence.Set("scanner-main", true)
	opts := testOptions(t, pub, dev)
	opts.Presence = presence
	opts.HeartbeatInterval = 20 * time.Millisecond
	supervisor, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	runUntil(t, supervisor, func() {
		<-dev.sent
		waitFor(t, func() bool {
			_, _, beats, _ := pub.snapshot()
			return len(beats) > 0 && beats[len(beats)-1].ScanCount == 1
		}, "heartbeat reporting the scan")
	})

	_, _, beats, _ := pub.snapshot()
	last := beats[len(beats)-1]
	if !last.DeviceOpen {
		t.Error("device_open = false, want true")
	}
	if last.LastScanTS == nil {
		t.Error("last_scan_ts is nil after a scan was published")
	}
	if last.PublishFailures != 0 {
		t.Errorf("publish_failures = %d, want 0", last.PublishFailures)
	}
	if last.Station != "pack-03" || last.InstanceID != "pack-03-b" || last.AgentVersion != "test" {
		t.Errorf("heartbeat identity = %s/%s version %s, want pack-03/pack-03-b version test",
			last.Station, last.InstanceID, last.AgentVersion)
	}
}

// Shutdown order is the point: devices stop, the buffer drains, the offline
// status goes out, and only then does the connection close. Closing earlier
// would make the broker publish the will, reporting a crash where there was an
// orderly stop.
func TestShutdownDrainsThenReportsOfflineThenCloses(t *testing.T) {
	pub := &fakePublisher{}
	dev := newFakeDevice("scanner-main", "one", "two")
	supervisor, err := New(testOptions(t, pub, dev))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	runUntil(t, supervisor, func() { <-dev.sent })

	scans, _, _, order := pub.snapshot()
	if len(scans) != 2 {
		t.Fatalf("published %d scans, want both before shutdown completed", len(scans))
	}
	if len(order) < 3 {
		t.Fatalf("call order = %v, want scans then status then close", order)
	}
	if order[len(order)-1] != "close" {
		t.Errorf("last call = %q, want close", order[len(order)-1])
	}
	if order[len(order)-2] != "status-offline-shutdown" {
		t.Errorf("second to last call = %q, want the offline status", order[len(order)-2])
	}
	if !pub.closed {
		t.Error("the connection was not closed")
	}
	// Disconnecting writes to the network. With the network gone, an unbounded
	// wait here is the difference between stopping and hanging until something
	// sends SIGKILL.
	if !pub.closeHadDeadline {
		t.Error("the disconnect was given an unbounded context")
	}
}

// A broker that has stopped acknowledging must not hold the process open for
// buffer_size times publish_timeout. Past the drain deadline the remaining
// scans are dropped, and recorded as dropped rather than vanishing.
func TestDrainDeadlineDropsRemainingScans(t *testing.T) {
	auditPath := filepath.Join(t.TempDir(), "audit.log")
	audit, err := logging.OpenAudit(auditPath, 1, 1)
	if err != nil {
		t.Fatalf("OpenAudit: %v", err)
	}
	defer audit.Close()

	gate := make(chan struct{})
	pub := &fakePublisher{gate: gate}
	dev := newFakeDevice("scanner-main", "one", "two", "three")
	opts := testOptions(t, pub, dev)
	opts.Audit = audit
	opts.DrainTimeout = 10 * time.Millisecond
	opts.PublishTimeout = 50 * time.Millisecond
	supervisor, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- supervisor.Run(ctx) }()

	<-dev.sent
	cancel()
	// Let the drain deadline pass while the publisher is still blocked, then
	// release it: the first scan completes, the rest are past the deadline.
	time.Sleep(60 * time.Millisecond)
	close(gate)

	select {
	case err := <-errc:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return")
	}

	if err := audit.Sync(); err != nil {
		t.Fatalf("audit sync: %v", err)
	}
	body, err := readFile(auditPath)
	if err != nil {
		t.Fatalf("read audit: %v", err)
	}
	var dropped int
	for _, line := range strings.Split(strings.TrimSpace(body), "\n") {
		var r logging.AuditRecord
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("unmarshal audit line %q: %v", line, err)
		}
		if r.Outcome == logging.OutcomeDropped {
			dropped++
			if !strings.Contains(r.Detail, "drain deadline") {
				t.Errorf("dropped record detail = %q, want it to name the drain deadline", r.Detail)
			}
		}
	}
	if dropped == 0 {
		t.Errorf("no scans recorded as dropped; audit was:\n%s", body)
	}
}

func TestNewRejectsUnusableOptions(t *testing.T) {
	pub := &fakePublisher{}
	dev := newFakeDevice("scanner-main")

	tests := []struct {
		name   string
		mutate func(*Options)
		want   string
	}{
		{"no publisher", func(o *Options) { o.Publisher = nil }, "publisher is required"},
		{"no builder", func(o *Options) { o.Builder = nil }, "event builder is required"},
		{"no devices", func(o *Options) { o.Devices = nil }, "at least one device"},
		{"zero buffer", func(o *Options) { o.BufferSize = 0 }, "buffer size must be positive"},
		{"zero ttl", func(o *Options) { o.ScanTTL = 0 }, "scan ttl must be positive"},
		{
			"publish timeout longer than ttl",
			func(o *Options) { o.ScanTTL = time.Second; o.PublishTimeout = 2 * time.Second },
			"longer than the scan ttl",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := testOptions(t, pub, dev)
			tc.mutate(&opts)
			_, err := New(opts)
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func readFile(path string) (string, error) {
	b, err := os.ReadFile(path)
	return string(b), err
}
