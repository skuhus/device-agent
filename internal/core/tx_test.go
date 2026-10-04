package core

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/skuhus/device-agent/internal/device"
	"github.com/skuhus/device-agent/internal/logging/logtest"
	"github.com/skuhus/device-agent/internal/wire"
)

// fakePrinter is a device that can be written: a fakeReader whose port is
// open or not, which records every write. A write can be held part way, or
// made to fail, and RetryOpen can open the port, as the reader opening it in
// answer to a tx would.
type fakePrinter struct {
	*fakeReader
	mu      sync.Mutex
	open    bool
	writes  [][]byte
	retries int
	report  func(device.Event)
	// openOnRetry opens the port when RetryOpen is called, and reports it.
	openOnRetry bool
	// failAfter fails a write once it has written that many bytes, with
	// failErr.
	failAfter int
	failErr   error
	// hold, when non-nil, stops each write after its first byte until it is
	// closed; writing receives a value when a write reaches it.
	hold    chan struct{}
	writing chan struct{}
}

// newFakePrinter makes a printer whose port starts open or closed; an open one
// reports it, as the reader does once it opens the port.
func newFakePrinter(id string, open bool) *fakePrinter {
	printer := &fakePrinter{fakeReader: newFakeReader(id), open: open, failAfter: -1}
	if open {
		printer.events = []device.Event{{DeviceID: id, Kind: device.PortOpened}}
	}
	return printer
}

func (printer *fakePrinter) Run(ctx context.Context, sink chan<- device.Frame, report func(device.Event)) error {
	printer.mu.Lock()
	printer.report = report
	printer.mu.Unlock()
	return printer.fakeReader.Run(ctx, sink, report)
}

func (printer *fakePrinter) Write(ctx context.Context, data []byte, progress func(int)) (int, error) {
	printer.mu.Lock()
	if !printer.open {
		printer.mu.Unlock()
		return 0, device.ErrNotOpen
	}
	hold, writing, failAfter, failErr := printer.hold, printer.writing, printer.failAfter, printer.failErr
	printer.mu.Unlock()

	written := 0
	if hold != nil && len(data) > 1 {
		progress(1)
		written = 1
		if writing != nil {
			writing <- struct{}{}
		}
		select {
		case <-hold:
		case <-ctx.Done():
			return written, ctx.Err()
		}
	}
	if failAfter >= 0 && failAfter < len(data) {
		progress(failAfter)
		printer.record(data[:failAfter])
		return failAfter, failErr
	}
	progress(len(data))
	printer.record(data)
	_ = written
	return len(data), nil
}

func (printer *fakePrinter) record(data []byte) {
	printer.mu.Lock()
	defer printer.mu.Unlock()
	printer.writes = append(printer.writes, bytes.Clone(data))
}

func (printer *fakePrinter) RetryOpen() {
	printer.mu.Lock()
	printer.retries++
	openNow, report := printer.openOnRetry, printer.report
	if openNow {
		printer.open = true
	}
	printer.mu.Unlock()
	if openNow && report != nil {
		report(device.Event{DeviceID: printer.id, Kind: device.PortOpened, At: time.Now()})
	}
}

func (printer *fakePrinter) snapshot() ([][]byte, int) {
	printer.mu.Lock()
	defer printer.mu.Unlock()
	return append([][]byte(nil), printer.writes...), printer.retries
}

// txRun is a core with one printer, taking tx from a channel the test feeds.
type txRun struct {
	t         *testing.T
	transport *fakeTransport
	printer   *fakePrinter
	txIn      chan TxMessage
	topics    wire.DeviceTopics
	log       *logtest.Log
	connected chan struct{}
	core      *Core
	cancel    context.CancelFunc
	done      chan error
}

func startTxRun(t *testing.T, transport *fakeTransport, printer *fakePrinter, attempts int, interval time.Duration) *txRun {
	t.Helper()
	return startTxRunWith(t, transport, printer, attempts, interval, func(*Options) {})
}

// startTxRunWith is startTxRun with options the test sets itself.
func startTxRunWith(t *testing.T, transport *fakeTransport, printer *fakePrinter, attempts int, interval time.Duration, set func(*Options)) *txRun {
	t.Helper()
	dev := coreDevice(t, printer, "epson-tm-t20iii")
	dev.TxOpenAttempts, dev.TxOpenInterval = attempts, interval
	opts := testOptions(t, transport, dev)
	set(&opts)
	logger, log := logtest.New(t, "debug")
	opts.Logger = logger
	txIn := make(chan TxMessage, 64)
	opts.TxIn = txIn
	connected := make(chan struct{}, 1)
	opts.Connected = connected
	running := newCore(t, opts)
	ctx, cancel := context.WithCancel(context.Background())
	run := &txRun{t: t, transport: transport, printer: printer, txIn: txIn, topics: dev.Topics, log: log,
		connected: connected, core: running, cancel: cancel, done: make(chan error, 1)}
	go func() { run.done <- running.Run(ctx) }()
	t.Cleanup(func() { run.stop() })
	return run
}

func (run *txRun) stop() {
	run.cancel()
	select {
	case err := <-run.done:
		if err != nil {
			run.t.Errorf("Run: %v", err)
		}
		run.done <- nil
	case <-time.After(10 * time.Second):
		run.t.Fatal("Run did not return within 10s of cancellation")
	}
}

// send publishes a tx as a sender would; expiry 0 means none.
func (run *txRun) send(id string, data []byte, expiry time.Duration) {
	payload, err := json.Marshal(map[string]any{"schema": 2, "id": id, "sender": "label-service", "raw_b64": base64.StdEncoding.EncodeToString(data)})
	if err != nil {
		run.t.Fatal(err)
	}
	run.sendRaw(payload, expiry)
}

func (run *txRun) sendRaw(payload []byte, expiry time.Duration) {
	run.txIn <- TxMessage{Topic: run.topics.Tx(), Payload: payload, Expiry: expiry, HasExpiry: expiry > 0, Received: time.Now()}
}

// results are the tx results published on the device's status topic.
func (run *txRun) results() []wire.TxResult {
	var results []wire.TxResult
	for _, message := range run.transport.of("event", run.topics.Status()) {
		var kind struct {
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal(message.payload, &kind); err != nil {
			run.t.Fatalf("status message is not JSON: %v", err)
		}
		if kind.Kind == wire.KindTxResult {
			results = append(results, decodeAll[wire.TxResult]([]published{message})...)
		}
	}
	return results
}

// resultsFor waits until the tx has a final result, written or failed, and
// returns all of its results in the order they were published. A rejected
// resend is not final: the tx it was rejected for carries on.
func (run *txRun) resultsFor(id string) []wire.TxResult {
	run.t.Helper()
	var mine []wire.TxResult
	waitUntil(run.t, "a final result for "+id, func() bool {
		mine = nil
		for _, result := range run.results() {
			if result.TxID != nil && *result.TxID == id {
				mine = append(mine, result)
			}
		}
		last := len(mine) - 1
		return last >= 0 && (mine[last].State == wire.TxWritten || mine[last].State == wire.TxFailed)
	})
	return mine
}

// counters has a keepalive go out, as a new connection does, and returns the
// printer's counters from it.
func (run *txRun) counters() wire.KeepaliveDevice {
	run.t.Helper()
	before := len(run.transport.keepalives())
	run.connected <- struct{}{}
	waitUntil(run.t, "a keepalive", func() bool { return len(run.transport.keepalives()) > before })
	keepalives := run.transport.keepalives()
	return keepalives[len(keepalives)-1].Devices[0]
}

func codes(results []wire.TxResult) string {
	var out []string
	for _, result := range results {
		out = append(out, string(result.State)+"/"+string(result.Code))
	}
	return strings.Join(out, " ")
}

const (
	txA = "0192a3b4-c5d6-7e8f-9a0b-1c2d3e4f5a6b"
	txB = "7e6d5c4b-3a29-4817-9f6e-5d4c3b2a1f0e"
)

// A tx is accepted, written through the device's port whole, and reported
// written, and the keepalive counts it.
func TestTxIsWrittenAndReported(t *testing.T) {
	transport := &fakeTransport{}
	printer := newFakePrinter("printer-1", true)
	run := startTxRun(t, transport, printer, 3, 10*time.Millisecond)
	job := []byte("\x1b@SKU-1042\n\x1dVA0")
	run.send(txA, job, 30*time.Second)

	results := run.resultsFor(txA)
	if got := codes(results); got != "accepted/accepted written/written" {
		t.Fatalf("results = %s, want accepted, then written", got)
	}
	written := results[1]
	if written.Detail["bytes_written"] != float64(len(job)) || written.Detail["open_attempts"] != float64(0) {
		t.Errorf("written detail = %v, want %d bytes and no open attempt", written.Detail, len(job))
	}
	if written.Sender == nil || *written.Sender != "label-service" || written.DeviceID != "printer-1" || !written.DeviceOpen {
		t.Errorf("written = sender %v, device %q, open %t", written.Sender, written.DeviceID, written.DeviceOpen)
	}
	writes, _ := printer.snapshot()
	if len(writes) != 1 || !bytes.Equal(writes[0], job) {
		t.Errorf("writes = %q, want the job once", writes)
	}
	if counters := run.counters(); counters.TxWritten != 1 || counters.TxFailed != 0 {
		t.Errorf("keepalive tx_written %d, tx_failed %d; want 1 and 0", counters.TxWritten, counters.TxFailed)
	}
	run.stop()
	requireRecord(t, run.log.WithMessage(t, "tx written"), "tx_id", txA, "outcome", "written", "bytes_written", float64(len(job)))
}

// A tx that cannot be read gets one failed result naming why, is not written,
// and counts as failed.
func TestTxThatCannotBeTakenFails(t *testing.T) {
	transport := &fakeTransport{}
	printer := newFakePrinter("printer-1", true)
	run := startTxRun(t, transport, printer, 3, 10*time.Millisecond)

	run.sendRaw([]byte(`^XA^FD`), 30*time.Second)
	run.send("job-1042", []byte("x"), 30*time.Second)
	results := run.resultsFor("job-1042")
	if got := codes(results); got != "failed/invalid_id" {
		t.Errorf("results for a non-UUID id = %s, want failed/invalid_id", got)
	}
	waitUntil(t, "the unreadable tx's result", func() bool {
		for _, result := range run.results() {
			if result.Code == wire.TxCodeInvalidMessage && result.TxID == nil {
				return true
			}
		}
		return false
	})
	if writes, _ := printer.snapshot(); len(writes) != 0 {
		t.Errorf("writes = %q, want none", writes)
	}
	if counters := run.counters(); counters.TxWritten != 0 || counters.TxFailed != 2 {
		t.Errorf("keepalive tx_written %d, tx_failed %d; want 0 and 2", counters.TxWritten, counters.TxFailed)
	}
	run.stop()
	if failed := len(run.log.WithMessage(t, "tx failed")); failed != 2 {
		t.Errorf("%d tx failed records, want 2", failed)
	}
}

// A resend of a tx being written is rejected with where the first stands,
// and the first is written once.
func TestTxResendWhileWritingIsRejected(t *testing.T) {
	transport := &fakeTransport{}
	printer := newFakePrinter("printer-1", true)
	printer.hold, printer.writing = make(chan struct{}), make(chan struct{}, 1)
	run := startTxRun(t, transport, printer, 3, 10*time.Millisecond)
	job := []byte("a long label")
	run.send(txA, job, 30*time.Second)
	<-printer.writing

	run.send(txA, job, 30*time.Second)
	var rejected wire.TxResult
	waitUntil(t, "the resend's result", func() bool {
		for _, result := range run.results() {
			if result.State == wire.TxRejected {
				rejected = result
				return true
			}
		}
		return false
	})
	if rejected.Code != wire.TxCodeInProgress || rejected.Detail["stage"] != string(wire.TxWriting) || rejected.Detail["bytes_written"] != float64(1) {
		t.Errorf("rejected = %s %v, want in_progress, stage writing, 1 byte so far", rejected.Code, rejected.Detail)
	}
	close(printer.hold)
	if got := codes(run.resultsFor(txA)); got != "accepted/accepted rejected/in_progress written/written" {
		t.Errorf("results = %s", got)
	}
	if writes, _ := printer.snapshot(); len(writes) != 1 {
		t.Errorf("%d writes, want the job written once", len(writes))
	}
}

// A tx that finds the port closed asks the reader to open it, the configured
// number of times, and then fails with the reason the port did not open.
func TestTxToAClosedPortFailsAfterItsAttempts(t *testing.T) {
	transport := &fakeTransport{}
	printer := newFakePrinter("printer-1", false)
	printer.events = []device.Event{{DeviceID: "printer-1", Kind: device.PortOpenFailed, ErrorClass: "absent",
		Err: errors.New("open /dev/cu.usbserial-111420: no such file or directory")}}
	run := startTxRun(t, transport, printer, 3, 10*time.Millisecond)
	waitUntil(t, "the failed open to be counted", func() bool { return len(transport.of("event", run.topics.Status())) > 0 })
	run.send(txA, []byte("label"), 30*time.Second)

	results := run.resultsFor(txA)
	if got := codes(results); got != "accepted/accepted failed/port_unavailable" {
		t.Fatalf("results = %s", got)
	}
	detail := results[1].Detail
	if detail["error_class"] != "absent" || detail["open_attempts"] != float64(3) ||
		detail["error"] != "open /dev/cu.usbserial-111420: no such file or directory" {
		t.Errorf("detail = %v, want class absent, the open error, 3 attempts", detail)
	}
	if writes, retries := printer.snapshot(); len(writes) != 0 || retries != 3 {
		t.Errorf("%d writes and %d retries, want none and 3", len(writes), retries)
	}
}

// A port that opens while the tx waits for it is written, with the attempt it
// took counted.
func TestTxWaitsForThePortToOpen(t *testing.T) {
	transport := &fakeTransport{}
	printer := newFakePrinter("printer-1", false)
	printer.openOnRetry = true
	run := startTxRun(t, transport, printer, 3, time.Second)
	run.send(txA, []byte("label"), 30*time.Second)

	results := run.resultsFor(txA)
	if got := codes(results); got != "accepted/accepted written/written" {
		t.Fatalf("results = %s", got)
	}
	if attempts := results[1].Detail["open_attempts"]; attempts != float64(1) {
		t.Errorf("open_attempts = %v, want 1", attempts)
	}
}

// No attempt is made once a tx's message expiry has passed, whether it was
// spent in the queue or waiting for the port.
func TestExpiredTxIsNotWritten(t *testing.T) {
	transport := &fakeTransport{}
	printer := newFakePrinter("printer-1", false)
	run := startTxRun(t, transport, printer, 100, 50*time.Millisecond)
	run.send(txA, []byte("label"), 120*time.Millisecond)

	results := run.resultsFor(txA)
	if got := codes(results); got != "accepted/accepted failed/expired" {
		t.Fatalf("results = %s", got)
	}
	attempts, _ := results[1].Detail["open_attempts"].(float64)
	if attempts < 1 || attempts > 4 {
		t.Errorf("open_attempts = %v, want the few that fit in 120 ms at 50 ms apart", attempts)
	}
	if writes, _ := printer.snapshot(); len(writes) != 0 {
		t.Errorf("writes = %q, want none", writes)
	}
}

// Once writing has started a failure is not retried, and the result says how
// far it got and why, in the reader's class.
func TestFailedWriteIsNotRetried(t *testing.T) {
	transport := &fakeTransport{}
	printer := newFakePrinter("printer-1", true)
	printer.failAfter = 4
	printer.failErr = &device.PortError{Class: "disconnected", Err: fmt.Errorf("write /dev/cu.usbserial-111420: %w", syscall.ENXIO)}
	run := startTxRun(t, transport, printer, 3, 10*time.Millisecond)
	run.send(txA, []byte("label-1042"), 30*time.Second)

	results := run.resultsFor(txA)
	if got := codes(results); got != "accepted/accepted failed/write_failed" {
		t.Fatalf("results = %s", got)
	}
	detail := results[1].Detail
	if detail["bytes_written"] != float64(4) || detail["error_class"] != "disconnected" || detail["open_attempts"] != float64(0) {
		t.Errorf("detail = %v, want 4 bytes, class disconnected", detail)
	}
	if writes, retries := printer.snapshot(); len(writes) != 1 || retries != 0 {
		t.Errorf("%d writes and %d retries, want one write and no retry", len(writes), retries)
	}
}

// One writer per port: many tx in quick succession reach the port whole, one
// after another, in the order they arrived.
func TestTxsAreWrittenWholeInArrivalOrder(t *testing.T) {
	transport := &fakeTransport{}
	printer := newFakePrinter("printer-1", true)
	run := startTxRun(t, transport, printer, 3, 10*time.Millisecond)
	var want [][]byte
	var ids []string
	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("00000000-0000-4000-8000-%012d", i)
		job := []byte(fmt.Sprintf("job %02d %s\n", i, strings.Repeat("#", i)))
		ids, want = append(ids, id), append(want, job)
		run.send(id, job, 30*time.Second)
	}
	for _, id := range ids {
		if got := codes(run.resultsFor(id)); got != "accepted/accepted written/written" {
			t.Errorf("%s: results = %s", id, got)
		}
	}
	writes, _ := printer.snapshot()
	if len(writes) != len(want) {
		t.Fatalf("%d writes, want %d", len(writes), len(want))
	}
	for i := range want {
		if !bytes.Equal(writes[i], want[i]) {
			t.Errorf("write %d = %q, want %q", i, writes[i], want[i])
		}
	}
}

// Results wait for the broker connection, as the device's events do, and go
// out once it is up.
func TestTxResultsWaitForTheConnection(t *testing.T) {
	transport := &fakeTransport{down: make(chan struct{})}
	printer := newFakePrinter("printer-1", true)
	run := startTxRun(t, transport, printer, 3, 10*time.Millisecond)
	run.send(txA, []byte("label"), 30*time.Second)
	waitUntil(t, "the tx to be written", func() bool {
		writes, _ := printer.snapshot()
		return len(writes) == 1
	})
	if published := run.results(); len(published) != 0 {
		t.Fatalf("%d results published while the connection was down", len(published))
	}
	close(transport.down)
	if got := codes(run.resultsFor(txA)); got != "accepted/accepted written/written" {
		t.Errorf("results = %s", got)
	}
}

// When the agent stops, a tx being written goes on within the drain, and one
// still queued is not started: it fails as agent_stopping, with nothing
// written (#19 Q1).
func TestQueuedTxFailsWhenTheAgentStops(t *testing.T) {
	transport := &fakeTransport{}
	printer := newFakePrinter("printer-1", true)
	printer.hold, printer.writing = make(chan struct{}), make(chan struct{}, 1)
	run := startTxRun(t, transport, printer, 3, 10*time.Millisecond)
	run.send(txA, []byte("first label"), 30*time.Second)
	<-printer.writing
	run.send(txB, []byte("second label"), 30*time.Second)
	waitUntil(t, "the second tx to be accepted", func() bool { return len(run.results()) >= 2 })

	go func() {
		time.Sleep(50 * time.Millisecond)
		close(printer.hold)
	}()
	run.stop()
	writes, _ := printer.snapshot()
	if len(writes) != 1 || string(writes[0]) != "first label" {
		t.Errorf("writes = %q, want the first label only", writes)
	}
	if got := codes(run.resultsFor(txA)); got != "accepted/accepted written/written" {
		t.Errorf("the tx being written: %s, want it written within the drain", got)
	}
	second := run.resultsFor(txB)
	if got := codes(second); got != "accepted/accepted failed/agent_stopping" || second[1].Detail["bytes_written"] != float64(0) {
		t.Errorf("the queued tx: %s %v, want agent_stopping with nothing written", got, second[len(second)-1].Detail)
	}
	requireRecord(t, run.log.WithMessage(t, "tx failed"), "tx_id", txB, "code", "agent_stopping", "data_text", "second label")
}

// A tx still being written when the drain runs out stops after the chunk in
// hand, and its result says how far it got.
func TestTxBeingWrittenIsCutAtTheDrain(t *testing.T) {
	transport := &fakeTransport{}
	printer := newFakePrinter("printer-1", true)
	printer.hold, printer.writing = make(chan struct{}), make(chan struct{}, 1)
	run := startTxRunWith(t, transport, printer, 3, 10*time.Millisecond, func(opts *Options) { opts.DrainTimeout = 100 * time.Millisecond })
	run.send(txA, []byte("a label that never finishes"), 30*time.Second)
	<-printer.writing

	started := time.Now()
	run.stop()
	if took := time.Since(started); took > 2*time.Second {
		t.Errorf("stopping took %s with a write held, want about the 100 ms drain", took)
	}
	results := run.resultsFor(txA)
	if got := codes(results); got != "accepted/accepted failed/agent_stopping" || results[1].Detail["bytes_written"] != float64(1) {
		t.Errorf("results = %s %v, want agent_stopping after the 1 byte written", got, results[len(results)-1].Detail)
	}
}

// A tx that arrives while the agent stops gets agent_stopping at once, and is
// not queued.
func TestTxArrivingWhileTheAgentStopsFails(t *testing.T) {
	transport := &fakeTransport{}
	printer := newFakePrinter("printer-1", true)
	printer.hold, printer.writing = make(chan struct{}), make(chan struct{}, 1)
	run := startTxRun(t, transport, printer, 3, 10*time.Millisecond)
	run.send(txA, []byte("held label"), 30*time.Second)
	<-printer.writing

	stopped := make(chan struct{})
	go func() {
		run.stop()
		close(stopped)
	}()
	waitUntil(t, "the agent to begin stopping", func() bool { return run.core.stopping.Load() })
	run.send(txB, []byte("late label"), 30*time.Second)
	waitUntil(t, "the late tx's result", func() bool {
		for _, result := range run.results() {
			if result.TxID != nil && *result.TxID == txB {
				return true
			}
		}
		return false
	})
	close(printer.hold)
	<-stopped
	late := run.resultsFor(txB)
	if got := codes(late); got != "failed/agent_stopping" {
		t.Errorf("the late tx: %s, want agent_stopping alone", got)
	}
	writes, _ := printer.snapshot()
	for _, write := range writes {
		if bytes.Contains(write, []byte("late")) {
			t.Error("the late tx was written")
		}
	}
}

// requireRecord fails unless one of the records holds every key with its value.
func requireRecord(t *testing.T, records []map[string]any, keyValues ...any) {
	t.Helper()
	for _, record := range records {
		match := true
		for i := 0; i+1 < len(keyValues); i += 2 {
			if record[keyValues[i].(string)] != keyValues[i+1] {
				match = false
				break
			}
		}
		if match {
			return
		}
	}
	t.Errorf("no record with %v among %d: %v", keyValues, len(records), records)
}
