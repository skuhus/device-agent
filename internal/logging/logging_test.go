package logging

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
)

func newTestLogger(t *testing.T, level string) (*slog.Logger, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	log, err := New(Options{
		Level: level, Out: &buf,
		Project: "acme", Site: "vasby", Station: "pack-03",
		Host: "pi-vasby-07", AgentVersion: "1.2.0",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return log, &buf
}

func decodeLine(t *testing.T, line string) map[string]any {
	t.Helper()
	var got map[string]any
	if err := json.Unmarshal([]byte(line), &got); err != nil {
		t.Fatalf("log line is not JSON: %v\n%s", err, line)
	}
	return got
}

// Every line carries the station identity, so a Loki query can select one
// station out of the fleet without parsing message text.
func TestLoggerAttachesIdentityToEveryLine(t *testing.T) {
	log, buf := newTestLogger(t, "info")
	log.Info("device open")
	log.With("device_id", "scanner-main").Warn("discarded partial frame", "reason", "oversize")

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2:\n%s", len(lines), buf.String())
	}
	for i, line := range lines {
		got := decodeLine(t, line)
		for k, want := range map[string]any{
			"project": "acme", "site": "vasby", "station": "pack-03",
			"host": "pi-vasby-07", "agent_version": "1.2.0",
		} {
			if got[k] != want {
				t.Errorf("line %d field %q = %#v, want %#v", i, k, got[k], want)
			}
		}
	}

	second := decodeLine(t, lines[1])
	if second["device_id"] != "scanner-main" {
		t.Errorf("device_id = %#v, want scanner-main", second["device_id"])
	}
	// The reason is a structured field, not interpolated into the message.
	if second["msg"] != "discarded partial frame" {
		t.Errorf("msg = %#v, want the bare message", second["msg"])
	}
	if second["reason"] != "oversize" {
		t.Errorf("reason = %#v, want oversize", second["reason"])
	}
}

func TestLoggerLevelFiltering(t *testing.T) {
	log, buf := newTestLogger(t, "warn")
	log.Debug("frame bytes")
	log.Info("device open")
	log.Warn("retrying")
	log.Error("publish failed")

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2 (warn and error):\n%s", len(lines), buf.String())
	}
	if got := decodeLine(t, lines[0])["level"]; got != "WARN" {
		t.Errorf("first line level = %#v, want WARN", got)
	}
}

// An unknown level is a configuration error, not a reason to quietly pick INFO
// and log at the wrong verbosity on a station nobody is watching.
func TestNewRejectsUnknownLevel(t *testing.T) {
	var buf bytes.Buffer
	if _, err := New(Options{Level: "verbose", Out: &buf}); err == nil {
		t.Fatal("an unknown level should be rejected")
	}
}

// Neither a file nor a stream is a valid configuration (#23 Q11a): the logger
// works and writes nothing, and reports nothing as a failure.
func TestNoDestinationWritesNothing(t *testing.T) {
	var errs bytes.Buffer
	log, err := New(Options{Level: "debug", Errors: &errs})
	if err != nil {
		t.Fatalf("New with no destination: %v", err)
	}
	log.Error("publish failed", "id", "e1")
	if errs.Len() != 0 {
		t.Errorf("a logger with no destination reported %q", errs.String())
	}
}

// Every record names the function, file and line that wrote it, so that a line
// in the log leads to the code without searching for its message.
func TestEveryRecordNamesWhereItWasWritten(t *testing.T) {
	log, buf := newTestLogger(t, "debug")
	_, _, line, _ := runtime.Caller(0)
	log.Debug("device read", "bytes", 3)
	log.With("device_id", "scanner-main").Warn("discarded partial frame")

	for i, text := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		source, ok := decodeLine(t, text)["source"].(map[string]any)
		if !ok {
			t.Fatalf("line %d has no source: %s", i, text)
		}
		if !strings.HasSuffix(source["function"].(string), ".TestEveryRecordNamesWhereItWasWritten") ||
			!strings.HasSuffix(source["file"].(string), "logging_test.go") || source["line"] != float64(line+1+i) {
			t.Errorf("line %d source = %v, want this test, logging_test.go:%d", i, source, line+1+i)
		}
	}
}

// A reading's record is written whatever the level says, with the code that
// wrote it as its source, and flushed as any INFO record is.
func TestRecordIgnoresTheLevel(t *testing.T) {
	file := &syncCounter{}
	log, err := New(Options{Level: "error", File: file})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	log.Info("device open")
	Record(log, slog.LevelInfo, "rx published", "seq", 1)

	if got := strings.Join(file.events, ", "); got != "write INFO, sync" {
		t.Errorf("file saw %s, want the record alone, flushed", got)
	}
	var captured bytes.Buffer
	log, _ = New(Options{Level: "error", Out: &captured})
	_, _, line, _ := runtime.Caller(0)
	Record(log, slog.LevelInfo, "rx published", "seq", 1)
	source := decodeLine(t, strings.TrimSpace(captured.String()))["source"].(map[string]any)
	if !strings.HasSuffix(source["function"].(string), ".TestRecordIgnoresTheLevel") || source["line"] != float64(line+1) {
		t.Errorf("source = %v, want this test, line %d", source, line+1)
	}
}

// syncCounter stands in for the log file, recording what was written and each
// flush, in order.
type syncCounter struct {
	mu     sync.Mutex
	events []string
	err    error
}

func (file *syncCounter) Write(p []byte) (int, error) {
	file.mu.Lock()
	defer file.mu.Unlock()
	if file.err != nil {
		return 0, file.err
	}
	var record map[string]any
	if err := json.Unmarshal(p, &record); err != nil {
		return 0, err
	}
	file.events = append(file.events, "write "+record["level"].(string))
	return len(p), nil
}

func (file *syncCounter) Sync() error {
	file.mu.Lock()
	defer file.mu.Unlock()
	file.events = append(file.events, "sync")
	return file.err
}

// Records at INFO and above are flushed to the file as they are written, each
// before the next is written, so that they survive a power cut. DEBUG records
// are not, because at DEBUG every read from a port is a record (#23 Q12).
func TestInfoAndAboveAreFlushedToTheFile(t *testing.T) {
	file := &syncCounter{}
	log, err := New(Options{Level: "debug", File: file})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	log.Debug("device read")
	log.Info("rx published")
	log.Debug("device read")
	log.Warn("rx dropped")
	log.Error("rx publish failed")

	want := "write DEBUG, write INFO, sync, write DEBUG, write WARN, sync, write ERROR, sync"
	if got := strings.Join(file.events, ", "); got != want {
		t.Errorf("file saw %s\nwant      %s", got, want)
	}
}

// With both destinations each record reaches both, and a file that fails does
// not silence the stream. The failure is reported where it can be seen, since
// slog drops what a handler returns.
func TestAFailingFileDoesNotSilenceTheStream(t *testing.T) {
	var stream, errs bytes.Buffer
	file := &syncCounter{err: errors.New("no space left on device")}
	log, err := New(Options{Level: "info", File: file, Out: &stream, Errors: &errs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	log.Error("rx publish failed", "id", "e1")

	if !strings.Contains(stream.String(), `"msg":"rx publish failed"`) {
		t.Errorf("the stream did not get the record: %q", stream.String())
	}
	for _, want := range []string{"ERROR", `"rx publish failed"`, "no space left on device"} {
		if !strings.Contains(errs.String(), want) {
			t.Errorf("the failure report %q does not mention %s", errs.String(), want)
		}
	}
}

func TestFileAndStreamEachGetEveryRecord(t *testing.T) {
	var stream bytes.Buffer
	file := &syncCounter{}
	log, err := New(Options{Level: "debug", File: file, Out: &stream})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	log.Debug("device read")
	log.Info("rx published")
	if lines := strings.Count(stream.String(), "\n"); lines != 2 {
		t.Errorf("stream got %d records, want 2", lines)
	}
	if got := strings.Join(file.events, ", "); got != "write DEBUG, write INFO, sync" {
		t.Errorf("file saw %s, want both records and one flush", got)
	}
}

// A reading's data goes on its line as hex, which keeps every byte, and as text
// when it is valid UTF-8; data that is not gets no text rather than a lossy
// rendering.
func TestPayload(t *testing.T) {
	cases := []struct {
		data []byte
		want []any
	}{
		{[]byte("A7393481008232"), []any{"data_hex", "4137333933343831303038323332", "data_text", "A7393481008232"}},
		{[]byte("]C1\x1d01"), []any{"data_hex", "5d43311d3031", "data_text", "]C1\x1d01"}},
		{[]byte{0xff, 0x0d}, []any{"data_hex", "ff0d"}},
	}
	for _, tc := range cases {
		if got := Payload(tc.data); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("Payload(%q) = %v, want %v", tc.data, got, tc.want)
		}
	}
}

func TestParseLevel(t *testing.T) {
	cases := map[string]slog.Level{
		"debug": slog.LevelDebug,
		"info":  slog.LevelInfo,
		"WARN":  slog.LevelWarn,
		" warn": slog.LevelWarn,
		"error": slog.LevelError,
	}
	for in, want := range cases {
		got, err := ParseLevel(in)
		if err != nil {
			t.Errorf("ParseLevel(%q): %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("ParseLevel(%q) = %v, want %v", in, got, want)
		}
	}
	// No default for an empty level and no second name for one: each
	// would be a value nobody wrote.
	for _, level := range []string{"trace", "", "warning"} {
		if _, err := ParseLevel(level); err == nil {
			t.Errorf("ParseLevel(%q) should fail", level)
		}
	}
}
