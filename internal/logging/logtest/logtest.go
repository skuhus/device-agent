// Package logtest captures the agent's log in tests, and holds every record to
// the rules of DESIGN-V2.md, "Logging: one common log": one line of JSON per
// record, with its severity and slog's source attribute, and no key repeated.
// It is imported only by tests.
package logtest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/skuhus/device-agent/internal/logging"
)

// Log is a captured log. Its writes and reads are safe from any goroutine.
type Log struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (log *Log) Write(data []byte) (int, error) {
	log.mu.Lock()
	defer log.mu.Unlock()
	return log.buf.Write(data)
}

// String is everything written so far.
func (log *Log) String() string {
	log.mu.Lock()
	defer log.mu.Unlock()
	return log.buf.String()
}

// Records decodes every line written so far, failing t on one that is not
// JSON.
func (log *Log) Records(t testing.TB) []map[string]any {
	t.Helper()
	var records []map[string]any
	for _, line := range lines(log.String()) {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("log line is not JSON: %v\n%s", err, line)
		}
		records = append(records, record)
	}
	return records
}

// WithMessage returns the decoded records whose message is msg.
func (log *Log) WithMessage(t testing.TB, msg string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, record := range log.Records(t) {
		if record["msg"] == msg {
			out = append(out, record)
		}
	}
	return out
}

// New returns a logger built as the agent builds its log, at level, with the
// returned Log as its only destination. When the test ends, every line is
// checked with Check.
func New(t testing.TB, level string) (*slog.Logger, *Log) {
	t.Helper()
	captured := &Log{}
	log, err := logging.New(logging.Options{
		Level: level, Out: captured,
		Project: "acme", Site: "vasby", Station: "pack-03",
		Host: "test-host", Instance: "pack-03", AgentVersion: "test",
	})
	if err != nil {
		t.Fatalf("logging.New: %v", err)
	}
	t.Cleanup(func() { captured.Check(t) })
	return log, captured
}

// Check fails t for every line that breaks a rule: not a JSON object, no
// level, message or time, no source naming a function, file and line, or a
// key repeated anywhere in the line.
func (log *Log) Check(t testing.TB) {
	t.Helper()
	CheckText(t, log.String())
}

// CheckText applies Check's rules to the agent's log captured some other way,
// such as the contents of its log file.
func CheckText(t testing.TB, text string) {
	t.Helper()
	for _, line := range lines(text) {
		if problem := lineProblem([]byte(line)); problem != "" {
			t.Errorf("log line breaks a rule: %s\n%s", problem, line)
		}
	}
}

func lineProblem(line []byte) string {
	repeated, err := RepeatedKeys(line)
	if err != nil {
		return err.Error()
	}
	if len(repeated) > 0 {
		return "repeated keys " + strings.Join(repeated, ", ")
	}
	var record map[string]any
	if err := json.Unmarshal(line, &record); err != nil {
		return err.Error()
	}
	for _, key := range []string{"time", "level", "msg"} {
		if _, ok := record[key]; !ok {
			return "no " + key
		}
	}
	source, ok := record["source"].(map[string]any)
	if !ok {
		return "no source"
	}
	if source["function"] == "" || source["function"] == nil || source["file"] == "" || source["file"] == nil || source["line"] == nil {
		return fmt.Sprintf("source %v does not name a function, file and line", source)
	}
	return ""
}

// RepeatedKeys returns every key that appears more than once in the same JSON
// object, anywhere in line, as a dotted path. encoding/json keeps the last of
// a repeated key and says nothing, so decoding into a map cannot see one.
func RepeatedKeys(line []byte) ([]string, error) {
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.UseNumber()
	first, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("not JSON: %w", err)
	}
	if first != json.Delim('{') {
		return nil, errors.New("not a JSON object")
	}
	var repeated []string
	var object func(path string) error
	var value func(path string) error
	object = func(path string) error {
		seen := map[string]bool{}
		for decoder.More() {
			token, err := decoder.Token()
			if err != nil {
				return err
			}
			key := token.(string)
			if seen[key] {
				repeated = append(repeated, path+key)
			}
			seen[key] = true
			if err := value(path + key + "."); err != nil {
				return err
			}
		}
		_, err := decoder.Token()
		return err
	}
	value = func(path string) error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		switch token {
		case json.Delim('{'):
			return object(path)
		case json.Delim('['):
			for decoder.More() {
				if err := value(path); err != nil {
					return err
				}
			}
			_, err := decoder.Token()
			return err
		}
		return nil
	}
	if err := object(""); err != nil {
		return nil, fmt.Errorf("not JSON: %w", err)
	}
	if decoder.More() {
		return nil, errors.New("more than one JSON value on the line")
	}
	return repeated, nil
}

func lines(text string) []string {
	var out []string
	for _, line := range strings.Split(text, "\n") {
		if strings.TrimSpace(line) != "" {
			out = append(out, line)
		}
	}
	return out
}
