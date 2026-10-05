package logtest

import (
	"log/slog"
	"reflect"
	"strings"
	"testing"

	"github.com/skuhus/device-agent/internal/logging"
)

func TestRepeatedKeys(t *testing.T) {
	cases := []struct {
		line string
		want []string
	}{
		{`{"a":1,"b":"a"}`, nil},
		{`{"a":1,"a":2}`, []string{"a"}},
		{`{"a":{"b":1,"b":2},"c":[{"d":1,"d":1},{"d":2}]}`, []string{"a.b", "c.d"}},
		{`{"a":{"x":1},"b":{"x":1}}`, nil},
	}
	for _, testCase := range cases {
		got, err := RepeatedKeys([]byte(testCase.line))
		if err != nil || !reflect.DeepEqual(got, testCase.want) {
			t.Errorf("RepeatedKeys(%s) = %v, %v; want %v", testCase.line, got, err, testCase.want)
		}
	}
	for _, bad := range []string{`[1]`, `not json`, `{"a":1}{"b":2}`, `{"a":`} {
		if _, err := RepeatedKeys([]byte(bad)); err == nil {
			t.Errorf("RepeatedKeys(%s) accepted it", bad)
		}
	}
}

// slog writes a key twice when a record repeats an attribute its logger
// already carries, and encoding/json would hide it. Check finds it, and a
// record without its source.
func TestCheckFindsWhatItExistsFor(t *testing.T) {
	captured := &Log{}
	log, err := logging.New(logging.Options{Level: "info", Out: captured, Station: "pack-03"})
	if err != nil {
		t.Fatalf("logging.New: %v", err)
	}
	log.With("device_id", "scanner-main").Info("rx published", "device_id", "scanner-main")
	log.Info("rx published", "station", "pack-03")
	slog.New(slog.NewJSONHandler(captured, nil)).Info("written without the agent's handler")
	log.Info("a good line", "device_id", "scanner-main")

	var problems []string
	for _, line := range lines(captured.String()) {
		problems = append(problems, lineProblem([]byte(line)))
	}
	want := []string{"repeated keys device_id", "repeated keys station", "no source", ""}
	if !reflect.DeepEqual(problems, want) {
		t.Errorf("problems = %q, want %q", problems, want)
	}
}

// New's logger carries the agent's identity and source, and its lines pass
// Check.
func TestNewPassesItsOwnCheck(t *testing.T) {
	log, captured := New(t, "debug")
	log.Debug("device read", "bytes", 3)
	log.Info("rx published", "id", "e1")
	records := captured.WithMessage(t, "rx published")
	if len(records) != 1 || records[0]["station"] != "pack-03" || records[0]["level"] != "INFO" {
		t.Errorf("records = %v", records)
	}
	if !strings.Contains(captured.String(), `"source":{"function":`) {
		t.Errorf("no source in %s", captured.String())
	}
}
