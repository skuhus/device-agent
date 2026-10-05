package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/skuhus/device-agent/internal/config"
	"github.com/skuhus/device-agent/internal/logging/logtest"
	"github.com/skuhus/device-agent/internal/transport/mqtt"
)

func TestRunRejectsPositionalArguments(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := runRun([]string{"scanner-main"}, &stdout, &stderr)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "positional") {
		t.Errorf("error = %v, want it to mention positional arguments", err)
	}
}

func TestRunHelpMentionsCredentialHandling(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if err := runRun([]string{"-h"}, &stdout, &stderr); err == nil {
		t.Fatal("expected flag.ErrHelp")
	}
	help := stderr.String()
	for _, want := range []string{"SH_DEV_AGENT_MQTT_PASSWORD", "no flag for them", "SIGTERM"} {
		if !strings.Contains(help, want) {
			t.Errorf("run help does not mention %q:\n%s", want, help)
		}
	}
}

func TestUsageListsRun(t *testing.T) {
	if !strings.Contains(usage, "run ") {
		t.Error("the top level usage does not list the run command")
	}
}

// The agent's whole log, from start to stop, keeps the log's rules, and its
// file and stdout receive the same records. The broker refuses and the device
// is absent, which runs startup, retries and shutdown without either. A line
// that repeated the record's own level once slipped through every package's
// tests; this is the test that sees runAgent's lines.
func TestAgentLogKeepsTheRulesInBothDestinations(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "agent.log")
	configFile := filepath.Join(dir, "agent.yaml")
	body := fmt.Sprintf(`identity: { project: acme, site: vasby, station: pack-03 }
broker:
  url: tcp://127.0.0.1:1
  insecure: true
  reconnect_interval: 100ms
devices:
  - id: scanner-main
    path: %s
    separator: "\r\n"
status:
  keepalive_interval: 1s
logging:
  level: debug
  log_payloads: true
  file: %s
  stdout: true
`, filepath.Join(dir, "no-such-tty"), logFile)
	if err := os.WriteFile(configFile, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	t.Setenv("SH_DEV_AGENT_MQTT_USERNAME", "station-pack-03")
	t.Setenv("SH_DEV_AGENT_MQTT_PASSWORD", "pack-03-dev")
	cfg, warnings, err := config.Load(config.Options{Path: configFile})
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	stdout := &logtest.Log{}
	if err := runAgent(ctx, cfg, warnings, stdout); err != nil {
		t.Fatalf("runAgent: %v", err)
	}

	inFile, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("read the log file: %v", err)
	}
	if string(inFile) != stdout.String() {
		t.Errorf("the log file and stdout differ:\nfile:\n%s\nstdout:\n%s", inFile, stdout.String())
	}
	logtest.CheckText(t, stdout.String())
	for _, msg := range []string{"starting", "log destinations", "device starting", "broker connection attempt failed",
		"shutting down", "stopped"} {
		if len(stdout.WithMessage(t, msg)) == 0 {
			t.Errorf("no %q line in the run's log", msg)
		}
	}
	if records := stdout.WithMessage(t, "log destinations"); len(records) != 1 || records[0]["log_level"] != "debug" || records[0]["level"] != "INFO" {
		t.Errorf("log destinations = %v, want INFO with log_level debug", records)
	}
	// Everything is written to the one log, the configuration's warnings
	// included: this configuration has a plaintext broker URL.
	if records := stdout.WithMessage(t, "configuration warning"); len(records) != 1 || records[0]["field"] != "broker.url" || records[0]["level"] != "WARN" {
		t.Errorf("configuration warning records = %v, want one WARN for broker.url", records)
	}
}

// The waits and sizes the configuration sets are the ones the agent keeps: a
// device's reopen_interval with its backoff off, its tx chunk and its
// remembered ids, the broker's reconnect_interval, the shutdown's
// drain_timeout and the tx intake's size. Each differs from its default, so a
// value taken from anywhere else shows.
func TestRunUsesTheConfiguredSettings(t *testing.T) {
	dir := t.TempDir()
	configFile := filepath.Join(dir, "agent.yaml")
	body := fmt.Sprintf(`identity: { project: acme, site: vasby, station: pack-03 }
broker:
  url: tls://127.0.0.1:1
  insecure: true
  reconnect_interval: 130ms
devices:
  - id: scanner-main
    path: %s
    separator: "\r\n"
    reopen_interval: 70ms
    reopen_backoff: { enabled: false }
    tx_chunk_bytes: 333
    tx_remembered_ids: 77
delivery:
  drain_timeout: 300ms
  tx_intake_size: 9
logging:
  level: debug
  stdout: true
`, filepath.Join(dir, "no-such-tty"))
	if err := os.WriteFile(configFile, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	t.Setenv("SH_DEV_AGENT_MQTT_USERNAME", "station-pack-03")
	t.Setenv("SH_DEV_AGENT_MQTT_PASSWORD", "pack-03-dev")
	cfg, warnings, err := config.Load(config.Options{Path: configFile})
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stdout := &logtest.Log{}
	if err := runAgent(ctx, cfg, warnings, stdout); err != nil {
		t.Fatalf("runAgent: %v", err)
	}

	settings := stdout.WithMessage(t, "device settings")
	if len(settings) != 1 || settings[0]["tx_chunk_bytes"] != float64(333) || settings[0]["reopen_interval"] != "70ms" ||
		settings[0]["reopen_backoff"] != false {
		t.Errorf("device settings records = %v, want one with tx_chunk_bytes 333 and a fixed 70ms reopen", settings)
	}
	starting := stdout.WithMessage(t, "device starting")
	if len(starting) != 1 || starting[0]["tx_remembered_ids"] != float64(77) {
		t.Errorf("device starting records = %v, want one with tx_remembered_ids 77", starting)
	}
	if connection := stdout.WithMessage(t, "broker connection settings"); len(connection) != 1 || connection[0]["tls"] != true {
		t.Errorf("broker connection records = %v, want one with tls, for the tls:// URL", connection)
	}
	if intake := stdout.WithMessage(t, "tx intake ready"); len(intake) != 1 || intake[0]["tx_intake_size"] != float64(9) {
		t.Errorf("tx intake records = %v, want one with the configured size 9", intake)
	}
	reopens := stdout.WithMessage(t, "device unavailable, reopening after backoff")
	if len(reopens) < 3 {
		t.Fatalf("%d reopen lines in a second, want one every 70ms", len(reopens))
	}
	for _, record := range reopens {
		if record["backoff"] != "70ms" {
			t.Errorf("reopen backoff = %v, want 70ms, the configured interval", record["backoff"])
		}
	}
	// Counted until the shutdown starts, because the attempts go on through
	// the drain after it. At the default 1 s there would be one or two.
	stopping := stdout.WithMessage(t, "shutting down")
	if len(stopping) != 1 {
		t.Fatalf("%d shutting down lines, want 1", len(stopping))
	}
	if stopping[0]["drain_timeout"] != "300ms" {
		t.Errorf("shutting down with drain_timeout %v, want 300ms", stopping[0]["drain_timeout"])
	}
	stoppedAt := recordTime(t, stopping[0])
	attempts := 0
	for _, record := range stdout.WithMessage(t, "broker connection attempt failed") {
		if recordTime(t, record).Before(stoppedAt) {
			attempts++
		}
	}
	if attempts < 4 {
		t.Errorf("%d broker attempts in the second before the shutdown, want one every 130ms", attempts)
	}
}

func recordTime(t *testing.T, record map[string]any) time.Time {
	t.Helper()
	text, _ := record["time"].(string)
	at, err := time.Parse(time.RFC3339Nano, text)
	if err != nil {
		t.Fatalf("record time %v: %v", record["time"], err)
	}
	return at
}

// When the agent stops on an error after its log exists, the error is in the
// log, not only on stderr: an operator who reads the log file must find why
// the agent stopped.
func TestAgentStopErrorIsInTheLog(t *testing.T) {
	dir := t.TempDir()
	configFile := filepath.Join(dir, "agent.yaml")
	body := fmt.Sprintf(`identity: { project: acme, site: vasby, station: pack-03 }
broker:
  url: tcp://127.0.0.1:1
  insecure: true
devices:
  - id: scanner-main
    path: %s
    separator: "\r\n"
logging:
  stdout: true
`, filepath.Join(dir, "no-such-tty"))
	if err := os.WriteFile(configFile, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	t.Setenv("SH_DEV_AGENT_MQTT_USERNAME", "station-pack-03")
	t.Setenv("SH_DEV_AGENT_MQTT_PASSWORD", "pack-03-dev")
	cfg, warnings, err := config.Load(config.Options{Path: configFile})
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	// The credentials vanish between loading and running, so the agent stops
	// after its log exists.
	t.Setenv("SH_DEV_AGENT_MQTT_USERNAME", "")
	t.Setenv("SH_DEV_AGENT_MQTT_PASSWORD", "")

	stdout := &logtest.Log{}
	runErr := runAgent(context.Background(), cfg, warnings, stdout)
	if runErr == nil {
		t.Fatal("runAgent started without credentials")
	}
	records := stdout.WithMessage(t, "agent stopped on an error")
	if len(records) != 1 || records[0]["level"] != "ERROR" || records[0]["error"] != runErr.Error() {
		t.Errorf("records = %v, want one ERROR carrying %q", records, runErr.Error())
	}
	logtest.CheckText(t, stdout.String())
}

// The intake holds as many tx as it is sized for. The next one is dropped,
// not waited for, and recorded at ERROR with its id, sender and data, because
// its sender will get no result.
func TestTxIntakeDropsWhatDoesNotFit(t *testing.T) {
	logger, log := logtest.New(t, "debug")
	intake, onMessage := newTxIntake(1, logger)
	first := []byte(`{"schema":2,"id":"0192a3b4-c5d6-7e8f-9a0b-1c2d3e4f5a6b","sender":"label-service","raw_b64":"QQ=="}`)
	second := []byte(`{"schema":2,"id":"7e6d5c4b-3a29-4817-9f6e-5d4c3b2a1f0e","sender":"label-service","raw_b64":"Qg=="}`)
	onMessage(mqtt.Message{Topic: "skuhus/acme/vasby/pack-03/printer-1/tx", Payload: first})

	returned := make(chan struct{})
	go func() {
		onMessage(mqtt.Message{Topic: "skuhus/acme/vasby/pack-03/printer-1/tx", Payload: second})
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(3 * time.Second):
		t.Fatal("a tx that did not fit waited for room; paho would stall behind it")
	}

	if len(intake) != 1 || !bytes.Equal((<-intake).Payload, first) {
		t.Errorf("the intake does not hold the first tx alone")
	}
	dropped := log.WithMessage(t, "tx dropped: the agent's intake is full")
	if len(dropped) != 1 || dropped[0]["level"] != "ERROR" || dropped[0]["tx_id"] != "7e6d5c4b-3a29-4817-9f6e-5d4c3b2a1f0e" ||
		dropped[0]["sender"] != "label-service" || dropped[0]["tx_intake_size"] != float64(1) || dropped[0]["data_hex"] == nil {
		t.Errorf("dropped records = %v, want one ERROR naming the second tx, with its data", dropped)
	}
}
