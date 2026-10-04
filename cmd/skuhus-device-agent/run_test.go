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
