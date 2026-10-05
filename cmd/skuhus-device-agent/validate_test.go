package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeConfig lays down a config file plus the credentials file and log
// directory that validation insists exist.
func writeConfig(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	credentials := filepath.Join(dir, "credentials")
	if err := os.WriteFile(credentials, []byte("username=u\npassword=p\n"), 0o600); err != nil {
		t.Fatalf("write credentials: %v", err)
	}
	body = strings.ReplaceAll(body, "{{credentials}}", credentials)
	body = strings.ReplaceAll(body, "{{log}}", filepath.Join(dir, "agent.log"))

	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

const goodConfig = `
identity: { project: acme, site: vasby, station: pack-03 }
broker:
  url: tls://mq.internal:8883
  credentials_file: {{credentials}}
devices:
  - id: scanner-main
    path: /dev/serial/by-id/usb-Honeywell_1470g-if00
    separator: "\r"
logging:
  file: {{log}}
`

func validate(t *testing.T, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	var out, errOut bytes.Buffer
	err = runValidate(args, &out, &errOut)
	return out.String(), errOut.String(), err
}

func TestRunValidateAcceptsGoodConfig(t *testing.T) {
	path := writeConfig(t, goodConfig)
	stdout, stderr, err := validate(t, "--config", path)
	if err != nil {
		t.Fatalf("runValidate: %v", err)
	}
	if !strings.Contains(stdout, "configuration is valid") {
		t.Errorf("stdout does not confirm validity:\n%s", stdout)
	}
	// Every key, under the file's name, with its value: the ones goodConfig
	// sets and the defaults it leaves to the agent.
	for _, want := range []string{
		`identity       project="acme" site="vasby" station="pack-03" instance="pack-03"`,
		"agent status   skuhus/acme/vasby/pack-03/agent/pack-03/status",
		`broker         url="tls://mq.internal:8883" credentials_file="`,
		`ca_file="" insecure=false keepalive=30s connect_timeout=10s reconnect_interval=1s reconnect_backoff={enabled: false, max: 1m0s, jitter: 0.3}`,
		`    id="scanner-main" kind="serial" path="/dev/serial/by-id/usb-Honeywell_1470g-if00" baud=9600 data_bits=8 parity="none" stop_bits="1"`,
		`separator="\r" max_frame_bytes=4096 inter_char_timeout=200ms message_expiry=30s device_type=""`,
		"reopen_interval=100ms reopen_backoff={enabled: true, max: 30s, jitter: 0.3}",
		"tx_open_attempts=3 tx_open_interval=1s tx_chunk_bytes=1024 tx_remembered_ids=1024",
		"      topics rx skuhus/acme/vasby/pack-03/scanner-main/rx status skuhus/acme/vasby/pack-03/scanner-main/status tx skuhus/acme/vasby/pack-03/scanner-main/tx",
		"delivery       publish_timeout=2s buffer_size=64 drain_timeout=5s tx_intake_size=256",
		"status         keepalive_interval=15s missed_keepalives=3 event_buffer_size=64",
		`logging        level="info" log_payloads=false file="`,
		`max_size_mb=64 keep=7 stdout=true`,
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("stdout does not show %s:\n%s", want, stdout)
		}
	}
	if stderr != "" {
		t.Errorf("unexpected stderr:\n%s", stderr)
	}
}

func TestRunValidateReportsBadConfig(t *testing.T) {
	path := writeConfig(t, strings.Replace(goodConfig, "station: pack-03", "station: pack_03", 1))
	_, _, err := validate(t, "--config", path)
	if err == nil {
		t.Fatal("an invalid station id should make validate fail")
	}
	if !strings.Contains(err.Error(), "identity.station") {
		t.Errorf("error does not name the field: %v", err)
	}
}

// Warnings go to stderr and must not turn a usable config into a failure.
func TestRunValidateWarningsDoNotFail(t *testing.T) {
	path := writeConfig(t, strings.Replace(goodConfig,
		"/dev/serial/by-id/usb-Honeywell_1470g-if00", "/dev/ttyACM0", 1))
	stdout, stderr, err := validate(t, "--config", path)
	if err != nil {
		t.Fatalf("a warning should not fail validation: %v", err)
	}
	if !strings.Contains(stderr, "warning:") || !strings.Contains(stderr, "by-id") {
		t.Errorf("the unstable path warning is missing from stderr:\n%s", stderr)
	}
	if !strings.Contains(stdout, "warnings       1") {
		t.Errorf("stdout does not report the warning count:\n%s", stdout)
	}
}

// The flags have to be wired to the loader, not merely accepted.
func TestRunValidateFlagsOverrideFile(t *testing.T) {
	path := writeConfig(t, goodConfig)
	stdout, _, err := validate(t, "--config", path, "--station", "pack-99", "--site", "malmo")
	if err != nil {
		t.Fatalf("runValidate: %v", err)
	}
	if !strings.Contains(stdout, "acme/malmo/pack-99") {
		t.Errorf("flags did not reach the loaded config:\n%s", stdout)
	}
}

func TestRunValidateRejectsPositionalArguments(t *testing.T) {
	path := writeConfig(t, goodConfig)
	_, _, err := validate(t, "--config", path, "extra")
	if err == nil {
		t.Fatal("a stray positional argument should be rejected")
	}
	if !strings.Contains(err.Error(), "usage") {
		t.Errorf("error should be a usage error: %v", err)
	}
}

func TestRunValidateMissingConfigFile(t *testing.T) {
	_, _, err := validate(t, "--config", filepath.Join(t.TempDir(), "absent.yaml"))
	if err == nil {
		t.Fatal("a missing config file should fail")
	}
}
