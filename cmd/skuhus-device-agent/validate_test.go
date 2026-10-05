package main

import (
	"bytes"
	"errors"
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
	err = validateCommand(args, &out, &errOut)
	return out.String(), errOut.String(), err
}

func TestRunValidateAcceptsGoodConfig(t *testing.T) {
	path := writeConfig(t, goodConfig)
	stdout, stderr, err := validate(t, "--config", path)
	if err != nil {
		t.Fatalf("validateCommand: %v", err)
	}
	if first, _, _ := strings.Cut(stdout, "\n"); first != "OK: "+path+" is valid" {
		t.Errorf("first line = %q, want the OK result naming the file", first)
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
		"broadcast_groups={project: [], site: [], station: []}",
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

// Each broadcast group's tx topic is shown under its device, from the same
// builder run subscribes with.
func TestRunValidateShowsBroadcastGroupTopics(t *testing.T) {
	path := writeConfig(t, strings.Replace(goodConfig, "    separator: \"\\r\"\n",
		"    separator: \"\\r\"\n    broadcast_groups: { project: [scales], station: [front, scales] }\n", 1))
	stdout, _, err := validate(t, "--config", path)
	if err != nil {
		t.Fatalf("validateCommand: %v", err)
	}
	want := "      broadcast group project scales tx skuhus/acme/group/scales/tx\n" +
		"      broadcast group station front tx skuhus/acme/vasby/pack-03/group/front/tx\n" +
		"      broadcast group station scales tx skuhus/acme/vasby/pack-03/group/scales/tx\n"
	if !strings.Contains(stdout, "broadcast_groups={project: [scales], site: [], station: [front scales]}") || !strings.Contains(stdout, want) {
		t.Errorf("stdout does not show the groups and, in order, their topics:\n%s", stdout)
	}
}

// An invalid file ends in ERROR, naming the file and how many problems it
// has, then each problem on its own line, all on stderr; main exits 1 without
// printing them again.
func TestRunValidateReportsBadConfig(t *testing.T) {
	path := writeConfig(t, strings.Replace(goodConfig, "station: pack-03", "station: pack_03", 1))
	stdout, stderr, err := validate(t, "--config", path)
	if !errors.Is(err, errReported) {
		t.Fatalf("err = %v, want errReported", err)
	}
	lines := strings.Split(strings.TrimSpace(stderr), "\n")
	want := "ERROR: " + path + " is not valid: 2 problems"
	if len(lines) != 3 || lines[0] != want || !strings.HasPrefix(lines[1], "  - identity.station \"pack_03\"") ||
		!strings.HasPrefix(lines[2], "  - identity.instance \"pack_03\"") {
		t.Errorf("stderr =\n%s\nwant %q, then the station and the instance it defaults to, a line each", stderr, want)
	}
	if stdout != "" {
		t.Errorf("stdout = %q, want nothing", stdout)
	}
}

// A file that cannot be read, or parsed, ends in ERROR too.
func TestRunValidateReportsUnreadableConfig(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "absent.yaml")
	_, stderr, err := validate(t, "--config", missing)
	if !errors.Is(err, errReported) || !strings.HasPrefix(stderr, "ERROR: "+missing+" is not valid: 1 problem\n  - open ") {
		t.Errorf("err = %v, stderr =\n%s\nwant ERROR naming the file and why it could not be opened", err, stderr)
	}
	unknownKey := writeConfig(t, goodConfig+"surprise: true\n")
	_, stderr, err = validate(t, "--config", unknownKey)
	if !errors.Is(err, errReported) || !strings.Contains(stderr, "is not valid: 1 problem\n  - line ") ||
		!strings.Contains(stderr, "field surprise not found") {
		t.Errorf("err = %v, stderr =\n%s\nwant ERROR with the unknown key", err, stderr)
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
	if first, _, _ := strings.Cut(stdout, "\n"); first != "OK: "+path+" is valid, with 1 warning above" {
		t.Errorf("first line = %q, want OK with the warning count", first)
	}
}

// The flags have to be wired to the loader, not merely accepted.
func TestRunValidateFlagsOverrideFile(t *testing.T) {
	path := writeConfig(t, goodConfig)
	stdout, _, err := validate(t, "--config", path, "--station", "pack-99", "--site", "malmo")
	if err != nil {
		t.Fatalf("validateCommand: %v", err)
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
