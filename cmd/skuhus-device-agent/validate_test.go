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
	for _, want := range []string{"acme/vasby/pack-03", "scanner-main", "tls://mq.internal:8883",
		"agent status   skuhus/acme/vasby/pack-03/agent/pack-03/status",
		"rx skuhus/acme/vasby/pack-03/scanner-main/rx status skuhus/acme/vasby/pack-03/scanner-main/status"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("stdout does not mention %q:\n%s", want, stdout)
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

func probe(t *testing.T, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	var out, errOut bytes.Buffer
	err = runProbe(args, &out, &errOut)
	return out.String(), errOut.String(), err
}

// probe --list must name the stable paths an operator should configure.
func TestRunProbeList(t *testing.T) {
	stdout, _, err := probe(t, "--list")
	if err != nil {
		t.Fatalf("probe --list: %v", err)
	}
	if !strings.Contains(stdout, "kernel-assigned device nodes:") {
		t.Errorf("listing is missing its heading:\n%s", stdout)
	}
}

func TestRunProbeRequiresExactlyOneSource(t *testing.T) {
	for _, args := range [][]string{
		{},
		{"--device", "scanner-main", "--path", "/dev/ttyACM0"},
	} {
		_, _, err := probe(t, args...)
		if err == nil {
			t.Errorf("probe %v should be rejected", args)
			continue
		}
		if !strings.Contains(err.Error(), "usage") {
			t.Errorf("probe %v: error should be a usage error: %v", args, err)
		}
	}
}

// probe applies the same device checks validate does, before it opens anything.
func TestRunProbeRejectsBadDevicePath(t *testing.T) {
	_, _, err := probe(t, "--path", "/dev/tty.usbmodem1234")
	if err == nil {
		t.Fatal("a macOS callin device should be rejected")
	}
	if !strings.Contains(err.Error(), "/dev/cu.") {
		t.Errorf("error should name the callout device: %v", err)
	}
}

func TestRunProbeRejectsUnknownDeviceID(t *testing.T) {
	path := writeConfig(t, goodConfig)
	_, _, err := probe(t, "--config", path, "--device", "no-such-device")
	if err == nil {
		t.Fatal("an unknown device id should be rejected")
	}
	if !strings.Contains(err.Error(), "scanner-main") {
		t.Errorf("error should list the device ids that do exist: %v", err)
	}
}

// The line format flags go through the same validation as the config file.
func TestRunProbeRejectsBadLineFormat(t *testing.T) {
	for flag, want := range map[string]string{
		"--parity=high":   `devices.probe.parity "high" is unknown`,
		"--stop-bits=1.5": `devices.probe.stop_bits 1.5 is not supported`,
		"--data-bits=9":   `devices.probe.data_bits must be 5, 6, 7 or 8, got 9`,
	} {
		_, _, err := probe(t, "--path", "/dev/serial/by-id/usb-x-if00", flag)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: err = %v, want it to contain %q", flag, err, want)
		}
	}
}

func TestRunProbeRejectsBadSeparator(t *testing.T) {
	_, _, err := probe(t, "--path", "/dev/serial/by-id/usb-x-if00", "--separator", `\q`)
	if err == nil || !strings.Contains(err.Error(), `cannot decode separator "\\q"`) {
		t.Fatalf("err = %v, want the undecodable separator named", err)
	}
}
