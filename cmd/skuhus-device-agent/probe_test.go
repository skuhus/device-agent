package main

import (
	"bytes"
	"errors"
	"strings"
	"syscall"
	"testing"

	"github.com/skuhus/device-agent/internal/device"
	"github.com/skuhus/device-agent/internal/logging/logtest"
)

func probe(t *testing.T, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	var out, errOut bytes.Buffer
	err = probeCommand(args, &out, &errOut)
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
	_, _, err := probe(t, "--path", "/dev/tty.usbmodem1234", "--separator", `\r`)
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
		"--data-bits=9":   `devices.probe.data_bits must be 5 to 8, got 9`,
	} {
		_, _, err := probe(t, "--path", "/dev/serial/by-id/usb-x-if00", "--separator", `\r`, flag)
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

// probe says a device is absent once, not on every retry, and says so again
// only when something changes.
func TestProbePresenceLogsOnlyChanges(t *testing.T) {
	log, logged := logtest.New(t, "debug")
	presence := &presenceLog{log: log.With("device_id", "scanner-main", "device_path", "/dev/ttyACM0")}
	absent := device.Event{Kind: device.PortOpenFailed, ErrorClass: "absent", Err: syscall.ENOENT}
	for _, event := range []device.Event{
		absent, absent, absent,
		{Kind: device.PortOpenFailed, ErrorClass: "permission_denied", Err: syscall.EACCES},
		{Kind: device.PortOpened},
		{Kind: device.BytesDiscarded, Reason: "oversize", Bytes: 9},
		{Kind: device.PortLost, ErrorClass: "disconnected", Err: syscall.EIO},
		absent, absent,
	} {
		presence.report(event)
	}
	var got []string
	for _, record := range logged.Records(t) {
		class, _ := record["error_class"].(string)
		got = append(got, record["msg"].(string)+"/"+class)
	}
	want := []string{"device absent/absent", "device absent/permission_denied", "device present/",
		"device absent/disconnected", "device absent/absent"}
	if strings.Join(got, " | ") != strings.Join(want, " | ") {
		t.Errorf("logged\n  %s\nwant\n  %s", strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}

// The separator is bytes, and the command line can only carry text, so escapes
// have to be decoded. Getting this wrong makes probe disagree with the config
// file about what ends a frame.
func TestParseSeparator(t *testing.T) {
	cases := []struct {
		in      string
		want    []byte
		wantErr bool
	}{
		{`\r`, []byte{'\r'}, false},
		{`\n`, []byte{'\n'}, false},
		{`\r\n`, []byte{'\r', '\n'}, false},
		{`\x1e`, []byte{0x1e}, false},
		{"#", []byte{'#'}, false},
		{"END", []byte("END"), false},
		{"", nil, true},
		{`\q`, nil, true},
	}
	for _, testCase := range cases {
		t.Run(testCase.in, func(t *testing.T) {
			got, err := parseSeparator(testCase.in)
			if gotErr := err != nil; gotErr != testCase.wantErr {
				t.Fatalf("error = %v, want error = %t", err, testCase.wantErr)
			}
			if testCase.wantErr {
				return
			}
			if !bytes.Equal(got, testCase.want) {
				t.Errorf("parseSeparator(%q) = % x, want % x", testCase.in, got, testCase.want)
			}
		})
	}
}

// A separator is per model, so probe has no default for it: --path without
// --separator is a usage error that says so.
func TestRunProbeRequiresASeparatorWithPath(t *testing.T) {
	_, _, err := probe(t, "--path", "/dev/serial/by-id/usb-x-if00")
	if err == nil || !errors.Is(err, errUsage) || !strings.Contains(err.Error(), "--separator is required with --path") {
		t.Errorf("err = %v, want a usage error asking for --separator", err)
	}
}

// A flag the chosen source would ignore is refused, not ignored: a device
// setting with --device, whose settings are the config file's, and --config
// with --path, which reads no file.
func TestRunProbeRefusesFlagsItWouldIgnore(t *testing.T) {
	configFile := writeConfig(t, goodConfig)
	cases := []struct {
		args []string
		want string
	}{
		{[]string{"--config", configFile, "--device", "scanner-main", "--baud", "4800", "--separator", `\r\n`},
			"with --device the settings come from the config file; remove --baud, --separator"},
		{[]string{"--config", configFile, "--path", "/dev/serial/by-id/usb-x-if00", "--separator", `\r`},
			"--config is read only with --device"},
	}
	for _, testCase := range cases {
		// A probe that took the flags would run until --duration, and fail
		// this test at once rather than at the test's timeout.
		_, _, err := probe(t, append(testCase.args, "--duration", "100ms")...)
		if err == nil || !errors.Is(err, errUsage) || !strings.Contains(err.Error(), testCase.want) {
			t.Errorf("probe %v: err = %v, want a usage error containing %q", testCase.args, err, testCase.want)
		}
	}
}
