package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// An operator upgrading a station from 0.3.0 learns what changed from the
// error: every removed key is named, with its line and its replacement, and
// only once. The fixture is the sample 0.3.0 shipped, unchanged.
func TestConfigFrom030IsRefusedNamingEveryRemovedKey(t *testing.T) {
	_, _, err := load(t, filepath.Join("testdata", "config-0.3.0.yaml"), noEnv(), Overrides{})
	if err == nil {
		t.Fatal("a 0.3.0 configuration was accepted")
	}
	want := []string{
		"line 89: devices[0].terminator was removed in 2.0.0; rename it to separator",
		"line 102: devices[0].assert_config was removed in 2.0.0; remove it; the agent does not configure devices",
		"line 59: broker.connect_backoff was removed in 2.0.0; use broker.reconnect_interval, and broker.reconnect_backoff to make the wait grow",
		"line 113: delivery.scan_ttl was removed in 2.0.0; set message_expiry on each device",
		"line 131: logging.audit_file was removed in 2.0.0; use logging.file",
		"line 132: logging.audit_max_size_mb was removed in 2.0.0; use logging.max_size_mb",
		"line 133: logging.audit_keep was removed in 2.0.0; use logging.keep",
	}
	for _, line := range want {
		if !strings.Contains(err.Error(), line) {
			t.Errorf("error does not contain %q:\n%v", line, err)
		}
	}
	if got := strings.Count(err.Error(), "was removed in 2.0.0"); got != len(want) {
		t.Errorf("%d removed keys reported, want %d:\n%v", got, len(want), err)
	}
	if strings.Contains(err.Error(), "not found in type") {
		t.Errorf("a removed key is also reported as an unknown field:\n%v", err)
	}
}

// A removed key does not hide an ordinary typo elsewhere in the same file.
func TestRemovedAndUnknownKeysAreBothReported(t *testing.T) {
	body := strings.Replace(validConfig, "  publish_timeout: 2s", "  publish_timeout: 2s\n  scan_ttl: 30s", 1)
	body = strings.Replace(body, "  log_payloads: false", "  log_payload: false", 1)
	_, _, err := load(t, newFixture(t, body).path, noEnv(), Overrides{})
	if err == nil {
		t.Fatal("a file with a removed key and a typo was accepted")
	}
	for _, want := range []string{"delivery.scan_ttl was removed in 2.0.0", "field log_payload not found"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not contain %q:\n%v", want, err)
		}
	}
}

func TestRemovedEnvironmentVariablesNameTheirReplacement(t *testing.T) {
	fixture := newFixture(t, validConfig)
	for variable, want := range map[string]string{
		EnvPrefix + "DELIVERY_SCAN_TTL=30s":         EnvPrefix + "DELIVERY_SCAN_TTL was removed in 2.0.0; set message_expiry on each device in the config file",
		EnvPrefix + "LOGGING_AUDIT_FILE=/var/log/a": EnvPrefix + "LOGGING_AUDIT_FILE was removed in 2.0.0; use " + EnvPrefix + "LOGGING_FILE",
		EnvPrefix + "LOGGING_AUDIT_MAX_SIZE_MB=64":  EnvPrefix + "LOGGING_AUDIT_MAX_SIZE_MB was removed in 2.0.0; use " + EnvPrefix + "LOGGING_MAX_SIZE_MB",
		EnvPrefix + "LOGGING_AUDIT_KEEP=7":          EnvPrefix + "LOGGING_AUDIT_KEEP was removed in 2.0.0; use " + EnvPrefix + "LOGGING_KEEP",
	} {
		_, _, err := load(t, fixture.path, []string{variable}, Overrides{})
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: error = %v, want it to contain %q", variable, err, want)
		}
	}
}

// Every device rule, with the exact message an operator reads.
func TestDeviceRejections(t *testing.T) {
	cases := []struct {
		name   string
		change func(*Device)
		want   string
	}{
		{"id with capitals", func(d *Device) { d.ID = "Scanner-1" },
			`devices[0].id "Scanner-1" must match [a-z0-9-]+; it is a level of the device's topics`},
		{"id with underscore", func(d *Device) { d.ID = "scanner_1" },
			`devices[0].id "scanner_1" must match [a-z0-9-]+; it is a level of the device's topics`},
		{"id agent", func(d *Device) { d.ID = "agent" },
			`devices[0].id "agent" is reserved for the agent's own topics`},
		{"id missing", func(d *Device) { d.ID = "" },
			`devices[0].id is required`},
		{"kind hid", func(d *Device) { d.Kind = KindHID },
			`devices.d.kind hid is not implemented; configure the device into USB-CDC mode instead`},
		{"kind unknown", func(d *Device) { d.Kind = "usb" },
			`devices.d.kind "usb" is unknown; expected serial`},
		{"path missing", func(d *Device) { d.Path = "" },
			`devices.d.path is required`},
		{"path relative", func(d *Device) { d.Path = "dev/ttyACM0" },
			`devices.d.path "dev/ttyACM0" must be absolute`},
		{"path macOS callin", func(d *Device) { d.Path = "/dev/tty.usbmodem1" },
			`devices.d.path "/dev/tty.usbmodem1" is a macOS callin device; opening it blocks on carrier detect forever. Use the matching /dev/cu.* callout device`},
		{"baud zero", func(d *Device) { d.Baud = 0 },
			`devices.d.baud must be positive, got 0`},
		{"data bits 9", func(d *Device) { d.DataBits = 9 },
			`devices.d.data_bits must be 5, 6, 7 or 8, got 9`},
		{"data bits 4", func(d *Device) { d.DataBits = 4 },
			`devices.d.data_bits must be 5, 6, 7 or 8, got 4`},
		{"parity unknown", func(d *Device) { d.Parity = "high" },
			`devices.d.parity "high" is unknown; expected none, odd, even, mark or space`},
		{"stop bits 1.5", func(d *Device) { d.StopBits = "1.5" },
			`devices.d.stop_bits 1.5 is not supported: the serial library refuses it on every Unix system; use 1 or 2`},
		{"stop bits 3", func(d *Device) { d.StopBits = "3" },
			`devices.d.stop_bits must be 1 or 2, got "3"`},
		{"separator missing", func(d *Device) { d.Separator = "" },
			`devices.d.separator is required; state it per device rather than relying on a default`},
		{"separator backslash", func(d *Device) { d.Separator = `\r` },
			`devices.d.separator "\\r" contains a literal backslash; write it as a double-quoted YAML scalar so escapes are decoded, for example separator: "\r"`},
		{"separator too long", func(d *Device) { d.Separator = "123456789" },
			`devices.d.separator is 9 bytes; expected at most 8`},
		{"max frame bytes zero", func(d *Device) { d.MaxFrameBytes = 0 },
			`devices.d.max_frame_bytes is the largest accepted payload excluding the separator and must be at least 1, got 0`},
		{"max frame bytes too large", func(d *Device) { d.MaxFrameBytes = 1<<20 + 1 },
			`devices.d.max_frame_bytes 1048577 exceeds 1048576; the limit exists to bound a stuck device`},
		{"inter char timeout zero", func(d *Device) { d.InterCharTimeout = 0 },
			`devices.d.inter_char_timeout must be positive, got 0s`},
		{"message expiry under a second", func(d *Device) { d.MessageExpiry = Duration(500 * time.Millisecond) },
			`devices.d.message_expiry must be a whole number of seconds, at least 1s, got 500ms; MQTT carries the message expiry in whole seconds`},
		{"message expiry with a fraction", func(d *Device) { d.MessageExpiry = Duration(1500 * time.Millisecond) },
			`devices.d.message_expiry must be a whole number of seconds, at least 1s, got 1.5s; MQTT carries the message expiry in whole seconds`},
		{"device type blank", func(d *Device) { d.DeviceType = "  " },
			`devices.d.device_type is blank; leave it out instead`},
		{"device type too long", func(d *Device) { d.DeviceType = strings.Repeat("x", 65) },
			`devices.d.device_type is 65 bytes; expected at most 64`},
		{"device type with a newline", func(d *Device) { d.DeviceType = "a\nb" },
			`devices.d.device_type "a\nb" contains a control character`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			device := validDevice("/dev/serial/by-id/usb-x-if00")
			tc.change(&device)
			_, err := ValidateDevice(device)
			if err == nil {
				t.Fatalf("accepted, want %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v\nwant it to contain %q", err, tc.want)
			}
		})
	}
	// And the device these cases start from is accepted as it is.
	if _, err := ValidateDevice(validDevice("/dev/serial/by-id/usb-x-if00")); err != nil {
		t.Fatalf("the base device is rejected: %v", err)
	}
}

// The settings every rule accepts at their edges.
func TestDeviceAcceptsEveryPortSetting(t *testing.T) {
	for _, change := range []func(*Device){
		func(d *Device) { d.DataBits = 5 },
		func(d *Device) { d.DataBits = 7 },
		func(d *Device) { d.StopBits = StopBitsTwo },
		func(d *Device) { d.MessageExpiry = Duration(time.Second) },
		func(d *Device) { d.DeviceType = strings.Repeat("x", 64) },
		func(d *Device) { d.DeviceType = "zebra-zt410" },
	} {
		device := validDevice("/dev/serial/by-id/usb-x-if00")
		change(&device)
		if _, err := ValidateDevice(device); err != nil {
			t.Errorf("%+v rejected: %v", device, err)
		}
	}
	for _, parity := range Parities {
		device := validDevice("/dev/serial/by-id/usb-x-if00")
		device.Parity = parity
		if _, err := ValidateDevice(device); err != nil {
			t.Errorf("parity %s rejected: %v", parity, err)
		}
	}
}

// YAML reads stop_bits: 2 as a number and stop_bits: "2" as a string; both mean
// the same, and 1.5 reaches validation as written, to be named there.
func TestStopBitsDecodesAsWritten(t *testing.T) {
	for written, want := range map[string]StopBits{`2`: "2", `"2"`: "2", `1`: "1", `1.5`: "1.5"} {
		body := strings.Replace(validConfig, "    stop_bits: 1", "    stop_bits: "+written, 1)
		cfg, _, err := Load(Options{Path: newFixture(t, body).path, Environ: noEnv, SkipValidate: true})
		if err != nil {
			t.Errorf("stop_bits: %s: %v", written, err)
			continue
		}
		if got := cfg.Devices[0].StopBits; got != want {
			t.Errorf("stop_bits: %s decoded to %q, want %q", written, got, want)
		}
	}
	_, _, err := Load(Options{Path: newFixture(t, strings.Replace(validConfig, "    stop_bits: 1", "    stop_bits: [1]", 1)).path, Environ: noEnv})
	if err == nil || !strings.Contains(err.Error(), "expected 1 or 2, got a sequence") {
		t.Errorf("stop_bits: [1]: error = %v, want it refused as a sequence", err)
	}
}

func TestStatusRejections(t *testing.T) {
	cases := []struct {
		status Status
		want   string
	}{
		{Status{KeepaliveInterval: 0, MissedKeepalives: 3, EventBufferSize: 64},
			`status.keepalive_interval must be a whole number of seconds, at least 1s, got 0s; each keepalive publishes it in whole seconds`},
		{Status{KeepaliveInterval: Duration(1500 * time.Millisecond), MissedKeepalives: 3, EventBufferSize: 64},
			`status.keepalive_interval must be a whole number of seconds, at least 1s, got 1.5s; each keepalive publishes it in whole seconds`},
		{Status{KeepaliveInterval: Duration(15 * time.Second), MissedKeepalives: 0, EventBufferSize: 64},
			`status.missed_keepalives must be at least 1, got 0`},
		{Status{KeepaliveInterval: Duration(15 * time.Second), MissedKeepalives: 3, EventBufferSize: 0},
			`status.event_buffer_size must be at least 1, got 0`},
	}
	for _, tc := range cases {
		problems := validateStatus(tc.status)
		if len(problems) != 1 || problems[0].Error() != tc.want {
			t.Errorf("%+v: problems = %v, want exactly %q", tc.status, problems, tc.want)
		}
	}
	if problems := validateStatus(Status{KeepaliveInterval: Duration(time.Second), MissedKeepalives: 1, EventBufferSize: 1}); len(problems) > 0 {
		t.Errorf("1s, 1 and 1 rejected: %v", problems)
	}
}

func TestLoggingRejections(t *testing.T) {
	base := func() Logging {
		return Logging{Level: "info", File: filepath.Join(t.TempDir(), "agent.log"), MaxSizeMB: 64, Keep: 7}
	}
	cases := []struct {
		change func(*Logging)
		want   string
	}{
		{func(l *Logging) { l.Level = "verbose" }, `logging.level "verbose" is unknown; expected debug, info, warn or error`},
		{func(l *Logging) { l.File = "agent.log" }, `logging.file "agent.log" must be absolute`},
		{func(l *Logging) { l.File = "/nonexistent-directory-for-tests/agent.log" }, `logging.file directory /nonexistent-directory-for-tests: `},
		{func(l *Logging) { l.MaxSizeMB = 0 }, `logging.max_size_mb must be at least 1, got 0`},
		{func(l *Logging) { l.Keep = -1 }, `logging.keep must not be negative, got -1`},
	}
	for _, tc := range cases {
		logging := base()
		tc.change(&logging)
		problems := validateLogging(logging)
		if len(problems) != 1 || !strings.HasPrefix(problems[0].Error(), tc.want) {
			t.Errorf("%+v: problems = %v, want one starting %q", logging, problems, tc.want)
		}
	}
}

// Where the agent logs, if anywhere, is the operator's business (#23 Q11a): no
// file and no stdout is a valid configuration.
func TestLoggingNeedsNoDestination(t *testing.T) {
	body := strings.Replace(validConfig, "  file: {{log}}\n", "", 1)
	body = strings.Replace(body, "  stdout: true", "  stdout: false", 1)
	cfg, _, err := load(t, newFixture(t, body).path, noEnv(), Overrides{})
	if err != nil {
		t.Fatalf("a configuration that logs nowhere was rejected: %v", err)
	}
	if cfg.Logging.File != "" || cfg.Logging.Stdout {
		t.Errorf("file %q, stdout %t, want neither", cfg.Logging.File, cfg.Logging.Stdout)
	}
}

// The sample shipped in every release and image validates. Its two paths that
// must exist on a station are pointed at files this test creates; the test
// fails if the sample stops naming them.
func TestSampleConfigValidates(t *testing.T) {
	body, err := os.ReadFile(filepath.Join("..", "..", "config.sample.yaml"))
	if err != nil {
		t.Fatalf("read the sample: %v", err)
	}
	fixture := newFixture(t, "")
	sample := string(body)
	for original, replacement := range map[string]string{
		"/etc/skuhus-device-agent/credentials":   fixture.credentialsFile,
		"/var/log/skuhus-device-agent/agent.log": fixture.logFile,
	} {
		if !strings.Contains(sample, original) {
			t.Fatalf("the sample no longer names %s; update this test with what replaced it", original)
		}
		sample = strings.ReplaceAll(sample, original, replacement)
	}
	if err := os.WriteFile(fixture.path, []byte(sample), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	cfg, warnings, err := load(t, fixture.path, noEnv(), Overrides{})
	if err != nil {
		t.Fatalf("the sample does not validate: %v", err)
	}
	if len(warnings) != 0 {
		t.Errorf("the sample produces warnings: %v", warnings)
	}
	if len(cfg.Devices) != 1 || cfg.Devices[0].Separator != "\r" {
		t.Errorf("devices = %+v, want the one sample device with a CR separator", cfg.Devices)
	}
}

// The agent tries the broker every second by default, with the backoff off,
// and a station can set another interval in its file or its environment
// (#13 Q3).
func TestReconnectEverySecondUnlessConfigured(t *testing.T) {
	withoutInterval := strings.Replace(validConfig, "  reconnect_interval: 1s\n", "", 1)
	if withoutInterval == validConfig {
		t.Fatal("validConfig no longer sets reconnect_interval; this test needs a file without it")
	}
	cfg, _, err := load(t, newFixture(t, withoutInterval).path, noEnv(), Overrides{})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := cfg.Broker.ReconnectInterval.Duration(); got != time.Second {
		t.Errorf("default reconnect_interval = %s, want 1s", got)
	}
	if cfg.Broker.ReconnectBackoff.Enabled {
		t.Error("the backoff is on by default, want off")
	}

	set := strings.Replace(validConfig, "  reconnect_interval: 1s\n",
		"  reconnect_interval: 5s\n  reconnect_backoff: { enabled: true, max: 2m }\n", 1)
	cfg, _, err = load(t, newFixture(t, set).path, noEnv(), Overrides{})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	backoff := cfg.Broker.ReconnectBackoff
	if cfg.Broker.ReconnectInterval.Duration() != 5*time.Second || !backoff.Enabled ||
		backoff.Max.Duration() != 2*time.Minute || backoff.Jitter != DefaultBackoffJitter {
		t.Errorf("reconnect_interval %s, reconnect_backoff %+v; want 5s, enabled, max 2m, the default jitter",
			cfg.Broker.ReconnectInterval, backoff)
	}

	cfg, _, err = load(t, newFixture(t, validConfig).path, []string{EnvPrefix + "BROKER_RECONNECT_INTERVAL=3s"}, Overrides{})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := cfg.Broker.ReconnectInterval.Duration(); got != 3*time.Second {
		t.Errorf("reconnect_interval from the environment = %s, want 3s", got)
	}
}

// Max and jitter are checked only while the backoff is on, since off they do
// nothing; the interval is always checked.
func TestReconnectSettingsAreValidated(t *testing.T) {
	cases := []struct {
		name, settings, want string
	}{
		{"zero interval", "  reconnect_interval: 0s\n",
			"broker.reconnect_interval and broker.reconnect_backoff: the interval must be positive, got 0s"},
		{"max below the interval", "  reconnect_interval: 5s\n  reconnect_backoff: { enabled: true, max: 1s }\n",
			"broker.reconnect_interval and broker.reconnect_backoff: max (1s) must be at least the interval (5s)"},
		{"jitter above 1", "  reconnect_interval: 1s\n  reconnect_backoff: { enabled: true, jitter: 1.5 }\n",
			"broker.reconnect_interval and broker.reconnect_backoff: jitter must be between 0 and 1, got 1.5"},
		{"backoff off ignores max and jitter", "  reconnect_interval: 5s\n  reconnect_backoff: { enabled: false, max: 1s, jitter: 1.5 }\n", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := strings.Replace(validConfig, "  reconnect_interval: 1s\n", tc.settings, 1)
			_, _, err := load(t, newFixture(t, body).path, noEnv(), Overrides{})
			switch {
			case tc.want == "" && err != nil:
				t.Errorf("rejected: %v", err)
			case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)):
				t.Errorf("error = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}

// A file that still has 0.3.0's connect_backoff is refused with its
// replacement named, rather than reconnecting on a schedule nobody chose.
func TestConnectBackoffIsRefusedNamingItsReplacement(t *testing.T) {
	body := strings.Replace(validConfig, "  reconnect_interval: 1s\n", "  connect_backoff: { initial: 1s, max: 60s, jitter: 0.3 }\n", 1)
	_, _, err := load(t, newFixture(t, body).path, noEnv(), Overrides{})
	if err == nil {
		t.Fatal("broker.connect_backoff was accepted")
	}
	want := "broker.connect_backoff was removed in 2.0.0; use broker.reconnect_interval, and broker.reconnect_backoff to make the wait grow"
	if !strings.Contains(err.Error(), want) {
		t.Errorf("error = %v, want it to contain %q", err, want)
	}
}

// A tx that finds its port closed waits about 3 s by default, and a device can
// set its own bounds.
func TestTxOpenSettings(t *testing.T) {
	cfg, _, err := load(t, newFixture(t, validConfig).path, noEnv(), Overrides{})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if device := cfg.Devices[0]; device.TxOpenAttempts != 3 || device.TxOpenInterval.Duration() != time.Second {
		t.Errorf("defaults: tx_open_attempts %d, tx_open_interval %s; want 3 and 1s", device.TxOpenAttempts, device.TxOpenInterval)
	}
	set := strings.Replace(validConfig, "    message_expiry: 30s\n", "    message_expiry: 30s\n    tx_open_attempts: 5\n    tx_open_interval: 250ms\n", 1)
	if set == validConfig {
		t.Fatal("validConfig no longer sets message_expiry; this test needs to add to a device")
	}
	cfg, _, err = load(t, newFixture(t, set).path, noEnv(), Overrides{})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if device := cfg.Devices[0]; device.TxOpenAttempts != 5 || device.TxOpenInterval.Duration() != 250*time.Millisecond {
		t.Errorf("set: tx_open_attempts %d, tx_open_interval %s; want 5 and 250ms", device.TxOpenAttempts, device.TxOpenInterval)
	}
	for settings, want := range map[string]string{
		"    tx_open_attempts: -1\n":  "devices.scanner-main.tx_open_attempts must be at least 1, got -1",
		"    tx_open_interval: -1s\n": "devices.scanner-main.tx_open_interval must be positive, got -1s",
	} {
		body := strings.Replace(validConfig, "    message_expiry: 30s\n", "    message_expiry: 30s\n"+settings, 1)
		_, _, err := load(t, newFixture(t, body).path, noEnv(), Overrides{})
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%q: error = %v, want %q", strings.TrimSpace(settings), err, want)
		}
	}
}
