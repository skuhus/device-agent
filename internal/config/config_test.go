package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fixture writes a config file plus the files validation insists on, and
// returns the config path. Placeholders let a test vary one section.
type fixture struct {
	dir             string
	path            string
	credentialsFile string
	auditFile       string
}

func newFixture(t *testing.T, body string) fixture {
	t.Helper()
	dir := t.TempDir()
	fixture := fixture{
		dir:             dir,
		path:            filepath.Join(dir, "config.yaml"),
		credentialsFile: filepath.Join(dir, "credentials"),
		auditFile:       filepath.Join(dir, "audit.log"),
	}
	if err := os.WriteFile(fixture.credentialsFile, []byte("username=pack-03\npassword=secret\n"), 0o600); err != nil {
		t.Fatalf("write credentials: %v", err)
	}
	body = strings.ReplaceAll(body, "{{credentials}}", fixture.credentialsFile)
	body = strings.ReplaceAll(body, "{{audit}}", fixture.auditFile)
	if err := os.WriteFile(fixture.path, []byte(body), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return fixture
}

const validConfig = `
identity:
  project: acme
  site: vasby
  station: pack-03

broker:
  url: tls://mq.internal:8883
  credentials_file: {{credentials}}
  keepalive: 30s
  connect_backoff: { initial: 1s, max: 60s, jitter: 0.3 }

devices:
  - id: scanner-main
    kind: serial
    path: /dev/serial/by-id/usb-Honeywell_1470g-if00
    baud: 9600
    terminator: "\r"
    max_frame_bytes: 4096
    inter_char_timeout: 200ms

delivery:
  scan_ttl: 30s
  publish_timeout: 2s
  buffer_size: 64

logging:
  level: info
  log_payloads: false
  audit_file: {{audit}}
  audit_max_size_mb: 64
  audit_keep: 7
`

func noEnv() []string { return nil }

func load(t *testing.T, path string, env []string, overrides Overrides) (*Config, []Warning, error) {
	t.Helper()
	return Load(Options{
		Path:      path,
		Environ:   func() []string { return env },
		Overrides: overrides,
	})
}

func TestLoadValidConfig(t *testing.T) {
	fixture := newFixture(t, validConfig)
	cfg, warnings, err := load(t, fixture.path, noEnv(), Overrides{})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(warnings) != 0 {
		t.Errorf("unexpected warnings: %v", warnings)
	}
	if cfg.Identity.Station != "pack-03" {
		t.Errorf("station = %q, want pack-03", cfg.Identity.Station)
	}
	if len(cfg.Devices) != 1 {
		t.Fatalf("got %d devices, want 1", len(cfg.Devices))
	}
	d := cfg.Devices[0]
	if d.Terminator != "\r" {
		t.Errorf("terminator = %q, want a carriage return", d.Terminator)
	}
	if d.InterCharTimeout.Duration() != 200*time.Millisecond {
		t.Errorf("inter_char_timeout = %s, want 200ms", d.InterCharTimeout)
	}
	if cfg.Delivery.ScanTTL.Duration() != 30*time.Second {
		t.Errorf("scan_ttl = %s, want 30s", cfg.Delivery.ScanTTL)
	}
}

// A device entry that sets only what it must gets the documented defaults.
func TestLoadAppliesDefaults(t *testing.T) {
	fixture := newFixture(t, `
identity: { project: acme, site: vasby, station: pack-03 }
broker:
  url: tls://mq.internal:8883
  credentials_file: {{credentials}}
devices:
  - id: scanner-main
    path: /dev/serial/by-id/usb-scanner-if00
    terminator: "\r"
logging:
  audit_file: {{audit}}
`)
	cfg, _, err := load(t, fixture.path, noEnv(), Overrides{})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	d := cfg.Devices[0]
	checks := []struct {
		name string
		got  any
		want any
	}{
		{"kind", d.Kind, KindSerial},
		{"baud", d.Baud, DefaultBaud},
		{"max_frame_bytes", d.MaxFrameBytes, DefaultMaxFrameBytes},
		{"inter_char_timeout", d.InterCharTimeout.Duration(), DefaultInterCharTimeout},
		{"keepalive", cfg.Broker.Keepalive.Duration(), DefaultKeepalive},
		{"connect_backoff.initial", cfg.Broker.ConnectBackoff.Initial.Duration(), DefaultBackoffInitial},
		{"connect_backoff.max", cfg.Broker.ConnectBackoff.Max.Duration(), DefaultBackoffMax},
		{"connect_backoff.jitter", cfg.Broker.ConnectBackoff.Jitter, DefaultBackoffJitter},
		{"scan_ttl", cfg.Delivery.ScanTTL.Duration(), DefaultScanTTL},
		{"publish_timeout", cfg.Delivery.PublishTimeout.Duration(), DefaultPublishTimeout},
		{"buffer_size", cfg.Delivery.BufferSize, DefaultBufferSize},
		{"logging.level", cfg.Logging.Level, DefaultLogLevel},
		{"logging.log_payloads", cfg.Logging.LogPayloads, false},
		{"audit_max_size_mb", cfg.Logging.AuditMaxSizeMB, DefaultAuditMaxSizeMB},
		{"audit_keep", cfg.Logging.AuditKeep, DefaultAuditKeep},
	}
	for _, cfg := range checks {
		if cfg.got != cfg.want {
			t.Errorf("%s = %v, want %v", cfg.name, cfg.got, cfg.want)
		}
	}
}

// Flags beat the environment, which beats the file, which beats the defaults.
func TestLoadPrecedence(t *testing.T) {
	fixture := newFixture(t, validConfig)

	fromEnv := []string{
		EnvPrefix + "IDENTITY_STATION=from-env",
		EnvPrefix + "LOGGING_LEVEL=warn",
	}
	cfg, _, err := load(t, fixture.path, fromEnv, Overrides{})
	if err != nil {
		t.Fatalf("Load with env: %v", err)
	}
	if cfg.Identity.Station != "from-env" {
		t.Errorf("station = %q, want the environment to beat the file", cfg.Identity.Station)
	}
	if cfg.Logging.Level != "warn" {
		t.Errorf("level = %q, want warn", cfg.Logging.Level)
	}

	station, level := "from-flag", "debug"
	cfg, _, err = load(t, fixture.path, fromEnv, Overrides{Station: &station, LogLevel: &level})
	if err != nil {
		t.Fatalf("Load with env and flags: %v", err)
	}
	if cfg.Identity.Station != "from-flag" {
		t.Errorf("station = %q, want the flag to beat the environment", cfg.Identity.Station)
	}
	if cfg.Logging.Level != "debug" {
		t.Errorf("level = %q, want debug", cfg.Logging.Level)
	}
}

// An unset flag must not overwrite anything, which is why Overrides holds
// pointers rather than values.
func TestLoadUnsetFlagDoesNotOverride(t *testing.T) {
	fixture := newFixture(t, validConfig)
	cfg, _, err := load(t, fixture.path, noEnv(), Overrides{Project: nil})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Identity.Project != "acme" {
		t.Errorf("project = %q, want acme", cfg.Identity.Project)
	}
}

// A misspelled key that is silently ignored leaves a station running settings
// nobody chose.
func TestLoadRejectsUnknownKey(t *testing.T) {
	fixture := newFixture(t, strings.Replace(validConfig, "  log_payloads: false", "  log_payload: false", 1))
	_, _, err := load(t, fixture.path, noEnv(), Overrides{})
	if err == nil {
		t.Fatal("an unknown key should be rejected")
	}
	if !strings.Contains(err.Error(), "log_payload") {
		t.Errorf("error does not name the offending key: %v", err)
	}
}

func TestLoadRejectsUnknownEnvironmentVariable(t *testing.T) {
	fixture := newFixture(t, validConfig)
	_, _, err := load(t, fixture.path, []string{EnvPrefix + "IDENTITY_STATON=typo"}, Overrides{})
	if err == nil {
		t.Fatal("a misspelled SH_DEV_AGENT_ variable should be rejected")
	}
	if !strings.Contains(err.Error(), "IDENTITY_STATON") {
		t.Errorf("error does not name the variable: %v", err)
	}
	if !strings.Contains(err.Error(), "IDENTITY_STATION") {
		t.Errorf("error does not list the recognised variables: %v", err)
	}
}

// A station upgraded without editing its unit file still sets variables with an
// earlier prefix. Ignoring them would run it on the file's values in silence;
// SKUHUS_AGENT_ shipped in 0.1.0 and SH_DEV_SER_SCANNER_ in 0.2.0 and 0.3.0.
func TestLoadRejectsEnvironmentFromEarlierReleases(t *testing.T) {
	fixture := newFixture(t, validConfig)
	env := []string{
		"SH_DEV_SER_SCANNER_IDENTITY_STATION=pack-04",
		"SKUHUS_AGENT_MQTT_PASSWORD=hunter2",
		EnvPrefix + "IDENTITY_SITE=vasby",
	}
	_, _, err := load(t, fixture.path, env, Overrides{})
	if err == nil {
		t.Fatal("variables with an earlier release's prefix were accepted")
	}
	for _, want := range []string{
		"SH_DEV_SER_SCANNER_IDENTITY_STATION", EnvPrefix + "IDENTITY_STATION",
		"SKUHUS_AGENT_MQTT_PASSWORD", EnvPrefix + "MQTT_PASSWORD",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not name %s: %v", want, err)
		}
	}
	if strings.Contains(err.Error(), "hunter2") {
		t.Errorf("the error repeats a variable's value, which can be a password: %v", err)
	}
}

// Every variable an earlier release defined has a current equivalent with the
// same suffix, so the replacement the error names always exists. When a later
// change removes one of these settings, as T5 does for the audit_* keys, this
// test fails on purpose: the error must then say the variable has no
// equivalent, rather than name one that does not exist.
func TestLegacyReplacementsAreRecognised(t *testing.T) {
	recognised := envTargets(&Config{})
	released := []string{
		"CONFIG", "MQTT_USERNAME", "MQTT_PASSWORD",
		"IDENTITY_PROJECT", "IDENTITY_SITE", "IDENTITY_STATION", "IDENTITY_INSTANCE",
		"BROKER_URL", "BROKER_CREDENTIALS_FILE", "BROKER_CA_FILE", "BROKER_INSECURE", "BROKER_KEEPALIVE",
		"DELIVERY_SCAN_TTL", "DELIVERY_PUBLISH_TIMEOUT", "DELIVERY_BUFFER_SIZE",
		"LOGGING_LEVEL", "LOGGING_LOG_PAYLOADS", "LOGGING_AUDIT_FILE", "LOGGING_AUDIT_MAX_SIZE_MB", "LOGGING_AUDIT_KEEP",
	}
	for _, suffix := range released {
		if _, ok := recognised[EnvPrefix+suffix]; !ok {
			t.Errorf("%s shipped in an earlier release but %s is not recognised", suffix, EnvPrefix+suffix)
		}
	}
}

// Variables not belonging to the agent must be ignored, not rejected.
func TestLoadIgnoresForeignEnvironmentVariables(t *testing.T) {
	fixture := newFixture(t, validConfig)
	_, _, err := load(t, fixture.path, []string{"PATH=/usr/bin", "HOME=/root"}, Overrides{})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
}

func TestLoadRejectsBadEnvironmentValue(t *testing.T) {
	fixture := newFixture(t, validConfig)
	_, _, err := load(t, fixture.path, []string{EnvPrefix + "BROKER_KEEPALIVE=soon"}, Overrides{})
	if err == nil {
		t.Fatal("an unparseable duration should be rejected")
	}
	if !strings.Contains(err.Error(), "BROKER_KEEPALIVE") {
		t.Errorf("error does not name the variable: %v", err)
	}
}

func TestLoadMissingFile(t *testing.T) {
	_, _, err := load(t, filepath.Join(t.TempDir(), "absent.yaml"), noEnv(), Overrides{})
	if err == nil {
		t.Fatal("a missing config file should be an error")
	}
}

func TestLoadRejectsBareNumberDuration(t *testing.T) {
	fixture := newFixture(t, strings.Replace(validConfig, "  scan_ttl: 30s", "  scan_ttl: 30", 1))
	_, _, err := load(t, fixture.path, noEnv(), Overrides{})
	if err == nil {
		t.Fatal("a bare number should not be accepted as a duration")
	}
}

// Every problem is reported, not just the first, so a misconfigured station is
// fixed in one pass.
func TestValidateReportsAllProblems(t *testing.T) {
	cfg := Defaults()
	cfg.Identity = Identity{Project: "Acme", Site: "", Station: "pack_03"}
	cfg.Devices = []Device{{ID: "d", Kind: KindSerial, Path: "/dev/x", Baud: 9600,
		Terminator: "\r", MaxFrameBytes: 4096, InterCharTimeout: Duration(time.Millisecond)}}
	cfg.Logging.AuditFile = "/nonexistent-directory-for-tests/audit.log"

	_, err := Validate(&cfg, nil)
	if err == nil {
		t.Fatal("expected validation to fail")
	}
	for _, want := range []string{"identity.project", "identity.site", "identity.station", "broker.url", "audit_file"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %s:\n%v", want, err)
		}
	}
}

func TestValidateIdentityCharacterSet(t *testing.T) {
	cases := map[string]bool{
		"acme":               true,
		"pack-03":            true,
		"a1":                 true,
		"Acme":               false,
		"pack_03":            false,
		"pack.03":            false,
		"pack 03":            false,
		"":                   false,
		"pack/03":            false,
		"pack+03":            false,
		"paket-\u00e5\u00e5": false, // a Swedish site name, rejected: the topic segment is ASCII only
	}
	for value, wantValid := range cases {
		problems := validateIdentity(Identity{Project: value, Site: "vasby", Station: "pack-03"})
		if gotValid := len(problems) == 0; gotValid != wantValid {
			t.Errorf("project %q: valid = %t, want %t (%v)", value, gotValid, wantValid, problems)
		}
	}
}

// The instance names the agent process to the broker and in every payload. It
// defaults to the station, which is what section 5.2 fixes the client id to.
func TestInstanceDefaultsToStation(t *testing.T) {
	fixture := newFixture(t, validConfig)
	cfg, _, err := load(t, fixture.path, nil, Overrides{})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Identity.Instance != cfg.Identity.Station {
		t.Errorf("instance = %q, want the station id %q", cfg.Identity.Instance, cfg.Identity.Station)
	}
}

// Two agents on one station need distinct client ids, or each disconnects the
// other from the broker.
func TestInstanceOverride(t *testing.T) {
	fixture := newFixture(t, validConfig)
	env := []string{EnvPrefix + "IDENTITY_INSTANCE=pack-03-second"}
	cfg, _, err := load(t, fixture.path, env, Overrides{})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Identity.Instance != "pack-03-second" {
		t.Errorf("instance = %q, want the environment value", cfg.Identity.Instance)
	}

	flagValue := "pack-03-third"
	cfg, _, err = load(t, fixture.path, env, Overrides{Instance: &flagValue})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Identity.Instance != flagValue {
		t.Errorf("instance = %q, want the flag to beat the environment", cfg.Identity.Instance)
	}
}

func TestInstanceCharacterSet(t *testing.T) {
	fixture := newFixture(t, validConfig)
	// Not a topic segment, so uppercase and dots are allowed; a slash is not,
	// because a client id with one is unreadable in broker tooling.
	for _, valid := range []string{"pack-03", "pack-03.b", "PACK_03", "pack-03-second"} {
		if _, _, err := load(t, fixture.path, []string{EnvPrefix + "IDENTITY_INSTANCE=" + valid}, Overrides{}); err != nil {
			t.Errorf("instance %q was rejected: %v", valid, err)
		}
	}
	for _, invalid := range []string{"pack 03", "pack/03", strings.Repeat("a", 65)} {
		_, _, err := load(t, fixture.path, []string{EnvPrefix + "IDENTITY_INSTANCE=" + invalid}, Overrides{})
		if err == nil {
			t.Errorf("instance %q was accepted", invalid)
			continue
		}
		if !strings.Contains(err.Error(), "identity.instance") {
			t.Errorf("instance %q error = %v, want it to name the field", invalid, err)
		}
	}
}

// Section 8 keeps credentials out of anything another user can read. A URL is
// visible in the process list, in every log line naming the broker, and in a
// config file pasted into a ticket.
func TestValidateRejectsCredentialsInBrokerURL(t *testing.T) {
	fixture := newFixture(t, strings.Replace(validConfig,
		"url: tls://mq.internal:8883", "url: tls://pack-03:hunter2@mq.internal:8883", 1))
	_, _, err := load(t, fixture.path, nil, Overrides{})
	if err == nil {
		t.Fatal("a broker URL carrying credentials should be rejected")
	}
	if !strings.Contains(err.Error(), "credentials_file") {
		t.Errorf("error should point at the credentials file: %v", err)
	}
	// The rejection must not quote the URL back, or the password lands in the
	// log of whoever ran validate.
	if strings.Contains(err.Error(), "hunter2") {
		t.Errorf("the error message repeats the password: %v", err)
	}
}

func TestRedactedURL(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want string
	}{
		{"no credentials", "tls://mq.internal:8883", "tls://mq.internal:8883"},
		{"password", "tls://pack-03:hunter2@mq.internal:8883", "tls://pack-03:xxxxx@mq.internal:8883"},
		{"unparseable", "://nope", "://nope"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := (Broker{URL: tc.url}).RedactedURL(); got != tc.want {
				t.Errorf("RedactedURL() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestValidateRejectsHIDDevice(t *testing.T) {
	fixture := newFixture(t, strings.Replace(validConfig, "    kind: serial", "    kind: hid", 1))
	_, _, err := load(t, fixture.path, noEnv(), Overrides{})
	if err == nil {
		t.Fatal("hid should be rejected in v1")
	}
	if !strings.Contains(err.Error(), "not implemented in v1") {
		t.Errorf("error should say hid is not implemented: %v", err)
	}
	if !strings.Contains(err.Error(), "USB-CDC") {
		t.Errorf("error should point at the supported mode: %v", err)
	}
}

// Opening a macOS callin device blocks on carrier detect forever, so it is
// rejected before the agent ever tries.
func TestValidateRejectsMacOSCallinDevice(t *testing.T) {
	warnings, err := ValidateDevice(Device{
		ID: "d", Kind: KindSerial, Path: "/dev/tty.usbmodem1234", Baud: 9600,
		Terminator: "\r", MaxFrameBytes: 4096, InterCharTimeout: Duration(200 * time.Millisecond),
	})
	if err == nil {
		t.Fatal("a /dev/tty.* path should be rejected")
	}
	if !strings.Contains(err.Error(), "/dev/cu.") {
		t.Errorf("error should name the callout device to use instead: %v", err)
	}
	if len(warnings) != 0 {
		t.Errorf("unexpected warnings: %v", warnings)
	}
}

// A kernel-assigned name works, but it moves between reboots, so it warns.
func TestValidateWarnsOnUnstableDevicePath(t *testing.T) {
	warnings, err := ValidateDevice(Device{
		ID: "d", Kind: KindSerial, Path: "/dev/ttyACM0", Baud: 9600,
		Terminator: "\r", MaxFrameBytes: 4096, InterCharTimeout: Duration(200 * time.Millisecond),
	})
	if err != nil {
		t.Fatalf("a kernel-assigned name should be usable: %v", err)
	}
	if len(warnings) != 1 {
		t.Fatalf("got %d warnings %v, want 1", len(warnings), warnings)
	}
	if !strings.Contains(warnings[0].Message, "by-id") {
		t.Errorf("warning should point at the stable path: %v", warnings[0])
	}
}

func TestValidateAcceptsStableDevicePaths(t *testing.T) {
	for _, path := range []string{
		"/dev/serial/by-id/usb-Honeywell_1470g-if00",
		"/dev/serial/by-path/pci-0000:01:00.0-usb-0:1.2:1.0-port0",
		"/dev/scanner-left",
		"/dev/cu.usbmodem1234",
	} {
		warnings, err := ValidateDevice(Device{
			ID: "d", Kind: KindSerial, Path: path, Baud: 9600,
			Terminator: "\r", MaxFrameBytes: 4096, InterCharTimeout: Duration(200 * time.Millisecond),
		})
		if err != nil {
			t.Errorf("path %q rejected: %v", path, err)
		}
		if len(warnings) != 0 {
			t.Errorf("path %q warned: %v", path, warnings)
		}
	}
}

// terminator: \r without quotes is the two characters backslash and r. It has
// to be caught, because it produces a device that silently never frames.
func TestValidateCatchesUnquotedTerminator(t *testing.T) {
	fixture := newFixture(t, strings.Replace(validConfig, `    terminator: "\r"`, `    terminator: '\r'`, 1))
	_, _, err := load(t, fixture.path, noEnv(), Overrides{})
	if err == nil {
		t.Fatal("a literal backslash in the terminator should be rejected")
	}
	if !strings.Contains(err.Error(), "double-quoted") {
		t.Errorf("error should explain the YAML quoting: %v", err)
	}
}

func TestValidateRejectsDuplicateDeviceIDs(t *testing.T) {
	fixture := newFixture(t, `
identity: { project: acme, site: vasby, station: pack-03 }
broker:
  url: tls://mq.internal:8883
  credentials_file: {{credentials}}
devices:
  - id: scanner-main
    path: /dev/serial/by-id/usb-Honeywell_1470g-if00
    terminator: "\r"
  - id: scanner-main
    path: /dev/serial/by-id/usb-other-if00
    terminator: "\r"
logging:
  audit_file: {{audit}}
`)
	_, _, err := load(t, fixture.path, noEnv(), Overrides{})
	if err == nil {
		t.Fatal("duplicate device ids should be rejected")
	}
	if !strings.Contains(err.Error(), "duplicates") {
		t.Errorf("error should say the id is duplicated: %v", err)
	}
}

func TestValidateBrokerScheme(t *testing.T) {
	cases := []struct {
		url      string
		insecure bool
		wantErr  bool
	}{
		{"tls://mq.internal:8883", false, false},
		{"mqtts://mq.internal:8883", false, false},
		{"ssl://mq.internal:8883", false, false},
		{"tcp://mq.internal:1883", false, true},
		{"mqtt://mq.internal:1883", false, true},
		{"tcp://mq.internal:1883", true, false},
		{"ftp://mq.internal:21", false, true},
		{"tls://", false, true},
	}
	for _, tc := range cases {
		name := tc.url
		if tc.insecure {
			name += " insecure"
		}
		t.Run(name, func(t *testing.T) {
			problems, _ := validateBroker(Broker{
				URL: tc.url, Insecure: tc.insecure, CredentialsFile: "",
				Keepalive:      Duration(DefaultKeepalive),
				ConnectBackoff: Backoff{Initial: Duration(time.Second), Max: Duration(time.Minute), Jitter: 0.3},
			}, map[string]string{EnvMQTTPassword: "x"})
			if gotErr := len(problems) > 0; gotErr != tc.wantErr {
				t.Errorf("error = %t, want %t (%v)", gotErr, tc.wantErr, problems)
			}
		})
	}
}

// Plaintext is allowed only when it is asked for explicitly, and it still warns
// on every load.
func TestValidatePlaintextBrokerWarnsWhenAllowed(t *testing.T) {
	problems, warnings := validateBroker(Broker{
		URL: "tcp://mq.internal:1883", Insecure: true,
		Keepalive:      Duration(DefaultKeepalive),
		ConnectBackoff: Backoff{Initial: Duration(time.Second), Max: Duration(time.Minute), Jitter: 0.3},
	}, map[string]string{EnvMQTTPassword: "x"})
	if len(problems) > 0 {
		t.Fatalf("insecure plaintext should be allowed: %v", problems)
	}
	if len(warnings) != 1 {
		t.Fatalf("got %d warnings %v, want 1", len(warnings), warnings)
	}
}

// Credentials in a world-readable file are credentials everyone on the host has.
func TestValidateRejectsReadableCredentialsFile(t *testing.T) {
	fixture := newFixture(t, validConfig)
	if err := os.Chmod(fixture.credentialsFile, 0o644); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	_, _, err := load(t, fixture.path, noEnv(), Overrides{})
	if err == nil {
		t.Fatal("a group or world readable credentials file should be rejected")
	}
	if !strings.Contains(err.Error(), "0600") {
		t.Errorf("error should say what the mode must be: %v", err)
	}
}

func TestValidateRequiresCredentials(t *testing.T) {
	fixture := newFixture(t, strings.Replace(validConfig, "  credentials_file: {{credentials}}", "", 1))
	_, _, err := load(t, fixture.path, noEnv(), Overrides{})
	if err == nil {
		t.Fatal("a broker with no credentials should be rejected")
	}
	// Supplying them through the environment instead is enough.
	_, _, err = load(t, fixture.path, []string{EnvMQTTPassword + "=secret"}, Overrides{})
	if err != nil {
		t.Errorf("credentials from the environment should be accepted: %v", err)
	}
}

// Telling the operator a scan failed after the broker already discarded it is
// worse than useless, so the two timings are checked against each other.
func TestValidateRejectsPublishTimeoutLongerThanTTL(t *testing.T) {
	problems := validateDelivery(Delivery{
		ScanTTL:        Duration(2 * time.Second),
		PublishTimeout: Duration(5 * time.Second),
		BufferSize:     64,
	})
	if len(problems) == 0 {
		t.Fatal("publish_timeout longer than scan_ttl should be rejected")
	}
	if !strings.Contains(errors.Join(problems...).Error(), "expired") {
		t.Errorf("error should explain the consequence: %v", problems)
	}
}

func TestValidateLoggingLevel(t *testing.T) {
	dir := t.TempDir()
	for _, level := range []string{"debug", "info", "warn", "error", "INFO"} {
		if problems := validateLogging(Logging{Level: level, AuditFile: filepath.Join(dir, "a.log"),
			AuditMaxSizeMB: 1, AuditKeep: 1}); len(problems) > 0 {
			t.Errorf("level %q rejected: %v", level, problems)
		}
	}
	if problems := validateLogging(Logging{Level: "verbose", AuditFile: filepath.Join(dir, "a.log"),
		AuditMaxSizeMB: 1, AuditKeep: 1}); len(problems) == 0 {
		t.Error("an unknown level should be rejected")
	}
}

// assert_config cannot be honoured without a per-model profile, so asking for
// it warns rather than passing quietly.
func TestValidateWarnsOnAssertConfig(t *testing.T) {
	warnings, err := ValidateDevice(Device{
		ID: "d", Kind: KindSerial, Path: "/dev/serial/by-id/usb-x-if00", Baud: 9600,
		Terminator: "\r", MaxFrameBytes: 4096, InterCharTimeout: Duration(200 * time.Millisecond),
		AssertConfig: true,
	})
	if err != nil {
		t.Fatalf("assert_config should not be fatal: %v", err)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0].Message, "no per-model scanner profile") {
		t.Errorf("warnings = %v, want one explaining assert_config is not implemented", warnings)
	}
}

func TestDefaultPathIsPlatformSpecific(t *testing.T) {
	got := DefaultPath()
	if got != linuxConfigPath && got != darwinConfigPath {
		t.Errorf("DefaultPath() = %q, want one of the two documented locations", got)
	}
}

func TestDurationRoundTrip(t *testing.T) {
	var d Duration
	if err := d.UnmarshalYAML(yamlScalar(t, "1500ms")); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if d.Duration() != 1500*time.Millisecond {
		t.Errorf("duration = %s, want 1.5s", d)
	}
	out, err := d.MarshalYAML()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if fmt.Sprint(out) != "1.5s" {
		t.Errorf("marshalled to %v, want 1.5s", out)
	}
}

// Two devices bound to the same port cannot both work: the library takes
// exclusive access, so the second open fails with EBUSY forever.
func TestValidateRejectsDuplicateDevicePaths(t *testing.T) {
	fixture := newFixture(t, `
identity: { project: acme, site: vasby, station: pack-03 }
broker:
  url: tls://mq.internal:8883
  credentials_file: {{credentials}}
devices:
  - id: scanner-left
    path: /dev/serial/by-id/usb-shared-if00
    terminator: "\r"
  - id: scanner-right
    path: /dev/serial/by-id/usb-shared-if00
    terminator: "\r"
logging:
  audit_file: {{audit}}
`)
	_, _, err := load(t, fixture.path, noEnv(), Overrides{})
	if err == nil {
		t.Fatal("two devices on one port should be rejected")
	}
	if !strings.Contains(err.Error(), "cannot share one port") {
		t.Errorf("error should explain why: %v", err)
	}
}

// Problems must come out in the same order every run, so a diff of validate
// output is meaningful.
func TestLoadEnvironmentErrorsAreDeterministic(t *testing.T) {
	fixture := newFixture(t, validConfig)
	env := []string{
		EnvPrefix + "DELIVERY_BUFFER_SIZE=many",
		EnvPrefix + "BROKER_KEEPALIVE=soon",
		EnvPrefix + "LOGGING_AUDIT_KEEP=lots",
	}
	var first string
	for i := 0; i < 20; i++ {
		_, _, err := load(t, fixture.path, env, Overrides{})
		if err == nil {
			t.Fatal("three unparseable values should fail")
		}
		if i == 0 {
			first = err.Error()
			continue
		}
		if err.Error() != first {
			t.Fatalf("error text varies between runs:\n%s\n---\n%s", first, err.Error())
		}
	}
}
