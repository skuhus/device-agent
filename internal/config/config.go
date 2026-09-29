// Package config loads and validates the agent configuration.
//
// Precedence is CLI flags > environment (SH_DEV_AGENT_*) > config file >
// defaults. Loading is strict in both directions: an unknown key in the YAML
// file and an unrecognised SH_DEV_AGENT_* variable are both errors, because a
// typo that is silently ignored produces a station running settings nobody
// intended.
package config

import (
	"fmt"
	"net/url"
	"time"

	"gopkg.in/yaml.v3"
)

// DeviceKind identifies the transport used to talk to a physical device.
type DeviceKind string

const (
	// KindSerial is USB-CDC (virtual COM) or real RS-232.
	KindSerial DeviceKind = "serial"
	// KindHID is reserved. HID keyboard mode is out of scope for v1: it needs
	// scancode-to-character translation against an assumed keyboard layout,
	// which corrupts non-alphanumeric payloads silently when a Swedish-layout
	// host meets a US-configured scanner.
	KindHID DeviceKind = "hid"
)

// Config is the whole agent configuration.
type Config struct {
	Identity Identity `yaml:"identity"`
	Broker   Broker   `yaml:"broker"`
	Devices  []Device `yaml:"devices"`
	Delivery Delivery `yaml:"delivery"`
	Logging  Logging  `yaml:"logging"`
}

// Identity is the station identity. It comes from configuration only; the
// hostname is a logged attribute, never an identity.
type Identity struct {
	Project string `yaml:"project"`
	Site    string `yaml:"site"`
	Station string `yaml:"station"`
	// Instance names this agent process. It is the MQTT client id and the
	// instance_id field in every payload, and it defaults to the station id,
	// which is what section 5.2 fixes the client id to.
	//
	// It is settable because a client id must be unique per broker connection:
	// two processes sharing one would repeatedly disconnect each other. A
	// second agent on the same station - a second device owned by its own
	// process, per open question 4 - needs its own value.
	Instance string `yaml:"instance"`
}

// Broker describes the MQTT connection. It is validated in v1 but not yet used:
// the transport lands with M2.
type Broker struct {
	URL             string   `yaml:"url"`
	CredentialsFile string   `yaml:"credentials_file"`
	CAFile          string   `yaml:"ca_file"`
	Insecure        bool     `yaml:"insecure"`
	Keepalive       Duration `yaml:"keepalive"`
	ConnectBackoff  Backoff  `yaml:"connect_backoff"`
}

// RedactedURL is the broker URL with any credentials replaced by "xxxxx". The
// configuration layer rejects a URL carrying credentials, so this is a second
// line rather than the first: nothing that prints a broker address should be
// the reason a password reaches a log file.
func (broker Broker) RedactedURL() string {
	parsed, err := url.Parse(broker.URL)
	if err != nil {
		return broker.URL
	}
	return parsed.Redacted()
}

// Backoff is an exponential backoff schedule with proportional jitter.
type Backoff struct {
	Initial Duration `yaml:"initial"`
	Max     Duration `yaml:"max"`
	Jitter  float64  `yaml:"jitter"`
}

// Device is one physically attached device owned by this agent.
type Device struct {
	ID   string     `yaml:"id"`
	Kind DeviceKind `yaml:"kind"`
	// Path must be a stable device path: /dev/serial/by-id/..., a by-path
	// entry, or a udev-created symlink. /dev/ttyACM0 is not stable across
	// reboots or replug order.
	Path string `yaml:"path"`
	// Baud is ignored by USB-CDC devices and required for real RS-232.
	Baud int `yaml:"baud"`
	// Terminator is taken literally as bytes. Write it as a double-quoted YAML
	// scalar so escapes are decoded: "\r", "\r\n", "\x1e".
	Terminator string `yaml:"terminator"`
	// MaxFrameBytes is the largest payload accepted, excluding the terminator.
	// A partial frame that grows past it is discarded rather than buffered, so
	// a stuck device cannot grow the buffer without bound.
	MaxFrameBytes    int      `yaml:"max_frame_bytes"`
	InterCharTimeout Duration `yaml:"inter_char_timeout"`
	// AssertConfig requests that the expected scanner serial configuration be
	// re-applied on every open. No per-model profiles exist yet, so enabling it
	// currently logs that it could not be honoured. See DESIGN.md.
	AssertConfig bool `yaml:"assert_config"`
}

// Delivery controls the perishability of a scan. See DESIGN.md section
// "Scans are perishable" before changing the defaults.
type Delivery struct {
	ScanTTL        Duration `yaml:"scan_ttl"`
	PublishTimeout Duration `yaml:"publish_timeout"`
	BufferSize     int      `yaml:"buffer_size"`
}

// Logging configures the structured log and the separate audit log.
type Logging struct {
	Level string `yaml:"level"`
	// LogPayloads allows scan contents into the log at DEBUG. Default false
	// keeps payloads out of the log at every level, and the device layer
	// applies the same rule to frames and discarded bytes. Without it a scan is
	// logged as an event_id, a device, a byte count and a validity flag, which
	// is enough to trace delivery and not enough to reconstruct a barcode.
	LogPayloads    bool   `yaml:"log_payloads"`
	AuditFile      string `yaml:"audit_file"`
	AuditMaxSizeMB int    `yaml:"audit_max_size_mb"`
	AuditKeep      int    `yaml:"audit_keep"`
}

// Duration is a time.Duration that unmarshals from a YAML string such as "30s".
type Duration time.Duration

// UnmarshalYAML decodes a Go duration string. A bare number is rejected rather
// than assumed to be nanoseconds or seconds.
func (deviceCfg *Duration) UnmarshalYAML(node *yaml.Node) error {
	var s string
	if err := node.Decode(&s); err != nil {
		return fmt.Errorf("expected a duration string such as \"200ms\" or \"30s\": %w", err)
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*deviceCfg = Duration(parsed)
	return nil
}

// MarshalYAML renders the duration back as a string.
func (deviceCfg Duration) MarshalYAML() (any, error) { return time.Duration(deviceCfg).String(), nil }

// Duration converts to the standard library type.
func (deviceCfg Duration) Duration() time.Duration { return time.Duration(deviceCfg) }

// String renders the duration.
func (deviceCfg Duration) String() string { return time.Duration(deviceCfg).String() }

// TerminatorBytes returns the frame terminator as raw bytes.
func (deviceCfg Device) TerminatorBytes() []byte { return []byte(deviceCfg.Terminator) }
