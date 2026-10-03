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
	// KindHID is reserved. HID keyboard mode is out of scope: it needs
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
	Status   Status   `yaml:"status"`
	Logging  Logging  `yaml:"logging"`
}

// Identity is the station identity. It comes from configuration only; the
// hostname is a logged attribute, never an identity.
type Identity struct {
	Project string `yaml:"project"`
	Site    string `yaml:"site"`
	Station string `yaml:"station"`
	// Instance names this agent process. It is the MQTT client id, the
	// instance_id field in every message and the <instance> level of the
	// agent's status topic, and it defaults to the station id.
	//
	// It is settable because a client id must be unique per broker connection:
	// two processes sharing one would repeatedly disconnect each other. A
	// second agent on the same station - a second device owned by its own
	// process, per open question 4 - needs its own value.
	Instance string `yaml:"instance"`
}

// Broker describes the MQTT connection.
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
	// ID is a level of the device's topics, so it follows the topic-level rule,
	// and "agent" is reserved for the agent's own topics.
	ID   string     `yaml:"id"`
	Kind DeviceKind `yaml:"kind"`
	// Path must be a stable device path: /dev/serial/by-id/..., a by-path
	// entry, or a udev-created symlink. /dev/ttyACM0 is not stable across
	// reboots or replug order.
	Path string `yaml:"path"`
	// Baud is ignored by USB-CDC devices and required for real RS-232, as are
	// the data bits, parity and stop bits.
	Baud     int      `yaml:"baud"`
	DataBits int      `yaml:"data_bits"`
	Parity   Parity   `yaml:"parity"`
	StopBits StopBits `yaml:"stop_bits"`
	// Separator ends each frame and is taken literally as bytes. Write it as a
	// double-quoted YAML scalar so escapes are decoded: "\r", "\r\n", "\x1e".
	Separator string `yaml:"separator"`
	// MaxFrameBytes is the largest payload accepted, excluding the separator.
	// A partial frame that grows past it is discarded rather than buffered, so
	// a stuck device cannot grow the buffer without bound.
	MaxFrameBytes    int      `yaml:"max_frame_bytes"`
	InterCharTimeout Duration `yaml:"inter_char_timeout"`
	// MessageExpiry is the MQTT message expiry of everything published about
	// this device: a reading the broker has held for longer is discarded
	// rather than delivered late.
	MessageExpiry Duration `yaml:"message_expiry"`
	// DeviceType is carried in every message about the device, for the
	// services behind the broker, and never interpreted by the agent.
	DeviceType string `yaml:"device_type"`
}

// Parity names a serial parity setting.
type Parity string

// Parities the port layer can apply. Mark and space need the Linux CMSPAR flag;
// elsewhere the port refuses them when it opens.
const (
	ParityNone  Parity = "none"
	ParityOdd   Parity = "odd"
	ParityEven  Parity = "even"
	ParityMark  Parity = "mark"
	ParitySpace Parity = "space"
)

// Parities lists every accepted parity, in the order errors name them.
var Parities = []Parity{ParityNone, ParityOdd, ParityEven, ParityMark, ParitySpace}

// StopBits holds the stop_bits value exactly as written, so that validation
// can name an unsupported value such as 1.5 instead of failing the decode.
type StopBits string

// Stop bit settings. go.bug.st/serial refuses 1.5 on every Unix system.
const (
	StopBitsOne StopBits = "1"
	StopBitsTwo StopBits = "2"
)

// UnmarshalYAML accepts the scalar as written, whether YAML reads it as a
// number or a string.
func (stopBits *StopBits) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode {
		return fmt.Errorf("expected 1 or 2, got a %s", nodeKindName(node.Kind))
	}
	*stopBits = StopBits(node.Value)
	return nil
}

// Delivery bounds how long a reading waits for the broker. See DESIGN-V2.md,
// "Scans are perishable", before changing the defaults.
type Delivery struct {
	PublishTimeout Duration `yaml:"publish_timeout"`
	BufferSize     int      `yaml:"buffer_size"`
}

// Status configures the agent's keepalive and device events (DESIGN-V2.md,
// "Message formats").
type Status struct {
	KeepaliveInterval Duration `yaml:"keepalive_interval"`
	// MissedKeepalives is how many intervals a consumer waits without a
	// keepalive before it treats the agent as gone. Every keepalive carries
	// the resulting time, so consumers apply this setting, not their own.
	MissedKeepalives int `yaml:"missed_keepalives"`
	// EventBufferSize is how many events each device keeps while they cannot
	// be published. When it is full the oldest is dropped, so a long broker
	// outage ends with the most recent events, not a flood (#13 Q1).
	EventBufferSize int `yaml:"event_buffer_size"`
}

// Logging configures the common log. It can go to a file with size rotation,
// to stdout, to both or to neither; where the agent logs is the operator's
// choice (DESIGN-V2.md, "Logging: one common log").
type Logging struct {
	Level string `yaml:"level"`
	// LogPayloads puts the data read from a port on the log lines of published
	// readings and of discarded bytes. Without it a published reading is
	// logged as an id, a device, a byte count and a validity flag, which is
	// enough to trace delivery and not enough to reconstruct a reading. A
	// reading the broker did not take carries its data regardless, because
	// nothing else holds it.
	LogPayloads bool   `yaml:"log_payloads"`
	File        string `yaml:"file"`
	MaxSizeMB   int    `yaml:"max_size_mb"`
	Keep        int    `yaml:"keep"`
	Stdout      bool   `yaml:"stdout"`
}

// Duration is a time.Duration that unmarshals from a YAML string such as "30s".
type Duration time.Duration

// UnmarshalYAML decodes a Go duration string. A bare number is rejected rather
// than assumed to be nanoseconds or seconds.
func (duration *Duration) UnmarshalYAML(node *yaml.Node) error {
	var s string
	if err := node.Decode(&s); err != nil {
		return fmt.Errorf("expected a duration string such as \"200ms\" or \"30s\": %w", err)
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*duration = Duration(parsed)
	return nil
}

// MarshalYAML renders the duration back as a string.
func (duration Duration) MarshalYAML() (any, error) { return time.Duration(duration).String(), nil }

// Duration converts to the standard library type.
func (duration Duration) Duration() time.Duration { return time.Duration(duration) }

// String renders the duration.
func (duration Duration) String() string { return time.Duration(duration).String() }

// SeparatorBytes returns the frame separator as raw bytes.
func (deviceCfg Device) SeparatorBytes() []byte { return []byte(deviceCfg.Separator) }

func nodeKindName(kind yaml.Kind) string {
	switch kind {
	case yaml.MappingNode:
		return "mapping"
	case yaml.SequenceNode:
		return "sequence"
	case yaml.AliasNode:
		return "alias"
	}
	return "document"
}
