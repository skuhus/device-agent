package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode"
)

// Warning is a non-fatal configuration problem. Warnings do not stop the agent;
// they are logged at WARN and printed by the validate subcommand.
type Warning struct {
	Field   string
	Message string
}

func (warning Warning) String() string { return warning.Field + ": " + warning.Message }

var (
	// topicSegment is the rule every topic level taken from configuration
	// follows: the identity, the instance and each device id (#23 Q1, #11 Q2).
	topicSegment = regexp.MustCompile(`^[a-z0-9-]+$`)
	// unstableDevPath matches kernel-assigned names that move between reboots
	// and replug order.
	unstableDevPath = regexp.MustCompile(`^/dev/tty(ACM|USB|S|AMA)[0-9]+$`)

	tlsSchemes       = map[string]bool{"tls": true, "ssl": true, "mqtts": true, "wss": true}
	plaintextSchemes = map[string]bool{"tcp": true, "mqtt": true, "ws": true}
)

// Validate checks the whole configuration and reports every problem it finds.
//
// env is consulted only to decide whether broker credentials are supplied
// without a credentials file; pass nil when that does not apply.
func Validate(cfg *Config, env map[string]string) ([]Warning, error) {
	var problems []error
	var warnings []Warning

	problems = append(problems, validateIdentity(cfg.Identity)...)

	brokerProblems, brokerWarnings := validateBroker(cfg.Broker, env)
	problems, warnings = append(problems, brokerProblems...), append(warnings, brokerWarnings...)

	deviceProblems, deviceWarnings := validateDevices(cfg.Devices)
	problems, warnings = append(problems, deviceProblems...), append(warnings, deviceWarnings...)
	problems = append(problems, validateDelivery(cfg.Delivery, cfg.Devices)...)
	problems = append(problems, validateStatus(cfg.Status)...)
	problems = append(problems, validateLogging(cfg.Logging)...)

	return warnings, errors.Join(problems...)
}

func validateIdentity(identity Identity) []error {
	var problems []error
	for _, field := range []struct{ name, value string }{
		{"identity.project", identity.Project},
		{"identity.site", identity.Site},
		{"identity.station", identity.Station},
	} {
		switch {
		case field.value == "":
			problems = append(problems, fmt.Errorf("%s is required; station identity comes from config, never from the hostname", field.name))
		case !topicSegment.MatchString(field.value):
			problems = append(problems, fmt.Errorf("%s %q must match [a-z0-9-]+; it is used verbatim as an MQTT topic segment", field.name, field.value))
		}
	}
	// The instance is a level of the agent's status topic, so it follows the
	// same rule. v1 allowed [A-Za-z0-9._-] because there it was only the MQTT
	// client id; it still is that, which is where the length limit comes from.
	switch {
	case identity.Instance == "":
		// Empty means it takes the station id, which is checked above. Load
		// fills that in before validating, so this only happens when Validate
		// is called on a configuration assembled by hand.
	case !topicSegment.MatchString(identity.Instance) || len(identity.Instance) > 64:
		problems = append(problems, fmt.Errorf(
			"identity.instance %q must match [a-z0-9-]+ and be at most 64 characters; it is a level of the agent's status topic and the MQTT client id", identity.Instance))
	}
	return problems
}

func validateBroker(broker Broker, env map[string]string) ([]error, []Warning) {
	var problems []error
	var warnings []Warning

	if broker.URL == "" {
		problems = append(problems, errors.New("broker.url is required"))
	} else if parsed, err := url.Parse(broker.URL); err != nil {
		problems = append(problems, fmt.Errorf("broker.url %q is not a valid URL: %w", broker.URL, err))
	} else {
		// Checked separately from the scheme, because a URL can be wrong in
		// both ways at once and an operator should hear about both.
		if parsed.User != nil {
			// Section 8 keeps credentials in a mode 0600 file or in the
			// environment. In a URL they reach the process list, every log line
			// that reports the broker, and any config file attached to a
			// ticket.
			problems = append(problems, errors.New(
				"broker.url carries credentials; move them to broker.credentials_file or to "+
					EnvMQTTUsername+" and "+EnvMQTTPassword))
		}
		switch {
		case parsed.Host == "":
			problems = append(problems, fmt.Errorf("broker.url %q has no host", broker.URL))
		case tlsSchemes[parsed.Scheme]:
			if broker.Insecure {
				warnings = append(warnings, Warning{"broker.insecure",
					"set with a TLS scheme; certificate verification will be skipped"})
			}
		case plaintextSchemes[parsed.Scheme]:
			if !broker.Insecure {
				problems = append(problems, fmt.Errorf(
					"broker.url %q is plaintext; set broker.insecure: true to allow it (development only)", broker.URL))
			} else {
				warnings = append(warnings, Warning{"broker.url",
					"plaintext connection to the broker, allowed only because broker.insecure is set"})
			}
		default:
			problems = append(problems, fmt.Errorf(
				"broker.url scheme %q is not supported; use tls, ssl, mqtts, wss, or tcp/mqtt/ws with broker.insecure", parsed.Scheme))
		}
	}

	_, hasEnvPassword := env[EnvMQTTPassword]
	switch {
	case broker.CredentialsFile == "" && !hasEnvPassword:
		problems = append(problems, fmt.Errorf(
			"broker.credentials_file is required, or set %s; credentials are never accepted as CLI arguments because ps exposes them", EnvMQTTPassword))
	case broker.CredentialsFile != "":
		problems = append(problems, checkSecretFile("broker.credentials_file", broker.CredentialsFile)...)
	}

	if broker.CAFile != "" {
		if _, err := os.Stat(broker.CAFile); err != nil {
			problems = append(problems, fmt.Errorf("broker.ca_file %s: %w", broker.CAFile, err))
		}
	}

	if broker.Keepalive <= 0 {
		problems = append(problems, fmt.Errorf("broker.keepalive must be positive, got %s", broker.Keepalive))
	}
	if broker.ConnectTimeout <= 0 {
		problems = append(problems, fmt.Errorf("broker.connect_timeout must be positive, got %s", broker.ConnectTimeout))
	}
	if err := broker.ReconnectPolicy().Validate(); err != nil {
		problems = append(problems, fmt.Errorf("broker.reconnect_interval and broker.reconnect_backoff: %w", err))
	}
	return problems, warnings
}

// checkSecretFile requires a regular file that no other user can read.
func checkSecretFile(field, path string) []error {
	info, err := os.Stat(path)
	if err != nil {
		return []error{fmt.Errorf("%s %s: %w", field, path, err)}
	}
	if !info.Mode().IsRegular() {
		return []error{fmt.Errorf("%s %s is not a regular file", field, path)}
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		return []error{fmt.Errorf("%s %s has mode %04o; it must not be readable by group or other (chmod 0600)", field, path, perm)}
	}
	return nil
}

func validateDevices(devices []Device) ([]error, []Warning) {
	var problems []error
	var warnings []Warning

	if len(devices) == 0 {
		return []error{errors.New("devices must contain at least one entry")}, nil
	}

	seen := make(map[string]int, len(devices))
	seenPaths := make(map[string]int, len(devices))
	for i, deviceCfg := range devices {
		where := fmt.Sprintf("devices[%d]", i)
		switch {
		case deviceCfg.ID == "":
			problems = append(problems, fmt.Errorf("%s.id is required", where))
		case !topicSegment.MatchString(deviceCfg.ID):
			problems = append(problems, fmt.Errorf("%s.id %q must match [a-z0-9-]+; it is a level of the device's topics", where, deviceCfg.ID))
		case deviceCfg.ID == reservedDeviceID:
			problems = append(problems, fmt.Errorf("%s.id %q is reserved for the agent's own topics", where, deviceCfg.ID))
		default:
			if prev, dup := seen[deviceCfg.ID]; dup {
				problems = append(problems, fmt.Errorf("%s.id %q duplicates devices[%d]", where, deviceCfg.ID, prev))
			}
			seen[deviceCfg.ID] = i
			where = "devices." + deviceCfg.ID
		}

		switch deviceCfg.Kind {
		case KindSerial:
		case KindHID:
			problems = append(problems, fmt.Errorf(
				"%s.kind hid is not implemented; configure the device into USB-CDC mode instead", where))
		case "":
			problems = append(problems, fmt.Errorf("%s.kind is required", where))
		default:
			problems = append(problems, fmt.Errorf("%s.kind %q is unknown; expected serial", where, deviceCfg.Kind))
		}

		pathProblems, pathWarnings := validateDevicePath(where, deviceCfg.Path)
		problems, warnings = append(problems, pathProblems...), append(warnings, pathWarnings...)
		if deviceCfg.Path != "" {
			// The second device to open the same port gets EBUSY, because the
			// library takes exclusive access. Catching it here names the
			// mistake instead of leaving one station permanently absent.
			if prev, dup := seenPaths[deviceCfg.Path]; dup {
				problems = append(problems, fmt.Errorf(
					"%s.path %q is already used by devices[%d]; two devices cannot share one port", where, deviceCfg.Path, prev))
			}
			seenPaths[deviceCfg.Path] = i
		}

		if deviceCfg.Baud <= 0 {
			problems = append(problems, fmt.Errorf("%s.baud must be positive, got %d", where, deviceCfg.Baud))
		}
		if deviceCfg.DataBits < 5 || deviceCfg.DataBits > 8 {
			problems = append(problems, fmt.Errorf("%s.data_bits must be 5, 6, 7 or 8, got %d", where, deviceCfg.DataBits))
		}
		if !slices.Contains(Parities, deviceCfg.Parity) {
			problems = append(problems, fmt.Errorf("%s.parity %q is unknown; expected none, odd, even, mark or space", where, deviceCfg.Parity))
		}
		switch deviceCfg.StopBits {
		case StopBitsOne, StopBitsTwo:
		case "1.5":
			problems = append(problems, fmt.Errorf(
				"%s.stop_bits 1.5 is not supported: the serial library refuses it on every Unix system; use 1 or 2", where))
		default:
			problems = append(problems, fmt.Errorf("%s.stop_bits must be 1 or 2, got %q", where, deviceCfg.StopBits))
		}

		problems = append(problems, validateSeparator(where, deviceCfg.Separator)...)

		if deviceCfg.MaxFrameBytes < 1 {
			problems = append(problems, fmt.Errorf(
				"%s.max_frame_bytes is the largest accepted payload excluding the separator and must be at least 1, got %d", where, deviceCfg.MaxFrameBytes))
		}
		if deviceCfg.MaxFrameBytes > 1<<20 {
			problems = append(problems, fmt.Errorf(
				"%s.max_frame_bytes %d exceeds 1048576; the limit exists to bound a stuck device", where, deviceCfg.MaxFrameBytes))
		}
		if deviceCfg.InterCharTimeout <= 0 {
			problems = append(problems, fmt.Errorf("%s.inter_char_timeout must be positive, got %s", where, deviceCfg.InterCharTimeout))
		}
		problems = append(problems, wholeSeconds(where+".message_expiry", deviceCfg.MessageExpiry,
			"MQTT carries the message expiry in whole seconds")...)
		if deviceCfg.TxOpenAttempts < 1 {
			problems = append(problems, fmt.Errorf("%s.tx_open_attempts must be at least 1, got %d", where, deviceCfg.TxOpenAttempts))
		}
		if deviceCfg.TxOpenInterval <= 0 {
			problems = append(problems, fmt.Errorf("%s.tx_open_interval must be positive, got %s", where, deviceCfg.TxOpenInterval))
		}
		if err := deviceCfg.ReopenPolicy().Validate(); err != nil {
			problems = append(problems, fmt.Errorf("%s.reopen_interval and %s.reopen_backoff: %w", where, where, err))
		}
		problems = append(problems, validateDeviceType(where, deviceCfg.DeviceType)...)
	}
	return problems, warnings
}

// reservedDeviceID is the topic level of the agent's own status, which no
// device may take (DESIGN-V2.md, "Topics").
const reservedDeviceID = "agent"

// validateDeviceType accepts any short text without control characters. It is
// published in every message about the device, where a newline or an escape
// would reach every consumer's logs.
func validateDeviceType(where, deviceType string) []error {
	switch {
	case deviceType == "":
		return nil
	case strings.TrimSpace(deviceType) == "":
		return []error{fmt.Errorf("%s.device_type is blank; leave it out instead", where)}
	case len(deviceType) > 64:
		return []error{fmt.Errorf("%s.device_type is %d bytes; expected at most 64", where, len(deviceType))}
	case strings.IndexFunc(deviceType, unicode.IsControl) >= 0:
		return []error{fmt.Errorf("%s.device_type %q contains a control character", where, deviceType)}
	}
	return nil
}

// wholeSeconds requires a duration of at least one second with no fraction,
// for values published or carried in whole seconds, where a fraction would be
// dropped without a word.
func wholeSeconds(field string, value Duration, why string) []error {
	if value.Duration() < time.Second || value.Duration()%time.Second != 0 {
		return []error{fmt.Errorf("%s must be a whole number of seconds, at least 1s, got %s; %s", field, value, why)}
	}
	return nil
}

func validateDevicePath(where, path string) ([]error, []Warning) {
	switch {
	case path == "":
		return []error{fmt.Errorf("%s.path is required", where)}, nil
	case !filepath.IsAbs(path):
		return []error{fmt.Errorf("%s.path %q must be absolute", where, path)}, nil
	case strings.HasPrefix(path, "/dev/tty."):
		return []error{fmt.Errorf(
			"%s.path %q is a macOS callin device; opening it blocks on carrier detect forever. Use the matching /dev/cu.* callout device", where, path)}, nil
	case unstableDevPath.MatchString(path):
		return nil, []Warning{{where + ".path", fmt.Sprintf(
			"%q is a kernel-assigned name that changes with reboot and replug order; bind to /dev/serial/by-id/... , a by-path entry, or a udev symlink", path)}}
	}
	return nil, nil
}

// validateSeparator rejects the single most likely YAML mistake: writing the
// separator unquoted or single-quoted, which yields the two characters
// backslash and r rather than a carriage return.
func validateSeparator(where, separator string) []error {
	if separator == "" {
		return []error{fmt.Errorf("%s.separator is required; state it per device rather than relying on a default", where)}
	}
	if strings.Contains(separator, `\`) {
		return []error{fmt.Errorf(
			"%s.separator %q contains a literal backslash; write it as a double-quoted YAML scalar so escapes are decoded, for example separator: \"\\r\"", where, separator)}
	}
	if len(separator) > 8 {
		return []error{fmt.Errorf("%s.separator is %d bytes; expected at most 8", where, len(separator))}
	}
	return nil
}

func validateDelivery(delivery Delivery, devices []Device) []error {
	var problems []error
	if delivery.PublishTimeout <= 0 {
		problems = append(problems, fmt.Errorf("delivery.publish_timeout must be positive, got %s", delivery.PublishTimeout))
	}
	// A reading the agent still reports as pending after the broker has
	// discarded it would tell the operator it failed when it had expired.
	for i, deviceCfg := range devices {
		if deviceCfg.MessageExpiry > 0 && delivery.PublishTimeout > deviceCfg.MessageExpiry {
			where := fmt.Sprintf("devices[%d]", i)
			if deviceCfg.ID != "" {
				where = "devices." + deviceCfg.ID
			}
			problems = append(problems, fmt.Errorf(
				"delivery.publish_timeout (%s) exceeds %s.message_expiry (%s); the operator would be told a reading failed after the broker had already expired it",
				delivery.PublishTimeout, where, deviceCfg.MessageExpiry))
		}
	}
	if delivery.BufferSize < 1 {
		problems = append(problems, fmt.Errorf("delivery.buffer_size must be at least 1, got %d", delivery.BufferSize))
	}
	return problems
}

func validateStatus(status Status) []error {
	problems := wholeSeconds("status.keepalive_interval", status.KeepaliveInterval,
		"each keepalive publishes it in whole seconds")
	if status.MissedKeepalives < 1 {
		problems = append(problems, fmt.Errorf("status.missed_keepalives must be at least 1, got %d", status.MissedKeepalives))
	}
	if status.EventBufferSize < 1 {
		problems = append(problems, fmt.Errorf("status.event_buffer_size must be at least 1, got %d", status.EventBufferSize))
	}
	return problems
}

func validateLogging(logging Logging) []error {
	var problems []error
	switch strings.ToLower(logging.Level) {
	case "debug", "info", "warn", "error":
	default:
		problems = append(problems, fmt.Errorf("logging.level %q is unknown; expected debug, info, warn or error", logging.Level))
	}
	// No file is a valid choice, and so is no destination at all: where the
	// agent logs, if anywhere, is the operator's business (#23 Q11a).
	switch {
	case logging.File == "":
	case !filepath.IsAbs(logging.File):
		problems = append(problems, fmt.Errorf("logging.file %q must be absolute", logging.File))
	default:
		dir := filepath.Dir(logging.File)
		info, err := os.Stat(dir)
		if err != nil {
			problems = append(problems, fmt.Errorf("logging.file directory %s: %w", dir, err))
		} else if !info.IsDir() {
			problems = append(problems, fmt.Errorf("logging.file directory %s is not a directory", dir))
		}
	}
	if logging.MaxSizeMB < 1 {
		problems = append(problems, fmt.Errorf("logging.max_size_mb must be at least 1, got %d", logging.MaxSizeMB))
	}
	if logging.Keep < 0 {
		problems = append(problems, fmt.Errorf("logging.keep must not be negative, got %d", logging.Keep))
	}
	return problems
}

// ValidateDevice checks a single device entry. It exists so the probe
// subcommand, which assembles a device from flags rather than a file, rejects
// exactly the mistakes the validate subcommand rejects.
func ValidateDevice(deviceCfg Device) ([]Warning, error) {
	problems, warnings := validateDevices([]Device{deviceCfg})
	return warnings, errors.Join(problems...)
}
