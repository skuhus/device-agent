package config

import (
	"runtime"
	"time"
)

// Default values. Anything with a documented default in the specification is
// listed here so there is one place to read them from.
const (
	DefaultBaud             = 9600
	DefaultDataBits         = 8
	DefaultParity           = ParityNone
	DefaultStopBits         = StopBitsOne
	DefaultMaxFrameBytes    = 4096
	DefaultInterCharTimeout = 200 * time.Millisecond
	// DefaultMessageExpiry is v1's delivery.scan_ttl, now set per device.
	DefaultMessageExpiry = 30 * time.Second

	DefaultKeepalive = 30 * time.Second
	// DefaultReconnectInterval is how often the agent tries the broker while it
	// cannot be reached, with the backoff off, which is its default (#13 Q3).
	DefaultReconnectInterval = 1 * time.Second
	// DefaultBackoffMax and DefaultBackoffJitter apply once the backoff is
	// enabled. They are the specification's connect_backoff values.
	DefaultBackoffMax    = 60 * time.Second
	DefaultBackoffJitter = 0.3

	DefaultPublishTimeout = 2 * time.Second
	DefaultBufferSize     = 64

	// DefaultKeepaliveInterval is v1's heartbeat interval, and three missed
	// keepalives, 45 seconds, is when a consumer treats the agent as gone
	// (#11 Q7).
	DefaultKeepaliveInterval = 15 * time.Second
	DefaultMissedKeepalives  = 3
	// DefaultEventBufferSize is delivery.buffer_size's default: as many events
	// as frames.
	DefaultEventBufferSize = 64

	DefaultLogLevel     = "info"
	DefaultLogMaxSizeMB = 64
	DefaultLogKeep      = 7
	// DefaultLogStdout sends the log to stdout, where journald and docker logs
	// collect it, unless the configuration says otherwise.
	DefaultLogStdout = true

	linuxConfigPath  = "/etc/skuhus-device-agent/config.yaml"
	darwinConfigPath = "/usr/local/etc/skuhus-device-agent/config.yaml"
)

// DefaultPath is the platform config file location.
func DefaultPath() string {
	if runtime.GOOS == "darwin" {
		return darwinConfigPath
	}
	return linuxConfigPath
}

// Defaults returns a Config with every non-device default applied.
func Defaults() Config {
	return Config{
		Broker: Broker{
			Keepalive:         Duration(DefaultKeepalive),
			ReconnectInterval: Duration(DefaultReconnectInterval),
			ReconnectBackoff: ReconnectBackoff{
				Max:    Duration(DefaultBackoffMax),
				Jitter: DefaultBackoffJitter,
			},
		},
		Delivery: Delivery{
			PublishTimeout: Duration(DefaultPublishTimeout),
			BufferSize:     DefaultBufferSize,
		},
		Status: Status{
			KeepaliveInterval: Duration(DefaultKeepaliveInterval),
			MissedKeepalives:  DefaultMissedKeepalives,
			EventBufferSize:   DefaultEventBufferSize,
		},
		Logging: Logging{
			Level:       DefaultLogLevel,
			LogPayloads: false,
			MaxSizeMB:   DefaultLogMaxSizeMB,
			Keep:        DefaultLogKeep,
			Stdout:      DefaultLogStdout,
		},
	}
}

// applyDeviceDefaults fills unset per-device fields. It runs after the file is
// decoded, because a device entry may set only some of its fields.
//
// Separator has no default on purpose: a scanner that suffixes CRLF where the
// previous one suffixed CR is exactly the substitution that produces a bug
// nobody can reproduce, so the value must be stated per device.
func applyDeviceDefaults(devices []Device) {
	for i := range devices {
		deviceCfg := &devices[i]
		if deviceCfg.Kind == "" {
			deviceCfg.Kind = KindSerial
		}
		if deviceCfg.Baud == 0 {
			deviceCfg.Baud = DefaultBaud
		}
		if deviceCfg.DataBits == 0 {
			deviceCfg.DataBits = DefaultDataBits
		}
		if deviceCfg.Parity == "" {
			deviceCfg.Parity = DefaultParity
		}
		if deviceCfg.StopBits == "" {
			deviceCfg.StopBits = DefaultStopBits
		}
		if deviceCfg.MaxFrameBytes == 0 {
			deviceCfg.MaxFrameBytes = DefaultMaxFrameBytes
		}
		if deviceCfg.InterCharTimeout == 0 {
			deviceCfg.InterCharTimeout = Duration(DefaultInterCharTimeout)
		}
		if deviceCfg.MessageExpiry == 0 {
			deviceCfg.MessageExpiry = Duration(DefaultMessageExpiry)
		}
	}
}
