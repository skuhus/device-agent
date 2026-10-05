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
	// DefaultTxOpenAttempts and DefaultTxOpenInterval bound how long a tx waits
	// for a closed port: about 3 s (PLAN-V2.md, T14).
	DefaultTxOpenAttempts = 3
	DefaultTxOpenInterval = 1 * time.Second
	// DefaultReopenInterval and the reopen backoff's defaults are v1's: from
	// 100 ms, doubling to 30 s. The backoff is on, because each failed attempt
	// is a port_open_failed event, and a device unplugged overnight would
	// otherwise publish ten a second (#11 Q8).
	DefaultReopenInterval       = 100 * time.Millisecond
	DefaultReopenBackoffEnabled = true
	DefaultReopenBackoffMax     = 30 * time.Second
	DefaultReopenBackoffJitter  = 0.3

	DefaultKeepalive = 30 * time.Second
	// DefaultConnectTimeout is autopaho's own default, which the agent used
	// before it was a setting.
	DefaultConnectTimeout = 10 * time.Second
	// DefaultReconnectInterval is how often the agent tries the broker while it
	// cannot be reached, with the backoff off, which is its default (#13 Q3).
	DefaultReconnectInterval = 1 * time.Second
	// DefaultReconnectBackoffMax and DefaultReconnectBackoffJitter apply once
	// the backoff is enabled. They are the specification's connect_backoff
	// values.
	DefaultReconnectBackoffMax    = 60 * time.Second
	DefaultReconnectBackoffJitter = 0.3

	DefaultPublishTimeout = 2 * time.Second
	DefaultBufferSize     = 64
	// DefaultDrainTimeout is the drain's bound since v1, where it was a
	// constant (DESIGN-V2.md, "The shutdown drain is bounded").
	DefaultDrainTimeout = 5 * time.Second

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

// Defaults is the configuration before the file is read: each key the file
// leaves out keeps this value. A device entry starts from DefaultDevice.
func Defaults() Config {
	return Config{
		Broker: Broker{
			Keepalive:         Duration(DefaultKeepalive),
			ConnectTimeout:    Duration(DefaultConnectTimeout),
			ReconnectInterval: Duration(DefaultReconnectInterval),
			ReconnectBackoff: Backoff{
				Max:    Duration(DefaultReconnectBackoffMax),
				Jitter: DefaultReconnectBackoffJitter,
			},
		},
		Delivery: Delivery{
			PublishTimeout: Duration(DefaultPublishTimeout),
			BufferSize:     DefaultBufferSize,
			DrainTimeout:   Duration(DefaultDrainTimeout),
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

// DefaultDevice is a device entry before its keys are read: each key the
// entry leaves out keeps this value. The file is decoded on top of it, so a key
// set to zero is read as zero and validated, not replaced.
//
// Separator has no default on purpose: a scanner that suffixes CRLF where the
// previous one suffixed CR is exactly the substitution that produces a bug
// nobody can reproduce, so the value must be stated per device.
func DefaultDevice() Device {
	return Device{
		Kind:             KindSerial,
		Baud:             DefaultBaud,
		DataBits:         DefaultDataBits,
		Parity:           DefaultParity,
		StopBits:         DefaultStopBits,
		MaxFrameBytes:    DefaultMaxFrameBytes,
		InterCharTimeout: Duration(DefaultInterCharTimeout),
		MessageExpiry:    Duration(DefaultMessageExpiry),
		TxOpenAttempts:   DefaultTxOpenAttempts,
		TxOpenInterval:   Duration(DefaultTxOpenInterval),
		ReopenInterval:   Duration(DefaultReopenInterval),
		ReopenBackoff: Backoff{
			Enabled: DefaultReopenBackoffEnabled,
			Max:     Duration(DefaultReopenBackoffMax),
			Jitter:  DefaultReopenBackoffJitter,
		},
	}
}

// applyDerivedDefaults sets the defaults that come from other settings. It
// runs after the environment and the flags, so that a value set anywhere wins
// over the default, and before validation, so that the default is validated
// too.
func applyDerivedDefaults(cfg *Config) {
	if cfg.Identity.Instance == "" {
		cfg.Identity.Instance = cfg.Identity.Station
	}
}
