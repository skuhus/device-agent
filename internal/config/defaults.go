package config

import (
	"runtime"
	"time"
)

// Default values. Anything with a documented default in the specification is
// listed here so there is one place to read them from.
const (
	DefaultBaud             = 9600
	DefaultMaxFrameBytes    = 4096
	DefaultInterCharTimeout = 200 * time.Millisecond

	DefaultKeepalive      = 30 * time.Second
	DefaultBackoffInitial = 1 * time.Second
	DefaultBackoffMax     = 60 * time.Second
	DefaultBackoffJitter  = 0.3

	DefaultScanTTL        = 30 * time.Second
	DefaultPublishTimeout = 2 * time.Second
	DefaultBufferSize     = 64

	DefaultLogLevel       = "info"
	DefaultAuditMaxSizeMB = 64
	DefaultAuditKeep      = 7

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
			Keepalive: Duration(DefaultKeepalive),
			ConnectBackoff: Backoff{
				Initial: Duration(DefaultBackoffInitial),
				Max:     Duration(DefaultBackoffMax),
				Jitter:  DefaultBackoffJitter,
			},
		},
		Delivery: Delivery{
			ScanTTL:        Duration(DefaultScanTTL),
			PublishTimeout: Duration(DefaultPublishTimeout),
			BufferSize:     DefaultBufferSize,
		},
		Logging: Logging{
			Level:          DefaultLogLevel,
			LogPayloads:    false,
			AuditMaxSizeMB: DefaultAuditMaxSizeMB,
			AuditKeep:      DefaultAuditKeep,
		},
	}
}

// applyDeviceDefaults fills unset per-device fields. It runs after the file is
// decoded, because a device entry may set only some of its fields.
//
// Terminator has no default on purpose: a scanner that suffixes CRLF where the
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
		if deviceCfg.MaxFrameBytes == 0 {
			deviceCfg.MaxFrameBytes = DefaultMaxFrameBytes
		}
		if deviceCfg.InterCharTimeout == 0 {
			deviceCfg.InterCharTimeout = Duration(DefaultInterCharTimeout)
		}
	}
}
