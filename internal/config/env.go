package config

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// EnvPrefix is the prefix for every environment override.
const EnvPrefix = "SH_DEV_AGENT_"

// legacyEnvPrefixes are the prefixes earlier releases used: SKUHUS_AGENT_ in
// 0.1.0, SH_DEV_SER_SCANNER_ in 0.2.0 and 0.3.0. Every variable any of them
// defined exists under EnvPrefix with the same suffix, or is in removedEnv.
var legacyEnvPrefixes = []string{"SKUHUS_AGENT_", "SH_DEV_SER_SCANNER_"}

// EnvConfigPath names the config file, equivalent to the --config flag.
const EnvConfigPath = EnvPrefix + "CONFIG"

// EnvMQTTPassword supplies the broker password without a credentials file.
// The name says MQTT rather than broker because it is the MQTT connection it
// authenticates; a station that later gains a second protocol would need its
// own. Config only records that this is present, so a deployment using it is
// not rejected for having no credentials file; LoadCredentials reads it.
const EnvMQTTPassword = EnvPrefix + "MQTT_PASSWORD"

// EnvMQTTUsername is the companion of EnvMQTTPassword.
const EnvMQTTUsername = EnvPrefix + "MQTT_USERNAME"

// rejectLegacyEnvironment fails on every variable that uses an earlier release's
// prefix, naming the variable that replaces it. Such a variable would otherwise
// be ignored in silence, because envMap selects only EnvPrefix: a unit file or
// container definition carried over from an earlier release would lose every
// override it sets, and the station would run on the file's values without a
// sign that anything was dropped. Only names are reported; a value can be a
// password.
func rejectLegacyEnvironment(environ []string) error {
	var names []string
	for _, entry := range environ {
		name, _, _ := strings.Cut(entry, "=")
		for _, legacy := range legacyEnvPrefixes {
			if strings.HasPrefix(name, legacy) {
				names = append(names, name)
				break
			}
		}
	}
	sort.Strings(names)
	problems := make([]error, 0, len(names))
	for _, name := range names {
		for _, legacy := range legacyEnvPrefixes {
			if suffix, found := strings.CutPrefix(name, legacy); found {
				if instead, gone := removedEnv[EnvPrefix+suffix]; gone {
					problems = append(problems, fmt.Errorf(
						"environment variable %s uses the prefix of an earlier release, and its setting was removed in 2.0.0; %s", name, instead))
					break
				}
				problems = append(problems, fmt.Errorf(
					"environment variable %s uses the prefix of an earlier release; rename it to %s", name, EnvPrefix+suffix))
				break
			}
		}
	}
	return errors.Join(problems...)
}

// EnvMap selects the SH_DEV_AGENT_* variables from a KEY=VALUE list. The
// transport needs it to read credentials, which are deliberately not part of
// Config.
func EnvMap(environ []string) map[string]string { return envMap(environ) }

func envMap(environ []string) map[string]string {
	out := make(map[string]string, len(environ))
	for _, entry := range environ {
		name, value, ok := strings.Cut(entry, "=")
		if ok && strings.HasPrefix(name, EnvPrefix) {
			out[name] = value
		}
	}
	return out
}

// envTargets maps each recognised variable to the field it writes. Devices are
// not settable from the environment: a list does not map onto flat variables
// without inventing an indexing scheme.
func envTargets(cfg *Config) map[string]func(string) error {
	return map[string]func(string) error{
		EnvConfigPath:   func(string) error { return nil }, // consumed before decode
		EnvMQTTUsername: func(string) error { return nil }, // read by the transport
		EnvMQTTPassword: func(string) error { return nil }, // read by the transport

		EnvPrefix + "IDENTITY_PROJECT":  setString(&cfg.Identity.Project),
		EnvPrefix + "IDENTITY_SITE":     setString(&cfg.Identity.Site),
		EnvPrefix + "IDENTITY_STATION":  setString(&cfg.Identity.Station),
		EnvPrefix + "IDENTITY_INSTANCE": setString(&cfg.Identity.Instance),

		EnvPrefix + "BROKER_URL":              setString(&cfg.Broker.URL),
		EnvPrefix + "BROKER_CREDENTIALS_FILE": setString(&cfg.Broker.CredentialsFile),
		EnvPrefix + "BROKER_CA_FILE":          setString(&cfg.Broker.CAFile),
		EnvPrefix + "BROKER_INSECURE":         setBool(&cfg.Broker.Insecure),
		EnvPrefix + "BROKER_KEEPALIVE":        setDuration(&cfg.Broker.Keepalive),
		EnvPrefix + "BROKER_CONNECT_TIMEOUT":  setDuration(&cfg.Broker.ConnectTimeout),
		// reconnect_backoff is a mapping and has no variable, as devices have
		// none: a mapping does not map onto flat variables.
		EnvPrefix + "BROKER_RECONNECT_INTERVAL": setDuration(&cfg.Broker.ReconnectInterval),

		EnvPrefix + "DELIVERY_PUBLISH_TIMEOUT": setDuration(&cfg.Delivery.PublishTimeout),
		EnvPrefix + "DELIVERY_BUFFER_SIZE":     setInt(&cfg.Delivery.BufferSize),
		EnvPrefix + "DELIVERY_DRAIN_TIMEOUT":   setDuration(&cfg.Delivery.DrainTimeout),
		EnvPrefix + "DELIVERY_TX_INTAKE_SIZE":  setInt(&cfg.Delivery.TxIntakeSize),

		EnvPrefix + "STATUS_KEEPALIVE_INTERVAL": setDuration(&cfg.Status.KeepaliveInterval),
		EnvPrefix + "STATUS_MISSED_KEEPALIVES":  setInt(&cfg.Status.MissedKeepalives),
		EnvPrefix + "STATUS_EVENT_BUFFER_SIZE":  setInt(&cfg.Status.EventBufferSize),

		EnvPrefix + "LOGGING_LEVEL":        setString(&cfg.Logging.Level),
		EnvPrefix + "LOGGING_LOG_PAYLOADS": setBool(&cfg.Logging.LogPayloads),
		EnvPrefix + "LOGGING_FILE":         setString(&cfg.Logging.File),
		EnvPrefix + "LOGGING_MAX_SIZE_MB":  setInt(&cfg.Logging.MaxSizeMB),
		EnvPrefix + "LOGGING_KEEP":         setInt(&cfg.Logging.Keep),
		EnvPrefix + "LOGGING_STDOUT":       setBool(&cfg.Logging.Stdout),
	}
}

// removedEnv are the variables earlier releases read and 2.0.0 does not, each
// with what replaces it, for the same reason as removedKeys.
var removedEnv = map[string]string{
	EnvPrefix + "DELIVERY_SCAN_TTL":         "set message_expiry on each device in the config file",
	EnvPrefix + "LOGGING_AUDIT_FILE":        "use " + EnvPrefix + "LOGGING_FILE",
	EnvPrefix + "LOGGING_AUDIT_MAX_SIZE_MB": "use " + EnvPrefix + "LOGGING_MAX_SIZE_MB",
	EnvPrefix + "LOGGING_AUDIT_KEEP":        "use " + EnvPrefix + "LOGGING_KEEP",
}

// applyEnv overlays environment variables and rejects unrecognised ones. A
// misspelled variable that is silently ignored leaves a station running a
// setting the operator believes they changed.
func applyEnv(cfg *Config, env map[string]string) error {
	targets := envTargets(cfg)
	// Sorted so that the reported problems come out in the same order every
	// run; map iteration order would shuffle them between invocations.
	names := make([]string, 0, len(env))
	for name := range env {
		names = append(names, name)
	}
	sort.Strings(names)

	var unknown []string
	var problems []error
	for _, name := range names {
		if instead, gone := removedEnv[name]; gone {
			problems = append(problems, fmt.Errorf("%s was removed in 2.0.0; %s", name, instead))
			continue
		}
		set, ok := targets[name]
		if !ok {
			unknown = append(unknown, name)
			continue
		}
		if err := set(env[name]); err != nil {
			problems = append(problems, fmt.Errorf("%s: %w", name, err))
		}
	}
	if len(unknown) > 0 {
		known := make([]string, 0, len(targets))
		for name := range targets {
			known = append(known, name)
		}
		sort.Strings(known)
		problems = append(problems, fmt.Errorf(
			"unrecognised environment variables %s (recognised: %s)",
			strings.Join(unknown, ", "), strings.Join(known, ", ")))
	}
	return errors.Join(problems...)
}

func setString(dst *string) func(string) error {
	return func(value string) error { *dst = value; return nil }
}

func setBool(dst *bool) func(string) error {
	return func(value string) error {
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("expected a boolean, got %q", value)
		}
		*dst = parsed
		return nil
	}
}

func setInt(dst *int) func(string) error {
	return func(value string) error {
		parsed, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("expected an integer, got %q", value)
		}
		*dst = parsed
		return nil
	}
}

func setDuration(dst *Duration) func(string) error {
	return func(value string) error {
		parsed, err := time.ParseDuration(value)
		if err != nil {
			return fmt.Errorf("expected a duration such as \"30s\", got %q", value)
		}
		*dst = Duration(parsed)
		return nil
	}
}
