package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// EnvPrefix is the prefix for every environment override.
const EnvPrefix = "SH_DEV_AGENT_"

// legacyEnvPrefixes are the prefixes earlier releases used: SKUHUS_AGENT_ in
// 0.1.0, SH_DEV_SER_SCANNER_ in 0.2.0 and 0.3.0. Every variable any of them
// defined exists under EnvPrefix with the same suffix.
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

// Overrides carries CLI flag values. A nil pointer means the flag was not set,
// which is what keeps flags above environment above file above defaults.
//
// There is deliberately no credential flag: command line arguments are visible
// in ps to every user on the host.
type Overrides struct {
	Project  *string
	Site     *string
	Station  *string
	Instance *string

	BrokerURL             *string
	BrokerCredentialsFile *string
	BrokerCAFile          *string
	BrokerInsecure        *bool

	LogLevel    *string
	LogPayloads *bool
}

// Options controls where Load reads from. The function fields exist so tests
// can drive the environment without mutating the process.
type Options struct {
	// Path is the config file. Empty means DefaultPath, or EnvConfigPath if set.
	Path string
	// Environ returns the environment as KEY=VALUE strings. Defaults to os.Environ.
	Environ func() []string
	// Overrides are the CLI flag values.
	Overrides Overrides
	// SkipValidate loads and merges without validating. Used by probe, which
	// needs a single device entry and does not care about the broker section.
	SkipValidate bool
}

// Load reads, merges and validates configuration.
//
// It returns the merged configuration and any non-fatal warnings. An error is
// returned for a missing or unreadable file, an unknown key, an unrecognised
// SH_DEV_AGENT_* variable, or any validation failure; the error text names
// every problem found rather than only the first.
func Load(opts Options) (*Config, []Warning, error) {
	environ := opts.Environ
	if environ == nil {
		environ = os.Environ
	}
	variables := environ()
	// Before anything reads the environment: a legacy SH_DEV_SER_SCANNER_CONFIG
	// would otherwise send loading to the default path and fail there, with an
	// error about a missing file rather than about the variable.
	if err := rejectLegacyEnvironment(variables); err != nil {
		return nil, nil, err
	}
	env := envMap(variables)

	path := opts.Path
	explicit := path != ""
	if path == "" {
		if value, ok := env[EnvConfigPath]; ok && value != "" {
			path, explicit = value, true
		} else {
			path = DefaultPath()
		}
	}

	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) && !explicit {
			return nil, nil, fmt.Errorf("no config file at the default location %s: create it or pass --config", path)
		}
		return nil, nil, fmt.Errorf("open config %s: %w", path, err)
	}
	defer file.Close()

	cfg, err := decode(file)
	if err != nil {
		return nil, nil, fmt.Errorf("parse config %s: %w", path, err)
	}

	if err := applyEnv(cfg, env); err != nil {
		return nil, nil, err
	}
	applyOverrides(cfg, opts.Overrides)
	applyDeviceDefaults(cfg.Devices)
	// After the overrides, so that an instance set anywhere wins over the
	// default, and before validation, so the default is validated too.
	if cfg.Identity.Instance == "" {
		cfg.Identity.Instance = cfg.Identity.Station
	}

	if opts.SkipValidate {
		return cfg, nil, nil
	}
	warnings, err := Validate(cfg, env)
	if err != nil {
		return nil, warnings, fmt.Errorf("invalid config %s: %w", path, err)
	}
	return cfg, warnings, nil
}

// decode reads YAML on top of the defaults, rejecting unknown keys.
func decode(r io.Reader) (*Config, error) {
	cfg := Defaults()
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.New("file is empty")
		}
		return nil, err
	}
	// A second document would silently override the first.
	var extra yaml.Node
	if err := dec.Decode(&extra); err == nil {
		return nil, errors.New("file contains more than one YAML document")
	} else if !errors.Is(err, io.EOF) {
		return nil, err
	}
	return &cfg, nil
}

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
	for _, kv := range environ {
		name, value, ok := strings.Cut(kv, "=")
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

		EnvPrefix + "DELIVERY_SCAN_TTL":        setDuration(&cfg.Delivery.ScanTTL),
		EnvPrefix + "DELIVERY_PUBLISH_TIMEOUT": setDuration(&cfg.Delivery.PublishTimeout),
		EnvPrefix + "DELIVERY_BUFFER_SIZE":     setInt(&cfg.Delivery.BufferSize),

		EnvPrefix + "LOGGING_LEVEL":             setString(&cfg.Logging.Level),
		EnvPrefix + "LOGGING_LOG_PAYLOADS":      setBool(&cfg.Logging.LogPayloads),
		EnvPrefix + "LOGGING_AUDIT_FILE":        setString(&cfg.Logging.AuditFile),
		EnvPrefix + "LOGGING_AUDIT_MAX_SIZE_MB": setInt(&cfg.Logging.AuditMaxSizeMB),
		EnvPrefix + "LOGGING_AUDIT_KEEP":        setInt(&cfg.Logging.AuditKeep),
	}
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

func applyOverrides(cfg *Config, o Overrides) {
	assign(&cfg.Identity.Project, o.Project)
	assign(&cfg.Identity.Site, o.Site)
	assign(&cfg.Identity.Station, o.Station)
	assign(&cfg.Broker.URL, o.BrokerURL)
	assign(&cfg.Identity.Instance, o.Instance)
	assign(&cfg.Broker.CredentialsFile, o.BrokerCredentialsFile)
	assign(&cfg.Broker.CAFile, o.BrokerCAFile)
	assign(&cfg.Broker.Insecure, o.BrokerInsecure)
	assign(&cfg.Logging.Level, o.LogLevel)
	assign(&cfg.Logging.LogPayloads, o.LogPayloads)
}

func assign[T any](dst *T, src *T) {
	if src != nil {
		*dst = *src
	}
}

func setString(dst *string) func(string) error {
	return func(value string) error { *dst = value; return nil }
}

func setBool(dst *bool) func(string) error {
	return func(value string) error {
		b, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("expected a boolean, got %q", value)
		}
		*dst = b
		return nil
	}
}

func setInt(dst *int) func(string) error {
	return func(value string) error {
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("expected an integer, got %q", value)
		}
		*dst = n
		return nil
	}
}

func setDuration(dst *Duration) func(string) error {
	return func(value string) error {
		d, err := time.ParseDuration(value)
		if err != nil {
			return fmt.Errorf("expected a duration such as \"30s\", got %q", value)
		}
		*dst = Duration(d)
		return nil
	}
}
