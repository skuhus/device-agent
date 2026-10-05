package config

import (
	"bytes"
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
	applyDerivedDefaults(cfg)

	if opts.SkipValidate {
		return cfg, nil, nil
	}
	warnings, err := Validate(cfg, env)
	if err != nil {
		return nil, warnings, fmt.Errorf("invalid config %s: %w", path, err)
	}
	return cfg, warnings, nil
}

// removedKeys are the keys earlier releases accepted and 2.0.0 does not, each
// with what replaces it. Strict decoding rejects them anyway, but only as a
// field not found; naming the replacement is what lets an operator upgrading a
// station learn what changed from the error rather than from the release notes.
// "devices[]" matches the key in every device entry.
var removedKeys = []struct{ path, instead string }{
	{"broker.connect_backoff", "use broker.reconnect_interval, and broker.reconnect_backoff to make the wait grow"},
	{"delivery.scan_ttl", "set message_expiry on each device"},
	{"logging.audit_file", "use logging.file"},
	{"logging.audit_max_size_mb", "use logging.max_size_mb"},
	{"logging.audit_keep", "use logging.keep"},
	{"devices[].terminator", "rename it to separator"},
	{"devices[].assert_config", "remove it; the agent does not configure devices"},
}

// decode reads YAML on top of the defaults, rejecting unknown keys and naming
// the replacement of every removed one.
func decode(r io.Reader) (*Config, error) {
	body, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	var doc yaml.Node
	dec := yaml.NewDecoder(bytes.NewReader(body))
	if err := dec.Decode(&doc); err != nil {
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

	removed, removedLines := findRemovedKeys(&doc)
	cfg := Defaults()
	strict := yaml.NewDecoder(bytes.NewReader(body))
	strict.KnownFields(true)
	problems := removed
	if err := strict.Decode(&cfg); err != nil {
		var typeErr *yaml.TypeError
		if !errors.As(err, &typeErr) {
			return nil, err
		}
		// A removed key is already reported above with its replacement; the
		// decoder's own "not found" for the same line would only repeat it.
		for _, message := range typeErr.Errors {
			if !removedLines[lineOf(message)] {
				problems = append(problems, errors.New(message))
			}
		}
	}
	if len(problems) > 0 {
		return nil, errors.Join(problems...)
	}
	if err := decodeDevicesOnDefaults(&doc, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// decodeDevicesOnDefaults decodes each device entry again, on top of
// DefaultDevice. The decoder makes a list's entries from zero values, so the
// strict pass above leaves out every default; it has already refused unknown
// keys and wrong types, with their lines.
func decodeDevicesOnDefaults(doc *yaml.Node, cfg *Config) error {
	devicesNode := mappingValue(doc, "devices")
	if devicesNode == nil || devicesNode.Kind != yaml.SequenceNode {
		return nil
	}
	for index, entry := range devicesNode.Content {
		deviceCfg := DefaultDevice()
		if err := entry.Decode(&deviceCfg); err != nil {
			return fmt.Errorf("devices[%d]: %w", index, err)
		}
		cfg.Devices[index] = deviceCfg
	}
	return nil
}

// mappingValue is the value of key in the document's top-level mapping, or
// nil when there is none.
func mappingValue(doc *yaml.Node, key string) *yaml.Node {
	if len(doc.Content) == 0 || doc.Content[0].Kind != yaml.MappingNode {
		return nil
	}
	root := doc.Content[0]
	for index := 0; index+1 < len(root.Content); index += 2 {
		if root.Content[index].Value == key {
			return root.Content[index+1]
		}
	}
	return nil
}

// findRemovedKeys reports every removed key in the document, with its line,
// and returns the lines so the decoder's errors for them can be dropped.
func findRemovedKeys(doc *yaml.Node) ([]error, map[int]bool) {
	var problems []error
	lines := map[int]bool{}
	if len(doc.Content) == 0 || doc.Content[0].Kind != yaml.MappingNode {
		return nil, lines
	}
	check := func(key *yaml.Node, generic, shown string) {
		for _, removed := range removedKeys {
			if removed.path == generic {
				problems = append(problems, fmt.Errorf("line %d: %s was removed in 2.0.0; %s", key.Line, shown, removed.instead))
				lines[key.Line] = true
			}
		}
	}
	root := doc.Content[0]
	for i := 0; i+1 < len(root.Content); i += 2 {
		section, value := root.Content[i].Value, root.Content[i+1]
		switch value.Kind {
		case yaml.MappingNode:
			for j := 0; j+1 < len(value.Content); j += 2 {
				key := value.Content[j]
				path := section + "." + key.Value
				check(key, path, path)
			}
		case yaml.SequenceNode:
			for index, entry := range value.Content {
				if entry.Kind != yaml.MappingNode {
					continue
				}
				for j := 0; j+1 < len(entry.Content); j += 2 {
					key := entry.Content[j]
					check(key, section+"[]."+key.Value, fmt.Sprintf("%s[%d].%s", section, index, key.Value))
				}
			}
		}
	}
	return problems, lines
}

// lineOf reads the line number yaml.v3 puts at the start of a type error,
// "line 12: field x not found in type config.Device", and returns 0 when
// there is none.
func lineOf(message string) int {
	rest, found := strings.CutPrefix(message, "line ")
	if !found {
		return 0
	}
	number, _, _ := strings.Cut(rest, ":")
	line, err := strconv.Atoi(number)
	if err != nil {
		return 0
	}
	return line
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
		EnvPrefix + "BROKER_CONNECT_TIMEOUT":  setDuration(&cfg.Broker.ConnectTimeout),
		// reconnect_backoff is a mapping and has no variable, as devices have
		// none: a mapping does not map onto flat variables.
		EnvPrefix + "BROKER_RECONNECT_INTERVAL": setDuration(&cfg.Broker.ReconnectInterval),

		EnvPrefix + "DELIVERY_PUBLISH_TIMEOUT": setDuration(&cfg.Delivery.PublishTimeout),
		EnvPrefix + "DELIVERY_BUFFER_SIZE":     setInt(&cfg.Delivery.BufferSize),

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
