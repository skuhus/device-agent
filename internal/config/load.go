package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

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

// LoadError is why Load failed: every problem it found, each on its own, and
// the file, once Load had chosen one. Its text is what run and probe print:
// what failed, then one problem a line.
type LoadError struct {
	// Path is the configuration file, or empty when loading stopped before
	// choosing one.
	Path     string
	Problems []error
	// failed says what failed, such as "invalid config /etc/x.yaml"; it comes
	// before the problems in the text, and is empty when they say it.
	failed string
}

func (loadErr *LoadError) Error() string {
	text := errors.Join(loadErr.Problems...).Error()
	if loadErr.failed == "" {
		return text
	}
	return loadErr.failed + ": " + text
}

// Unwrap gives the problems to errors.Is and errors.As.
func (loadErr *LoadError) Unwrap() []error { return loadErr.Problems }

// problemsOf splits an error that joins several problems into them.
func problemsOf(err error) []error {
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		return joined.Unwrap()
	}
	return []error{err}
}

// Load reads, merges and validates configuration.
//
// It returns the merged configuration and any non-fatal warnings. It fails,
// with a *LoadError, for a missing or unreadable file, an unknown key, an
// unrecognised SH_DEV_AGENT_* variable, or any validation failure, and names
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
		return nil, nil, &LoadError{Problems: problemsOf(err)}
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
			return nil, nil, &LoadError{Path: path, Problems: []error{
				fmt.Errorf("no config file at the default location %s: create it or pass --config", path)}}
		}
		return nil, nil, &LoadError{Path: path, Problems: []error{err}, failed: "open config " + path}
	}
	defer file.Close()

	cfg, err := decode(file)
	if err != nil {
		return nil, nil, &LoadError{Path: path, Problems: problemsOf(err), failed: "parse config " + path}
	}
	cfg.File = path

	if err := applyEnv(cfg, env); err != nil {
		return nil, nil, &LoadError{Path: path, Problems: problemsOf(err)}
	}
	applyOverrides(cfg, opts.Overrides)
	applyDerivedDefaults(cfg)

	if opts.SkipValidate {
		return cfg, nil, nil
	}
	warnings, err := Validate(cfg, env)
	if err != nil {
		return nil, warnings, &LoadError{Path: path, Problems: problemsOf(err), failed: "invalid config " + path}
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
func decode(reader io.Reader) (*Config, error) {
	body, err := io.ReadAll(reader)
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
	for sectionIndex := 0; sectionIndex+1 < len(root.Content); sectionIndex += 2 {
		section, value := root.Content[sectionIndex].Value, root.Content[sectionIndex+1]
		switch value.Kind {
		case yaml.MappingNode:
			for keyIndex := 0; keyIndex+1 < len(value.Content); keyIndex += 2 {
				key := value.Content[keyIndex]
				path := section + "." + key.Value
				check(key, path, path)
			}
		case yaml.SequenceNode:
			for index, entry := range value.Content {
				if entry.Kind != yaml.MappingNode {
					continue
				}
				for keyIndex := 0; keyIndex+1 < len(entry.Content); keyIndex += 2 {
					key := entry.Content[keyIndex]
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

func applyOverrides(cfg *Config, overrides Overrides) {
	assign(&cfg.Identity.Project, overrides.Project)
	assign(&cfg.Identity.Site, overrides.Site)
	assign(&cfg.Identity.Station, overrides.Station)
	assign(&cfg.Broker.URL, overrides.BrokerURL)
	assign(&cfg.Identity.Instance, overrides.Instance)
	assign(&cfg.Broker.CredentialsFile, overrides.BrokerCredentialsFile)
	assign(&cfg.Broker.CAFile, overrides.BrokerCAFile)
	assign(&cfg.Broker.Insecure, overrides.BrokerInsecure)
	assign(&cfg.Logging.Level, overrides.LogLevel)
	assign(&cfg.Logging.LogPayloads, overrides.LogPayloads)
}

func assign[Value any](dst *Value, src *Value) {
	if src != nil {
		*dst = *src
	}
}
