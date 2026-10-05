package config

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// configKey is one key the configuration file takes: its path, such as
// broker.url or devices[].baud, and whether it sits inside a mapping or a
// list, where no environment variable reaches it.
type configKey struct {
	path   string
	nested bool
}

// configKeys lists every key the Config type decodes, from its yaml tags.
func configKeys(structType reflect.Type, prefix string, nested bool) []configKey {
	var keys []configKey
	for index := 0; index < structType.NumField(); index++ {
		field := structType.Field(index)
		name, _, _ := strings.Cut(field.Tag.Get("yaml"), ",")
		if name == "" || name == "-" {
			continue
		}
		path := prefix + name
		switch {
		case field.Type.Kind() == reflect.Struct && prefix == "":
			keys = append(keys, configKeys(field.Type, path+".", false)...)
		case field.Type.Kind() == reflect.Struct:
			keys = append(keys, configKey{path: path, nested: true})
			keys = append(keys, configKeys(field.Type, path+".", true)...)
		case field.Type.Kind() == reflect.Slice && field.Type.Elem().Kind() == reflect.Struct:
			keys = append(keys, configKey{path: path, nested: true})
			keys = append(keys, configKeys(field.Type.Elem(), path+"[].", true)...)
		default:
			keys = append(keys, configKey{path: path, nested: nested})
		}
	}
	return keys
}

// documentKeys lists every key a YAML document sets, in the same form.
func documentKeys(node *yaml.Node, prefix string, keys map[string]bool) {
	switch node.Kind {
	case yaml.DocumentNode:
		for _, child := range node.Content {
			documentKeys(child, prefix, keys)
		}
	case yaml.MappingNode:
		for index := 0; index+1 < len(node.Content); index += 2 {
			path := prefix + node.Content[index].Value
			keys[path] = true
			value := node.Content[index+1]
			switch value.Kind {
			case yaml.MappingNode:
				documentKeys(value, path+".", keys)
			case yaml.SequenceNode:
				for _, entry := range value.Content {
					documentKeys(entry, path+"[].", keys)
				}
			}
		}
	}
}

// sampleConfig is the shipped sample with its two paths that must exist on a
// station pointed at files the fixture creates.
func sampleConfig(t *testing.T) (fixture, string) {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "..", "config.sample.yaml"))
	if err != nil {
		t.Fatalf("read the sample: %v", err)
	}
	fixture := newFixture(t, "")
	sample := string(body)
	for original, replacement := range map[string]string{
		"/etc/skuhus-device-agent/credentials":   fixture.credentialsFile,
		"/var/log/skuhus-device-agent/agent.log": fixture.logFile,
	} {
		if !strings.Contains(sample, original) {
			t.Fatalf("the sample no longer names %s; update this test with what replaced it", original)
		}
		sample = strings.ReplaceAll(sample, original, replacement)
	}
	return fixture, sample
}

// The sample is where an operator learns the settings, so every key the
// configuration takes is in it as a key, not only in a comment.
func TestSampleConfigShowsEveryKey(t *testing.T) {
	_, sample := sampleConfig(t)
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte(sample), &doc); err != nil {
		t.Fatalf("parse the sample: %v", err)
	}
	present := map[string]bool{}
	documentKeys(&doc, "", present)
	keys := configKeys(reflect.TypeOf(Config{}), "", false)
	if len(keys) < 40 {
		t.Fatalf("found %d keys in Config, fewer than it has; the walk is broken", len(keys))
	}
	for _, key := range keys {
		if !present[key.path] {
			t.Errorf("%s is not in config.sample.yaml", key.path)
		}
	}
}

// Every key outside devices and outside a mapping has an environment
// variable named after its section and key, as README.md, "Configuration",
// says.
func TestEveryScalarKeyHasAnEnvironmentVariable(t *testing.T) {
	recognised := envTargets(&Config{})
	for _, key := range configKeys(reflect.TypeOf(Config{}), "", false) {
		if key.nested {
			continue
		}
		name := EnvPrefix + strings.ToUpper(strings.ReplaceAll(key.path, ".", "_"))
		if _, found := recognised[name]; !found {
			t.Errorf("%s has no environment variable %s", key.path, name)
		}
	}
}

// The sample's values are the defaults, except where a station has to say
// its own: its identity, its broker and credentials, its log file, and each
// device's id, path, separator and type. An operator who leaves a key as the
// sample has it gets what the agent does without it.
func TestSampleConfigShowsTheDefaults(t *testing.T) {
	fixture, sample := sampleConfig(t)
	if err := os.WriteFile(fixture.path, []byte(sample), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	cfg, _, err := load(t, fixture.path, noEnv(), Overrides{})
	if err != nil {
		t.Fatalf("the sample does not validate: %v", err)
	}

	expected := Defaults()
	expected.Identity = cfg.Identity
	expected.Broker.URL, expected.Broker.CredentialsFile = cfg.Broker.URL, cfg.Broker.CredentialsFile
	expected.Logging.File = cfg.Logging.File
	// File is where the configuration came from, not a key.
	expected.File = cfg.File
	for _, deviceCfg := range cfg.Devices {
		device := DefaultDevice()
		device.ID, device.Path, device.Separator, device.DeviceType = deviceCfg.ID, deviceCfg.Path, deviceCfg.Separator, deviceCfg.DeviceType
		expected.Devices = append(expected.Devices, device)
	}
	for _, difference := range differences(reflect.ValueOf(*cfg), reflect.ValueOf(expected), "") {
		t.Errorf("the sample sets %s", difference)
	}
}

// differences names each field where got and want differ, with both values.
func differences(got, want reflect.Value, path string) []string {
	switch got.Kind() {
	case reflect.Struct:
		var found []string
		for index := 0; index < got.NumField(); index++ {
			found = append(found, differences(got.Field(index), want.Field(index), path+"."+got.Type().Field(index).Name)...)
		}
		return found
	case reflect.Slice:
		if got.Len() != want.Len() {
			return []string{fmt.Sprintf("%s to %d entries, the default has %d", path, got.Len(), want.Len())}
		}
		var found []string
		for index := 0; index < got.Len(); index++ {
			found = append(found, differences(got.Index(index), want.Index(index), fmt.Sprintf("%s[%d]", path, index))...)
		}
		return found
	default:
		if !reflect.DeepEqual(got.Interface(), want.Interface()) {
			return []string{fmt.Sprintf("%s to %v, the default is %v", path, got.Interface(), want.Interface())}
		}
		return nil
	}
}
