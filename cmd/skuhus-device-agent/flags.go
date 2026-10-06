package main

import (
	"errors"
	"flag"
	"fmt"
	"strconv"

	"github.com/skuhus/device-agent/internal/config"
)

// configFlags are the flags run and validate share: the config file, and the
// overrides of the identity, the broker and the log, which come before the
// environment and the file (README.md, "Configuration").
type configFlags struct {
	path                                         *string
	project, site, station, instance             stringFlag
	brokerURL, credentialsFile, caFile, logLevel stringFlag
	insecure, logPayloads                        boolFlag
}

// newConfigFlags declares the shared flags on flags.
func newConfigFlags(flags *flag.FlagSet) *configFlags {
	shared := &configFlags{path: flags.String("config", "", "config file path (default "+config.DefaultPath()+")")}
	flags.Var(&shared.project, "project", "override identity.project")
	flags.Var(&shared.site, "site", "override identity.site")
	flags.Var(&shared.station, "station", "override identity.station")
	flags.Var(&shared.instance, "instance", "override identity.instance, the MQTT client id (default: the station id)")
	flags.Var(&shared.brokerURL, "broker-url", "override broker.url")
	flags.Var(&shared.credentialsFile, "broker-credentials-file", "override broker.credentials_file")
	flags.Var(&shared.caFile, "broker-ca-file", "override broker.ca_file")
	flags.Var(&shared.insecure, "broker-insecure", "override broker.insecure (development only)")
	flags.Var(&shared.logLevel, "log-level", "override logging.level")
	flags.Var(&shared.logPayloads, "log-payloads", "override logging.log_payloads")
	return shared
}

// loadOptions are the options config.Load takes from the flags: the file, and
// an override for each flag that was set.
func (shared *configFlags) loadOptions() config.Options {
	return config.Options{
		Path: *shared.path,
		Overrides: config.Overrides{
			Project:               shared.project.value,
			Site:                  shared.site.value,
			Station:               shared.station.value,
			Instance:              shared.instance.value,
			BrokerURL:             shared.brokerURL.value,
			BrokerCredentialsFile: shared.credentialsFile.value,
			BrokerCAFile:          shared.caFile.value,
			BrokerInsecure:        shared.insecure.value,
			LogLevel:              shared.logLevel.value,
			LogPayloads:           shared.logPayloads.value,
		},
	}
}

// parseCommandLine parses a command's flags and refuses positional arguments.
// A flag that does not parse, or an argument no command takes, is a usage
// error, which exits 2; -h stays flag.ErrHelp, which exits 0.
func parseCommandLine(flags *flag.FlagSet, args []string) error {
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return err
		}
		return fmt.Errorf("%w: %v", errUsage, err)
	}
	if flags.NArg() > 0 {
		return fmt.Errorf("%w: %s takes no positional arguments, got %q", errUsage, flags.Name(), flags.Arg(0))
	}
	return nil
}

// stringFlag records whether a flag was set, which is what keeps flags above
// the environment above the config file. The flag package has no built-in way
// to distinguish "set to the zero value" from "not set".
type stringFlag struct {
	value *string
}

func (flagValue *stringFlag) String() string {
	if flagValue == nil || flagValue.value == nil {
		return ""
	}
	return *flagValue.value
}

func (flagValue *stringFlag) Set(raw string) error {
	flagValue.value = &raw
	return nil
}

type boolFlag struct {
	value *bool
}

func (flagValue *boolFlag) String() string {
	if flagValue == nil || flagValue.value == nil {
		return "false"
	}
	return strconv.FormatBool(*flagValue.value)
}

func (flagValue *boolFlag) Set(raw string) error {
	parsed, err := strconv.ParseBool(raw)
	if err != nil {
		return fmt.Errorf("expected a boolean, got %q", raw)
	}
	flagValue.value = &parsed
	return nil
}

func (flagValue *boolFlag) IsBoolFlag() bool { return true }
