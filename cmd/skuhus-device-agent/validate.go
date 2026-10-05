package main

import (
	"flag"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"

	"github.com/skuhus/device-agent/internal/config"
	"github.com/skuhus/device-agent/internal/wire"
)

func validateCommand(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("validate", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.Usage = func() {
		fmt.Fprint(flags.Output(), "Usage: skuhus-device-agent validate [flags]\n\n"+
			"Loads the configuration, applies environment and flag overrides, and\n"+
			"reports every problem found. Exits 0 only when the configuration is\n"+
			"usable. Warnings do not affect the exit code.\n\n")
		flags.PrintDefaults()
	}

	path := flags.String("config", "", "config file path (default "+config.DefaultPath()+")")
	var project, site, station, instance stringFlag
	var brokerURL, credentialsFile, caFile, logLevel stringFlag
	var insecure, logPayloads boolFlag
	flags.Var(&project, "project", "override identity.project")
	flags.Var(&site, "site", "override identity.site")
	flags.Var(&station, "station", "override identity.station")
	flags.Var(&instance, "instance", "override identity.instance, the MQTT client id (default: the station id)")
	flags.Var(&brokerURL, "broker-url", "override broker.url")
	flags.Var(&credentialsFile, "broker-credentials-file", "override broker.credentials_file")
	flags.Var(&caFile, "broker-ca-file", "override broker.ca_file")
	flags.Var(&insecure, "broker-insecure", "override broker.insecure (development only)")
	flags.Var(&logLevel, "log-level", "override logging.level")
	flags.Var(&logPayloads, "log-payloads", "override logging.log_payloads")

	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() > 0 {
		return fmt.Errorf("%w: validate takes no positional arguments, got %q", errUsage, flags.Arg(0))
	}

	cfg, warnings, err := config.Load(config.Options{
		Path: *path,
		Overrides: config.Overrides{
			Project:               project.value,
			Site:                  site.value,
			Station:               station.value,
			Instance:              instance.value,
			BrokerURL:             brokerURL.value,
			BrokerCredentialsFile: credentialsFile.value,
			BrokerCAFile:          caFile.value,
			BrokerInsecure:        insecure.value,
			LogLevel:              logLevel.value,
			LogPayloads:           logPayloads.value,
		},
	})

	for _, warning := range warnings {
		fmt.Fprintln(stderr, "warning: "+warning.String())
	}
	if err != nil {
		return err
	}

	// Validation has checked every topic level, so building the topics cannot
	// fail here; an error would be a bug, and is returned as one.
	stationTopics, err := wire.NewStationTopics(cfg.Identity.Project, cfg.Identity.Site, cfg.Identity.Station)
	if err != nil {
		return err
	}
	agentTopics, err := stationTopics.Agent(cfg.Identity.Instance)
	if err != nil {
		return err
	}

	// Each section is printed under the names the file uses, every key, so
	// that what validate shows can be compared with the file key by key.
	broker := cfg.Broker
	broker.URL = broker.RedactedURL()
	fmt.Fprintf(stdout, "configuration is valid\n")
	fmt.Fprintf(stdout, "  identity       %s\n", describeSettings(cfg.Identity))
	fmt.Fprintf(stdout, "  agent status   %s\n", agentTopics.Status())
	fmt.Fprintf(stdout, "  broker         %s\n", describeSettings(broker))
	fmt.Fprintf(stdout, "  devices        %d\n", len(cfg.Devices))
	for _, deviceCfg := range cfg.Devices {
		fmt.Fprintf(stdout, "    %s\n", describeSettings(deviceCfg))
		topics, err := stationTopics.Device(deviceCfg.ID)
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "      topics rx %s status %s tx %s\n", topics.Rx(), topics.Status(), topics.Tx())
	}
	fmt.Fprintf(stdout, "  delivery       %s\n", describeSettings(cfg.Delivery))
	fmt.Fprintf(stdout, "  status         %s\n", describeSettings(cfg.Status))
	fmt.Fprintf(stdout, "  logging        %s\n", describeSettings(cfg.Logging))
	if len(warnings) > 0 {
		fmt.Fprintf(stdout, "  warnings       %d (listed on stderr)\n", len(warnings))
	}
	return nil
}

// describeSettings is a section of the configuration, or a device entry, as
// key=value for every key it has, named by its yaml tag. A nested mapping is
// written as the file would write it, {enabled: true, max: 30s}.
func describeSettings(section any) string {
	return strings.Join(settingPairs(reflect.ValueOf(section), "="), " ")
}

func settingPairs(section reflect.Value, separator string) []string {
	pairs := make([]string, 0, section.NumField())
	for index := 0; index < section.NumField(); index++ {
		name, _, _ := strings.Cut(section.Type().Field(index).Tag.Get("yaml"), ",")
		if name == "" || name == "-" {
			continue
		}
		pairs = append(pairs, name+separator+settingValue(section.Field(index)))
	}
	return pairs
}

func settingValue(value reflect.Value) string {
	if duration, isDuration := value.Interface().(config.Duration); isDuration {
		return duration.Duration().String()
	}
	switch value.Kind() {
	case reflect.String:
		return strconv.Quote(value.String())
	case reflect.Struct:
		return "{" + strings.Join(settingPairs(value, ": "), ", ") + "}"
	default:
		return fmt.Sprint(value.Interface())
	}
}
