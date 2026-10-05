package main

import (
	"flag"
	"fmt"
	"io"

	"github.com/skuhus/device-agent/internal/config"
	"github.com/skuhus/device-agent/internal/wire"
)

func runValidate(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), "Usage: skuhus-device-agent validate [flags]\n\n"+
			"Loads the configuration, applies environment and flag overrides, and\n"+
			"reports every problem found. Exits 0 only when the configuration is\n"+
			"usable. Warnings do not affect the exit code.\n\n")
		fs.PrintDefaults()
	}

	path := fs.String("config", "", "config file path (default "+config.DefaultPath()+")")
	var project, site, station, instance stringFlag
	var brokerURL, credentialsFile, caFile, logLevel stringFlag
	var insecure, logPayloads boolFlag
	fs.Var(&project, "project", "override identity.project")
	fs.Var(&site, "site", "override identity.site")
	fs.Var(&station, "station", "override identity.station")
	fs.Var(&instance, "instance", "override identity.instance, the MQTT client id (default: the station id)")
	fs.Var(&brokerURL, "broker-url", "override broker.url")
	fs.Var(&credentialsFile, "broker-credentials-file", "override broker.credentials_file")
	fs.Var(&caFile, "broker-ca-file", "override broker.ca_file")
	fs.Var(&insecure, "broker-insecure", "override broker.insecure (development only)")
	fs.Var(&logLevel, "log-level", "override logging.level")
	fs.Var(&logPayloads, "log-payloads", "override logging.log_payloads")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("%w: validate takes no positional arguments, got %q", errUsage, fs.Arg(0))
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

	fmt.Fprintf(stdout, "configuration is valid\n")
	fmt.Fprintf(stdout, "  station        %s/%s/%s\n", cfg.Identity.Project, cfg.Identity.Site, cfg.Identity.Station)
	fmt.Fprintf(stdout, "  instance       %s (MQTT client id)\n", cfg.Identity.Instance)
	fmt.Fprintf(stdout, "  agent status   %s\n", agentTopics.Status())
	fmt.Fprintf(stdout, "  broker         %s keepalive=%s connect_timeout=%s reconnect_interval=%s reconnect_backoff=%s\n",
		cfg.Broker.RedactedURL(), cfg.Broker.Keepalive, cfg.Broker.ConnectTimeout,
		cfg.Broker.ReconnectInterval, describeBackoff(cfg.Broker.ReconnectBackoff))
	fmt.Fprintf(stdout, "  devices        %d\n", len(cfg.Devices))
	for _, deviceCfg := range cfg.Devices {
		fmt.Fprintf(stdout, "    %-16s %s kind=%s baud=%d format=%d/%s/%s separator=%q max_frame=%d inter_char=%s message_expiry=%s device_type=%q tx_open_attempts=%d tx_open_interval=%s reopen_interval=%s reopen_backoff=%s\n",
			deviceCfg.ID, deviceCfg.Path, deviceCfg.Kind, deviceCfg.Baud,
			deviceCfg.DataBits, deviceCfg.Parity, deviceCfg.StopBits, deviceCfg.Separator,
			deviceCfg.MaxFrameBytes, deviceCfg.InterCharTimeout, deviceCfg.MessageExpiry, deviceCfg.DeviceType,
			deviceCfg.TxOpenAttempts, deviceCfg.TxOpenInterval, deviceCfg.ReopenInterval, describeBackoff(deviceCfg.ReopenBackoff))
		topics, err := stationTopics.Device(deviceCfg.ID)
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "    %-16s rx %s status %s tx %s\n", "", topics.Rx(), topics.Status(), topics.Tx())
	}
	fmt.Fprintf(stdout, "  delivery       publish_timeout=%s buffer_size=%d drain_timeout=%s\n",
		cfg.Delivery.PublishTimeout, cfg.Delivery.BufferSize, cfg.Delivery.DrainTimeout)
	fmt.Fprintf(stdout, "  status         keepalive_interval=%s missed_keepalives=%d event_buffer_size=%d\n",
		cfg.Status.KeepaliveInterval, cfg.Status.MissedKeepalives, cfg.Status.EventBufferSize)
	fmt.Fprintf(stdout, "  logging        level=%s log_payloads=%t file=%q max=%dMB keep=%d stdout=%t\n",
		cfg.Logging.Level, cfg.Logging.LogPayloads, cfg.Logging.File,
		cfg.Logging.MaxSizeMB, cfg.Logging.Keep, cfg.Logging.Stdout)
	if len(warnings) > 0 {
		fmt.Fprintf(stdout, "  warnings       %d (listed on stderr)\n", len(warnings))
	}
	return nil
}

// describeBackoff is a backoff as validate prints it: "off", or its ceiling
// and jitter, which apply only when it is on.
func describeBackoff(settings config.Backoff) string {
	if !settings.Enabled {
		return "off"
	}
	return fmt.Sprintf("max=%s jitter=%v", settings.Max, settings.Jitter)
}
