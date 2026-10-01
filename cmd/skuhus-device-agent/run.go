package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/skuhus/device-agent/internal/config"
	"github.com/skuhus/device-agent/internal/core"
	serialdev "github.com/skuhus/device-agent/internal/device/serial"
	"github.com/skuhus/device-agent/internal/logging"
	"github.com/skuhus/device-agent/internal/transport/mqtt"
	buildinfo "github.com/skuhus/device-agent/internal/version"
	"github.com/skuhus/device-agent/internal/wire"
	goserial "go.bug.st/serial"
)

func runRun(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), "Usage: skuhus-device-agent run [flags]\n\n"+
			"Opens the configured devices, connects to the broker, and publishes\n"+
			"what each device reads until stopped. SIGTERM and SIGINT drain what is\n"+
			"already framed, publish the offline message and disconnect.\n\n"+
			"Broker credentials come from broker.credentials_file or from\n"+
			"SH_DEV_AGENT_MQTT_USERNAME and SH_DEV_AGENT_MQTT_PASSWORD. There is\n"+
			"no flag for them: ps would expose them to every user on the host.\n\n")
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
		return fmt.Errorf("%w: run takes no positional arguments, got %q", errUsage, fs.Arg(0))
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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return runAgent(ctx, cfg, stdout)
}

// runAgent assembles the agent from its parts and runs it: the log, the
// credentials, the topics, a reader per device, the connection with its will,
// and the core with its keepalive settings. It builds and does nothing else;
// every rule it relies on was checked when the configuration loaded.
func runAgent(ctx context.Context, cfg *config.Config, stdout io.Writer) error {
	started := time.Now()

	// The log file is opened first and closed last, so that it holds every
	// record, the last one included. Its failure to close can only be reported
	// on stderr.
	var file *logging.File
	if cfg.Logging.File != "" {
		opened, err := logging.OpenFile(cfg.Logging.File, cfg.Logging.MaxSizeMB, cfg.Logging.Keep)
		if err != nil {
			return err
		}
		file = opened
		defer func() {
			if err := file.Close(); err != nil {
				fmt.Fprintf(os.Stderr, "log file close failed: %v\n", err)
			}
		}()
	}
	logOpts := logging.Options{
		Level:        cfg.Logging.Level,
		Project:      cfg.Identity.Project,
		Site:         cfg.Identity.Site,
		Station:      cfg.Identity.Station,
		Host:         hostname(),
		Instance:     cfg.Identity.Instance,
		AgentVersion: buildinfo.Version(),
	}
	// Assigned only when set: a nil *logging.File in the interface would not
	// compare equal to nil, and the log would write to it.
	if file != nil {
		logOpts.File = file
	}
	if cfg.Logging.Stdout {
		logOpts.Out = stdout
	}
	log, err := logging.New(logOpts)
	if err != nil {
		return err
	}
	log.Info("starting", "version", buildinfo.Version(), "commit", buildinfo.Commit(), "built", buildinfo.Date(),
		"devices", len(cfg.Devices), "broker", cfg.Broker.RedactedURL())
	log.Info("log destinations", "file", cfg.Logging.File, "max_size_mb", cfg.Logging.MaxSizeMB, "keep", cfg.Logging.Keep,
		"stdout", cfg.Logging.Stdout, "log_level", cfg.Logging.Level, "log_payloads", cfg.Logging.LogPayloads)

	creds, err := config.LoadCredentials(cfg.Broker, config.EnvMap(os.Environ()))
	if err != nil {
		return err
	}

	station, err := wire.NewStationTopics(cfg.Identity.Project, cfg.Identity.Site, cfg.Identity.Station)
	if err != nil {
		return err
	}
	agentTopics, err := station.Agent(cfg.Identity.Instance)
	if err != nil {
		return err
	}
	builder := wire.NewBuilder(wire.Agent{
		Project:      cfg.Identity.Project,
		Site:         cfg.Identity.Site,
		Station:      cfg.Identity.Station,
		InstanceID:   cfg.Identity.Instance,
		AgentVersion: buildinfo.Version(),
	}, nil)

	devices := make([]core.Device, 0, len(cfg.Devices))
	for _, deviceCfg := range cfg.Devices {
		topics, err := station.Device(deviceCfg.ID)
		if err != nil {
			return err
		}
		parity, stopBits, err := lineFormat(deviceCfg)
		if err != nil {
			return err
		}
		reader, err := serialdev.New(serialdev.Options{
			ID:               deviceCfg.ID,
			Path:             deviceCfg.Path,
			Baud:             deviceCfg.Baud,
			DataBits:         deviceCfg.DataBits,
			Parity:           parity,
			StopBits:         stopBits,
			Terminator:       deviceCfg.SeparatorBytes(),
			MaxFrameBytes:    deviceCfg.MaxFrameBytes,
			InterCharTimeout: deviceCfg.InterCharTimeout.Duration(),
			LogPayloads:      cfg.Logging.LogPayloads,
			Logger:           log,
		})
		if err != nil {
			return err
		}
		devices = append(devices, core.Device{
			Reader: reader,
			Wire:   wire.Device{ID: deviceCfg.ID, Type: deviceCfg.DeviceType, Expiry: deviceCfg.MessageExpiry.Duration()},
			Topics: topics,
		})
	}
	log.Info("topics", "agent_status", agentTopics.Status(), "every_device_rx", station.EveryDeviceRx(),
		"every_device_status", station.EveryDeviceStatus(),
		"keepalive_interval", cfg.Status.KeepaliveInterval.Duration().String(), "missed_keepalives", cfg.Status.MissedKeepalives,
		"event_buffer_size", cfg.Status.EventBufferSize)

	// The will is composed for every connection attempt, because the broker
	// takes it in the CONNECT packet; its agent_ts is therefore when that
	// connection was made, as DESIGN-V2.md, "Message formats", says.
	will := func() ([]byte, error) {
		return json.Marshal(builder.Offline(wire.OfflineWill, time.Now()))
	}

	// The connection is dialled with its own context, not the run context. The
	// shutdown sequence publishes the offline message and sends DISCONNECT after
	// the run context is already cancelled; a connection torn down with that
	// context would leave the broker publishing the will instead, reporting a
	// crash where there was an orderly stop.
	connCtx, closeConn := context.WithCancel(context.Background())
	defer closeConn()
	// connected tells the core the connection came up, so that a keepalive goes
	// out at once. OnUp must not block, and one pending signal says all a
	// second would.
	connected := make(chan struct{}, 1)
	client, err := mqtt.Dial(connCtx, mqtt.Options{
		URL:            cfg.Broker.URL,
		ClientID:       cfg.Identity.Instance,
		Username:       creds.Username,
		Password:       creds.Password,
		CAFile:         cfg.Broker.CAFile,
		Insecure:       cfg.Broker.Insecure,
		Keepalive:      cfg.Broker.Keepalive.Duration(),
		BackoffInitial: cfg.Broker.ConnectBackoff.Initial.Duration(),
		BackoffMax:     cfg.Broker.ConnectBackoff.Max.Duration(),
		BackoffJitter:  cfg.Broker.ConnectBackoff.Jitter,
		WillTopic:      agentTopics.Status(),
		Will:           will,
		Logger:         log,
		OnUp: func() {
			select {
			case connected <- struct{}{}:
			default:
			}
		},
	})
	if err != nil {
		return err
	}

	// From here the connection exists, so every exit path closes it. A process
	// that returns without a DISCONNECT makes the broker publish the will,
	// which tells consumers a station crashed when it in fact refused to start.
	running, err := core.New(core.Options{
		Devices:           devices,
		Transport:         client,
		Builder:           builder,
		AgentStatus:       agentTopics.Status(),
		PublishTimeout:    cfg.Delivery.PublishTimeout.Duration(),
		BufferSize:        cfg.Delivery.BufferSize,
		EventBufferSize:   cfg.Status.EventBufferSize,
		KeepaliveInterval: cfg.Status.KeepaliveInterval.Duration(),
		MissedKeepalives:  cfg.Status.MissedKeepalives,
		Connected:         connected,
		Started:           started,
		LogPayloads:       cfg.Logging.LogPayloads,
		Logger:            log,
	})
	if err != nil {
		closeCtx, cancelClose := context.WithTimeout(context.Background(), cfg.Delivery.PublishTimeout.Duration())
		defer cancelClose()
		if closeErr := client.Close(closeCtx); closeErr != nil {
			log.Warn("broker disconnect failed", "error", closeErr.Error())
		}
		return err
	}
	return running.Run(ctx)
}

// lineFormat maps a device's parity and stop bits to the serial library's
// values. Validation accepts only mapped names, so a miss here is a bug, and
// it fails rather than opening the port with the zero value, 8N1.
func lineFormat(deviceCfg config.Device) (goserial.Parity, goserial.StopBits, error) {
	parity, ok := serialdev.ParityByName[string(deviceCfg.Parity)]
	if !ok {
		return 0, 0, fmt.Errorf("device %s: parity %q has no serial library value", deviceCfg.ID, deviceCfg.Parity)
	}
	stopBits, ok := serialdev.StopBitsByName[string(deviceCfg.StopBits)]
	if !ok {
		return 0, 0, fmt.Errorf("device %s: stop_bits %q has no serial library value", deviceCfg.ID, deviceCfg.StopBits)
	}
	return parity, stopBits, nil
}
