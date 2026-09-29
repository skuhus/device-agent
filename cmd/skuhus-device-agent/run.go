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

	"github.com/skuhus/device-agent/internal/agent"
	"github.com/skuhus/device-agent/internal/config"
	"github.com/skuhus/device-agent/internal/device"
	serialdev "github.com/skuhus/device-agent/internal/device/serial"
	"github.com/skuhus/device-agent/internal/event"
	"github.com/skuhus/device-agent/internal/logging"
	"github.com/skuhus/device-agent/internal/transport/mqtt"
	buildinfo "github.com/skuhus/device-agent/internal/version"
	goserial "go.bug.st/serial"
)

func runRun(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), "Usage: skuhus-device-agent run [flags]\n\n"+
			"Opens the configured devices, connects to the broker, and publishes\n"+
			"scans until stopped. SIGTERM and SIGINT drain what is already framed,\n"+
			"publish an offline status and disconnect.\n\n"+
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

// runAgent wires the supervisor to the real device, transport, logging and
// audit implementations. It is separate from flag parsing so the wiring can be
// read without the flags around it.
func runAgent(ctx context.Context, cfg *config.Config, stdout io.Writer) error {
	// Until the common log (T9), logging.stdout decides whether this process
	// log is written at all, and logging.file receives the delivery records.
	processLog := stdout
	if !cfg.Logging.Stdout {
		processLog = io.Discard
	}
	log, err := logging.New(logging.Options{
		Level:        cfg.Logging.Level,
		Out:          processLog,
		Project:      cfg.Identity.Project,
		Site:         cfg.Identity.Site,
		Station:      cfg.Identity.Station,
		Host:         hostname(),
		Instance:     cfg.Identity.Instance,
		AgentVersion: buildinfo.Version(),
	})
	if err != nil {
		return err
	}
	log.Info("starting", "version", buildinfo.Version(), "commit", buildinfo.Commit(), "built", buildinfo.Date(),
		"devices", len(cfg.Devices), "broker", cfg.Broker.RedactedURL())

	creds, err := config.LoadCredentials(cfg.Broker, config.EnvMap(os.Environ()))
	if err != nil {
		return err
	}

	var audit *logging.Audit
	if cfg.Logging.File != "" {
		audit, err = logging.OpenAudit(cfg.Logging.File, cfg.Logging.MaxSizeMB, cfg.Logging.Keep)
		if err != nil {
			return err
		}
		defer func() {
			if err := audit.Close(); err != nil {
				log.Error("log file close failed", "error", err.Error())
			}
		}()
		log.Info("log file open for delivery records", "path", cfg.Logging.File,
			"max_size_mb", cfg.Logging.MaxSizeMB, "keep", cfg.Logging.Keep)
	} else {
		log.Info("no logging.file configured; delivery outcomes are recorded only in the process log")
	}

	// The v1 supervisor publishes every device's frames with one expiry. Until
	// the v2 core (T7) publishes per device, the shortest configured expiry is
	// used: a reading may then be discarded early, never delivered late.
	expiry := shortestExpiry(cfg.Devices)
	for _, deviceCfg := range cfg.Devices {
		if deviceCfg.MessageExpiry.Duration() != expiry {
			log.Warn("devices set different message_expiry values; every device publishes with the shortest until the v2 core",
				"message_expiry", expiry.String())
			break
		}
	}

	ids := make([]string, 0, len(cfg.Devices))
	for _, deviceCfg := range cfg.Devices {
		ids = append(ids, deviceCfg.ID)
	}
	presence := agent.NewPresence(ids...)

	devices := make([]device.Device, 0, len(cfg.Devices))
	for _, deviceCfg := range cfg.Devices {
		parity, stopBits, err := lineFormat(deviceCfg)
		if err != nil {
			return err
		}
		sd, err := serialdev.New(serialdev.Options{
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
			OnPresence: func(present bool, _ error) {
				presence.Set(deviceCfg.ID, present)
			},
		})
		if err != nil {
			return err
		}
		devices = append(devices, sd)
	}

	identity := event.Identity{
		Project:      cfg.Identity.Project,
		Site:         cfg.Identity.Site,
		Station:      cfg.Identity.Station,
		InstanceID:   cfg.Identity.Instance,
		AgentVersion: buildinfo.Version(),
	}

	topics := mqtt.NewTopics(cfg.Identity.Project, cfg.Identity.Site, cfg.Identity.Station)
	log.Info("topics", "scan", topics.Scan(), "status", topics.Status(), "heartbeat", topics.Heartbeat())

	// The will is built before the connection, because the broker needs it in
	// the CONNECT packet. device_present is false in it: a will is delivered
	// when the agent is gone, and an agent that is gone has no open device.
	now := time.Now()
	will, err := json.Marshal(event.NewStatus(identity, event.StateOffline, event.ReasonWill,
		false, now, now))
	if err != nil {
		return fmt.Errorf("encode will payload: %w", err)
	}

	// Buffered by one: the connection callback must not block, and a second
	// connection event arriving before the first is handled means the same
	// thing as one.
	connected := make(chan struct{}, 1)

	// The connection is dialled with its own context, not the run context. The
	// shutdown sequence publishes an offline status and sends DISCONNECT after
	// the run context is already cancelled; a connection torn down with that
	// context would leave the broker publishing the will instead, reporting a
	// crash where there was an orderly stop.
	connCtx, closeConn := context.WithCancel(context.Background())
	defer closeConn()

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
		Topics:         topics,
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

	// From here the connection exists, so every exit path has to close it. A
	// process that returns without a DISCONNECT makes the broker publish the
	// will, which tells consumers a station crashed when it in fact refused to
	// start.
	supervisor, err := agent.New(agent.Options{
		Devices:        devices,
		Publisher:      client,
		Builder:        event.NewBuilder(identity, nil),
		Presence:       presence,
		Connected:      connected,
		ScanTTL:        expiry,
		PublishTimeout: cfg.Delivery.PublishTimeout.Duration(),
		BufferSize:     cfg.Delivery.BufferSize,
		Identity:       identity,
		LogPayloads:    cfg.Logging.LogPayloads,
		Logger:         log,
		Audit:          audit,
	})
	if err != nil {
		closeCtx, cancelClose := context.WithTimeout(context.Background(), cfg.Delivery.PublishTimeout.Duration())
		defer cancelClose()
		if closeErr := client.Close(closeCtx); closeErr != nil {
			log.Warn("broker disconnect failed", "error", closeErr.Error())
		}
		return err
	}
	return supervisor.Run(ctx)
}

// shortestExpiry is the smallest message_expiry among the devices. Validation
// guarantees at least one device, each with an expiry of at least a second.
func shortestExpiry(devices []config.Device) time.Duration {
	shortest := devices[0].MessageExpiry.Duration()
	for _, deviceCfg := range devices[1:] {
		shortest = min(shortest, deviceCfg.MessageExpiry.Duration())
	}
	return shortest
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
