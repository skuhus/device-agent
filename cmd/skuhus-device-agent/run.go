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
	log, err := logging.New(logging.Options{
		Level:        cfg.Logging.Level,
		Out:          stdout,
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
	if cfg.Logging.AuditFile != "" {
		audit, err = logging.OpenAudit(cfg.Logging.AuditFile, cfg.Logging.AuditMaxSizeMB, cfg.Logging.AuditKeep)
		if err != nil {
			return err
		}
		defer func() {
			if err := audit.Close(); err != nil {
				log.Error("audit log close failed", "error", err.Error())
			}
		}()
		log.Info("audit log open", "path", cfg.Logging.AuditFile,
			"max_size_mb", cfg.Logging.AuditMaxSizeMB, "keep", cfg.Logging.AuditKeep)
	} else {
		log.Warn("no audit file configured; scan outcomes are recorded only in the process log")
	}

	ids := make([]string, 0, len(cfg.Devices))
	for _, deviceCfg := range cfg.Devices {
		ids = append(ids, deviceCfg.ID)
	}
	presence := agent.NewPresence(ids...)

	devices := make([]device.Device, 0, len(cfg.Devices))
	for _, deviceCfg := range cfg.Devices {
		sd, err := serialdev.New(serialdev.Options{
			ID:               deviceCfg.ID,
			Path:             deviceCfg.Path,
			Baud:             deviceCfg.Baud,
			Terminator:       deviceCfg.TerminatorBytes(),
			MaxFrameBytes:    deviceCfg.MaxFrameBytes,
			InterCharTimeout: deviceCfg.InterCharTimeout.Duration(),
			AssertConfig:     deviceCfg.AssertConfig,
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
		ScanTTL:        cfg.Delivery.ScanTTL.Duration(),
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
