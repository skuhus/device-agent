package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/skuhus/device-agent/internal/config"
	"github.com/skuhus/device-agent/internal/core"
	"github.com/skuhus/device-agent/internal/logging"
	"github.com/skuhus/device-agent/internal/transport/mqtt"
	buildinfo "github.com/skuhus/device-agent/internal/version"
	"github.com/skuhus/device-agent/internal/wire"
)

func runCommand(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.Usage = func() {
		fmt.Fprint(flags.Output(), "Usage: skuhus-device-agent run [flags]\n\n"+
			"Opens the configured devices, connects to the broker, publishes what each\n"+
			"device reads, and writes to each device the tx it is sent, until stopped.\n"+
			"SIGTERM and SIGINT drain what is already framed, publish the offline\n"+
			"message and disconnect.\n\n"+
			"Broker credentials come from broker.credentials_file or from\n"+
			"SH_DEV_AGENT_MQTT_USERNAME and SH_DEV_AGENT_MQTT_PASSWORD. There is\n"+
			"no flag for them: ps would expose them to every user on the host.\n\n")
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

	if err := parseCommandLine(flags, args); err != nil {
		return err
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
	if err != nil {
		// There is no log yet, so the warnings go with the error to stderr.
		// Once the configuration loads, runAgent writes them to the log.
		for _, warning := range warnings {
			fmt.Fprintln(stderr, "warning: "+warning.String())
		}
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return runAgent(ctx, cfg, warnings, stdout)
}

// runAgent assembles the agent from its parts and runs it: the log, the
// credentials, the topics, a reader per device, the connection with its will,
// and the core with its keepalive settings. It builds and does nothing else;
// every rule it relies on was checked when the configuration loaded.
//
// Everything goes to the one log once it exists: the configuration's warnings,
// and the error that stops the agent, which main also prints to stderr.
func runAgent(ctx context.Context, cfg *config.Config, warnings []config.Warning, stdout io.Writer) (err error) {
	started := time.Now()

	log, closeLog, err := openLog(cfg, stdout)
	if err != nil {
		return err
	}
	defer closeLog()
	// Registered after closeLog, so it runs before it.
	defer func() {
		if err != nil {
			log.Error("agent stopped on an error", "error", err.Error())
		}
	}()
	log.Info("starting", "version", buildinfo.Version(), "commit", buildinfo.Commit(), "built", buildinfo.Date(),
		"devices", len(cfg.Devices), "broker", cfg.Broker.RedactedURL())
	log.Info("log destinations", "file", cfg.Logging.File, "max_size_mb", cfg.Logging.MaxSizeMB, "keep", cfg.Logging.Keep,
		"stdout", cfg.Logging.Stdout, "log_level", cfg.Logging.Level, "log_payloads", cfg.Logging.LogPayloads)
	for _, warning := range warnings {
		log.Warn("configuration warning", "field", warning.Field, "warning", warning.Message)
	}

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

	devices, subscriptions, err := buildDevices(cfg, station, log)
	if err != nil {
		return err
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
	txIn, onMessage := newTxIntake(cfg.Delivery.TxIntakeSize, log)
	client, err := mqtt.Dial(connCtx, mqtt.Options{
		URL:            cfg.Broker.URL,
		ClientID:       cfg.Identity.Instance,
		Username:       creds.Username,
		Password:       creds.Password,
		TLS:            cfg.Broker.UsesTLS(),
		CAFile:         cfg.Broker.CAFile,
		Insecure:       cfg.Broker.Insecure,
		Keepalive:      cfg.Broker.Keepalive.Duration(),
		ConnectTimeout: cfg.Broker.ConnectTimeout.Duration(),
		Reconnect:      cfg.Broker.ReconnectPolicy(),
		Subscriptions:  subscriptions,
		OnMessage:      onMessage,
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
		DrainTimeout:      cfg.Delivery.DrainTimeout.Duration(),
		BufferSize:        cfg.Delivery.BufferSize,
		EventBufferSize:   cfg.Status.EventBufferSize,
		KeepaliveInterval: cfg.Status.KeepaliveInterval.Duration(),
		MissedKeepalives:  cfg.Status.MissedKeepalives,
		Connected:         connected,
		Started:           started,
		TxIn:              txIn,
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

// openLog opens the one log: the file, when one is configured, and stdout,
// when it is on. The file is opened first and closed last, by closeLog, so
// that it holds every record, the last one included; its failure to close can
// only be reported on stderr.
func openLog(cfg *config.Config, stdout io.Writer) (log *slog.Logger, closeLog func(), err error) {
	logOpts := logging.Options{
		Level:        cfg.Logging.Level,
		Project:      cfg.Identity.Project,
		Site:         cfg.Identity.Site,
		Station:      cfg.Identity.Station,
		Host:         hostname(),
		Instance:     cfg.Identity.Instance,
		AgentVersion: buildinfo.Version(),
	}
	closeLog = func() {}
	if cfg.Logging.File != "" {
		file, err := logging.OpenFile(cfg.Logging.File, cfg.Logging.MaxSizeMB, cfg.Logging.Keep)
		if err != nil {
			return nil, nil, err
		}
		// Assigned only when set: a nil *logging.File in the interface would
		// not compare equal to nil, and the log would write to it.
		logOpts.File = file
		closeLog = func() {
			if err := file.Close(); err != nil {
				fmt.Fprintf(os.Stderr, "log file close failed: %v\n", err)
			}
		}
	}
	if cfg.Logging.Stdout {
		logOpts.Out = stdout
	}
	log, err = logging.New(logOpts)
	if err != nil {
		closeLog()
		return nil, nil, err
	}
	return log, closeLog, nil
}

// buildDevices builds each configured device as the core runs it, with its
// reader, its topics and its broadcast groups' topics, and lists the topics the
// connection subscribes to: every device's tx topic, and each group's once,
// however many devices are in the group. The same values go to the core and to
// the subscription, so the keepalive reports the topics as subscribed.
func buildDevices(cfg *config.Config, station wire.StationTopics, log *slog.Logger) ([]core.Device, []string, error) {
	devices := make([]core.Device, 0, len(cfg.Devices))
	subscriptions := make([]string, 0, len(cfg.Devices))
	subscribed := make(map[string]bool, len(cfg.Devices))
	subscribe := func(topic string) {
		if !subscribed[topic] {
			subscribed[topic] = true
			subscriptions = append(subscriptions, topic)
		}
	}
	for _, deviceCfg := range cfg.Devices {
		topics, err := station.Device(deviceCfg.ID)
		if err != nil {
			return nil, nil, err
		}
		groups, err := broadcastGroupRoutes(station, deviceCfg.BroadcastGroups)
		if err != nil {
			return nil, nil, err
		}
		reader, err := newSerialReader(deviceCfg, cfg.Logging.LogPayloads, log)
		if err != nil {
			return nil, nil, err
		}
		devices = append(devices, core.Device{
			Reader:          reader,
			Wire:            wire.Device{ID: deviceCfg.ID, Type: deviceCfg.DeviceType, Expiry: deviceCfg.MessageExpiry.Duration()},
			Topics:          topics,
			TxOpenAttempts:  deviceCfg.TxOpenAttempts,
			TxOpenInterval:  deviceCfg.TxOpenInterval.Duration(),
			TxRememberedIDs: deviceCfg.TxRememberedIDs,
			BroadcastGroups: groups,
		})
		subscribe(topics.Tx())
		for _, route := range groups {
			subscribe(route.Topic)
		}
	}
	return devices, subscriptions, nil
}

// newTxIntake returns the channel that carries each tx from paho's goroutine
// to the core, holding at most size, and the function paho hands each message
// to. paho delivers one message after another and must not wait, or every
// acknowledgement behind it waits too, so a tx that finds the intake full is
// recorded as dropped, with its data. Its sender gets no result and treats it
// as not written.
func newTxIntake(size int, log *slog.Logger) (chan core.TxMessage, func(mqtt.Message)) {
	intake := make(chan core.TxMessage, size)
	log.Debug("tx intake ready", "tx_intake_size", size)
	onMessage := func(message mqtt.Message) {
		select {
		case intake <- core.TxMessage{Topic: message.Topic, Payload: message.Payload, Expiry: message.Expiry,
			HasExpiry: message.HasExpiry, Received: message.Received}:
		default:
			ref, _, _ := wire.ReadTx(message.Payload)
			logging.Record(log, slog.LevelError, "tx dropped: the agent's intake is full",
				append([]any{"topic", message.Topic, "tx_id", ref.ID, "sender", ref.Sender, "tx_intake_size", size},
					logging.Payload(message.Payload)...)...)
		}
	}
	return intake, onMessage
}
