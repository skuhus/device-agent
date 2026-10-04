package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	"github.com/skuhus/device-agent/internal/config"
	"github.com/skuhus/device-agent/internal/device"
	serialdev "github.com/skuhus/device-agent/internal/device/serial"
	"github.com/skuhus/device-agent/internal/logging"
	buildinfo "github.com/skuhus/device-agent/internal/version"
	"github.com/skuhus/device-agent/internal/wire"
	goserial "go.bug.st/serial"
)

func runProbe(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("probe", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), "Usage: skuhus-device-agent probe [flags]\n\n"+
			"With --list, enumerates candidate serial devices and the stable paths\n"+
			"that point at them. Otherwise opens one device and prints every framed\n"+
			"payload to stdout until interrupted.\n\n"+
			"Take the device settings either from the config file (--device NAME) or\n"+
			"from flags (--path ...). Diagnostic output goes to stdout and the\n"+
			"structured log to stderr, so the two can be redirected separately.\n\n"+
			"probe prints payload contents by design; logging.log_payloads does not\n"+
			"apply to it.\n\n")
		fs.PrintDefaults()
	}

	list := fs.Bool("list", false, "enumerate candidate devices and exit")
	cfgPath := fs.String("config", "", "config file path (default "+config.DefaultPath()+")")
	deviceID := fs.String("device", "", "take settings from this device id in the config file")
	path := fs.String("path", "", "device path, when not using --device")
	baud := fs.Int("baud", config.DefaultBaud, "baud rate (ignored by USB-CDC devices)")
	dataBits := fs.Int("data-bits", config.DefaultDataBits, "data bits, 5 to 8 (ignored by USB-CDC devices)")
	parity := fs.String("parity", string(config.DefaultParity), "parity: none, odd, even, mark or space (ignored by USB-CDC devices)")
	stopBits := fs.String("stop-bits", string(config.DefaultStopBits), "stop bits, 1 or 2 (ignored by USB-CDC devices)")
	separator := fs.String("separator", `\r`, "frame separator, backslash escapes decoded")
	maxFrame := fs.Int("max-frame-bytes", config.DefaultMaxFrameBytes, "discard a partial frame longer than this")
	interChar := fs.Duration("inter-char-timeout", config.DefaultInterCharTimeout, "discard a partial frame idle for longer than this")
	asJSON := fs.Bool("json", false, "print the rx message that would be published")
	duration := fs.Duration("duration", 0, "stop after this long (0 means run until interrupted)")
	logLevel := fs.String("log-level", "info", "log level for the structured log on stderr")
	logPayloads := fs.Bool("log-payloads", false, "put discarded bytes on each discard's log line, as hex and as text when valid UTF-8; use this when a device frames nothing and the separator is unknown")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("%w: probe takes no positional arguments, got %q", errUsage, fs.Arg(0))
	}
	if *list {
		return listDevices(stdout)
	}
	if (*deviceID == "") == (*path == "") {
		return fmt.Errorf("%w: pass exactly one of --device (with a config file) or --path", errUsage)
	}

	// The identity is filled from the config below when --device names one, so
	// that --json prints the message run would publish rather than a lookalike.
	identity := wire.Agent{InstanceID: "probe", AgentVersion: buildinfo.Version()}
	// probe publishes nothing, so the message expiry only has to satisfy
	// validation.
	dev := config.Device{
		ID:               "probe",
		Kind:             config.KindSerial,
		Path:             *path,
		Baud:             *baud,
		DataBits:         *dataBits,
		Parity:           config.Parity(*parity),
		StopBits:         config.StopBits(*stopBits),
		MaxFrameBytes:    *maxFrame,
		InterCharTimeout: config.Duration(*interChar),
		MessageExpiry:    config.Duration(config.DefaultMessageExpiry),
		// probe writes nothing; these only have to satisfy validation.
		TxOpenAttempts: config.DefaultTxOpenAttempts,
		TxOpenInterval: config.Duration(config.DefaultTxOpenInterval),
	}

	if *deviceID != "" {
		cfg, _, err := config.Load(config.Options{Path: *cfgPath, SkipValidate: true})
		if err != nil {
			return err
		}
		found := false
		for _, deviceCfg := range cfg.Devices {
			if deviceCfg.ID == *deviceID {
				dev, found = deviceCfg, true
				break
			}
		}
		if !found {
			ids := make([]string, 0, len(cfg.Devices))
			for _, deviceCfg := range cfg.Devices {
				ids = append(ids, deviceCfg.ID)
			}
			return fmt.Errorf("no device %q in the config file (have: %s)", *deviceID, strings.Join(ids, ", "))
		}
		identity.Project = cfg.Identity.Project
		identity.Site = cfg.Identity.Site
		identity.Station = cfg.Identity.Station
		identity.InstanceID = cfg.Identity.Instance
	} else {
		sep, err := parseSeparator(*separator)
		if err != nil {
			return fmt.Errorf("%w: %s", errUsage, err)
		}
		dev.Separator = string(sep)
	}

	if warnings, err := config.ValidateDevice(dev); err != nil {
		return err
	} else {
		for _, w := range warnings {
			fmt.Fprintln(stderr, "warning: "+w.String())
		}
	}

	log, err := logging.New(logging.Options{
		Level:        *logLevel,
		Out:          stderr,
		Project:      identity.Project,
		Site:         identity.Site,
		Station:      identity.Station,
		Host:         hostname(),
		AgentVersion: identity.AgentVersion,
	})
	if err != nil {
		return err
	}

	lineParity, lineStopBits, err := lineFormat(dev)
	if err != nil {
		return err
	}
	sd, err := serialdev.New(serialdev.Options{
		ID:               dev.ID,
		Path:             dev.Path,
		Baud:             dev.Baud,
		DataBits:         dev.DataBits,
		Parity:           lineParity,
		StopBits:         lineStopBits,
		Terminator:       dev.SeparatorBytes(),
		MaxFrameBytes:    dev.MaxFrameBytes,
		InterCharTimeout: dev.InterCharTimeout.Duration(),
		LogPayloads:      *logPayloads,
		Logger:           log,
		Reopen:           serialdev.DefaultReopen,
	})
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	presence := &presenceLog{log: log.With("device_id", dev.ID, "device_path", dev.Path)}
	frames := make(chan device.Frame, 16)
	done := make(chan error, 1)
	go func() { done <- sd.Run(ctx, frames, presence.report) }()

	builder := wire.NewBuilder(identity, nil)
	wireDevice := wire.Device{ID: dev.ID, Type: dev.DeviceType, Expiry: dev.MessageExpiry.Duration()}
	var seq uint64

	fmt.Fprintf(stdout, "probing %s (%d baud %d/%s/%s, separator %q, max frame %d, inter-char %s); press Ctrl-C to stop\n",
		dev.Path, dev.Baud, dev.DataBits, dev.Parity, dev.StopBits, dev.Separator, dev.MaxFrameBytes, dev.InterCharTimeout)

	for {
		select {
		case <-ctx.Done():
			<-done
			return nil
		case frame := <-frames:
			seq++
			if err := printRx(stdout, builder.Rx(wireDevice, seq, frame.Raw, frame.At), frame.Raw, *asJSON); err != nil {
				return err
			}
		}
	}
}

// presenceLog says whether the probed device is there, once per change. The
// reader reports a failed open on every retry, and a line per retry would bury
// the one that matters; a change of error class, such as absent becoming
// permission_denied, is a change worth a line. It is called only from the
// reader's goroutine.
type presenceLog struct {
	log     *slog.Logger
	known   bool
	present bool
	class   string
}

func (presence *presenceLog) report(event device.Event) {
	switch event.Kind {
	case device.PortOpened:
		presence.set(true, event)
	case device.PortLost, device.PortOpenFailed:
		presence.set(false, event)
	}
}

func (presence *presenceLog) set(present bool, event device.Event) {
	if presence.known && presence.present == present && presence.class == event.ErrorClass {
		return
	}
	presence.known, presence.present, presence.class = true, present, event.ErrorClass
	if present {
		presence.log.Info("device present")
		return
	}
	presence.log.Warn("device absent", "error_class", event.ErrorClass, "error", errString(event.Err))
}

func printRx(w io.Writer, rx wire.Rx, raw []byte, asJSON bool) error {
	if asJSON {
		enc := json.NewEncoder(w)
		enc.SetEscapeHTML(false)
		return enc.Encode(rx)
	}
	text := "<not valid utf-8>"
	if rx.Text != nil {
		text = fmt.Sprintf("%q", *rx.Text)
	}
	fmt.Fprintf(w, "%s seq=%d frame_bytes=%d utf8=%t\n",
		rx.AgentTS, rx.Seq, len(raw), rx.TextValid)
	fmt.Fprintf(w, "  hex   %s\n", hex.EncodeToString(raw))
	fmt.Fprintf(w, "  text  %s\n", text)
	return nil
}

// listDevices enumerates what the host offers and, on Linux, the stable
// by-id and by-path symlinks that should be configured instead of the
// kernel-assigned names.
func listDevices(w io.Writer) error {
	ports, err := goserial.GetPortsList()
	if err != nil {
		return fmt.Errorf("enumerate serial ports: %w", err)
	}
	sort.Strings(ports)

	fmt.Fprintln(w, "kernel-assigned device nodes:")
	if len(ports) == 0 {
		fmt.Fprintln(w, "  (none found; check that the scanner is in USB-CDC mode, that it is not")
		fmt.Fprintln(w, "   claimed by ModemManager or brltty, and that this user is in the dialout group)")
	}
	for _, p := range ports {
		note := ""
		if strings.HasPrefix(p, "/dev/tty.") {
			note = "  [unusable: macOS callin device, opening it blocks on carrier detect; use the /dev/cu.* twin]"
		}
		fmt.Fprintf(w, "  %s%s\n", p, note)
	}

	for _, dir := range []string{"/dev/serial/by-id", "/dev/serial/by-path"} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			fmt.Fprintf(w, "\n%s: %v\n", dir, err)
			continue
		}
		fmt.Fprintf(w, "\nstable paths in %s (configure these, not the nodes above):\n", dir)
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		sort.Strings(names)
		for _, name := range names {
			full := filepath.Join(dir, name)
			target, err := filepath.EvalSymlinks(full)
			if err != nil {
				fmt.Fprintf(w, "  %s -> (unresolvable: %v)\n", full, err)
				continue
			}
			fmt.Fprintf(w, "  %s -> %s\n", full, target)
		}
	}
	return nil
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return h
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
