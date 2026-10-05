package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/skuhus/device-agent/internal/config"
	"github.com/skuhus/device-agent/internal/device"
	"github.com/skuhus/device-agent/internal/logging"
	buildinfo "github.com/skuhus/device-agent/internal/version"
	"github.com/skuhus/device-agent/internal/wire"
	goserial "go.bug.st/serial"
)

func probeCommand(args []string, stdout, stderr io.Writer) error {
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
	separator := fs.String("separator", "", "frame separator, backslash escapes decoded, such as \\r or \\r\\n; required with --path")
	maxFrame := fs.Int("max-frame-bytes", config.DefaultMaxFrameBytes, "discard a partial frame longer than this")
	interChar := fs.Duration("inter-char-timeout", config.DefaultInterCharTimeout, "discard a partial frame idle for longer than this")
	asJSON := fs.Bool("json", false, "print the rx message that would be published")
	duration := fs.Duration("duration", 0, "stop after this long (0 means run until interrupted)")
	logLevel := fs.String("log-level", config.DefaultLogLevel, "log level for the structured log on stderr")
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
	if err := checkProbeSources(fs, *deviceID != "", *separator); err != nil {
		return err
	}

	// The identity is filled from the config below when --device names one, so
	// that --json prints the message run would publish rather than a lookalike.
	identity := wire.Agent{InstanceID: "probe", AgentVersion: buildinfo.Version()}
	// Without --device, the flags set what they name and every other key
	// keeps the configuration's default, as an entry that leaves it out does.
	dev := config.DefaultDevice()
	dev.ID = "probe"
	dev.Path = *path
	dev.Baud = *baud
	dev.DataBits = *dataBits
	dev.Parity = config.Parity(*parity)
	dev.StopBits = config.StopBits(*stopBits)
	dev.MaxFrameBytes = *maxFrame
	dev.InterCharTimeout = config.Duration(*interChar)

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
		for _, warning := range warnings {
			fmt.Fprintln(stderr, "warning: "+warning.String())
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

	sd, err := newSerialReader(dev, *logPayloads, log)
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
	frames := make(chan device.Frame, config.DefaultBufferSize)
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
			if err := printReading(stdout, builder.Rx(wireDevice, seq, frame.Raw, frame.At), frame.Raw, *asJSON); err != nil {
				return err
			}
		}
	}
}

// deviceSettingFlags are the flags that set what a config file's device entry
// sets. With --device the entry is used, and none of them is.
var deviceSettingFlags = map[string]bool{
	"baud": true, "data-bits": true, "parity": true, "stop-bits": true,
	"separator": true, "max-frame-bytes": true, "inter-char-timeout": true,
}

// checkProbeSources refuses a flag the chosen source would ignore: a device
// setting with --device, which takes the config file's, and --config with
// --path, which reads no file. It requires --separator with --path, because a
// separator is per model and a wrong one corrupts readings without failing
// (DESIGN-V2.md, "A CR/CRLF mismatch is the one wrong separator that is not
// loud"), so it has no default.
func checkProbeSources(fs *flag.FlagSet, fromConfig bool, separator string) error {
	var ignored []string
	fs.Visit(func(set *flag.Flag) {
		if (fromConfig && deviceSettingFlags[set.Name]) || (!fromConfig && set.Name == "config") {
			ignored = append(ignored, "--"+set.Name)
		}
	})
	switch {
	case len(ignored) > 0 && fromConfig:
		return fmt.Errorf("%w: with --device the settings come from the config file; remove %s", errUsage, strings.Join(ignored, ", "))
	case len(ignored) > 0:
		return fmt.Errorf("%w: --config is read only with --device; remove it, or name the device with --device", errUsage)
	case !fromConfig && separator == "":
		return fmt.Errorf("%w: --separator is required with --path, such as '\\r' or '\\r\\n'; it must match the device", errUsage)
	}
	return nil
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
	class   wire.ErrorClass
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
	presence.log.Warn("device absent", "error_class", string(event.ErrorClass), "error", event.ErrorText())
}

func printReading(out io.Writer, rx wire.Rx, raw []byte, asJSON bool) error {
	if asJSON {
		enc := json.NewEncoder(out)
		enc.SetEscapeHTML(false)
		return enc.Encode(rx)
	}
	text := "<not valid utf-8>"
	if rx.Text != nil {
		text = fmt.Sprintf("%q", *rx.Text)
	}
	fmt.Fprintf(out, "%s seq=%d frame_bytes=%d utf8=%t\n",
		rx.AgentTS, rx.Seq, len(raw), rx.TextValid)
	fmt.Fprintf(out, "  hex   %s\n", hex.EncodeToString(raw))
	fmt.Fprintf(out, "  text  %s\n", text)
	return nil
}

// listDevices enumerates what the host offers and, on Linux, the stable
// by-id and by-path symlinks that should be configured instead of the
// kernel-assigned names.
func listDevices(out io.Writer) error {
	ports, err := goserial.GetPortsList()
	if err != nil {
		return fmt.Errorf("enumerate serial ports: %w", err)
	}
	sort.Strings(ports)

	fmt.Fprintln(out, "kernel-assigned device nodes:")
	if len(ports) == 0 {
		fmt.Fprintln(out, "  (none found; check that the scanner is in USB-CDC mode, that it is not")
		fmt.Fprintln(out, "   claimed by ModemManager or brltty, and that this user is in the dialout group)")
	}
	for _, portName := range ports {
		note := ""
		if strings.HasPrefix(portName, "/dev/tty.") {
			note = "  [unusable: macOS callin device, opening it blocks on carrier detect; use the /dev/cu.* twin]"
		}
		fmt.Fprintf(out, "  %s%s\n", portName, note)
	}

	for _, dir := range []string{"/dev/serial/by-id", "/dev/serial/by-path"} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			fmt.Fprintf(out, "\n%s: %v\n", dir, err)
			continue
		}
		fmt.Fprintf(out, "\nstable paths in %s (configure these, not the nodes above):\n", dir)
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		sort.Strings(names)
		for _, name := range names {
			full := filepath.Join(dir, name)
			target, err := filepath.EvalSymlinks(full)
			if err != nil {
				fmt.Fprintf(out, "  %s -> (unresolvable: %v)\n", full, err)
				continue
			}
			fmt.Fprintf(out, "  %s -> %s\n", full, target)
		}
	}
	return nil
}

// parseSeparator decodes a separator written on the command line, so that
// --separator '\r' means a carriage return rather than two characters. A value
// with no backslash is taken literally.
func parseSeparator(raw string) ([]byte, error) {
	if raw == "" {
		return nil, errors.New("separator must not be empty")
	}
	if !strings.Contains(raw, `\`) {
		return []byte(raw), nil
	}
	unquoted, err := strconv.Unquote(`"` + raw + `"`)
	if err != nil {
		return nil, fmt.Errorf("cannot decode separator %q: %w (write it as \\r, \\n, \\r\\n or \\x1e)", raw, err)
	}
	return []byte(unquoted), nil
}
