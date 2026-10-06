// Command skuhus-device-agent gives network access to devices physically attached to a
// host. It is a transport shim: it moves bytes and adds an envelope, and knows
// nothing about what the bytes mean.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"

	buildinfo "github.com/skuhus/device-agent/internal/version"
)

// Injected by the linker at build time. The version is not among them: it lives
// in internal/version, so that a binary cannot report a version its source does
// not carry.
var (
	commit string
	date   string
)

const usage = `skuhus-device-agent - SKU Hus device agent

Usage:
  skuhus-device-agent <command> [flags]

Commands:
  run        Open the configured devices, connect to the broker, publish what
             they read and write what they are sent, until stopped.
  validate   Load the configuration, report every problem, and exit non-zero if
             any are fatal.
  probe      Enumerate candidate devices, or open one and print decoded frames.
             This is the field diagnosis tool.
  version    Print the build identity.

Run "skuhus-device-agent <command> -h" for the flags of a command.

Configuration precedence is CLI flags, then SH_DEV_AGENT_* environment
variables, then the config file, then defaults. Broker credentials are never
accepted as CLI arguments, because ps exposes them to every user on the host.
`

// errUsage means the command line itself was wrong, which exits 2.
var errUsage = errors.New("usage")

// errReported means the command has already printed why it failed, so main
// exits 1 without repeating it.
var errReported = errors.New("reported")

func main() {
	buildinfo.Set(commit, date)

	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}

	var err error
	switch os.Args[1] {
	case "run":
		err = runCommand(os.Args[2:], os.Stdout, os.Stderr)
	case "validate":
		err = validateCommand(os.Args[2:], os.Stdout, os.Stderr)
	case "probe":
		err = probeCommand(os.Args[2:], os.Stdout, os.Stderr)
	case "version", "--version", "-version":
		fmt.Println(buildinfo.String())
		return
	case "help", "-h", "--help":
		fmt.Print(usage)
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n%s", os.Args[1], usage)
		os.Exit(2)
	}

	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		if errors.Is(err, errReported) {
			os.Exit(1)
		}
		fmt.Fprintln(os.Stderr, "error: "+err.Error())
		if errors.Is(err, errUsage) {
			os.Exit(2)
		}
		os.Exit(1)
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
