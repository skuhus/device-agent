// Command skuhus-device-agent gives network access to devices physically attached to a
// host. It is a transport shim: it moves bytes and adds an envelope, and knows
// nothing about what the bytes mean.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

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

// The exit codes: a failure, and a command line that is itself wrong, as the
// flag package and most commands use them.
const (
	exitFailure = 1
	exitUsage   = 2
)

// errUsage means the command line itself was wrong, which exits exitUsage.
var errUsage = errors.New("usage")

// errReported means the command has already printed why it failed, so main
// exits exitFailure without repeating it.
var errReported = errors.New("reported")

func main() {
	buildinfo.Set(commit, date)

	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(exitUsage)
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
		os.Exit(exitUsage)
	}

	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		if errors.Is(err, errReported) {
			os.Exit(exitFailure)
		}
		fmt.Fprintln(os.Stderr, "error: "+err.Error())
		if errors.Is(err, errUsage) {
			os.Exit(exitUsage)
		}
		os.Exit(exitFailure)
	}
}
