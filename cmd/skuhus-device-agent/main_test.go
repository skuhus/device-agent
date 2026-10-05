package main

import (
	"errors"
	"flag"
	"io"
	"testing"
)

// An unset flag must stay unset so it does not override the config file.
func TestFlagValuesRecordWhetherTheyWereSet(t *testing.T) {
	var stringValue stringFlag
	if stringValue.value != nil {
		t.Error("a string flag starts unset")
	}
	if err := stringValue.Set("x"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if stringValue.value == nil || *stringValue.value != "x" {
		t.Errorf("after Set the value is %v, want x", stringValue.value)
	}

	var boolValue boolFlag
	if boolValue.value != nil {
		t.Error("a bool flag starts unset")
	}
	if err := boolValue.Set("false"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if boolValue.value == nil || *boolValue.value {
		t.Errorf("after Set(false) the value is %v, want an explicit false", boolValue.value)
	}
	if err := boolValue.Set("maybe"); err == nil {
		t.Error("a non-boolean should be rejected")
	}
}

// A command line that does not parse exits 2, as an unknown command does: a
// flag that does not exist, a flag without its value, and a positional
// argument. -h is not an error.
func TestCommandLineErrorsAreUsageErrors(t *testing.T) {
	commands := map[string]func([]string, io.Writer, io.Writer) error{
		"run": runCommand, "validate": validateCommand, "probe": probeCommand,
	}
	for name, command := range commands {
		for _, args := range [][]string{{"--no-such-flag"}, {"--config"}, {"extra"}} {
			err := command(args, io.Discard, io.Discard)
			if err == nil || !errors.Is(err, errUsage) {
				t.Errorf("%s %v: err = %v, want a usage error", name, args, err)
			}
		}
		if err := command([]string{"-h"}, io.Discard, io.Discard); !errors.Is(err, flag.ErrHelp) {
			t.Errorf("%s -h: err = %v, want flag.ErrHelp", name, err)
		}
	}
}
