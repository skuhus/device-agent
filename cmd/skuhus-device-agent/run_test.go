package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunRejectsPositionalArguments(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := runRun([]string{"scanner-main"}, &stdout, &stderr)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "positional") {
		t.Errorf("error = %v, want it to mention positional arguments", err)
	}
}

func TestRunHelpMentionsCredentialHandling(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if err := runRun([]string{"-h"}, &stdout, &stderr); err == nil {
		t.Fatal("expected flag.ErrHelp")
	}
	help := stderr.String()
	for _, want := range []string{"SH_DEV_AGENT_MQTT_PASSWORD", "no flag for them", "SIGTERM"} {
		if !strings.Contains(help, want) {
			t.Errorf("run help does not mention %q:\n%s", want, help)
		}
	}
}

func TestUsageListsRun(t *testing.T) {
	if !strings.Contains(usage, "run ") {
		t.Error("the top level usage does not list the run command")
	}
}
