package main

import (
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
