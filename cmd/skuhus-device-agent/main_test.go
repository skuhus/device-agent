package main

import (
	"testing"
)

// An unset flag must stay unset so it does not override the config file.
func TestFlagValuesRecordWhetherTheyWereSet(t *testing.T) {
	var s stringFlag
	if s.value != nil {
		t.Error("a string flag starts unset")
	}
	if err := s.Set("x"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if s.value == nil || *s.value != "x" {
		t.Errorf("after Set the value is %v, want x", s.value)
	}

	var b boolFlag
	if b.value != nil {
		t.Error("a bool flag starts unset")
	}
	if err := b.Set("false"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if b.value == nil || *b.value {
		t.Errorf("after Set(false) the value is %v, want an explicit false", b.value)
	}
	if err := b.Set("maybe"); err == nil {
		t.Error("a non-boolean should be rejected")
	}
}
