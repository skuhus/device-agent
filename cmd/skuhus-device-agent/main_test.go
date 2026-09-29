package main

import (
	"bytes"
	"testing"
)

// The terminator is bytes, and the command line can only carry text, so escapes
// have to be decoded. Getting this wrong makes probe disagree with the config
// file about what ends a frame.
func TestParseTerminator(t *testing.T) {
	cases := []struct {
		in      string
		want    []byte
		wantErr bool
	}{
		{`\r`, []byte{'\r'}, false},
		{`\n`, []byte{'\n'}, false},
		{`\r\n`, []byte{'\r', '\n'}, false},
		{`\x1e`, []byte{0x1e}, false},
		{"#", []byte{'#'}, false},
		{"END", []byte("END"), false},
		{"", nil, true},
		{`\q`, nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := parseTerminator(tc.in)
			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Fatalf("error = %v, want error = %t", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if !bytes.Equal(got, tc.want) {
				t.Errorf("parseTerminator(%q) = % x, want % x", tc.in, got, tc.want)
			}
		})
	}
}

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
