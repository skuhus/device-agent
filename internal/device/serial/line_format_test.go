package serial

import (
	"strings"
	"testing"
	"time"

	goserial "go.bug.st/serial"
)

// The port is opened with the configured line format. A device on 7E2 opened
// as 8N1 reads garbage without any error, so a setting that was accepted and
// then not applied would be the worst outcome.
func TestOpenUsesTheConfiguredLineFormat(t *testing.T) {
	cases := []struct {
		name     string
		dataBits int
		parity   goserial.Parity
		stopBits goserial.StopBits
		want     goserial.Mode
	}{
		{"unset is 8N1", 0, 0, 0, goserial.Mode{BaudRate: 9600, DataBits: 8, Parity: goserial.NoParity, StopBits: goserial.OneStopBit}},
		{"7E2", 7, goserial.EvenParity, goserial.TwoStopBits, goserial.Mode{BaudRate: 9600, DataBits: 7, Parity: goserial.EvenParity, StopBits: goserial.TwoStopBits}},
		{"5 bits, odd", 5, goserial.OddParity, goserial.OneStopBit, goserial.Mode{BaudRate: 9600, DataBits: 5, Parity: goserial.OddParity, StopBits: goserial.OneStopBit}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opened := make(chan goserial.Mode, 1)
			opts := serialOpts("format", "/dev/fake", "\r")
			opts.DataBits, opts.Parity, opts.StopBits = tc.dataBits, tc.parity, tc.stopBits
			opts.Open = func(_ string, mode *goserial.Mode) (goserial.Port, error) {
				select {
				case opened <- *mode:
				default:
				}
				return &blockingPort{hold: time.Hour}, nil
			}
			runDevice(t, opts, 1)
			select {
			case got := <-opened:
				if got != tc.want {
					t.Errorf("opened with %+v, want %+v", got, tc.want)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("the device never opened the port")
			}
		})
	}
}

func TestNewRejectsDataBitsOutOfRange(t *testing.T) {
	for _, bits := range []int{4, 9} {
		opts := serialOpts("format", "/dev/fake", "\r")
		opts.DataBits = bits
		if _, err := New(opts); err == nil || !strings.Contains(err.Error(), "data bits must be 5 to 8") {
			t.Errorf("data bits %d: err = %v, want a rejection", bits, err)
		}
	}
}
