package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/skuhus/device-agent/internal/config"
	serialdev "github.com/skuhus/device-agent/internal/device/serial"
	goserial "go.bug.st/serial"
)

// newSerialReader builds the reader for a configured device, as run and probe
// both open one. logPayloads puts discarded bytes on the discard log lines.
func newSerialReader(deviceCfg config.Device, logPayloads bool, log *slog.Logger) (*serialdev.Device, error) {
	parity, stopBits, err := lineFormat(deviceCfg)
	if err != nil {
		return nil, err
	}
	return serialdev.New(serialdev.Options{
		ID:               deviceCfg.ID,
		Path:             deviceCfg.Path,
		Baud:             deviceCfg.Baud,
		DataBits:         deviceCfg.DataBits,
		Parity:           parity,
		StopBits:         stopBits,
		Terminator:       deviceCfg.SeparatorBytes(),
		MaxFrameBytes:    deviceCfg.MaxFrameBytes,
		InterCharTimeout: deviceCfg.InterCharTimeout.Duration(),
		LogPayloads:      logPayloads,
		Logger:           log,
		TxChunkBytes:     deviceCfg.TxChunkBytes,
		Reopen:           deviceCfg.ReopenPolicy(),
	})
}

// lineFormat maps a device's parity and stop bits to the serial library's
// values. Validation accepts only mapped names, so a miss here is a bug, and
// it fails rather than opening the port with the zero value, 8N1.
func lineFormat(deviceCfg config.Device) (goserial.Parity, goserial.StopBits, error) {
	parity, ok := serialdev.ParityByName[string(deviceCfg.Parity)]
	if !ok {
		return 0, 0, fmt.Errorf("device %s: parity %q has no serial library value", deviceCfg.ID, deviceCfg.Parity)
	}
	stopBits, ok := serialdev.StopBitsByName[string(deviceCfg.StopBits)]
	if !ok {
		return 0, 0, fmt.Errorf("device %s: stop_bits %q has no serial library value", deviceCfg.ID, deviceCfg.StopBits)
	}
	return parity, stopBits, nil
}

// hostname is the host's name for the log's host attribute, or "unknown":
// the host is logged, never used as an identity ("instance_id, not host").
func hostname() string {
	name, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return name
}
