// Package serial implements the Device interface over USB-CDC (virtual COM) and
// real RS-232 ports.
//
// HID keyboard mode is not implemented. It needs scancode-to-character
// translation against an assumed keyboard layout, and a Swedish-layout host
// reading a US-configured scanner corrupts non-alphanumeric payloads silently
// and intermittently. Scanners must be configured into CDC mode instead.
package serial

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"syscall"
	"time"

	"github.com/skuhus/device-agent/internal/backoff"
	"github.com/skuhus/device-agent/internal/device"
	"github.com/skuhus/device-agent/internal/logging"
	"github.com/skuhus/device-agent/internal/wire"
	goserial "go.bug.st/serial"
)

// ParityByName maps the configuration's parity names to the library's. The
// configuration package owns the names; a test keeps the two in step.
var ParityByName = map[string]goserial.Parity{
	"none":  goserial.NoParity,
	"odd":   goserial.OddParity,
	"even":  goserial.EvenParity,
	"mark":  goserial.MarkParity,
	"space": goserial.SpaceParity,
}

// StopBitsByName maps the configuration's stop_bits values to the library's.
// 1.5 is absent: the library refuses it on every Unix system.
var StopBitsByName = map[string]goserial.StopBits{
	"1": goserial.OneStopBit,
	"2": goserial.TwoStopBits,
}

var (
	parityNames   = invert(ParityByName)
	stopBitsNames = invert(StopBitsByName)
)

func invert[V comparable](byName map[string]V) map[V]string {
	names := make(map[V]string, len(byName))
	for name, value := range byName {
		names[value] = name
	}
	return names
}

// minDataBits and maxDataBits are the character sizes a serial line has. The
// configuration checks the same range; a test keeps the two in step.
const (
	minDataBits = 5
	maxDataBits = 8
)

// OpenFunc opens a serial port. It is a field on Options so tests can inject
// failures that no PTY can reproduce, such as EIO on a removed USB device.
type OpenFunc func(path string, mode *goserial.Mode) (goserial.Port, error)

// Options configures a serial device.
type Options struct {
	ID   string
	Path string
	Baud int
	// DataBits, Parity and StopBits set the line format. DataBits is 5 to 8;
	// the zero values of Parity and StopBits are none and 1.
	DataBits         int
	Parity           goserial.Parity
	StopBits         goserial.StopBits
	Terminator       []byte
	MaxFrameBytes    int
	InterCharTimeout time.Duration
	// LogPayloads puts the discarded bytes on each discard's log line. Default
	// false: reason and count only.
	LogPayloads bool

	Logger *slog.Logger

	// TxChunkBytes is how much of a tx one write takes the port's lock for. A
	// close, and a stopping agent, wait for the chunk in hand, not for the
	// whole tx: at 9600 baud a kilobyte is about a second on the line once
	// the operating system's buffer is full.
	TxChunkBytes int

	// Reopen is the wait before each attempt to open the port again after it
	// failed or closed. A session that lasts as long as the policy's longest
	// wait counts as healthy, and the wait starts over.
	Reopen backoff.Policy
	// Open defaults to the real serial port opener.
	Open OpenFunc
}

// Device is a serial-attached device.
type Device struct {
	opts Options
	mode *goserial.Mode
	// readBufferBytes holds a whole frame with its separator, so that a frame
	// that arrives at once is read in one call.
	readBufferBytes int
	log             *slog.Logger
	open            OpenFunc

	// retry cuts the reopen backoff short, for a tx waiting for the port.
	retry chan struct{}
	// mu guards current, the port while a session holds it open.
	mu      sync.Mutex
	current *sharedPort
}

var (
	_ device.Device = (*Device)(nil)
	_ device.Writer = (*Device)(nil)
)

// New validates the options and builds a device. It does not open the port;
// opening happens in Run and is retried, because a device that is unplugged at
// startup is an expected condition rather than a configuration error.
func New(opts Options) (*Device, error) {
	if opts.ID == "" {
		return nil, errors.New("device id is required")
	}
	if opts.Path == "" {
		return nil, errors.New("device path is required")
	}
	if opts.Baud <= 0 {
		return nil, fmt.Errorf("device %s: baud must be positive, got %d", opts.ID, opts.Baud)
	}
	if opts.InterCharTimeout <= 0 {
		return nil, fmt.Errorf("device %s: inter-character timeout must be positive, got %s", opts.ID, opts.InterCharTimeout)
	}
	if opts.DataBits < minDataBits || opts.DataBits > maxDataBits {
		return nil, fmt.Errorf("device %s: data bits must be %d to %d, got %d", opts.ID, minDataBits, maxDataBits, opts.DataBits)
	}
	if _, err := NewFramer(opts.Terminator, opts.MaxFrameBytes); err != nil {
		return nil, fmt.Errorf("device %s: %w", opts.ID, err)
	}
	if opts.TxChunkBytes < 1 {
		return nil, fmt.Errorf("device %s: tx chunk bytes must be at least 1, got %d", opts.ID, opts.TxChunkBytes)
	}
	if err := opts.Reopen.Validate(); err != nil {
		return nil, fmt.Errorf("device %s: reopen: %w", opts.ID, err)
	}

	dev := &Device{
		opts: opts,
		mode: &goserial.Mode{
			BaudRate: opts.Baud,
			DataBits: opts.DataBits,
			Parity:   opts.Parity,
			StopBits: opts.StopBits,
			// InitialStatusBits is deliberately left nil. Setting it makes the
			// library query the modem lines during open and fail the open
			// outright on any port without modem control. The lines are raised
			// after open instead, where failing to do so is not fatal.
		},
		readBufferBytes: opts.MaxFrameBytes + len(opts.Terminator),
		log:             opts.Logger,
		open:            opts.Open,
		retry:           make(chan struct{}, 1),
	}
	if dev.log == nil {
		dev.log = slog.New(slog.DiscardHandler)
	}
	dev.log = dev.log.With("device_id", opts.ID, "device_path", opts.Path)
	if dev.open == nil {
		dev.open = goserial.Open
	}
	return dev, nil
}

// ID is the configured device identifier.
func (dev *Device) ID() string { return dev.opts.ID }

// Kind is always "serial".
func (dev *Device) Kind() string { return "serial" }

// Path is the configured device path.
func (dev *Device) Path() string { return dev.opts.Path }

// Direction is Inbound: a scanner produces data, it does not consume it.
func (dev *Device) Direction() device.Direction { return device.Inbound }

// Run opens the device, frames what it reads and sends frames to sink until
// ctx is cancelled, reporting every port event to report, which may be nil.
// Open failures and disconnects are retried as Options.Reopen says; Run
// returns only on context cancellation.
func (dev *Device) Run(ctx context.Context, sink chan<- device.Frame, report func(device.Event)) error {
	if report == nil {
		report = func(device.Event) {}
	}
	policy := dev.opts.Reopen
	stableAfter := policy.Max
	dev.log.Info("device settings",
		"baud", dev.opts.Baud, "data_bits", dev.mode.DataBits, "parity", parityNames[dev.mode.Parity],
		"stop_bits", stopBitsNames[dev.mode.StopBits], "terminator_hex", hex.EncodeToString(dev.opts.Terminator),
		"max_frame_bytes", dev.opts.MaxFrameBytes, "inter_char_timeout", dev.opts.InterCharTimeout.String(),
		"read_buffer_bytes", dev.readBufferBytes, "tx_chunk_bytes", dev.opts.TxChunkBytes,
		"reopen_interval", policy.Interval.String(), "reopen_backoff", policy.Grow,
		"reopen_backoff_max", policy.Max.String(), "reopen_backoff_jitter", policy.Jitter)
	retry := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		started := time.Now()
		worked, err := dev.session(ctx, sink, report)
		lasted := time.Since(started)
		if ctxErr := ctx.Err(); ctxErr != nil {
			dev.log.Info("device stopped", "reason", "context cancelled")
			return ctxErr
		}

		// The wait starts over only after a session that stayed up, not after
		// one that merely opened. A failing cable lets the port open and read
		// once before it drops, and starting over on that reopens the device
		// several times a second for as long as the fault lasts.
		if lasted >= stableAfter {
			retry = 0
		}
		retry++
		wait := policy.Wait(retry)
		dev.log.Warn("device unavailable, reopening after backoff",
			"error", err.Error(), "error_class", string(classify(err)),
			"backoff", wait.String(), "retry", retry, "session_worked", worked,
			"session_duration", lasted.String(), "stable_after", stableAfter.String())

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		case <-dev.retry:
			dev.log.Debug("reopening before the backoff ends, for a tx waiting for the port")
		}
	}
}

// session opens the port and reads until it fails. It reports whether the port
// ever read successfully, which is what distinguishes a working device that
// went away from one that never came up.
func (dev *Device) session(ctx context.Context, sink chan<- device.Frame, report func(device.Event)) (worked bool, err error) {
	port, err := dev.open(dev.opts.Path, dev.mode)
	if err != nil {
		dev.log.Debug("device open failed", "error", err.Error(), "error_class", string(classify(err)))
		report(dev.event(device.PortOpenFailed, func(event *device.Event) {
			event.ErrorClass, event.Err = classify(err), err
		}))
		return false, fmt.Errorf("open %s: %w", dev.opts.Path, err)
	}
	dev.assertModemLines(port)
	dev.logModemStatus(port)
	dev.log.Info("device open", "baud", dev.opts.Baud,
		"data_bits", dev.mode.DataBits, "parity", parityNames[dev.mode.Parity], "stop_bits", stopBitsNames[dev.mode.StopBits],
		"read_buffer_bytes", dev.readBufferBytes,
		"inter_char_timeout", dev.opts.InterCharTimeout.String(),
		"max_frame_bytes", dev.opts.MaxFrameBytes,
		"terminator_hex", hex.EncodeToString(dev.opts.Terminator))

	// Writes go through the same port, under a lock that closing takes too
	// (DESIGN-V2.md, "Writing: tx"). The port takes writes before it is
	// reported open, because the report is what a tx waiting for the port
	// acts on: reported first, the tx's write could find no port and spend an
	// attempt.
	shared := &sharedPort{port: port, chunkBytes: dev.opts.TxChunkBytes}
	dev.setCurrent(shared)
	report(dev.event(device.PortOpened, nil))

	// Read blocks in select(2) and does not observe ctx. Closing the port is
	// what unblocks it; the library signals pending reads through an internal
	// pipe on Close. The port stops taking writes first, and the close waits
	// for a chunk already being written.
	sessCtx, cancel := context.WithCancel(ctx)
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		<-sessCtx.Done()
		dev.setCurrent(nil)
		if cerr := shared.close(); cerr != nil {
			dev.log.Debug("device close returned an error", "error", cerr.Error())
		}
	}()
	defer func() {
		cancel()
		<-closed
		// A cancelled context means the agent is stopping, not that the device
		// went away. Reporting it as lost would put a station into the
		// device-missing state on every clean shutdown.
		if ctx.Err() != nil {
			report(dev.event(device.PortClosed, nil))
			return
		}
		report(dev.event(device.PortLost, func(event *device.Event) {
			event.ErrorClass, event.Err = classify(err), err
		}))
	}()

	if terr := port.SetReadTimeout(dev.opts.InterCharTimeout); terr != nil {
		return false, fmt.Errorf("set read timeout on %s: %w", dev.opts.Path, terr)
	}

	framer, ferr := NewFramer(dev.opts.Terminator, dev.opts.MaxFrameBytes)
	if ferr != nil {
		return false, ferr
	}

	buf := make([]byte, dev.readBufferBytes)
	for {
		// Checked here as well as on the sink send. Otherwise the only way out
		// of this loop is a read error, which makes shutdown depend on the port
		// implementation returning one from Close. A port that keeps reporting
		// read timeouts would never let the goroutine exit.
		if err := ctx.Err(); err != nil {
			return worked, err
		}
		readBytes, rerr := port.Read(buf)
		if rerr != nil {
			if ctx.Err() != nil {
				return worked, ctx.Err()
			}
			return worked, fmt.Errorf("read %s: %w", dev.opts.Path, rerr)
		}
		worked = true

		if readBytes == 0 {
			// Zero bytes with no error is the read timeout expiring.
			if discard, ok := framer.Timeout(); ok {
				dev.logDiscard(discard, report)
			}
			continue
		}

		dev.log.Debug("device read", "bytes", readBytes, "pending", framer.Pending(), "resyncing", framer.Resyncing())
		report(dev.event(device.BytesRead, func(event *device.Event) { event.Bytes = readBytes }))
		frames, discards := framer.Append(buf[:readBytes])
		for _, discard := range discards {
			dev.logDiscard(discard, report)
		}
		for _, raw := range frames {
			// The frame's data goes on its INFO record, where the reading is
			// published, under log_payloads (DESIGN-V2.md, #23 Q13).
			dev.log.Debug("frame", "bytes", len(raw))
			frame := device.Frame{DeviceID: dev.opts.ID, Raw: raw, At: time.Now()}
			select {
			case sink <- frame:
			case <-ctx.Done():
				return worked, ctx.Err()
			}
		}
	}
}

// Write writes data through the port the reader has open, in chunks, and loops
// over partial writes until every byte is written. It returns ErrNotOpen when
// no session holds the port, or when the session closes it mid-write, and
// ctx's error when ctx ends between chunks; any other failure comes back as a
// *device.PortError with its class.
func (dev *Device) Write(ctx context.Context, data []byte, progress func(written int)) (int, error) {
	shared := dev.getCurrent()
	if shared == nil {
		return 0, device.ErrNotOpen
	}
	written, err := shared.write(ctx, data, progress)
	if err != nil && !errors.Is(err, device.ErrNotOpen) && ctx.Err() == nil {
		err = &device.PortError{Class: classify(err), Err: fmt.Errorf("write %s: %w", dev.opts.Path, err)}
	}
	return written, err
}

// RetryOpen cuts the reopen backoff short, so that the reader tries the port
// now. A request already pending is enough.
func (dev *Device) RetryOpen() {
	select {
	case dev.retry <- struct{}{}:
	default:
	}
}

func (dev *Device) setCurrent(shared *sharedPort) {
	dev.mu.Lock()
	defer dev.mu.Unlock()
	dev.current = shared
}

func (dev *Device) getCurrent() *sharedPort {
	dev.mu.Lock()
	defer dev.mu.Unlock()
	return dev.current
}

// sharedPort is a port the reader holds open and a writer writes through. The
// library's Write takes no lock and does not check that the port is open, so
// a write racing a close would reach whatever descriptor reused the number
// (serial_unix.go:112-118 in go.bug.st/serial v1.8.0). The lock is held across
// each chunk and across Close, and a closed port takes no more writes.
type sharedPort struct {
	mu         sync.Mutex
	port       goserial.Port
	chunkBytes int
	closed     bool
}

func (shared *sharedPort) write(ctx context.Context, data []byte, progress func(int)) (int, error) {
	written := 0
	for written < len(data) {
		if err := ctx.Err(); err != nil {
			return written, err
		}
		end := min(written+shared.chunkBytes, len(data))
		n, err := shared.writeChunk(data[written:end])
		written += n
		if n > 0 && progress != nil {
			progress(written)
		}
		if err != nil {
			return written, err
		}
	}
	return written, nil
}

// writeChunk writes one chunk under the lock. The library makes one write(2)
// call per Write and does not continue after a partial write, so this does.
func (shared *sharedPort) writeChunk(chunk []byte) (int, error) {
	shared.mu.Lock()
	defer shared.mu.Unlock()
	if shared.closed {
		return 0, device.ErrNotOpen
	}
	written := 0
	for written < len(chunk) {
		n, err := shared.port.Write(chunk[written:])
		written += n
		if err != nil {
			return written, err
		}
		if n == 0 {
			// write(2) returning nothing and no error would loop forever.
			return written, errors.New("the port accepted no bytes")
		}
	}
	return written, nil
}

func (shared *sharedPort) close() error {
	shared.mu.Lock()
	defer shared.mu.Unlock()
	shared.closed = true
	return shared.port.Close()
}

// assertModemLines raises DTR and RTS.
//
// On USB CDC-ACM, DTR is the host telling the device that a terminal is
// present, and some devices hold their output until they see it; this is what
// minicom does on connect. The library leaves the lines untouched when
// InitialStatusBits is nil, despite documenting otherwise, so without this the
// state would be whatever the operating system happened to leave them at.
//
// Ports with no modem control, such as pseudo-terminals and some USB serial
// drivers, return ENOTTY. That is logged rather than treated as an error: the
// port is still perfectly usable for reading.
func (dev *Device) assertModemLines(port goserial.Port) {
	if err := port.SetDTR(true); err != nil {
		dev.log.Debug("could not assert DTR", "error", err.Error())
	}
	if err := port.SetRTS(true); err != nil {
		dev.log.Debug("could not assert RTS", "error", err.Error())
	}
}

// logModemStatus records the input modem lines at open. On a silent device this
// is the first thing worth knowing: DSR and DCD say whether the peer considers
// itself connected, and no other log line carries that.
func (dev *Device) logModemStatus(port goserial.Port) {
	bits, err := port.GetModemStatusBits()
	if err != nil {
		dev.log.Debug("modem status bits unavailable", "error", err.Error())
		return
	}
	dev.log.Debug("modem status bits", "cts", bits.CTS, "dsr", bits.DSR, "dcd", bits.DCD, "ri", bits.RI)
}

// logDiscard logs and reports thrown-away bytes, on one line. Oversize and
// timeout are WARN because a scan was lost; resync and empty frames are DEBUG
// because they are the expected consequence of the discard already logged.
// With log_payloads the line carries the discarded data, which is what shows
// a misconfigured separator for what it is. Every discard is reported, so
// each can be counted.
func (dev *Device) logDiscard(discard Discard, report func(device.Event)) {
	report(dev.event(device.BytesDiscarded, func(event *device.Event) {
		event.Reason, event.Bytes = discard.Reason, discard.Bytes
	}))
	attrs := []any{"reason", string(discard.Reason), "bytes", discard.Bytes}
	if dev.opts.LogPayloads && len(discard.Data) > 0 {
		attrs = append(attrs, logging.Payload(discard.Data)...)
	}
	switch discard.Reason {
	case wire.DiscardOversize, wire.DiscardInterCharTimeout:
		dev.log.Warn("discarded partial frame", attrs...)
	default:
		dev.log.Debug("discarded bytes", attrs...)
	}
}

// event builds a port event for this device, stamped now, with fill adding the
// fields that depend on its kind.
func (dev *Device) event(kind device.EventKind, fill func(*device.Event)) device.Event {
	event := device.Event{DeviceID: dev.opts.ID, Kind: kind, At: time.Now()}
	if fill != nil {
		fill(&event)
	}
	return event
}

// classify names a failure, err, so that a log reader can tell a missing
// device from a permissions problem from a device that was pulled out
// mid-read. The names are internal/wire's error classes, which every event
// and keepalive carries.
func classify(err error) wire.ErrorClass {
	switch {
	case errors.Is(err, syscall.EIO), errors.Is(err, syscall.ENODEV), errors.Is(err, syscall.ENXIO):
		return wire.ErrorDisconnected
	case errors.Is(err, syscall.ENOENT):
		return wire.ErrorAbsent
	case errors.Is(err, syscall.EACCES), errors.Is(err, syscall.EPERM):
		return wire.ErrorPermissionDenied
	case errors.Is(err, syscall.EROFS):
		// Seen when a device node is bind-mounted read-only into a container.
		// The port is opened read-write because a scanner may need commands
		// sent to it, so a read-only mount fails at open.
		return wire.ErrorReadOnly
	case errors.Is(err, syscall.EBUSY):
		return wire.ErrorBusy
	}
	var pe *goserial.PortError
	if errors.As(err, &pe) {
		switch pe.Code() {
		case goserial.PortClosed:
			// The library also returns this when a read finds the port in the
			// zero-length-readable state a disconnect leaves behind.
			return wire.ErrorDisconnected
		case goserial.PortNotFound:
			return wire.ErrorAbsent
		case goserial.PermissionDenied:
			return wire.ErrorPermissionDenied
		case goserial.PortBusy:
			return wire.ErrorBusy
		default:
			return wire.ErrorPortError
		}
	}
	return wire.ErrorUnknown
}
