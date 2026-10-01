// Package logging builds the agent's one log (DESIGN-V2.md, "Logging: one
// common log").
//
// Every record is a line of JSON that carries its severity, the station
// identity and slog's source attribute: the function, file and line that wrote
// it. The log goes to a file with size rotation, to a stream such as stdout,
// to both, or to neither. Records at INFO and above are flushed to the file as
// they are written.
package logging

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"
	"unicode/utf8"
)

// FileWriter is a log file: *File, or a stand-in in tests.
type FileWriter interface {
	io.Writer
	Sync() error
}

// Options configures the log.
type Options struct {
	Level string
	// Out is a stream, such as stdout, that receives every record. Nil means
	// none. A stream is not flushed: what it holds is the receiving side's to
	// keep.
	Out io.Writer
	// File receives every record, and is flushed after each one at INFO and
	// above. Nil means none; assign it only once the file is open, because a
	// nil *File in the interface does not compare equal to nil.
	File FileWriter
	// Errors receives a line for each record that could not be written or
	// flushed, which the log cannot report itself. It defaults to os.Stderr.
	Errors io.Writer
	// Identity fields are attached to every line. They are structured
	// attributes, never interpolated into the message.
	Project string
	Site    string
	Station string
	// Host is the machine name. It is a log attribute rather than an identity:
	// which box this is matters when reading logs, and never identifies the
	// station. Instance names the agent process, and is what the payloads and
	// the broker connection carry.
	Host         string
	Instance     string
	AgentVersion string
}

// New builds the log. It returns an error for an unknown level rather than
// quietly falling back to INFO. With no destination it returns a logger that
// writes nothing, which is a valid configuration (#23 Q11a).
func New(opts Options) (*slog.Logger, error) {
	level, err := ParseLevel(opts.Level)
	if err != nil {
		return nil, err
	}
	var destinations fanout
	if opts.File != nil {
		destinations = append(destinations, opts.File)
	}
	if opts.Out != nil {
		destinations = append(destinations, opts.Out)
	}
	var handler slog.Handler = slog.DiscardHandler
	if len(destinations) > 0 {
		errs := opts.Errors
		if errs == nil {
			errs = os.Stderr
		}
		handler = &flushing{
			inner:  slog.NewJSONHandler(destinations, &slog.HandlerOptions{Level: level, AddSource: true}),
			file:   opts.File,
			errors: errs,
		}
	}
	return slog.New(handler).With(
		"project", opts.Project,
		"site", opts.Site,
		"station", opts.Station,
		"host", opts.Host,
		"instance_id", opts.Instance,
		"agent_version", opts.AgentVersion,
	), nil
}

// Payload returns the attributes that put data read from a port on a log line:
// data_hex always, and data_text as well when the data is valid UTF-8. Whether
// a line may carry them is logging.log_payloads, except on the record of a
// reading the broker did not take, which always does.
func Payload(data []byte) []any {
	attrs := []any{"data_hex", hex.EncodeToString(data)}
	if utf8.Valid(data) {
		attrs = append(attrs, "data_text", string(data))
	}
	return attrs
}

// ParseLevel maps a configuration string to a slog level.
func ParseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info", "":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	}
	return 0, fmt.Errorf("logging: unknown level %q, expected debug, info, warn or error", s)
}

// flushing writes each record through the JSON handler, and then flushes the
// file after a record at INFO or above, so that it survives a power cut. DEBUG
// records are not flushed one by one, because at DEBUG every read from a port
// is a record (#23 Q12).
//
// A record that could not be written or flushed is reported on errors: slog
// drops whatever a handler returns, and a log that fails in silence is how a
// station loses its record of a reading.
type flushing struct {
	inner  slog.Handler
	file   FileWriter
	errors io.Writer
}

func (handler *flushing) Enabled(ctx context.Context, level slog.Level) bool {
	return handler.inner.Enabled(ctx, level)
}

func (handler *flushing) Handle(ctx context.Context, record slog.Record) error {
	err := handler.inner.Handle(ctx, record)
	if handler.file != nil && record.Level >= slog.LevelInfo {
		err = errors.Join(err, handler.file.Sync())
	}
	if err != nil {
		fmt.Fprintf(handler.errors, "%s logging: a %s record (%q) was not written or flushed: %v\n",
			time.Now().UTC().Format(time.RFC3339Nano), record.Level, record.Message, err)
	}
	return err
}

func (handler *flushing) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &flushing{inner: handler.inner.WithAttrs(attrs), file: handler.file, errors: handler.errors}
}

func (handler *flushing) WithGroup(name string) slog.Handler {
	return &flushing{inner: handler.inner.WithGroup(name), file: handler.file, errors: handler.errors}
}

// fanout writes each record to every destination. Unlike io.MultiWriter it
// does not stop at the first failure: a full disk must not also silence
// stdout.
type fanout []io.Writer

func (destinations fanout) Write(record []byte) (int, error) {
	var errs []error
	for _, destination := range destinations {
		if _, err := destination.Write(record); err != nil {
			errs = append(errs, err)
		}
	}
	return len(record), errors.Join(errs...)
}
