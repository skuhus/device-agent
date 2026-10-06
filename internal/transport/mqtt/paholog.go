package mqtt

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"time"
)

// callersAboveEmit is how many frames emit skips to find the line that logged:
// runtime.Callers, emit itself, and logAdapter's Println or Printf.
const callersAboveEmit = 3

// logAdapter bridges paho's logger interface to slog. paho logs connection
// detail that is worth having when a station will not connect, and worth
// keeping out of the way otherwise.
//
// Each record's source is the paho code that called it, not this adapter: a
// source that names the adapter on every paho line identifies nothing.
//
// Once Close has returned, paho's lines are discarded. autopaho cannot tell
// when paho has finished shutting down (autopaho/auto.go, Done), and its
// goroutines go on logging about the connection the agent just closed, after
// the agent has closed its log file. Measured: one DEBUG line per clean stop,
// "handleError received extra error", a few milliseconds after the file closed.
type logAdapter struct {
	log   *slog.Logger
	level slog.Level
	lines *lineGate
}

func (adapter logAdapter) Println(values ...any) {
	adapter.emit(fmt.Sprint(values...))
}

func (adapter logAdapter) Printf(format string, values ...any) {
	adapter.emit(fmt.Sprintf(format, values...))
}

// emit writes one record with the caller of Println or Printf as its source,
// as log/slog's documentation shows for wrapping its output methods.
func (adapter logAdapter) emit(msg string) {
	ctx := context.Background()
	if !adapter.log.Enabled(ctx, adapter.level) {
		return
	}
	var pcs [1]uintptr
	runtime.Callers(callersAboveEmit, pcs[:])
	record := slog.NewRecord(time.Now(), adapter.level, msg, pcs[0])
	adapter.lines.pass(func() {
		// The agent's handler reports a record it could not write itself.
		_ = adapter.log.Handler().Handle(ctx, record)
	})
}

// lineGate lets paho's lines through until the connection is closed. A line
// holds it while it is written, so close waits for a line already on its way:
// checking a flag first and writing after leaves a window, measured at 2 ms,
// in which a line written after the log file closed still gets through.
type lineGate struct {
	mu     sync.RWMutex
	closed bool
}

func (gate *lineGate) pass(write func()) {
	gate.mu.RLock()
	defer gate.mu.RUnlock()
	if !gate.closed {
		write()
	}
}

func (gate *lineGate) close() {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	gate.closed = true
}
