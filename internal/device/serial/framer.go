package serial

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/skuhus/device-agent/internal/wire"
)

// Discard reports bytes that did not become a frame.
type Discard struct {
	Reason wire.DiscardReason
	Bytes  int
	// Data holds the discarded bytes when the framer still had them, which is
	// the case for oversize and timeout discards. Whether it reaches a log is
	// the caller's decision, governed by logging.log_payloads. Without it a
	// scanner sending the wrong separator can only be diagnosed as a byte
	// count, which does not say what the separator actually is.
	Data []byte
}

func (discard Discard) String() string {
	return fmt.Sprintf("%s (%d bytes)", discard.Reason, discard.Bytes)
}

// Framer splits a byte stream into separator-delimited frames.
//
// It makes no assumption that one read equals one scan: a scan may arrive
// across several reads, and several scans may arrive in one read.
//
// After any discard the framer resynchronises by dropping everything up to and
// including the next separator. The alternative - resuming mid-frame - emits
// the tail of a broken frame as if it were a short barcode, which is silent
// corruption. Dropping the remainder is a visible loss the operator can act on.
type Framer struct {
	separator []byte
	maxFrame  int

	buf      []byte
	dropping bool
	dropped  int
}

// NewFramer builds a framer.
//
// maxFrame is the largest payload accepted, in bytes, not counting the
// separator.
func NewFramer(separator []byte, maxFrame int) (*Framer, error) {
	if len(separator) == 0 {
		return nil, errors.New("separator must not be empty")
	}
	if maxFrame < 1 {
		return nil, fmt.Errorf("max frame must be at least 1 byte, got %d", maxFrame)
	}
	return &Framer{separator: bytes.Clone(separator), maxFrame: maxFrame}, nil
}

// Append consumes src and returns the frames it completed together with any
// discards. Returned frames own their bytes and outlive the next call.
func (framer *Framer) Append(src []byte) ([][]byte, []Discard) {
	var frames [][]byte
	var discards []Discard

	framer.buf = append(framer.buf, src...)

	for {
		i := bytes.Index(framer.buf, framer.separator)
		if i < 0 {
			break
		}
		switch {
		case framer.dropping:
			framer.dropped += i + len(framer.separator)
			discards = append(discards, Discard{Reason: wire.DiscardResync, Bytes: framer.dropped})
			framer.dropping, framer.dropped = false, 0
		case i == 0:
			discards = append(discards, Discard{Reason: wire.DiscardEmptyFrame, Bytes: len(framer.separator)})
		case i > framer.maxFrame:
			// The separator arrived in the same read that took the payload
			// past the limit. The frame is over size and is dropped here; no
			// resynchronisation is needed because the separator has been
			// consumed and the next byte starts a fresh frame. A scanner sends
			// a scan and its separator in one read, so this is where its
			// oversize scans land, and the data goes with the discard.
			discards = append(discards, Discard{Reason: wire.DiscardOversize, Bytes: i, Data: bytes.Clone(framer.buf[:i])})
		default:
			frames = append(frames, bytes.Clone(framer.buf[:i]))
		}
		framer.consume(i + len(framer.separator))
	}

	if framer.dropping {
		// Keep only enough trailing bytes to recognise a separator split
		// across two reads, so a device that never terminates cannot grow the
		// buffer.
		framer.dropped += framer.trimTo(framer.carry())
	} else if len(framer.buf) > framer.maxFrame+framer.carry() {
		// The tolerance of carry() bytes lets a maximum-size payload arrive
		// with only part of its separator without being called over size.
		pending := len(framer.buf)
		data := bytes.Clone(framer.buf)
		framer.dropping = true
		framer.dropped = framer.trimTo(framer.carry())
		discards = append(discards, Discard{Reason: wire.DiscardOversize, Bytes: pending, Data: data})
	}

	return frames, discards
}

// Timeout reports that no byte arrived within the inter-character timeout.
//
// A partial frame is abandoned and the framer resynchronises. A timeout while
// already resynchronising ends the resynchronisation: the device has fallen
// silent, so the burst that produced the broken frame is over and the next byte
// starts a fresh frame.
func (framer *Framer) Timeout() (Discard, bool) {
	if framer.dropping {
		consumed := framer.dropped + len(framer.buf)
		framer.Reset()
		if consumed == 0 {
			return Discard{}, false
		}
		return Discard{Reason: wire.DiscardResync, Bytes: consumed}, true
	}
	if len(framer.buf) == 0 {
		return Discard{}, false
	}
	consumed := len(framer.buf)
	data := bytes.Clone(framer.buf)
	framer.buf = framer.buf[:0]
	framer.dropping, framer.dropped = true, 0
	return Discard{Reason: wire.DiscardInterCharTimeout, Bytes: consumed, Data: data}, true
}

// Pending is the number of buffered bytes not yet part of a frame.
func (framer *Framer) Pending() int { return len(framer.buf) }

// Resyncing reports whether the framer is dropping bytes to the next separator.
func (framer *Framer) Resyncing() bool { return framer.dropping }

// Reset clears all state, as after reopening the device.
func (framer *Framer) Reset() {
	framer.buf = framer.buf[:0]
	framer.dropping, framer.dropped = false, 0
}

// carry is the number of trailing bytes that must be kept while dropping so a
// separator spanning two reads is still found.
func (framer *Framer) carry() int { return len(framer.separator) - 1 }

// trimTo drops all but the last keep bytes and reports how many it dropped.
func (framer *Framer) trimTo(keep int) int {
	if len(framer.buf) <= keep {
		return 0
	}
	consumed := len(framer.buf) - keep
	framer.consume(consumed)
	return consumed
}

func (framer *Framer) consume(consumed int) {
	framer.buf = framer.buf[:copy(framer.buf, framer.buf[consumed:])]
}
