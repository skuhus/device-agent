package serial

import (
	"bytes"
	"strings"
	"testing"

	"github.com/skuhus/device-agent/internal/wire"
)

// feed drives the framer with a sequence of reads and collects everything it
// produced, so a test can state the read boundaries explicitly. One read is
// never assumed to be one scan.
func feed(t *testing.T, framer *Framer, reads ...string) ([][]byte, []Discard) {
	t.Helper()
	var frames [][]byte
	var discards []Discard
	for _, r := range reads {
		fr, d := framer.Append([]byte(r))
		frames = append(frames, fr...)
		discards = append(discards, d...)
	}
	return frames, discards
}

func framesAsStrings(frames [][]byte) []string {
	out := make([]string, len(frames))
	for i, framer := range frames {
		out[i] = string(framer)
	}
	return out
}

func wantFrames(t *testing.T, got [][]byte, want ...string) {
	t.Helper()
	gotStrings := framesAsStrings(got)
	if len(gotStrings) != len(want) {
		t.Fatalf("got %d frames %q, want %d %q", len(gotStrings), gotStrings, len(want), want)
	}
	for i := range want {
		if gotStrings[i] != want[i] {
			t.Errorf("frame %d = %q, want %q", i, gotStrings[i], want[i])
		}
	}
}

func wantDiscards(t *testing.T, got []Discard, want ...wire.DiscardReason) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d discards %v, want %d %v", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i].Reason != want[i] {
			t.Errorf("discard %d = %s, want %s", i, got[i].Reason, want[i])
		}
	}
}

func newFramer(t *testing.T, separator string, maxFrame int) *Framer {
	t.Helper()
	framer, err := NewFramer([]byte(separator), maxFrame)
	if err != nil {
		t.Fatalf("NewFramer(%q, %d): %v", separator, maxFrame, err)
	}
	return framer
}

func TestFramerSingleFrame(t *testing.T) {
	framer := newFramer(t, "\r", 4096)
	frames, discards := feed(t, framer, "0123456789\r")
	wantFrames(t, frames, "0123456789")
	wantDiscards(t, discards)
	if framer.Pending() != 0 {
		t.Errorf("Pending() = %d, want 0", framer.Pending())
	}
}

// A scan arriving in pieces must still produce exactly one frame. This is the
// case that breaks whenever someone assumes one read equals one scan.
func TestFramerFrameSplitAcrossReads(t *testing.T) {
	framer := newFramer(t, "\r", 4096)
	frames, discards := feed(t, framer, "012", "345", "678", "9", "\r")
	wantFrames(t, frames, "0123456789")
	wantDiscards(t, discards)
}

func TestFramerMultipleFramesInOneRead(t *testing.T) {
	framer := newFramer(t, "\r", 4096)
	frames, discards := feed(t, framer, "AAA\rBBB\rCCC\r")
	wantFrames(t, frames, "AAA", "BBB", "CCC")
	wantDiscards(t, discards)
}

func TestFramerTrailingPartialFrameIsNotEmitted(t *testing.T) {
	framer := newFramer(t, "\r", 4096)
	frames, _ := feed(t, framer, "AAA\rBB")
	wantFrames(t, frames, "AAA")
	if framer.Pending() != 2 {
		t.Errorf("Pending() = %d, want 2", framer.Pending())
	}
}

// A CR inside the payload must not split a frame when the separator is CRLF,
// and the separator must be found even when it straddles two reads.
func TestFramerCRLFSeparator(t *testing.T) {
	framer := newFramer(t, "\r\n", 4096)
	frames, discards := feed(t, framer, "AB\rCD\r", "\nEF\r\n")
	wantFrames(t, frames, "AB\rCD", "EF")
	wantDiscards(t, discards)
}

func TestFramerEmptyFrameIsDiscarded(t *testing.T) {
	framer := newFramer(t, "\r", 4096)
	frames, discards := feed(t, framer, "AAA\r\r\rBBB\r")
	wantFrames(t, frames, "AAA", "BBB")
	wantDiscards(t, discards, wire.DiscardEmptyFrame, wire.DiscardEmptyFrame)
}

// GS1-128 payloads carry 0x1D group separators. They must survive framing as
// bytes; treating the payload as text corrupts them.
func TestFramerPreservesGroupSeparators(t *testing.T) {
	framer := newFramer(t, "\r", 4096)
	payload := "]C1\x1d0104912345123459\x1d17250101"
	frames, discards := feed(t, framer, payload+"\r")
	wantFrames(t, frames, payload)
	wantDiscards(t, discards)
	if !bytes.Contains(frames[0], []byte{0x1d}) {
		t.Error("group separator was lost")
	}
}

func TestFramerPreservesNonUTF8Bytes(t *testing.T) {
	framer := newFramer(t, "\r", 4096)
	raw := []byte{0x41, 0xff, 0xfe, 0x42, '\r'}
	frames, _ := framer.Append(raw)
	if len(frames) != 1 {
		t.Fatalf("got %d frames, want 1", len(frames))
	}
	if !bytes.Equal(frames[0], []byte{0x41, 0xff, 0xfe, 0x42}) {
		t.Errorf("frame = % x, want 41 ff fe 42", frames[0])
	}
}

// The separator arriving in the same read that took the frame past the limit
// must still be rejected, not emitted as a very long barcode.
func TestFramerOversizeFrameWithSeparatorInSameRead(t *testing.T) {
	framer := newFramer(t, "\r", 8)
	frames, discards := feed(t, framer, strings.Repeat("X", 20)+"\rGOOD\r")
	wantFrames(t, frames, "GOOD")
	wantDiscards(t, discards, wire.DiscardOversize)
	if discards[0].Bytes != 20 {
		t.Errorf("oversize discard reported %d bytes, want 20", discards[0].Bytes)
	}
}

// When the limit is passed before any separator arrives, the framer must drop
// the remainder of that frame too rather than emitting its tail.
func TestFramerOversizeResyncsToNextSeparator(t *testing.T) {
	framer := newFramer(t, "\r", 8)
	frames, discards := feed(t, framer, strings.Repeat("X", 20), "TAIL\r", "GOOD\r")
	wantFrames(t, frames, "GOOD")
	wantDiscards(t, discards, wire.DiscardOversize, wire.DiscardResync)
}

// A separator split across two reads must still end the resynchronisation.
func TestFramerResyncFindsSplitSeparator(t *testing.T) {
	framer := newFramer(t, "\r\n", 8)
	frames, discards := feed(t, framer, strings.Repeat("X", 30), "TAIL\r", "\nGOOD\r\n")
	wantFrames(t, frames, "GOOD")
	wantDiscards(t, discards, wire.DiscardOversize, wire.DiscardResync)
}

// A payload of exactly max_frame_bytes is legal, including when only part of
// its separator has arrived.
func TestFramerAcceptsExactlyMaxFrameBytes(t *testing.T) {
	framer := newFramer(t, "\r\n", 8)
	frames, discards := feed(t, framer, "12345678\r")
	wantFrames(t, frames)
	wantDiscards(t, discards)
	frames, discards = feed(t, framer, "\n")
	wantFrames(t, frames, "12345678")
	wantDiscards(t, discards)
}

func TestFramerRejectsOneByteOverMaxFrameBytes(t *testing.T) {
	framer := newFramer(t, "\r", 8)
	frames, discards := feed(t, framer, "123456789\r")
	wantFrames(t, frames)
	wantDiscards(t, discards, wire.DiscardOversize)
}

// A device that sends without ever terminating must not grow the buffer.
func TestFramerBoundsBufferOnEndlessInput(t *testing.T) {
	const maxFrame = 64
	framer := newFramer(t, "\r", maxFrame)
	for i := 0; i < 1000; i++ {
		framer.Append(bytes.Repeat([]byte("Z"), 512))
		if framer.Pending() > maxFrame+len(framer.separator) {
			t.Fatalf("after %d reads Pending() = %d, want at most %d", i, framer.Pending(), maxFrame+len(framer.separator))
		}
	}
	if !framer.Resyncing() {
		t.Error("framer should be resynchronising after unterminated input")
	}
}

func TestFramerTimeoutDiscardsPartialFrame(t *testing.T) {
	framer := newFramer(t, "\r", 4096)
	feed(t, framer, "PARTIAL")
	discard, ok := framer.Timeout()
	if !ok {
		t.Fatal("Timeout() reported nothing to discard")
	}
	if discard.Reason != wire.DiscardInterCharTimeout || discard.Bytes != 7 {
		t.Errorf("discard = %v, want inter_char_timeout (7 bytes)", discard)
	}
	if framer.Pending() != 0 {
		t.Errorf("Pending() = %d, want 0", framer.Pending())
	}
}

func TestFramerTimeoutOnIdleDeviceIsSilent(t *testing.T) {
	framer := newFramer(t, "\r", 4096)
	if _, ok := framer.Timeout(); ok {
		t.Error("Timeout() on an empty buffer should report nothing")
	}
	feed(t, framer, "AAA\r")
	if _, ok := framer.Timeout(); ok {
		t.Error("Timeout() after a complete frame should report nothing")
	}
}

// After a timeout the framer resynchronises, so the tail of the stalled frame
// is dropped rather than emitted as a short barcode.
func TestFramerTimeoutResyncsBeforeNextFrame(t *testing.T) {
	framer := newFramer(t, "\r", 4096)
	feed(t, framer, "PART")
	if _, ok := framer.Timeout(); !ok {
		t.Fatal("expected a timeout discard")
	}
	frames, discards := feed(t, framer, "IAL\r", "GOOD\r")
	wantFrames(t, frames, "GOOD")
	wantDiscards(t, discards, wire.DiscardResync)
}

// A second timeout means the device has fallen silent, so the burst that
// produced the broken frame is over and resynchronisation ends. Otherwise a
// scanner configured with the wrong separator would eat the following scan
// forever.
func TestFramerSecondTimeoutEndsResync(t *testing.T) {
	framer := newFramer(t, "\r", 4096)
	feed(t, framer, "PART")
	framer.Timeout()
	if !framer.Resyncing() {
		t.Fatal("expected the framer to be resynchronising")
	}
	if _, ok := framer.Timeout(); ok {
		t.Error("a second timeout with nothing buffered should report nothing")
	}
	if framer.Resyncing() {
		t.Error("resynchronisation should have ended after the device fell silent")
	}
	frames, discards := feed(t, framer, "GOOD\r")
	wantFrames(t, frames, "GOOD")
	wantDiscards(t, discards)
}

func TestFramerResetClearsState(t *testing.T) {
	framer := newFramer(t, "\r", 8)
	feed(t, framer, strings.Repeat("X", 20))
	framer.Reset()
	if framer.Pending() != 0 || framer.Resyncing() {
		t.Fatalf("after Reset: Pending=%d Resyncing=%t, want 0 and false", framer.Pending(), framer.Resyncing())
	}
	frames, discards := feed(t, framer, "GOOD\r")
	wantFrames(t, frames, "GOOD")
	wantDiscards(t, discards)
}

// Returned frames must own their bytes: the framer reuses its buffer.
func TestFramerFramesDoNotAliasBuffer(t *testing.T) {
	framer := newFramer(t, "\r", 4096)
	frames, _ := framer.Append([]byte("FIRST\r"))
	framer.Append([]byte("SECOND-AND-LONGER\r"))
	if got := string(frames[0]); got != "FIRST" {
		t.Errorf("first frame became %q after a later read", got)
	}
}

func TestNewFramerRejectsBadArguments(t *testing.T) {
	if _, err := NewFramer(nil, 4096); err == nil {
		t.Error("empty separator should be rejected")
	}
	if _, err := NewFramer([]byte("\r"), 0); err == nil {
		t.Error("zero max frame should be rejected")
	}
}

// A discard has to carry the bytes it threw away, or a scanner sending the
// wrong separator can only ever be diagnosed as a byte count. Whether they
// reach a log is a separate decision, governed by logging.log_payloads.
func TestFramerDiscardCarriesTheDiscardedBytes(t *testing.T) {
	t.Run("timeout", func(t *testing.T) {
		framer := newFramer(t, "\r", 4096)
		feed(t, framer, "SKU-12345\n")
		discard, ok := framer.Timeout()
		if !ok {
			t.Fatal("expected a timeout discard")
		}
		if string(discard.Data) != "SKU-12345\n" {
			t.Errorf("Data = %q, want the buffered bytes including the wrong separator", discard.Data)
		}
		if discard.Bytes != len(discard.Data) {
			t.Errorf("Bytes = %d but Data is %d bytes", discard.Bytes, len(discard.Data))
		}
	})

	t.Run("oversize", func(t *testing.T) {
		framer := newFramer(t, "\r", 8)
		_, discards := feed(t, framer, strings.Repeat("X", 20))
		if len(discards) != 1 || discards[0].Reason != wire.DiscardOversize {
			t.Fatalf("discards = %v, want one oversize", discards)
		}
		if string(discards[0].Data) != strings.Repeat("X", 20) {
			t.Errorf("Data = %q, want the 20 buffered bytes", discards[0].Data)
		}
	})

	// A scanner sends a scan and its separator in one read, so this is how an
	// oversize scan arrives: measured on the Symbol 05e0:1701, a 37-byte Data
	// Matrix with max_frame_bytes 10 was discarded without its data.
	t.Run("oversize, its separator in the same read", func(t *testing.T) {
		framer := newFramer(t, "\r\n", 10)
		_, discards := feed(t, framer, "P00TKME1FNEG6DGXZCWWXQUL4CCJ6996JVV\r\n")
		if len(discards) != 1 || discards[0].Reason != wire.DiscardOversize {
			t.Fatalf("discards = %v, want one oversize", discards)
		}
		if string(discards[0].Data) != "P00TKME1FNEG6DGXZCWWXQUL4CCJ6996JVV" {
			t.Errorf("Data = %q, want the 35-byte payload without its separator", discards[0].Data)
		}
		if discards[0].Bytes != len(discards[0].Data) {
			t.Errorf("Bytes = %d but Data is %d bytes", discards[0].Bytes, len(discards[0].Data))
		}
	})
}

// Discarded bytes must not outlive the framer's buffer reuse.
func TestFramerDiscardDataDoesNotAliasBuffer(t *testing.T) {
	framer := newFramer(t, "\r", 4096)
	feed(t, framer, "FIRST")
	discard, _ := framer.Timeout()
	feed(t, framer, "COMPLETELY-DIFFERENT-CONTENT\r")
	if string(discard.Data) != "FIRST" {
		t.Errorf("discarded bytes became %q after later reads", discard.Data)
	}
}
