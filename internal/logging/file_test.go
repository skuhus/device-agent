package logging

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func openFile(t *testing.T, maxSizeMB, keep int) (*File, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "agent.log")
	file, err := OpenFile(path, maxSizeMB, keep)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	t.Cleanup(func() { file.Close() })
	return file, path
}

func write(t *testing.T, file *File, record string) {
	t.Helper()
	if _, err := file.Write([]byte(record + "\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}
}

func readLines(t *testing.T, path string) []string {
	t.Helper()
	opened, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer opened.Close()
	var out []string
	scanner := bufio.NewScanner(opened)
	// One test writes a record larger than the default 64 KB token limit.
	scanner.Buffer(make([]byte, 0, 64*1024), 8<<20)
	for scanner.Scan() {
		out = append(out, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan %s: %v", path, err)
	}
	return out
}

// Reopening must not truncate the file: the log survives agent restarts.
func TestFileAppendsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.log")
	first, err := OpenFile(path, 1, 3)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	write(t, first, "before-restart")
	first.Close()

	second, err := OpenFile(path, 1, 3)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer second.Close()
	write(t, second, "after-restart")

	if got := readLines(t, path); strings.Join(got, ",") != "before-restart,after-restart" {
		t.Fatalf("lines = %v, want both in order", got)
	}
}

// The file rotates at its size limit and keeps exactly the configured number
// of old files, oldest last, so that the history reads in order.
func TestFileRotatesAndKeepsTheConfiguredNumberOfFiles(t *testing.T) {
	const keep = 3
	file, path := openFile(t, 1, keep)
	pad := strings.Repeat("x", 4096)
	for index := 0; index < 1200; index++ {
		write(t, file, fmt.Sprintf("r%04d %s", index, pad))
	}

	matches, err := filepath.Glob(path + "*")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	if len(matches) != keep+1 {
		t.Errorf("files = %v, want the live file and %d rotated ones", matches, keep)
	}
	for index := 1; index <= keep; index++ {
		info, err := os.Stat(fmt.Sprintf("%s.%d", path, index))
		if err != nil {
			t.Fatalf("expected rotated file %d: %v", index, err)
		}
		if info.Size() == 0 || info.Size() > 1<<20 {
			t.Errorf("%s.%d is %d bytes, want some, at most 1 MB", path, index, info.Size())
		}
	}

	// Each older file ends before the newer one starts.
	newer := readLines(t, path)
	for index := 1; index <= keep; index++ {
		older := readLines(t, fmt.Sprintf("%s.%d", path, index))
		if older[len(older)-1] >= newer[0] {
			t.Errorf("%s.%d ends at %.5s but the next file starts at %.5s", path, index, older[len(older)-1], newer[0])
		}
		newer = older
	}
}

// keep: 0 means rotate and discard, which must not leave a growing file behind.
func TestFileKeepZeroDiscardsHistory(t *testing.T) {
	file, path := openFile(t, 1, 0)
	pad := strings.Repeat("x", 4096)
	for index := 0; index < 600; index++ {
		write(t, file, fmt.Sprintf("r%04d %s", index, pad))
	}
	if _, err := os.Stat(path + ".1"); !os.IsNotExist(err) {
		t.Errorf("keep 0 should leave no rotated files: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat live file: %v", err)
	}
	if info.Size() > 1<<20 {
		t.Errorf("live file is %d bytes, want at most 1 MB", info.Size())
	}
}

// A single record larger than the limit is still written whole rather than
// looping on rotation.
func TestFileWritesRecordLargerThanLimit(t *testing.T) {
	file, path := openFile(t, 1, 1)
	write(t, file, "small")
	huge := strings.Repeat("y", 2<<20)
	write(t, file, huge)
	if got := readLines(t, path); len(got) != 1 || got[0] != huge {
		t.Fatalf("got %d lines, want the oversized record alone after rotation", len(got))
	}
}

// Records written from many goroutines arrive whole, one per line.
func TestFileConcurrentWrites(t *testing.T) {
	file, path := openFile(t, 8, 2)
	const writers, each = 8, 200
	var wg sync.WaitGroup
	for writer := 0; writer < writers; writer++ {
		wg.Add(1)
		go func(writer int) {
			defer wg.Done()
			for index := 0; index < each; index++ {
				if _, err := file.Write([]byte(fmt.Sprintf("w%d-r%d\n", writer, index))); err != nil {
					t.Errorf("Write: %v", err)
					return
				}
			}
		}(writer)
	}
	wg.Wait()
	if err := file.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if got := readLines(t, path); len(got) != writers*each {
		t.Errorf("got %d lines, want %d", len(got), writers*each)
	}
}

func TestOpenFileRejectsBadArguments(t *testing.T) {
	dir := t.TempDir()
	cases := []struct {
		name            string
		path            string
		maxSizeMB, keep int
	}{
		{"empty path", "", 1, 1},
		{"zero size", filepath.Join(dir, "a.log"), 0, 1},
		{"negative keep", filepath.Join(dir, "a.log"), 1, -1},
		{"missing directory", filepath.Join(dir, "nope", "a.log"), 1, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := OpenFile(tc.path, tc.maxSizeMB, tc.keep); err == nil {
				t.Error("expected an error")
			}
		})
	}
}

func TestFileWriteAfterCloseFails(t *testing.T) {
	file, _ := openFile(t, 1, 1)
	if err := file.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := file.Write([]byte("late\n")); err == nil {
		t.Error("writing to a closed log file should fail")
	}
	if err := file.Sync(); err == nil {
		t.Error("flushing a closed log file should fail")
	}
}

// An operator who deletes the live file to free space must not stop the log:
// the next rotation finds nothing to rotate, and writing goes on in a new
// file at the same path.
func TestFileKeepsWritingAfterTheLiveFileIsRemoved(t *testing.T) {
	file, path := openFile(t, 1, 2)
	write(t, file, "before")
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove the live file: %v", err)
	}
	pad := strings.Repeat("x", 4096)
	for index := 0; index < 300; index++ {
		write(t, file, fmt.Sprintf("r%04d %s", index, pad))
	}
	write(t, file, "after")
	lines := readLines(t, path)
	if len(lines) == 0 || lines[len(lines)-1] != "after" {
		t.Fatalf("the live file does not end with the last record; it holds %d lines", len(lines))
	}
}

// A rotation that fails is reported, and the record is still written: the
// file goes on growing past its limit rather than going dark, and each write
// past the limit tries again and reports again until the cause is cleared.
// Here the oldest rotated name is taken by a directory that cannot be removed.
func TestFileKeepsWritingWhenRotationFails(t *testing.T) {
	file, path := openFile(t, 1, 1)
	if err := os.MkdirAll(filepath.Join(path+".1", "in-the-way"), 0o755); err != nil {
		t.Fatalf("block the rotated name: %v", err)
	}
	pad := strings.Repeat("x", 4096)
	var rotationErr error
	for index := 0; index < 300; index++ {
		if _, err := file.Write([]byte(fmt.Sprintf("r%04d %s\n", index, pad))); err != nil && rotationErr == nil {
			rotationErr = err
		}
	}
	if rotationErr == nil || !strings.Contains(rotationErr.Error(), "rotating") {
		t.Errorf("the failed rotation was not reported: %v", rotationErr)
	}
	if _, err := file.Write([]byte("after\n")); err == nil || !strings.Contains(err.Error(), "rotating") {
		t.Errorf("a write past the limit with rotation still blocked: err = %v, want the rotation reported again", err)
	}
	lines := readLines(t, path)
	if len(lines) != 301 || lines[len(lines)-1] != "after" {
		t.Errorf("the live file holds %d lines, want all 301, ending with the last", len(lines))
	}
}
