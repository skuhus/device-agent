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
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	var out []string
	scanner := bufio.NewScanner(f)
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
	for i := 0; i < 1200; i++ {
		write(t, file, fmt.Sprintf("r%04d %s", i, pad))
	}

	matches, err := filepath.Glob(path + "*")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	if len(matches) != keep+1 {
		t.Errorf("files = %v, want the live file and %d rotated ones", matches, keep)
	}
	for i := 1; i <= keep; i++ {
		info, err := os.Stat(fmt.Sprintf("%s.%d", path, i))
		if err != nil {
			t.Fatalf("expected rotated file %d: %v", i, err)
		}
		if info.Size() == 0 || info.Size() > 1<<20 {
			t.Errorf("%s.%d is %d bytes, want some, at most 1 MB", path, i, info.Size())
		}
	}

	// Each older file ends before the newer one starts.
	newer := readLines(t, path)
	for i := 1; i <= keep; i++ {
		older := readLines(t, fmt.Sprintf("%s.%d", path, i))
		if older[len(older)-1] >= newer[0] {
			t.Errorf("%s.%d ends at %.5s but the next file starts at %.5s", path, i, older[len(older)-1], newer[0])
		}
		newer = older
	}
}

// keep: 0 means rotate and discard, which must not leave a growing file behind.
func TestFileKeepZeroDiscardsHistory(t *testing.T) {
	file, path := openFile(t, 1, 0)
	pad := strings.Repeat("x", 4096)
	for i := 0; i < 600; i++ {
		write(t, file, fmt.Sprintf("r%04d %s", i, pad))
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
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				if _, err := file.Write([]byte(fmt.Sprintf("w%d-r%d\n", w, i))); err != nil {
					t.Errorf("Write: %v", err)
					return
				}
			}
		}(w)
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
