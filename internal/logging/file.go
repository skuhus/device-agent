package logging

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// File is the log file: an append-only file rotated by size. Each Write is one
// record, and rotation happens between records, never inside one.
type File struct {
	path     string
	maxBytes int64
	keep     int

	mu   sync.Mutex
	file *os.File
	size int64
}

// OpenFile opens or creates the log file, appending to what it holds.
//
// maxSizeMB is the size at which the file rotates; keep is how many rotated
// files are retained. keep of zero means rotate and discard the previous file.
// The parent directory must already exist: creating it here would paper over a
// packaging step that failed.
func OpenFile(path string, maxSizeMB, keep int) (*File, error) {
	if path == "" {
		return nil, errors.New("logging: log file path is required")
	}
	if maxSizeMB < 1 {
		return nil, fmt.Errorf("logging: max size must be at least 1 MB, got %d", maxSizeMB)
	}
	if keep < 0 {
		return nil, fmt.Errorf("logging: keep must not be negative, got %d", keep)
	}
	if info, err := os.Stat(filepath.Dir(path)); err != nil {
		return nil, fmt.Errorf("logging: directory %s: %w", filepath.Dir(path), err)
	} else if !info.IsDir() {
		return nil, fmt.Errorf("logging: %s is not a directory", filepath.Dir(path))
	}

	file := &File{path: path, maxBytes: int64(maxSizeMB) * 1024 * 1024, keep: keep}
	if err := file.open(); err != nil {
		return nil, err
	}
	return file, nil
}

// Path is the live file's path.
func (file *File) Path() string { return file.path }

func (file *File) open() error {
	handle, err := os.OpenFile(file.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return fmt.Errorf("logging: open %s: %w", file.path, err)
	}
	info, err := handle.Stat()
	if err != nil {
		handle.Close()
		return fmt.Errorf("logging: stat %s: %w", file.path, err)
	}
	file.file, file.size = handle, info.Size()
	return nil
}

// Write appends one record, rotating first if the record would take the file
// past its size limit. A record larger than the limit is still written whole,
// into a file of its own.
func (file *File) Write(record []byte) (int, error) {
	file.mu.Lock()
	defer file.mu.Unlock()
	if file.file == nil {
		return 0, fmt.Errorf("logging: %s is closed", file.path)
	}
	if file.size > 0 && file.size+int64(len(record)) > file.maxBytes {
		if err := file.rotate(); err != nil {
			return 0, err
		}
	}
	written, err := file.file.Write(record)
	file.size += int64(written)
	if err != nil {
		return written, fmt.Errorf("logging: write %s: %w", file.path, err)
	}
	return written, nil
}

// rotate renames the current file to .1, shifting existing rotated files up and
// discarding anything past keep. The caller holds the mutex.
func (file *File) rotate() error {
	if err := file.file.Close(); err != nil {
		return fmt.Errorf("logging: close %s before rotation: %w", file.path, err)
	}
	file.file = nil

	if file.keep == 0 {
		if err := os.Remove(file.path); err != nil {
			return fmt.Errorf("logging: discard %s: %w", file.path, err)
		}
		return file.open()
	}

	if err := os.Remove(file.rotatedPath(file.keep)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("logging: remove oldest rotated file: %w", err)
	}
	for i := file.keep - 1; i >= 1; i-- {
		from, to := file.rotatedPath(i), file.rotatedPath(i+1)
		if err := os.Rename(from, to); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("logging: rotate %s to %s: %w", from, to, err)
		}
	}
	if err := os.Rename(file.path, file.rotatedPath(1)); err != nil {
		return fmt.Errorf("logging: rotate %s: %w", file.path, err)
	}
	return file.open()
}

func (file *File) rotatedPath(index int) string { return fmt.Sprintf("%s.%d", file.path, index) }

// Sync makes the kernel put what was written on the disk.
func (file *File) Sync() error {
	file.mu.Lock()
	defer file.mu.Unlock()
	if file.file == nil {
		return fmt.Errorf("logging: %s is closed", file.path)
	}
	if err := file.file.Sync(); err != nil {
		return fmt.Errorf("logging: flush %s: %w", file.path, err)
	}
	return nil
}

// Close flushes and closes the file.
func (file *File) Close() error {
	file.mu.Lock()
	defer file.mu.Unlock()
	if file.file == nil {
		return nil
	}
	syncErr := file.file.Sync()
	closeErr := file.file.Close()
	file.file = nil
	return errors.Join(syncErr, closeErr)
}
