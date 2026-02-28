package wal

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/dummydb/internal/disk"
)

// WALImpl implements the WAL interface using file I/O
type WALImpl struct {
	path   string
	fileIO disk.FileIO
	mu     sync.Mutex
	file   disk.File
	closed bool
}

// NewWAL creates a new WAL instance
func NewWAL(path string, fileIO disk.FileIO) (*WALImpl, error) {
	w := &WALImpl{
		path:   path,
		fileIO: fileIO,
		closed: false,
	}

	// Open or create the WAL file
	file, err := fileIO.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	w.file = file
	return w, nil
}

// Append adds an entry to the WAL
func (w *WALImpl) Append(entry *Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrWALClosed
	}

	encoded := entry.Encode()
	_, err := w.file.WriteString(encoded)
	if err != nil {
		return fmt.Errorf("failed to append to WAL: %w", err)
	}

	return nil
}

// Sync forces a sync of the WAL to disk
func (w *WALImpl) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrWALClosed
	}

	return w.file.Sync()
}

// Recover reads all entries from the WAL
func (w *WALImpl) Recover() ([]*Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil, ErrWALClosed
	}

	// Read the entire WAL file
	contents, err := w.fileIO.ReadFile(w.path)
	if err != nil {
		// If file doesn't exist, return empty list
		if os.IsNotExist(err) || strings.Contains(err.Error(), "does not exist") {
			return []*Entry{}, nil
		}
		return nil, fmt.Errorf("failed to read WAL: %w", err)
	}

	if len(contents) == 0 {
		return []*Entry{}, nil
	}

	// Parse entries
	entriesStr := strings.Split(string(contents), ";")
	entries := make([]*Entry, 0, len(entriesStr))

	for _, entryStr := range entriesStr {
		entryStr = strings.TrimSpace(entryStr)
		if len(entryStr) == 0 {
			continue
		}

		entry, err := DecodeEntry(entryStr)
		if err != nil {
			// Log warning but continue with other entries
			fmt.Printf("Warning: skipping corrupted WAL entry: %v\n", err)
			continue
		}

		entries = append(entries, entry)
	}

	return entries, nil
}

// Truncate empties the WAL
func (w *WALImpl) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrWALClosed
	}

	// Close existing file
	if w.file != nil {
		w.file.Close()
	}

	// Open with truncate flag
	file, err := w.fileIO.OpenFile(w.path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to truncate WAL: %w", err)
	}

	// Close the file immediately since we'll reopen in append mode
	file.Close()

	// Reopen in append mode
	file, err = w.fileIO.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to reopen WAL after truncate: %w", err)
	}

	w.file = file
	return nil
}

// Close closes the WAL
func (w *WALImpl) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	w.closed = true

	if w.file != nil {
		return w.file.Close()
	}

	return nil
}

// Path returns the path to the WAL file
func (w *WALImpl) Path() string {
	return w.path
}
