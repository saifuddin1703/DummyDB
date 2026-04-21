package wal

import (
	"errors"
)

var (
	// ErrWALClosed is returned when trying to use a closed WAL
	ErrWALClosed = errors.New("wal is closed")

	// ErrWALCorrupted is returned when WAL data is corrupted
	ErrWALCorrupted = errors.New("wal is corrupted")
)

// WAL defines the write-ahead log interface
type WAL interface {
	// Append adds an entry to the WAL
	Append(entry *Entry) error

	// Sync forces a sync of the WAL to disk
	Sync() error

	// Recover reads all entries from the WAL
	Recover() ([]*Entry, error)

	// Truncate empties the WAL
	Truncate() error

	// Close closes the WAL
	Close() error

	// Path returns the path to the WAL file
	Path() string
}
