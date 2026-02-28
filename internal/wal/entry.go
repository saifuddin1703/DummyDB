package wal

import (
	"fmt"
	"strings"
	"time"
)

// EntryType represents the type of WAL entry
type EntryType int

const (
	// EntryTypePut represents a put operation
	EntryTypePut EntryType = iota
	// EntryTypeDelete represents a delete operation
	EntryTypeDelete
)

// Entry represents a single entry in the WAL
type Entry struct {
	Key       string
	Value     string
	Timestamp time.Time
	Type      EntryType
}

// Encode encodes the entry to the WAL format: "key:value;"
// For backward compatibility with existing format
func (e *Entry) Encode() string {
	return fmt.Sprintf("%s:%s;", e.Key, e.Value)
}

// DecodeEntry decodes a WAL entry from string format
func DecodeEntry(s string) (*Entry, error) {
	s = strings.TrimSuffix(s, ";")
	parts := strings.SplitN(s, ":", 2)

	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid entry format: %s", s)
	}

	return &Entry{
		Key:       parts[0],
		Value:     parts[1],
		Timestamp: time.Now(),
		Type:      EntryTypePut,
	}, nil
}
