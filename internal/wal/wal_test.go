package wal

import (
	"strings"
	"testing"

	"github.com/dummydb/internal/disk"
)

func TestWAL_AppendAndRecover(t *testing.T) {
	t.Parallel()

	fs := disk.NewMemoryFileIO()
	wal, err := NewWAL("test.wal", fs)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Append some entries
	entries := []*Entry{
		{Key: "key1", Value: "value1", Type: EntryTypePut},
		{Key: "key2", Value: "value2", Type: EntryTypePut},
		{Key: "key3", Value: "value3", Type: EntryTypePut},
	}

	for _, entry := range entries {
		err := wal.Append(entry)
		if err != nil {
			t.Fatalf("failed to append entry: %v", err)
		}
	}

	// Recover entries
	recovered, err := wal.Recover()
	if err != nil {
		t.Fatalf("failed to recover: %v", err)
	}

	if len(recovered) != len(entries) {
		t.Errorf("expected %d entries, got %d", len(entries), len(recovered))
	}

	for i, entry := range recovered {
		if entry.Key != entries[i].Key {
			t.Errorf("entry %d: expected key %s, got %s", i, entries[i].Key, entry.Key)
		}
		if entry.Value != entries[i].Value {
			t.Errorf("entry %d: expected value %s, got %s", i, entries[i].Value, entry.Value)
		}
	}
}

func TestWAL_Truncate(t *testing.T) {
	t.Parallel()

	fs := disk.NewMemoryFileIO()
	wal, err := NewWAL("test.wal", fs)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Append entries
	err = wal.Append(&Entry{Key: "key1", Value: "value1", Type: EntryTypePut})
	if err != nil {
		t.Fatalf("failed to append: %v", err)
	}

	// Truncate
	err = wal.Truncate()
	if err != nil {
		t.Fatalf("failed to truncate: %v", err)
	}

	// Recover - should be empty
	recovered, err := wal.Recover()
	if err != nil {
		t.Fatalf("failed to recover after truncate: %v", err)
	}

	if len(recovered) != 0 {
		t.Errorf("expected 0 entries after truncate, got %d", len(recovered))
	}
}

func TestWAL_AppendAfterTruncate(t *testing.T) {
	t.Parallel()

	fs := disk.NewMemoryFileIO()
	wal, err := NewWAL("test.wal", fs)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Append, truncate, append again
	wal.Append(&Entry{Key: "old", Value: "old_value", Type: EntryTypePut})
	wal.Truncate()
	wal.Append(&Entry{Key: "new", Value: "new_value", Type: EntryTypePut})

	recovered, err := wal.Recover()
	if err != nil {
		t.Fatalf("failed to recover: %v", err)
	}

	if len(recovered) != 1 {
		t.Errorf("expected 1 entry, got %d", len(recovered))
	}

	if recovered[0].Key != "new" {
		t.Errorf("expected key 'new', got '%s'", recovered[0].Key)
	}
}

func TestWAL_EmptyFile(t *testing.T) {
	t.Parallel()

	fs := disk.NewMemoryFileIO()
	wal, err := NewWAL("test.wal", fs)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Recover from empty file
	recovered, err := wal.Recover()
	if err != nil {
		t.Fatalf("failed to recover from empty file: %v", err)
	}

	if len(recovered) != 0 {
		t.Errorf("expected 0 entries from empty file, got %d", len(recovered))
	}
}

func TestWAL_Close(t *testing.T) {
	t.Parallel()

	fs := disk.NewMemoryFileIO()
	wal, err := NewWAL("test.wal", fs)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}

	// Append an entry
	err = wal.Append(&Entry{Key: "key1", Value: "value1", Type: EntryTypePut})
	if err != nil {
		t.Fatalf("failed to append: %v", err)
	}

	// Close
	err = wal.Close()
	if err != nil {
		t.Fatalf("failed to close: %v", err)
	}

	// Try to append after close - should fail
	err = wal.Append(&Entry{Key: "key2", Value: "value2", Type: EntryTypePut})
	if err != ErrWALClosed {
		t.Errorf("expected ErrWALClosed, got %v", err)
	}
}

func TestWAL_Sync(t *testing.T) {
	t.Parallel()

	fs := disk.NewMemoryFileIO()
	wal, err := NewWAL("test.wal", fs)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Append and sync
	err = wal.Append(&Entry{Key: "key1", Value: "value1", Type: EntryTypePut})
	if err != nil {
		t.Fatalf("failed to append: %v", err)
	}

	err = wal.Sync()
	if err != nil {
		t.Fatalf("failed to sync: %v", err)
	}
}

func TestWAL_Path(t *testing.T) {
	t.Parallel()

	fs := disk.NewMemoryFileIO()
	expectedPath := "test.wal"
	wal, err := NewWAL(expectedPath, fs)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.Close()

	if wal.Path() != expectedPath {
		t.Errorf("expected path %s, got %s", expectedPath, wal.Path())
	}
}

func TestEntry_Encode(t *testing.T) {
	t.Parallel()

	entry := &Entry{
		Key:   "mykey",
		Value: "myvalue",
		Type:  EntryTypePut,
	}

	encoded := entry.Encode()
	expected := "mykey:myvalue;"

	if encoded != expected {
		t.Errorf("expected %s, got %s", expected, encoded)
	}
}

func TestDecodeEntry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		input     string
		expectErr bool
		expectKey string
		expectVal string
	}{
		{
			name:      "valid entry",
			input:     "key1:value1",
			expectErr: false,
			expectKey: "key1",
			expectVal: "value1",
		},
		{
			name:      "valid entry with semicolon",
			input:     "key1:value1;",
			expectErr: false,
			expectKey: "key1",
			expectVal: "value1",
		},
		{
			name:      "value with colon",
			input:     "key1:val:ue1",
			expectErr: false,
			expectKey: "key1",
			expectVal: "val:ue1",
		},
		{
			name:      "invalid entry - no colon",
			input:     "keyvalue",
			expectErr: true,
		},
		{
			name:      "invalid entry - only key",
			input:     "key:",
			expectErr: false,
			expectKey: "key",
			expectVal: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry, err := DecodeEntry(tt.input)

			if tt.expectErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if entry.Key != tt.expectKey {
				t.Errorf("expected key %s, got %s", tt.expectKey, entry.Key)
			}

			if entry.Value != tt.expectVal {
				t.Errorf("expected value %s, got %s", tt.expectVal, entry.Value)
			}
		})
	}
}

func TestWAL_CorruptedEntry(t *testing.T) {
	t.Parallel()

	fs := disk.NewMemoryFileIO()

	// Write corrupted data directly
	err := fs.WriteFile("corrupted.wal", []byte("key1:value1;invalidentry;key2:value2;"), 0644)
	if err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	wal, err := NewWAL("corrupted.wal", fs)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Should recover valid entries and skip corrupted ones
	recovered, err := wal.Recover()
	if err != nil {
		t.Fatalf("failed to recover: %v", err)
	}

	// Should have 2 valid entries (key1 and key2)
	if len(recovered) != 2 {
		t.Errorf("expected 2 valid entries, got %d", len(recovered))
	}
}

func TestWAL_LargeEntries(t *testing.T) {
	t.Parallel()

	fs := disk.NewMemoryFileIO()
	wal, err := NewWAL("large.wal", fs)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Create large value
	largeValue := strings.Repeat("x", 10000)

	entry := &Entry{
		Key:   "largekey",
		Value: largeValue,
		Type:  EntryTypePut,
	}

	err = wal.Append(entry)
	if err != nil {
		t.Fatalf("failed to append large entry: %v", err)
	}

	recovered, err := wal.Recover()
	if err != nil {
		t.Fatalf("failed to recover large entry: %v", err)
	}

	if len(recovered) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(recovered))
	}

	if recovered[0].Value != largeValue {
		t.Error("large value mismatch")
	}
}
