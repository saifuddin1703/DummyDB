package sstable

import (
	"fmt"
	"testing"

	"github.com/dummydb/internal/config"
	"github.com/dummydb/internal/disk"
)

func TestWriterAndReader_Basic(t *testing.T) {
	t.Parallel()

	fs := disk.NewMemoryFileIO()
	path := "test.sst"

	// Create writer
	opts := Options{
		FileIO:        fs,
		IndexInterval: config.DefaultSparseIndexInterval,
		IsMerged:      false,
	}

	writer, err := NewWriter(path, opts)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	// Write some data in sorted order
	testData := []KeyValue{
		{Key: "key1", Value: []byte("value1")},
		{Key: "key2", Value: []byte("value2")},
		{Key: "key3", Value: []byte("value3")},
	}

	for _, kv := range testData {
		err := writer.Append(kv.Key, kv.Value)
		if err != nil {
			t.Fatalf("failed to append: %v", err)
		}
	}

	// Finalize
	sst, err := writer.Finalize()
	if err != nil {
		t.Fatalf("failed to finalize: %v", err)
	}
	defer sst.Close()

	// Read data back
	for _, kv := range testData {
		value, err := sst.Get(kv.Key)
		if err != nil {
			t.Errorf("failed to get key %s: %v", kv.Key, err)
			continue
		}

		if string(value) != string(kv.Value) {
			t.Errorf("key %s: expected %s, got %s", kv.Key, kv.Value, value)
		}
	}
}

func TestWriter_SortedOrderEnforcement(t *testing.T) {
	t.Parallel()

	fs := disk.NewMemoryFileIO()
	path := "test.sst"

	opts := Options{
		FileIO:        fs,
		IndexInterval: config.DefaultSparseIndexInterval,
		IsMerged:      false,
	}

	writer, err := NewWriter(path, opts)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}
	defer writer.Close()

	// Write in correct order
	err = writer.Append("a", []byte("value_a"))
	if err != nil {
		t.Fatalf("failed to append 'a': %v", err)
	}

	err = writer.Append("b", []byte("value_b"))
	if err != nil {
		t.Fatalf("failed to append 'b': %v", err)
	}

	// Try to write out of order - should fail
	err = writer.Append("a", []byte("value_a2"))
	if err == nil {
		t.Error("expected error when appending out of order, got nil")
	}
}

func TestSSTable_Get_NotFound(t *testing.T) {
	t.Parallel()

	fs := disk.NewMemoryFileIO()
	path := "test.sst"

	opts := Options{
		FileIO:        fs,
		IndexInterval: config.DefaultSparseIndexInterval,
		IsMerged:      false,
	}

	writer, err := NewWriter(path, opts)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	writer.Append("key1", []byte("value1"))
	writer.Append("key2", []byte("value2"))

	sst, err := writer.Finalize()
	if err != nil {
		t.Fatalf("failed to finalize: %v", err)
	}
	defer sst.Close()

	// Try to get non-existent key
	_, err = sst.Get("key999")
	if err == nil {
		t.Error("expected error for non-existent key, got nil")
	}
}

func TestSSTable_Has(t *testing.T) {
	t.Parallel()

	fs := disk.NewMemoryFileIO()
	path := "test.sst"

	opts := Options{
		FileIO:        fs,
		IndexInterval: config.DefaultSparseIndexInterval,
		IsMerged:      false,
	}

	writer, err := NewWriter(path, opts)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	writer.Append("key1", []byte("value1"))
	writer.Append("key2", []byte("value2"))

	sst, err := writer.Finalize()
	if err != nil {
		t.Fatalf("failed to finalize: %v", err)
	}
	defer sst.Close()

	if !sst.Has("key1") {
		t.Error("expected key1 to exist")
	}

	if !sst.Has("key2") {
		t.Error("expected key2 to exist")
	}

	if sst.Has("key999") {
		t.Error("expected key999 to not exist")
	}
}

func TestSSTable_Iterator(t *testing.T) {
	t.Parallel()

	fs := disk.NewMemoryFileIO()
	path := "test.sst"

	opts := Options{
		FileIO:        fs,
		IndexInterval: config.DefaultSparseIndexInterval,
		IsMerged:      false,
	}

	writer, err := NewWriter(path, opts)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	testData := []KeyValue{
		{Key: "a", Value: []byte("value_a")},
		{Key: "b", Value: []byte("value_b")},
		{Key: "c", Value: []byte("value_c")},
	}

	for _, kv := range testData {
		writer.Append(kv.Key, kv.Value)
	}

	sst, err := writer.Finalize()
	if err != nil {
		t.Fatalf("failed to finalize: %v", err)
	}
	defer sst.Close()

	// Iterate and verify
	iter := sst.Iterator()
	defer iter.Close()

	idx := 0
	for iter.Next() {
		if idx >= len(testData) {
			t.Error("iterator returned more items than expected")
			break
		}

		key := iter.Key()
		value := iter.Value()

		if key != testData[idx].Key {
			t.Errorf("expected key %s, got %s", testData[idx].Key, key)
		}

		if string(value) != string(testData[idx].Value) {
			t.Errorf("expected value %s, got %s", testData[idx].Value, value)
		}

		idx++
	}

	if idx != len(testData) {
		t.Errorf("expected %d items, got %d", len(testData), idx)
	}

	if iter.Err() != nil {
		t.Errorf("iterator error: %v", iter.Err())
	}
}

func TestSSTable_LargeDataset(t *testing.T) {
	t.Parallel()

	fs := disk.NewMemoryFileIO()
	path := "large.sst"

	opts := Options{
		FileIO:        fs,
		IndexInterval: 1024, // Small interval for testing
		IsMerged:      false,
	}

	writer, err := NewWriter(path, opts)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	// Write 1000 entries
	numEntries := 1000
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key%04d", i)
		value := []byte(fmt.Sprintf("value%04d", i))
		err := writer.Append(key, value)
		if err != nil {
			t.Fatalf("failed to append entry %d: %v", i, err)
		}
	}

	sst, err := writer.Finalize()
	if err != nil {
		t.Fatalf("failed to finalize: %v", err)
	}
	defer sst.Close()

	// Verify random reads
	testKeys := []int{0, 100, 500, 999}
	for _, i := range testKeys {
		key := fmt.Sprintf("key%04d", i)
		expectedValue := fmt.Sprintf("value%04d", i)

		value, err := sst.Get(key)
		if err != nil {
			t.Errorf("failed to get key %s: %v", key, err)
			continue
		}

		if string(value) != expectedValue {
			t.Errorf("key %s: expected %s, got %s", key, expectedValue, value)
		}
	}
}

func TestSSTable_Delete(t *testing.T) {
	t.Parallel()

	fs := disk.NewMemoryFileIO()
	path := "delete.sst"

	opts := Options{
		FileIO:        fs,
		IndexInterval: config.DefaultSparseIndexInterval,
		IsMerged:      false,
	}

	writer, err := NewWriter(path, opts)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	writer.Append("key1", []byte("value1"))

	sst, err := writer.Finalize()
	if err != nil {
		t.Fatalf("failed to finalize: %v", err)
	}

	// Delete the SSTable
	err = sst.Delete()
	if err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	// Try to open again - should fail
	_, err = Open(path, fs)
	if err == nil {
		t.Error("expected error opening deleted SSTable, got nil")
	}
}

func TestSSTable_Path(t *testing.T) {
	t.Parallel()

	fs := disk.NewMemoryFileIO()
	path := "test.sst"

	opts := Options{
		FileIO:        fs,
		IndexInterval: config.DefaultSparseIndexInterval,
		IsMerged:      false,
	}

	writer, err := NewWriter(path, opts)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	writer.Append("key1", []byte("value1"))

	sst, err := writer.Finalize()
	if err != nil {
		t.Fatalf("failed to finalize: %v", err)
	}
	defer sst.Close()

	if sst.Path() != path {
		t.Errorf("expected path %s, got %s", path, sst.Path())
	}
}

func TestSSTable_Size(t *testing.T) {
	t.Parallel()

	fs := disk.NewMemoryFileIO()
	path := "test.sst"

	opts := Options{
		FileIO:        fs,
		IndexInterval: config.DefaultSparseIndexInterval,
		IsMerged:      false,
	}

	writer, err := NewWriter(path, opts)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	writer.Append("key1", []byte("value1"))
	writer.Append("key2", []byte("value2"))

	sst, err := writer.Finalize()
	if err != nil {
		t.Fatalf("failed to finalize: %v", err)
	}
	defer sst.Close()

	size := sst.Size()
	if size <= 0 {
		t.Errorf("expected positive size, got %d", size)
	}

	// Size should be approximately len("key1:value1;key2:value2;") = 26
	expectedSize := int64(len("key1:value1;key2:value2;"))
	if size != expectedSize {
		t.Errorf("expected size %d, got %d", expectedSize, size)
	}
}

func TestSSTable_IsMerged(t *testing.T) {
	t.Parallel()

	fs := disk.NewMemoryFileIO()

	tests := []struct {
		name     string
		isMerged bool
	}{
		{"not merged", false},
		{"merged", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := Options{
				FileIO:        fs,
				IndexInterval: config.DefaultSparseIndexInterval,
				IsMerged:      tt.isMerged,
			}

			writer, err := NewWriter(fmt.Sprintf("test_%s.sst", tt.name), opts)
			if err != nil {
				t.Fatalf("failed to create writer: %v", err)
			}

			writer.Append("key1", []byte("value1"))

			sst, err := writer.Finalize()
			if err != nil {
				t.Fatalf("failed to finalize: %v", err)
			}
			defer sst.Close()

			if sst.IsMerged() != tt.isMerged {
				t.Errorf("expected IsMerged=%v, got %v", tt.isMerged, sst.IsMerged())
			}
		})
	}
}

func TestSparseIndex(t *testing.T) {
	t.Parallel()

	index := NewSparseIndex()

	// Add some entries
	index.Put("aaa", 0)
	index.Put("mmm", 1000)
	index.Put("zzz", 2000)

	// Test Get
	offset, ok := index.Get("mmm")
	if !ok || offset != 1000 {
		t.Errorf("expected offset 1000, got %d (ok=%v)", offset, ok)
	}

	// Test FindNearestBefore
	tests := []struct {
		key            string
		expectedOffset int64
	}{
		{"aaa", 0},
		{"bbb", 0},    // Should find "aaa"
		{"mmm", 1000},
		{"nnn", 1000}, // Should find "mmm"
		{"zzz", 2000},
		{"zzza", 2000}, // Should find "zzz"
	}

	for _, tt := range tests {
		offset := index.FindNearestBefore(tt.key)
		if offset != tt.expectedOffset {
			t.Errorf("key %s: expected offset %d, got %d", tt.key, tt.expectedOffset, offset)
		}
	}

	// Test Len
	if index.Len() != 3 {
		t.Errorf("expected len 3, got %d", index.Len())
	}
}

func TestWriter_Close(t *testing.T) {
	t.Parallel()

	fs := disk.NewMemoryFileIO()
	path := "test.sst"

	opts := Options{
		FileIO:        fs,
		IndexInterval: config.DefaultSparseIndexInterval,
		IsMerged:      false,
	}

	writer, err := NewWriter(path, opts)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	writer.Append("key1", []byte("value1"))

	// Close without finalizing
	err = writer.Close()
	if err != nil {
		t.Fatalf("failed to close: %v", err)
	}

	// File should not exist
	_, err = fs.ReadFile(path)
	if err == nil {
		t.Error("expected file to not exist after close without finalize")
	}
}

func TestOpen_ExistingSSTable(t *testing.T) {
	t.Parallel()

	fs := disk.NewMemoryFileIO()
	path := "test.sst"

	// Write data directly
	data := "key1:value1;key2:value2;key3:value3;"
	err := fs.WriteFile(path, []byte(data), 0644)
	if err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	// Open it
	sst, err := Open(path, fs)
	if err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer sst.Close()

	// Verify we can read
	value, err := sst.Get("key2")
	if err != nil {
		t.Fatalf("failed to get key2: %v", err)
	}

	if string(value) != "value2" {
		t.Errorf("expected value2, got %s", value)
	}
}
