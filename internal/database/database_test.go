package database

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dummydb/internal/bloom"
	"github.com/dummydb/internal/compaction"
	"github.com/dummydb/internal/config"
	"github.com/dummydb/internal/disk"
	"github.com/dummydb/internal/storage/lsm"
	"github.com/dummydb/internal/storage/sstable"
	"github.com/dummydb/internal/wal"
)

func setupTestDatabase(t *testing.T) (*DB, func()) {
	t.Helper()

	cfg := config.DefaultConfig()
	cfg.SegmentDir = fmt.Sprintf("test-segments-%d", time.Now().UnixNano())
	cfg.WALPath = fmt.Sprintf("test-wal-%d", time.Now().UnixNano())
	cfg.MemTableSizeBytes = 10 * 1024 * 1024 // Large to prevent auto-flush in tests

	fs := disk.NewMemoryFileIO()
	fs.MkdirAll(cfg.SegmentDir, 0755)

	walImpl, err := wal.NewWAL(cfg.WALPath, fs)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}

	filter := bloom.NewFilter(10000, 0.01)

	sstableOpts := sstable.Options{
		FileIO:        fs,
		IndexInterval: cfg.SparseIndexIntervalBytes,
	}

	compactor := compaction.NewLeveledCompactor(cfg, sstableOpts)

	engine, err := lsm.NewEngine(cfg, fs, walImpl, compactor, filter)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	db, err := NewDatabase(
		WithConfig(cfg),
		WithStorageEngine(engine),
	)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	cleanup := func() {
		db.Close()
	}

	return db, cleanup
}

func TestNewDatabase_RequiresEngine(t *testing.T) {
	t.Parallel()

	cfg := config.DefaultConfig()

	// Try to create database without engine
	_, err := NewDatabase(WithConfig(cfg))
	if err == nil {
		t.Error("expected error when creating database without engine")
	}
}

func TestDatabase_PutAndGet(t *testing.T) {
	t.Parallel()

	db, cleanup := setupTestDatabase(t)
	defer cleanup()

	// Put a value
	err := db.Put("key1", []byte("value1"))
	if err != nil {
		t.Fatalf("failed to put: %v", err)
	}

	// Get the value
	val, err := db.Get("key1")
	if err != nil {
		t.Fatalf("failed to get: %v", err)
	}

	if string(val) != "value1" {
		t.Errorf("expected value1, got %s", val)
	}
}

func TestDatabase_GetNotFound(t *testing.T) {
	t.Parallel()

	db, cleanup := setupTestDatabase(t)
	defer cleanup()

	_, err := db.Get("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent key")
	}

	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected 'not found' error, got: %v", err)
	}
}

func TestDatabase_Update(t *testing.T) {
	t.Parallel()

	db, cleanup := setupTestDatabase(t)
	defer cleanup()

	// Put initial value
	db.Put("key1", []byte("value1"))

	// Update value
	db.Put("key1", []byte("value2"))

	// Get updated value
	val, err := db.Get("key1")
	if err != nil {
		t.Fatalf("failed to get: %v", err)
	}

	if string(val) != "value2" {
		t.Errorf("expected value2, got %s", val)
	}
}

func TestDatabase_Delete(t *testing.T) {
	t.Parallel()

	db, cleanup := setupTestDatabase(t)
	defer cleanup()

	// Put a value
	db.Put("key1", []byte("value1"))

	// Verify it exists
	_, err := db.Get("key1")
	if err != nil {
		t.Error("expected to find key1 before delete")
	}

	// Delete it
	err = db.Delete("key1")
	if err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	// Verify it's gone
	_, err = db.Get("key1")
	if err == nil {
		t.Error("expected error after delete")
	}
}

func TestDatabase_Keys(t *testing.T) {
	t.Parallel()

	db, cleanup := setupTestDatabase(t)
	defer cleanup()

	// Put some values
	db.Put("key1", []byte("value1"))
	db.Put("key2", []byte("value2"))
	db.Put("key3", []byte("value3"))

	keys, err := db.Keys()
	if err != nil {
		t.Fatalf("failed to get keys: %v", err)
	}

	keysStr := string(keys)
	keysList := strings.Split(keysStr, ",")

	if len(keysList) != 3 {
		t.Errorf("expected 3 keys, got %d", len(keysList))
	}

	expectedKeys := map[string]bool{
		"key1": true,
		"key2": true,
		"key3": true,
	}

	for _, key := range keysList {
		if !expectedKeys[key] {
			t.Errorf("unexpected key: %s", key)
		}
	}
}

func TestDatabase_KeysEmpty(t *testing.T) {
	t.Parallel()

	db, cleanup := setupTestDatabase(t)
	defer cleanup()

	keys, err := db.Keys()
	if err != nil {
		t.Fatalf("failed to get keys: %v", err)
	}

	if len(keys) != 0 {
		t.Errorf("expected empty keys, got %s", keys)
	}
}

func TestDatabase_KeysWithDeletes(t *testing.T) {
	t.Parallel()

	db, cleanup := setupTestDatabase(t)
	defer cleanup()

	// Put values
	db.Put("key1", []byte("value1"))
	db.Put("key2", []byte("value2"))
	db.Put("key3", []byte("value3"))

	// Delete one
	db.Delete("key2")

	keys, err := db.Keys()
	if err != nil {
		t.Fatalf("failed to get keys: %v", err)
	}

	keysStr := string(keys)

	// key2 should not be in the list
	if strings.Contains(keysStr, "key2") {
		t.Error("deleted key2 should not be in keys list")
	}

	// key1 and key3 should be present
	if !strings.Contains(keysStr, "key1") {
		t.Error("expected key1 in keys list")
	}
	if !strings.Contains(keysStr, "key3") {
		t.Error("expected key3 in keys list")
	}
}

func TestDatabase_Close(t *testing.T) {
	t.Parallel()

	db, _ := setupTestDatabase(t)

	// Put some data
	db.Put("key1", []byte("value1"))

	// Close
	err := db.Close()
	if err != nil {
		t.Fatalf("failed to close: %v", err)
	}

	// Try to put after close - should fail
	err = db.Put("key2", []byte("value2"))
	if err == nil {
		t.Error("expected error when putting after close")
	}

	// Try to get after close - should fail
	_, err = db.Get("key1")
	if err == nil {
		t.Error("expected error when getting after close")
	}

	// Close again - should be fine
	err = db.Close()
	if err != nil {
		t.Fatalf("failed to close again: %v", err)
	}
}

func TestDatabase_MultipleOperations(t *testing.T) {
	t.Parallel()

	db, cleanup := setupTestDatabase(t)
	defer cleanup()

	// Perform multiple operations
	operations := []struct {
		op    string
		key   string
		value string
	}{
		{"put", "key1", "value1"},
		{"put", "key2", "value2"},
		{"get", "key1", "value1"},
		{"put", "key1", "updated1"},
		{"get", "key1", "updated1"},
		{"delete", "key2", ""},
		{"put", "key3", "value3"},
	}

	for i, op := range operations {
		switch op.op {
		case "put":
			err := db.Put(op.key, []byte(op.value))
			if err != nil {
				t.Errorf("operation %d: failed to put %s: %v", i, op.key, err)
			}

		case "get":
			val, err := db.Get(op.key)
			if err != nil {
				t.Errorf("operation %d: failed to get %s: %v", i, op.key, err)
			} else if string(val) != op.value {
				t.Errorf("operation %d: expected %s, got %s", i, op.value, val)
			}

		case "delete":
			err := db.Delete(op.key)
			if err != nil {
				t.Errorf("operation %d: failed to delete %s: %v", i, op.key, err)
			}
		}
	}

	// Verify final state
	keys, _ := db.Keys()
	keysStr := string(keys)

	if !strings.Contains(keysStr, "key1") {
		t.Error("expected key1 in final state")
	}
	if strings.Contains(keysStr, "key2") {
		t.Error("key2 should be deleted in final state")
	}
	if !strings.Contains(keysStr, "key3") {
		t.Error("expected key3 in final state")
	}
}

func TestDatabase_LargeValues(t *testing.T) {
	t.Parallel()

	db, cleanup := setupTestDatabase(t)
	defer cleanup()

	// Create a large value (10KB)
	largeValue := make([]byte, 10*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	// Put large value
	err := db.Put("largekey", largeValue)
	if err != nil {
		t.Fatalf("failed to put large value: %v", err)
	}

	// Get it back
	val, err := db.Get("largekey")
	if err != nil {
		t.Fatalf("failed to get large value: %v", err)
	}

	if len(val) != len(largeValue) {
		t.Errorf("expected length %d, got %d", len(largeValue), len(val))
	}

	// Verify content
	for i := range val {
		if val[i] != largeValue[i] {
			t.Errorf("byte %d: expected %d, got %d", i, largeValue[i], val[i])
			break
		}
	}
}

func TestDatabase_ManyKeys(t *testing.T) {
	t.Parallel()

	db, cleanup := setupTestDatabase(t)
	defer cleanup()

	// Insert many keys
	numKeys := 1000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%04d", i)
		value := fmt.Sprintf("value%04d", i)
		err := db.Put(key, []byte(value))
		if err != nil {
			t.Fatalf("failed to put key %s: %v", key, err)
		}
	}

	// Verify a sample
	testKeys := []int{0, 100, 500, 999}
	for _, i := range testKeys {
		key := fmt.Sprintf("key%04d", i)
		expectedValue := fmt.Sprintf("value%04d", i)

		val, err := db.Get(key)
		if err != nil {
			t.Errorf("failed to get key %s: %v", key, err)
			continue
		}

		if string(val) != expectedValue {
			t.Errorf("key %s: expected %s, got %s", key, expectedValue, val)
		}
	}

	// Verify count
	keys, _ := db.Keys()
	keyCount := len(strings.Split(string(keys), ","))

	if keyCount != numKeys {
		t.Errorf("expected %d keys, got %d", numKeys, keyCount)
	}
}
