package lsm

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dummydb/internal/bloom"
	"github.com/dummydb/internal/compaction"
	"github.com/dummydb/internal/config"
	"github.com/dummydb/internal/disk"
	"github.com/dummydb/internal/storage/sstable"
	"github.com/dummydb/internal/wal"
)

func setupTestEngine(t *testing.T) (*Engine, func()) {
	t.Helper()

	cfg := config.DefaultConfig()
	cfg.SegmentDir = fmt.Sprintf("test-segments-%d", time.Now().UnixNano())
	cfg.WALPath = fmt.Sprintf("test-wal-%d", time.Now().UnixNano())
	cfg.MemTableSizeBytes = 1024 // Small for testing
	cfg.MaxTablesBeforeCompaction = 4

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

	engine, err := NewEngine(cfg, fs, walImpl, compactor, filter)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	cleanup := func() {
		engine.Close()
	}

	return engine, cleanup
}

func TestEngine_PutAndGet(t *testing.T) {
	t.Parallel()

	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Put a value
	err := engine.Put("key1", []byte("value1"))
	if err != nil {
		t.Fatalf("failed to put: %v", err)
	}

	// Get the value
	val, found := engine.Get("key1")
	if !found {
		t.Error("expected to find key1")
	}

	if string(val) != "value1" {
		t.Errorf("expected value1, got %s", val)
	}
}

func TestEngine_GetNotFound(t *testing.T) {
	t.Parallel()

	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	_, found := engine.Get("nonexistent")
	if found {
		t.Error("expected not to find nonexistent key")
	}
}

func TestEngine_Update(t *testing.T) {
	t.Parallel()

	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Put initial value
	engine.Put("key1", []byte("value1"))

	// Update value
	engine.Put("key1", []byte("value2"))

	// Get updated value
	val, found := engine.Get("key1")
	if !found {
		t.Error("expected to find key1")
	}

	if string(val) != "value2" {
		t.Errorf("expected value2, got %s", val)
	}
}

func TestEngine_Delete(t *testing.T) {
	t.Parallel()

	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Put a value
	engine.Put("key1", []byte("value1"))

	// Verify it exists
	_, found := engine.Get("key1")
	if !found {
		t.Error("expected to find key1 before delete")
	}

	// Delete it
	err := engine.Delete("key1")
	if err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	// Verify it's gone
	_, found = engine.Get("key1")
	if found {
		t.Error("expected not to find key1 after delete")
	}
}

func TestEngine_Keys(t *testing.T) {
	t.Parallel()

	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Put some values
	engine.Put("key1", []byte("value1"))
	engine.Put("key2", []byte("value2"))
	engine.Put("key3", []byte("value3"))

	keys := engine.Keys()

	if len(keys) != 3 {
		t.Errorf("expected 3 keys, got %d", len(keys))
	}

	expectedKeys := map[string]bool{
		"key1": true,
		"key2": true,
		"key3": true,
	}

	for _, key := range keys {
		if !expectedKeys[key] {
			t.Errorf("unexpected key: %s", key)
		}
	}
}

func TestEngine_KeysWithDeletes(t *testing.T) {
	t.Parallel()

	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Put values
	engine.Put("key1", []byte("value1"))
	engine.Put("key2", []byte("value2"))
	engine.Put("key3", []byte("value3"))

	// Delete one
	engine.Delete("key2")

	keys := engine.Keys()

	if len(keys) != 2 {
		t.Errorf("expected 2 keys, got %d", len(keys))
	}

	// key2 should not be in the list
	for _, key := range keys {
		if key == "key2" {
			t.Error("deleted key2 should not be in keys list")
		}
	}
}

func TestEngine_MemtableFlush(t *testing.T) {
	t.Parallel()

	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Write enough data to trigger a flush
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d_with_some_extra_data_to_increase_size", i)
		engine.Put(key, []byte(value))
	}

	// Give time for flush to complete
	time.Sleep(100 * time.Millisecond)

	// Verify data is still accessible
	val, found := engine.Get("key50")
	if !found {
		t.Error("expected to find key50 after flush")
	}

	expected := "value50_with_some_extra_data_to_increase_size"
	if string(val) != expected {
		t.Errorf("expected %s, got %s", expected, val)
	}
}

func TestEngine_ConcurrentWrites(t *testing.T) {
	t.Parallel()

	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	var wg sync.WaitGroup
	numGoroutines := 10
	numOpsPerGoroutine := 50

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := 0; j < numOpsPerGoroutine; j++ {
				key := fmt.Sprintf("key_%d_%d", id, j)
				value := fmt.Sprintf("value_%d_%d", id, j)
				engine.Put(key, []byte(value))
			}
		}(i)
	}

	wg.Wait()

	// Verify a sample of data
	val, found := engine.Get("key_5_25")
	if !found {
		t.Error("expected to find key_5_25")
	}

	if string(val) != "value_5_25" {
		t.Errorf("expected value_5_25, got %s", val)
	}
}

func TestEngine_ConcurrentReadsAndWrites(t *testing.T) {
	t.Parallel()

	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Pre-populate some data
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		engine.Put(key, []byte(value))
	}

	var wg sync.WaitGroup

	// Writers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := 0; j < 20; j++ {
				key := fmt.Sprintf("wkey_%d_%d", id, j)
				value := fmt.Sprintf("wvalue_%d_%d", id, j)
				engine.Put(key, []byte(value))
			}
		}(i)
	}

	// Readers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for j := 0; j < 50; j++ {
				key := fmt.Sprintf("key%d", j%50)
				engine.Get(key)
			}
		}()
	}

	wg.Wait()

	// Give time for any pending flushes
	time.Sleep(100 * time.Millisecond)

	// Verify engine is still functional
	val, found := engine.Get("key10")
	if !found {
		t.Error("expected to find key10 after concurrent operations")
	}

	if string(val) != "value10" {
		t.Errorf("expected value10, got %s", val)
	}
}

func TestEngine_Close(t *testing.T) {
	t.Parallel()

	engine, _ := setupTestEngine(t)

	// Put some data
	engine.Put("key1", []byte("value1"))

	// Close
	err := engine.Close()
	if err != nil {
		t.Fatalf("failed to close: %v", err)
	}

	// Try to put after close - should fail
	err = engine.Put("key2", []byte("value2"))
	if err == nil {
		t.Error("expected error when putting after close")
	}

	// Close again - should be fine
	err = engine.Close()
	if err != nil {
		t.Fatalf("failed to close again: %v", err)
	}
}

func TestEngine_WALRecovery(t *testing.T) {
	t.Parallel()

	cfg := config.DefaultConfig()
	cfg.SegmentDir = fmt.Sprintf("test-segments-%d", time.Now().UnixNano())
	cfg.WALPath = fmt.Sprintf("test-wal-%d", time.Now().UnixNano())
	cfg.MemTableSizeBytes = 10 * 1024 * 1024 // Large to prevent flush

	fs := disk.NewMemoryFileIO()
	fs.MkdirAll(cfg.SegmentDir, 0755)

	// Create first engine
	walImpl1, _ := wal.NewWAL(cfg.WALPath, fs)
	filter1 := bloom.NewFilter(10000, 0.01)

	sstableOpts := sstable.Options{
		FileIO:        fs,
		IndexInterval: cfg.SparseIndexIntervalBytes,
	}

	compactor1 := compaction.NewLeveledCompactor(cfg, sstableOpts)

	engine1, _ := NewEngine(cfg, fs, walImpl1, compactor1, filter1)

	// Write data
	engine1.Put("key1", []byte("value1"))
	engine1.Put("key2", []byte("value2"))

	// Close without flushing memtable
	engine1.Close()

	// Create second engine (should recover from WAL)
	walImpl2, _ := wal.NewWAL(cfg.WALPath, fs)
	filter2 := bloom.NewFilter(10000, 0.01)
	compactor2 := compaction.NewLeveledCompactor(cfg, sstableOpts)

	engine2, err := NewEngine(cfg, fs, walImpl2, compactor2, filter2)
	if err != nil {
		t.Fatalf("failed to create engine2: %v", err)
	}
	defer engine2.Close()

	// Verify data recovered
	val, found := engine2.Get("key1")
	if !found {
		t.Error("expected to find key1 after recovery")
	}

	if string(val) != "value1" {
		t.Errorf("expected value1, got %s", val)
	}

	val, found = engine2.Get("key2")
	if !found {
		t.Error("expected to find key2 after recovery")
	}

	if string(val) != "value2" {
		t.Errorf("expected value2, got %s", val)
	}
}

func TestEngine_BloomFilterOptimization(t *testing.T) {
	t.Parallel()

	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Put some data
	engine.Put("key1", []byte("value1"))
	engine.Put("key2", []byte("value2"))

	// Query for a key that definitely doesn't exist
	// Bloom filter should return false quickly
	_, found := engine.Get("definitely_not_there_xyz_123")
	if found {
		t.Error("bloom filter should have returned false")
	}
}
