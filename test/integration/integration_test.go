package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/dummydb/internal/bloom"
	"github.com/dummydb/internal/compaction"
	"github.com/dummydb/internal/config"
	"github.com/dummydb/internal/database"
	"github.com/dummydb/internal/disk"
	"github.com/dummydb/internal/storage/lsm"
	"github.com/dummydb/internal/storage/sstable"
	"github.com/dummydb/internal/wal"
)

// TestFullCycle tests the complete write -> flush -> compact -> read cycle
func TestFullCycle(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.SegmentDir = fmt.Sprintf("test-segments-%d", time.Now().UnixNano())
	cfg.WALPath = fmt.Sprintf("test-wal-%d", time.Now().UnixNano())
	cfg.MemTableSizeBytes = 1024 // Small to trigger flushes
	cfg.MaxTablesBeforeCompaction = 3

	fs := disk.NewMemoryFileIO()
	fs.MkdirAll(cfg.SegmentDir, 0755)

	walImpl, _ := wal.NewWAL(cfg.WALPath, fs)
	filter := bloom.NewFilter(10000, 0.01)

	sstableOpts := sstable.Options{
		FileIO:        fs,
		IndexInterval: cfg.SparseIndexIntervalBytes,
	}

	compactor := compaction.NewLeveledCompactor(cfg, sstableOpts)
	engine, _ := lsm.NewEngine(cfg, fs, walImpl, compactor, filter)

	db, _ := database.NewDatabase(
		database.WithConfig(cfg),
		database.WithStorageEngine(engine),
	)
	defer db.Close()

	// Write enough data to trigger flushes and compaction
	numWrites := 200
	for i := 0; i < numWrites; i++ {
		key := fmt.Sprintf("key%05d", i)
		value := fmt.Sprintf("value%05d_with_extra_data_to_increase_size", i)
		err := db.Put(key, []byte(value))
		if err != nil {
			t.Fatalf("failed to put key %s: %v", key, err)
		}
	}

	// Give time for background operations
	time.Sleep(200 * time.Millisecond)

	// Verify all data is readable
	for i := 0; i < numWrites; i++ {
		key := fmt.Sprintf("key%05d", i)
		expectedValue := fmt.Sprintf("value%05d_with_extra_data_to_increase_size", i)

		val, err := db.Get(key)
		if err != nil {
			t.Errorf("failed to get key %s: %v", key, err)
			continue
		}

		if string(val) != expectedValue {
			t.Errorf("key %s: expected %s, got %s", key, expectedValue, val)
		}
	}
}

// TestCrashRecovery tests WAL recovery after simulated crash
func TestCrashRecovery(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.SegmentDir = fmt.Sprintf("test-segments-%d", time.Now().UnixNano())
	cfg.WALPath = fmt.Sprintf("test-wal-%d", time.Now().UnixNano())
	cfg.MemTableSizeBytes = 10 * 1024 * 1024 // Large to prevent flush

	fs := disk.NewMemoryFileIO()
	fs.MkdirAll(cfg.SegmentDir, 0755)

	// Create first database instance
	walImpl1, _ := wal.NewWAL(cfg.WALPath, fs)
	filter1 := bloom.NewFilter(10000, 0.01)

	sstableOpts := sstable.Options{
		FileIO:        fs,
		IndexInterval: cfg.SparseIndexIntervalBytes,
	}

	compactor1 := compaction.NewLeveledCompactor(cfg, sstableOpts)
	engine1, _ := lsm.NewEngine(cfg, fs, walImpl1, compactor1, filter1)

	db1, _ := database.NewDatabase(
		database.WithConfig(cfg),
		database.WithStorageEngine(engine1),
	)

	// Write data
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	for key, value := range testData {
		db1.Put(key, []byte(value))
	}

	// Close without flushing (simulating crash)
	db1.Close()

	// Create second database instance (recovery)
	walImpl2, _ := wal.NewWAL(cfg.WALPath, fs)
	filter2 := bloom.NewFilter(10000, 0.01)
	compactor2 := compaction.NewLeveledCompactor(cfg, sstableOpts)
	engine2, _ := lsm.NewEngine(cfg, fs, walImpl2, compactor2, filter2)

	db2, _ := database.NewDatabase(
		database.WithConfig(cfg),
		database.WithStorageEngine(engine2),
	)
	defer db2.Close()

	// Verify all data recovered
	for key, expectedValue := range testData {
		val, err := db2.Get(key)
		if err != nil {
			t.Errorf("failed to get key %s after recovery: %v", key, err)
			continue
		}

		if string(val) != expectedValue {
			t.Errorf("key %s: expected %s, got %s", key, expectedValue, val)
		}
	}
}

// TestConcurrentOperations tests concurrent read/write operations
func TestConcurrentOperations(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.SegmentDir = fmt.Sprintf("test-segments-%d", time.Now().UnixNano())
	cfg.WALPath = fmt.Sprintf("test-wal-%d", time.Now().UnixNano())

	fs := disk.NewMemoryFileIO()
	fs.MkdirAll(cfg.SegmentDir, 0755)

	walImpl, _ := wal.NewWAL(cfg.WALPath, fs)
	filter := bloom.NewFilter(10000, 0.01)

	sstableOpts := sstable.Options{
		FileIO:        fs,
		IndexInterval: cfg.SparseIndexIntervalBytes,
	}

	compactor := compaction.NewLeveledCompactor(cfg, sstableOpts)
	engine, _ := lsm.NewEngine(cfg, fs, walImpl, compactor, filter)

	db, _ := database.NewDatabase(
		database.WithConfig(cfg),
		database.WithStorageEngine(engine),
	)
	defer db.Close()

	// Pre-populate with some data
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("initial_%d", i)
		value := fmt.Sprintf("value_%d", i)
		db.Put(key, []byte(value))
	}

	// Run concurrent operations
	errChan := make(chan error, 100)
	done := make(chan bool)

	// Writers
	for i := 0; i < 5; i++ {
		go func(id int) {
			for j := 0; j < 20; j++ {
				key := fmt.Sprintf("writer_%d_key_%d", id, j)
				value := fmt.Sprintf("writer_%d_value_%d", id, j)
				if err := db.Put(key, []byte(value)); err != nil {
					errChan <- err
					return
				}
			}
			done <- true
		}(i)
	}

	// Readers
	for i := 0; i < 5; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				key := fmt.Sprintf("initial_%d", j%100)
				db.Get(key) // Don't care about result, just testing concurrency
			}
			done <- true
		}()
	}

	// Wait for completion
	for i := 0; i < 10; i++ {
		select {
		case err := <-errChan:
			t.Fatalf("concurrent operation failed: %v", err)
		case <-done:
			// Good
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for concurrent operations")
		}
	}

	// Verify database is still functional
	val, err := db.Get("initial_50")
	if err != nil {
		t.Errorf("failed to get after concurrent operations: %v", err)
	}
	if string(val) != "value_50" {
		t.Errorf("expected value_50, got %s", val)
	}
}

// TestDeleteAndCompaction tests deletion and compaction
func TestDeleteAndCompaction(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.SegmentDir = fmt.Sprintf("test-segments-%d", time.Now().UnixNano())
	cfg.WALPath = fmt.Sprintf("test-wal-%d", time.Now().UnixNano())
	cfg.MemTableSizeBytes = 512
	cfg.MaxTablesBeforeCompaction = 2

	fs := disk.NewMemoryFileIO()
	fs.MkdirAll(cfg.SegmentDir, 0755)

	walImpl, _ := wal.NewWAL(cfg.WALPath, fs)
	filter := bloom.NewFilter(10000, 0.01)

	sstableOpts := sstable.Options{
		FileIO:        fs,
		IndexInterval: cfg.SparseIndexIntervalBytes,
	}

	compactor := compaction.NewLeveledCompactor(cfg, sstableOpts)
	engine, _ := lsm.NewEngine(cfg, fs, walImpl, compactor, filter)

	db, _ := database.NewDatabase(
		database.WithConfig(cfg),
		database.WithStorageEngine(engine),
	)
	defer db.Close()

	// Write, delete, write pattern
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d_with_extra_data", i)
		db.Put(key, []byte(value))
	}

	// Delete half
	for i := 0; i < 25; i++ {
		key := fmt.Sprintf("key%d", i)
		db.Delete(key)
	}

	// Write more to trigger compaction
	for i := 50; i < 100; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d_with_extra_data", i)
		db.Put(key, []byte(value))
	}

	// Give time for compaction
	time.Sleep(200 * time.Millisecond)

	// Verify deleted keys are gone
	for i := 0; i < 25; i++ {
		key := fmt.Sprintf("key%d", i)
		_, err := db.Get(key)
		if err == nil {
			t.Errorf("expected key %s to be deleted", key)
		}
	}

	// Verify remaining keys exist
	for i := 25; i < 100; i++ {
		key := fmt.Sprintf("key%d", i)
		_, err := db.Get(key)
		if err != nil {
			t.Errorf("expected key %s to exist: %v", key, err)
		}
	}
}

// TestLargeDataset tests handling of large datasets
func TestLargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large dataset test in short mode")
	}

	cfg := config.DefaultConfig()
	cfg.SegmentDir = fmt.Sprintf("test-segments-%d", time.Now().UnixNano())
	cfg.WALPath = fmt.Sprintf("test-wal-%d", time.Now().UnixNano())

	fs := disk.NewMemoryFileIO()
	fs.MkdirAll(cfg.SegmentDir, 0755)

	walImpl, _ := wal.NewWAL(cfg.WALPath, fs)
	filter := bloom.NewFilter(100000, 0.01)

	sstableOpts := sstable.Options{
		FileIO:        fs,
		IndexInterval: cfg.SparseIndexIntervalBytes,
	}

	compactor := compaction.NewLeveledCompactor(cfg, sstableOpts)
	engine, _ := lsm.NewEngine(cfg, fs, walImpl, compactor, filter)

	db, _ := database.NewDatabase(
		database.WithConfig(cfg),
		database.WithStorageEngine(engine),
	)
	defer db.Close()

	// Write 10,000 keys
	numKeys := 10000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%08d", i)
		value := fmt.Sprintf("value%08d", i)
		if err := db.Put(key, []byte(value)); err != nil {
			t.Fatalf("failed to put key %d: %v", i, err)
		}
	}

	// Give time for background operations
	time.Sleep(500 * time.Millisecond)

	// Sample verification
	samples := []int{0, 1000, 5000, 9999}
	for _, i := range samples {
		key := fmt.Sprintf("key%08d", i)
		expectedValue := fmt.Sprintf("value%08d", i)

		val, err := db.Get(key)
		if err != nil {
			t.Errorf("failed to get key %s: %v", key, err)
			continue
		}

		if string(val) != expectedValue {
			t.Errorf("key %s: expected %s, got %s", key, expectedValue, val)
		}
	}
}
