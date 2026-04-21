package benchmark

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
	"github.com/dummydb/internal/testutil"
	"github.com/dummydb/internal/wal"
)

func setupBenchmarkDB(b *testing.B) (*database.DB, func()) {
	b.Helper()

	cfg := config.DefaultConfig()
	cfg.SegmentDir = fmt.Sprintf("bench-segments-%d", time.Now().UnixNano())
	cfg.WALPath = fmt.Sprintf("bench-wal-%d", time.Now().UnixNano())

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

	cleanup := func() {
		db.Close()
	}

	return db, cleanup
}

func BenchmarkPut(b *testing.B) {
	db, cleanup := setupBenchmarkDB(b)
	defer cleanup()

	data := testutil.GenerateBenchmarkData(b.N, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := db.Put(data.Keys[i], data.Values[i])
		if err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	db, cleanup := setupBenchmarkDB(b)
	defer cleanup()

	// Pre-populate
	numKeys := 10000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%08d", i)
		value := fmt.Sprintf("value%08d", i)
		db.Put(key, []byte(value))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keyIdx := i % numKeys
		key := fmt.Sprintf("key%08d", keyIdx)
		_, _ = db.Get(key)
	}
}

func BenchmarkPutGet(b *testing.B) {
	db, cleanup := setupBenchmarkDB(b)
	defer cleanup()

	data := testutil.GenerateBenchmarkData(b.N, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Put
		err := db.Put(data.Keys[i], data.Values[i])
		if err != nil {
			b.Fatalf("Put failed: %v", err)
		}

		// Get
		_, err = db.Get(data.Keys[i])
		if err != nil {
			b.Fatalf("Get failed: %v", err)
		}
	}
}

func BenchmarkDelete(b *testing.B) {
	db, cleanup := setupBenchmarkDB(b)
	defer cleanup()

	// Pre-populate
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%08d", i)
		value := fmt.Sprintf("value%08d", i)
		db.Put(key, []byte(value))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%08d", i)
		err := db.Delete(key)
		if err != nil {
			b.Fatalf("Delete failed: %v", err)
		}
	}
}

func BenchmarkKeys(b *testing.B) {
	db, cleanup := setupBenchmarkDB(b)
	defer cleanup()

	// Pre-populate with 1000 keys
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%08d", i)
		value := fmt.Sprintf("value%08d", i)
		db.Put(key, []byte(value))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.Keys()
		if err != nil {
			b.Fatalf("Keys failed: %v", err)
		}
	}
}

func BenchmarkSmallValues(b *testing.B) {
	db, cleanup := setupBenchmarkDB(b)
	defer cleanup()

	data := testutil.GenerateBenchmarkData(b.N, 10) // 10 byte values

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Put(data.Keys[i], data.Values[i])
	}
}

func BenchmarkMediumValues(b *testing.B) {
	db, cleanup := setupBenchmarkDB(b)
	defer cleanup()

	data := testutil.GenerateBenchmarkData(b.N, 1024) // 1KB values

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Put(data.Keys[i], data.Values[i])
	}
}

func BenchmarkLargeValues(b *testing.B) {
	db, cleanup := setupBenchmarkDB(b)
	defer cleanup()

	// Use sequential keys to avoid sorting issues with large random data
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("large_key_%08d", i)
		value := testutil.GenerateRandomValue(10 * 1024) // 10KB values
		db.Put(key, value)
	}
}

func BenchmarkConcurrentPuts(b *testing.B) {
	db, cleanup := setupBenchmarkDB(b)
	defer cleanup()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key_%d", i)
			value := fmt.Sprintf("value_%d", i)
			db.Put(key, []byte(value))
			i++
		}
	})
}

func BenchmarkConcurrentGets(b *testing.B) {
	db, cleanup := setupBenchmarkDB(b)
	defer cleanup()

	// Pre-populate
	numKeys := 10000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%08d", i)
		value := fmt.Sprintf("value%08d", i)
		db.Put(key, []byte(value))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key%08d", i%numKeys)
			db.Get(key)
			i++
		}
	})
}
