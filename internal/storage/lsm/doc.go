// Package lsm implements a Log-Structured Merge (LSM) tree storage engine.
//
// The LSM engine provides high write throughput by buffering writes in an
// in-memory structure (memtable) and periodically flushing to disk as
// immutable sorted files (SSTables). Background compaction merges SSTables
// to maintain read performance and reclaim space.
//
// Architecture:
//
//	┌─────────────┐
//	│   Memtable  │ ← Active writes (in-memory red-black tree)
//	└─────────────┘
//	      ↓ Flush when full
//	┌─────────────┐
//	│  SSTable 1  │ ← Immutable on-disk file
//	└─────────────┘
//	┌─────────────┐
//	│  SSTable 2  │
//	└─────────────┘
//	      ↓ Compaction
//	┌─────────────┐
//	│   Merged    │ ← Deduplicated, sorted
//	└─────────────┘
//
// Write path:
//  1. Append to write-ahead log (WAL) for durability
//  2. Insert into memtable (O(log n))
//  3. When memtable reaches threshold, flush to SSTable
//  4. Trigger compaction if too many SSTables
//
// Read path:
//  1. Check memtable (most recent data)
//  2. For each SSTable (newest to oldest):
//     a. Check bloom filter (skip if definitely not present)
//     b. Binary search in SSTable using sparse index
//  3. Return first match or not found
//
// The engine is thread-safe and supports concurrent reads and writes.
// It uses RWMutex for synchronization with minimal lock contention.
//
// Example usage:
//
//	cfg := config.DefaultConfig()
//	fileIO := disk.NewOSFileIO()
//	walImpl := wal.NewWAL(cfg.WALPath, fileIO)
//	filter := bloom.NewFilter(1000000, 0.01)
//	compactor := compaction.NewLeveledCompactor(cfg, sstableOpts)
//
//	engine, err := lsm.NewEngine(cfg, fileIO, walImpl, compactor, filter)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer engine.Close()
//
//	// Writes
//	engine.Put("key1", []byte("value1"))
//	engine.Put("key2", []byte("value2"))
//
//	// Reads
//	value, found := engine.Get("key1")
//
//	// Deletes
//	engine.Delete("key1")
//
package lsm
