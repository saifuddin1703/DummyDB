# Migration Guide

This guide helps you migrate from the old DummyDB architecture to the new refactored version.

## Table of Contents

- [What Changed](#what-changed)
- [Breaking Changes](#breaking-changes)
- [Backward Compatibility](#backward-compatibility)
- [Migration Steps](#migration-steps)
- [Code Examples](#code-examples)
- [Troubleshooting](#troubleshooting)

## What Changed

### Architecture Overhaul

The refactoring transformed DummyDB from a monolithic design with global singletons into a modular, testable architecture:

**Before**:
- Single `LSMTree` struct (489 lines) handling everything
- Global `LSMT` variable preventing multiple instances
- Tight coupling between components
- Race conditions in concurrent operations
- Difficult to test (shared global state)

**After**:
- Clean Architecture with separated layers
- Dependency injection throughout
- Interface-based design for testability
- Thread-safe concurrent operations
- Comprehensive test coverage

### Package Reorganization

```
Old Structure:              New Structure:
├── main.go                 ├── cmd/dummydb/main.go
├── db/index.go             ├── internal/database/
├── lsmtree/index.go        ├── internal/storage/lsm/
├── lsmtree/sstable.go      ├── internal/storage/sstable/
├── utils/constants.go      ├── internal/config/
└── client/main.go          ├── internal/wal/
                            ├── internal/compaction/
                            ├── internal/bloom/
                            ├── internal/disk/
                            ├── internal/server/
                            └── client/main.go (unchanged)
```

### Eliminated Global State

**Before**:
```go
// Old code with globals
var LSMT *LSMTree

func init() {
    LSMT = &LSMTree{...}
}

// Use global
LSMT.Put("key", "value")
```

**After**:
```go
// New code with dependency injection
cfg := config.DefaultConfig()
fileIO := disk.NewOSFileIO()
walImpl := wal.NewWAL(cfg.WALPath, fileIO)
engine := lsm.NewEngine(cfg, fileIO, walImpl, ...)
db := database.NewDatabase(
    database.WithConfig(cfg),
    database.WithStorageEngine(engine),
)

// Use instance
db.Put("key", []byte("value"))
```

## Breaking Changes

### 1. Package Imports

**Before**:
```go
import (
    "github.com/dummydb/db"
    "github.com/dummydb/lsmtree"
)
```

**After**:
```go
import (
    "github.com/dummydb/internal/database"
    "github.com/dummydb/internal/storage/lsm"
    "github.com/dummydb/internal/config"
)
```

### 2. Database Initialization

**Before**:
```go
// Global singleton
database := db.GetNewDatabase("mydb")
```

**After**:
```go
// Explicit construction with configuration
cfg := config.DefaultConfig()
cfg.DataDir = "./mydata"
cfg.MemTableSizeBytes = 1024 * 1024  // 1MB

// Build dependencies
fileIO := disk.NewOSFileIO()
walImpl := wal.NewWAL(cfg.WALPath, fileIO)
filter := bloom.NewFilter(cfg.BloomFilterExpectedItems, cfg.BloomFilterFalsePositive)

sstableOpts := sstable.Options{
    FileIO:        fileIO,
    IndexInterval: cfg.SparseIndexIntervalBytes,
}

compactor := compaction.NewLeveledCompactor(cfg, sstableOpts)
engine := lsm.NewEngine(cfg, fileIO, walImpl, compactor, filter)

db := database.NewDatabase(
    database.WithConfig(cfg),
    database.WithStorageEngine(engine),
)
defer db.Close()
```

### 3. API Signatures

**Before**:
```go
err := db.Put("key", []byte("value"))
val, err := db.Get("key")
keys, err := db.Keys()
err = db.Delete("key")
```

**After**:
```go
// Same API signatures! No changes needed for basic operations
err := db.Put("key", []byte("value"))
val, err := db.Get("key")
keys, err := db.Keys()
err = db.Delete("key")
```

### 4. Configuration

**Before**:
```go
// Hardcoded constants
utils.TABLE_SIZE = 512 * 1024
utils.MAX_TABLE_COUNT = 4
```

**After**:
```go
// Centralized configuration
cfg := config.DefaultConfig()
cfg.MemTableSizeBytes = 512 * 1024
cfg.MaxTablesBeforeCompaction = 4

// Or via environment variables
export DUMMYDB_MEMTABLE_SIZE_BYTES=524288
export DUMMYDB_MAX_TABLES_BEFORE_COMPACTION=4
```

### 5. Error Handling

**Before**:
```go
// Panic on errors
log.Fatal("Error:", err)
```

**After**:
```go
// Return errors for caller to handle
if err != nil {
    return fmt.Errorf("operation failed: %w", err)
}
```

## Backward Compatibility

### What's Compatible

✅ **On-Disk Format**: SSTable and WAL formats unchanged
- Existing segment files can be read
- No data migration needed
- `key:value;` format preserved

✅ **Client Protocol**: Commands unchanged
- SET, GET, DEL, KEYS work identically
- Client binary unchanged
- Existing clients continue working

✅ **Data Directory**: Can reuse existing data
```sh
# Old server creates:
./segments/sstable-1234567890.db
./dummydb-wal

# New server reads same files:
./segments/sstable-1234567890.db
./dummydb-wal
```

### What's NOT Compatible

❌ **Import Paths**: Packages moved to `internal/`
- Must update imports in your code
- Global variables removed

❌ **API Surface**: Direct LSMTree access removed
- Use Database interface instead
- No more `lsmtree.LSMT` global

❌ **Constructor Signatures**: Require configuration
- Can't just `new(Database)`
- Must build dependency graph

## Migration Steps

### For Server Operators

**Step 1: Backup Data**
```sh
# Backup existing data
cp -r segments segments.backup
cp dummydb-wal dummydb-wal.backup
```

**Step 2: Stop Old Server**
```sh
# Stop the old server gracefully
pkill -SIGTERM server
```

**Step 3: Update Code**
```sh
# Pull latest changes
git pull origin main

# Rebuild
make build
```

**Step 4: Start New Server**
```sh
# Server will automatically load existing data
make run_server
```

**Step 5: Verify**
```sh
# Connect with client
make run_client

# Try operations
> GET existing_key
existing_value

> SET new_key new_value
OK
```

### For Application Developers

**Step 1: Update Dependencies**
```sh
go get github.com/dummydb@latest
go mod tidy
```

**Step 2: Update Imports**
```go
// Old
import "github.com/dummydb/db"

// New
import (
    "github.com/dummydb/internal/database"
    "github.com/dummydb/internal/config"
)
```

**Step 3: Update Initialization**

See [Code Examples](#code-examples) below for detailed patterns.

**Step 4: Test**
```sh
# Run your tests
go test ./...

# With race detector
go test -race ./...
```

### For Library Users

If you were embedding DummyDB in your application:

**Before**:
```go
package main

import "github.com/dummydb/db"

func main() {
    database := db.GetNewDatabase("myapp")
    database.Put("key", []byte("value"))
}
```

**After**:
```go
package main

import (
    "log"

    "github.com/dummydb/internal/database"
    "github.com/dummydb/internal/config"
    "github.com/dummydb/internal/disk"
    "github.com/dummydb/internal/wal"
    "github.com/dummydb/internal/storage/lsm"
    "github.com/dummydb/internal/compaction"
    "github.com/dummydb/internal/bloom"
    "github.com/dummydb/internal/storage/sstable"
)

func main() {
    // Configuration
    cfg := config.DefaultConfig()
    cfg.DataDir = "./myapp-data"
    cfg.SegmentDir = "./myapp-data/segments"
    cfg.WALPath = "./myapp-data/wal"

    // Build dependencies
    fileIO := disk.NewOSFileIO()
    if err := fileIO.MkdirAll(cfg.SegmentDir, 0755); err != nil {
        log.Fatal(err)
    }

    walImpl, err := wal.NewWAL(cfg.WALPath, fileIO)
    if err != nil {
        log.Fatal(err)
    }

    filter := bloom.NewFilter(
        cfg.BloomFilterExpectedItems,
        cfg.BloomFilterFalsePositive,
    )

    sstableOpts := sstable.Options{
        FileIO:        fileIO,
        IndexInterval: cfg.SparseIndexIntervalBytes,
    }

    compactor := compaction.NewLeveledCompactor(cfg, sstableOpts)

    engine, err := lsm.NewEngine(cfg, fileIO, walImpl, compactor, filter)
    if err != nil {
        log.Fatal(err)
    }

    db, err := database.NewDatabase(
        database.WithConfig(cfg),
        database.WithStorageEngine(engine),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Use database
    if err := db.Put("key", []byte("value")); err != nil {
        log.Fatal(err)
    }

    val, err := db.Get("key")
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Got value: %s", val)
}
```

## Code Examples

### Basic Usage

```go
package main

import (
    "log"

    "github.com/dummydb/internal/database"
    "github.com/dummydb/internal/config"
    "github.com/dummydb/internal/disk"
    "github.com/dummydb/internal/wal"
    "github.com/dummydb/internal/storage/lsm"
    "github.com/dummydb/internal/compaction"
    "github.com/dummydb/internal/bloom"
    "github.com/dummydb/internal/storage/sstable"
)

func main() {
    db := setupDatabase()
    defer db.Close()

    // Write
    if err := db.Put("user:1", []byte("alice")); err != nil {
        log.Fatal(err)
    }

    // Read
    val, err := db.Get("user:1")
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Value: %s", val)

    // Delete
    if err := db.Delete("user:1"); err != nil {
        log.Fatal(err)
    }

    // List keys
    keys, err := db.Keys()
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Keys: %v", keys)
}

func setupDatabase() *database.DB {
    cfg := config.DefaultConfig()
    fileIO := disk.NewOSFileIO()
    fileIO.MkdirAll(cfg.SegmentDir, 0755)

    walImpl, _ := wal.NewWAL(cfg.WALPath, fileIO)
    filter := bloom.NewFilter(cfg.BloomFilterExpectedItems, cfg.BloomFilterFalsePositive)

    sstableOpts := sstable.Options{
        FileIO:        fileIO,
        IndexInterval: cfg.SparseIndexIntervalBytes,
    }

    compactor := compaction.NewLeveledCompactor(cfg, sstableOpts)
    engine, _ := lsm.NewEngine(cfg, fileIO, walImpl, compactor, filter)

    db, _ := database.NewDatabase(
        database.WithConfig(cfg),
        database.WithStorageEngine(engine),
    )

    return db
}
```

### Custom Configuration

```go
func setupCustomDatabase() *database.DB {
    // Custom configuration
    cfg := config.Config{
        Host:                         "localhost",
        Port:                         4000,
        DataDir:                      "./custom-data",
        SegmentDir:                   "./custom-data/segments",
        WALPath:                      "./custom-data/wal",
        MemTableSizeBytes:            1024 * 1024, // 1MB memtable
        MaxTablesBeforeCompaction:    8,           // More tables before compact
        BloomFilterExpectedItems:     10000000,    // 10M items
        BloomFilterFalsePositive:     0.001,       // 0.1% FP rate
        SparseIndexIntervalBytes:     50 * 1024,   // Index every 50KB
    }

    if err := cfg.Validate(); err != nil {
        log.Fatal(err)
    }

    // Build with custom config
    fileIO := disk.NewOSFileIO()
    fileIO.MkdirAll(cfg.SegmentDir, 0755)

    walImpl, _ := wal.NewWAL(cfg.WALPath, fileIO)
    filter := bloom.NewFilter(cfg.BloomFilterExpectedItems, cfg.BloomFilterFalsePositive)

    sstableOpts := sstable.Options{
        FileIO:        fileIO,
        IndexInterval: cfg.SparseIndexIntervalBytes,
    }

    compactor := compaction.NewLeveledCompactor(&cfg, sstableOpts)
    engine, _ := lsm.NewEngine(&cfg, fileIO, walImpl, compactor, filter)

    db, _ := database.NewDatabase(
        database.WithConfig(&cfg),
        database.WithStorageEngine(engine),
    )

    return db
}
```

### Testing with Mocks

```go
package myapp

import (
    "testing"

    "github.com/dummydb/internal/database"
    "github.com/dummydb/internal/testutil"
)

func TestMyApp(t *testing.T) {
    // Use mock storage engine for fast tests
    mockEngine := testutil.NewMockEngine()

    db, _ := database.NewDatabase(
        database.WithStorageEngine(mockEngine),
    )
    defer db.Close()

    // Test your application logic
    err := db.Put("test", []byte("value"))
    if err != nil {
        t.Fatal(err)
    }

    // Verify mock was called
    if mockEngine.PutCalls != 1 {
        t.Errorf("Expected 1 Put call, got %d", mockEngine.PutCalls)
    }
}
```

### In-Memory Database for Tests

```go
package myapp

import (
    "testing"

    "github.com/dummydb/internal/database"
    "github.com/dummydb/internal/config"
    "github.com/dummydb/internal/disk"
    "github.com/dummydb/internal/wal"
    "github.com/dummydb/internal/storage/lsm"
    "github.com/dummydb/internal/compaction"
    "github.com/dummydb/internal/bloom"
    "github.com/dummydb/internal/storage/sstable"
)

func TestWithInMemoryDB(t *testing.T) {
    cfg := config.DefaultConfig()
    cfg.SegmentDir = "test-segments"
    cfg.WALPath = "test-wal"

    // Use in-memory file system for fast tests
    fileIO := disk.NewMemoryFileIO()
    fileIO.MkdirAll(cfg.SegmentDir, 0755)

    walImpl, _ := wal.NewWAL(cfg.WALPath, fileIO)
    filter := bloom.NewFilter(10000, 0.01)

    sstableOpts := sstable.Options{
        FileIO:        fileIO,
        IndexInterval: cfg.SparseIndexIntervalBytes,
    }

    compactor := compaction.NewLeveledCompactor(cfg, sstableOpts)
    engine, _ := lsm.NewEngine(cfg, fileIO, walImpl, compactor, filter)

    db, _ := database.NewDatabase(
        database.WithConfig(cfg),
        database.WithStorageEngine(engine),
    )
    defer db.Close()

    // Run tests - no disk I/O!
    db.Put("key", []byte("value"))
    val, _ := db.Get("key")
    if string(val) != "value" {
        t.Error("Unexpected value")
    }
}
```

## Troubleshooting

### Issue: Import errors after upgrade

**Error**:
```
cannot find package "github.com/dummydb/db"
```

**Solution**:
Update imports to use `internal/` packages:
```go
import "github.com/dummydb/internal/database"
```

### Issue: Cannot access LSMT global variable

**Error**:
```
undefined: lsmtree.LSMT
```

**Solution**:
Global variables removed. Create database instance:
```go
db := setupDatabase()  // See examples above
```

### Issue: Data not found after migration

**Symptoms**:
- GET returns "key not found" for existing keys
- Empty database after startup

**Solution**:
Verify data directory configuration:
```go
cfg := config.DefaultConfig()
cfg.SegmentDir = "./segments"  // Must match old location
cfg.WALPath = "./dummydb-wal"  // Must match old location
```

### Issue: Race conditions detected

**Error**:
```
WARNING: DATA RACE
```

**Solution**:
The new version fixes all known race conditions. If you see races:
1. Ensure you're using the latest version
2. Report the issue with reproduction steps
3. Check you're not sharing database instances unsafely

### Issue: Performance degradation

**Symptoms**:
- Slower writes than before
- Higher memory usage

**Solution**:
Tune configuration for your workload:
```go
cfg := config.DefaultConfig()

// For write-heavy workloads
cfg.MemTableSizeBytes = 2 * 1024 * 1024  // Larger memtable
cfg.MaxTablesBeforeCompaction = 8         // Less frequent compaction

// For read-heavy workloads
cfg.MemTableSizeBytes = 256 * 1024        // Smaller memtable
cfg.MaxTablesBeforeCompaction = 3         // More frequent compaction
cfg.BloomFilterExpectedItems = 10000000   // Larger bloom filter
```

### Issue: Old code still in repository

**Question**:
Why do `lsmtree/` and `db/` directories still exist?

**Answer**:
Kept for reference during migration period. Will be removed in future version.
- **DO NOT** use old code
- **DO** migrate to new `internal/` packages
- Old code is deprecated and unsupported

### Issue: Tests fail with "too many open files"

**Solution**:
Close database instances in tests:
```go
func TestSomething(t *testing.T) {
    db := setupDatabase()
    defer db.Close()  // Important!

    // Your test code
}
```

### Issue: Compaction not running

**Symptoms**:
- Growing number of SSTable files
- Read performance degrading

**Solution**:
Check configuration:
```go
cfg.MaxTablesBeforeCompaction = 4  // Lower = more frequent compaction
```

Verify compactor is started:
```go
engine, err := lsm.NewEngine(cfg, fileIO, walImpl, compactor, filter)
// Compactor starts automatically in NewEngine
```

## Getting Help

If you encounter issues during migration:

1. **Check documentation**: README.md, ARCHITECTURE.md, CONTRIBUTING.md
2. **Search issues**: GitHub issues for similar problems
3. **Run tests**: `make test-race` to verify correctness
4. **Open issue**: Provide reproduction steps and error messages
5. **Ask questions**: Open a discussion on GitHub

## Summary

The refactoring provides:
- ✅ Better performance (fixed race conditions)
- ✅ Easier testing (mocks and in-memory mode)
- ✅ Cleaner code (SOLID principles)
- ✅ More features (configuration, monitoring hooks)
- ✅ **Same on-disk format** (no data migration!)

Migration is straightforward:
1. Update imports
2. Replace global usage with instances
3. Add configuration
4. Test thoroughly

The new architecture is production-ready with comprehensive tests and documentation.
