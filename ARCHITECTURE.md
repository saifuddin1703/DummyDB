# DummyDB Architecture

This document describes the architecture and design decisions behind DummyDB.

## Table of Contents

- [Overview](#overview)
- [Architecture Principles](#architecture-principles)
- [System Layers](#system-layers)
- [Core Components](#core-components)
- [Data Flow](#data-flow)
- [Concurrency Model](#concurrency-model)
- [Storage Format](#storage-format)
- [Design Decisions](#design-decisions)

## Overview

DummyDB is a persistent key-value store built on the Log-Structured Merge (LSM) tree data structure. It provides:

- **Fast writes**: O(log n) in-memory writes with sequential disk I/O
- **Durable storage**: Write-ahead log ensures no data loss
- **Efficient reads**: Bloom filters and sparse indexing minimize disk access
- **Space efficiency**: Background compaction removes duplicates and reclaims space
- **Thread safety**: Concurrent reads and writes with minimal lock contention

## Architecture Principles

DummyDB follows Clean Architecture and SOLID principles:

### 1. Dependency Inversion

Dependencies point inward toward business logic:

```
Infrastructure → Domain → Application → Presentation
```

- **Presentation** (cmd/dummydb, client) depends on **Application**
- **Application** (database package) depends on **Domain** interfaces
- **Domain** (storage, wal, compaction) defines interfaces, doesn't depend on infrastructure
- **Infrastructure** (disk, bloom) implements domain interfaces

### 2. Interface Segregation

Small, focused interfaces rather than large ones:

```go
// Good: Focused interfaces
type Engine interface {
    Put(key string, value []byte) error
    Get(key string) ([]byte, error)
    Delete(key string) error
    Keys() []string
    Close() error
}

type Compactor interface {
    Compact(tables []sstable.SSTable) (sstable.SSTable, error)
    ShouldCompact(tableCount int) bool
    Start() error
    Stop() error
}
```

### 3. Single Responsibility

Each component has one clear purpose:

- **Memtable**: In-memory sorted storage
- **SSTable**: Immutable on-disk storage
- **WAL**: Durability logging
- **Compactor**: Merge and cleanup
- **BloomFilter**: Probabilistic membership test

### 4. Dependency Injection

No global state, all dependencies injected:

```go
// Old (Bad): Global singleton
var LSMT *LSMTree
func init() {
    LSMT = &LSMTree{...}
}

// New (Good): Dependency injection
func NewEngine(
    cfg *config.Config,
    fileIO disk.FileIO,
    wal wal.WAL,
    compactor compaction.Compactor,
    filter bloom.Filter,
) (*Engine, error)
```

## System Layers

### Layer 1: Presentation

**Location**: `cmd/dummydb/`, `client/`

**Purpose**: User interaction and protocol handling

- **Server**: TCP listener, connection management, graceful shutdown
- **Protocol**: Command parsing (SET, GET, DEL, KEYS)
- **Handler**: Execute commands against database
- **Client**: Interactive CLI for humans

**Key files**:
- `cmd/dummydb/main.go` - Entry point with dependency wiring
- `internal/server/server.go` - Server lifecycle
- `internal/server/protocol.go` - Command parsing
- `internal/server/handler.go` - Command execution

### Layer 2: Application

**Location**: `internal/database/`

**Purpose**: Orchestrate domain components to fulfill use cases

- Coordinate WAL + storage engine for durable writes
- Manage database lifecycle (init, shutdown)
- Transaction coordination (future)

**Key files**:
- `internal/database/database.go` - DB struct and methods
- `internal/database/options.go` - Functional options pattern

**Interface**:
```go
type Database interface {
    Put(key string, value []byte) error
    Get(key string) ([]byte, error)
    Delete(key string) error
    Keys() ([]string, error)
    Close() error
}
```

### Layer 3: Domain

**Location**: `internal/storage/`, `internal/wal/`, `internal/compaction/`

**Purpose**: Core business logic and storage abstractions

#### Storage Engine (`internal/storage/lsm/`)

The LSM engine manages:
- Active memtable for writes
- List of immutable SSTables for reads
- Background flush and compaction
- Crash recovery from WAL

```go
type Engine struct {
    config         *config.Config
    activeMemtable Memtable
    mu             sync.RWMutex
    tables         []sstable.SSTable  // protected by mu
    mergedTables   []sstable.SSTable  // protected by mu
    fileIO         disk.FileIO
    compactor      compaction.Compactor
    filter         bloom.Filter
    flushWG        sync.WaitGroup
    closed         atomic.Bool
}
```

#### Memtable (`internal/storage/lsm/memtable.go`)

In-memory red-black tree for active writes:

```go
type Memtable interface {
    Put(key string, value string) error
    Get(key string) (string, bool)
    Delete(key string) error
    Size() int64
    Iterator() MemtableIterator
    Clear()
    IsEmpty() bool
}
```

**Properties**:
- Thread-safe with RWMutex
- Maintains sorted order (red-black tree)
- Tracks size for flush threshold
- O(log n) operations

#### SSTable (`internal/storage/sstable/`)

Immutable sorted string table on disk:

```go
type SSTable interface {
    Get(key string) ([]byte, error)
    Has(key string) bool
    Iterator() Iterator
    Path() string
    Size() int64
    IsMerged() bool
    Close() error
    Delete() error
}
```

**Format**:
```
[key1:value1;key2:value2;...keyN:valueN;]
```

**Sparse Index**: Every 100KB of data, store:
```go
type IndexEntry struct {
    Key    string
    Offset int64
}
```

**Properties**:
- Immutable once written
- Sorted by key
- Binary search on sparse index
- Bloom filter per SSTable

#### Write-Ahead Log (`internal/wal/`)

Append-only log for durability:

```go
type WAL interface {
    Append(entry *Entry) error
    Sync() error
    Recover() ([]*Entry, error)
    Truncate() error
    Close() error
    Path() string
}
```

**Entry format**:
```
key:value;
```

**Purpose**:
- Survive crashes before memtable flush
- Replay on restart
- Truncate after successful flush

#### Compaction (`internal/compaction/`)

Merges SSTables to optimize storage:

```go
type Compactor interface {
    Compact(tables []sstable.SSTable) (sstable.SSTable, error)
    ShouldCompact(tableCount int) bool
    Start() error
    Stop() error
}
```

**Leveled strategy**:
- Trigger when table count exceeds threshold (4 default)
- Merge overlapping tables in sorted order
- Keep latest value for each key
- Remove deletion markers

**Benefits**:
- Fewer files = faster reads
- Remove old versions = less space
- Remove deletes = reclaim space

### Layer 4: Infrastructure

**Location**: `internal/disk/`, `internal/bloom/`, `internal/config/`

**Purpose**: External interfaces and utilities

#### File I/O (`internal/disk/`)

Abstraction over filesystem:

```go
type FileIO interface {
    Open(name string) (File, error)
    Create(name string) (File, error)
    Remove(name string) error
    ReadDir(dirname string) ([]os.DirEntry, error)
    ReadFile(name string) ([]byte, error)
    WriteFile(name string, data []byte, perm os.FileMode) error
    MkdirAll(path string, perm os.FileMode) error
}
```

**Implementations**:
- **OSFileIO**: Real filesystem (production)
- **MemoryFileIO**: In-memory map (tests)

#### Bloom Filter (`internal/bloom/`)

Probabilistic membership test:

```go
type Filter interface {
    Add(item []byte)
    Test(item []byte) bool
    TestAndAdd(item []byte) bool
    EstimatedFPRate() float64
    Clear()
    Count() uint
}
```

**Configuration**:
- Expected items: 1,000,000
- False positive rate: 1%

**Benefits**:
- Avoid disk reads for non-existent keys
- ~10x read performance improvement

## Data Flow

### Write Path

```
Client
  ↓ SET key value
Server (protocol.go)
  ↓ ParseCommand
Handler (handler.go)
  ↓ db.Put(key, value)
Database (database.go)
  ↓ engine.Put(key, value)
LSM Engine (lsm.go)
  ↓ (1) wal.Append(entry)           [Durability]
  ↓ (2) memtable.Put(key, value)    [Fast write]
  ↓ (3) Check size threshold
  └─→ If exceeded:
       ├─ Swap new empty memtable
       └─ go flushMemtable(old)
           ↓
           SSTable Writer
             ↓ (1) Sort entries
             ↓ (2) Write to disk
             ↓ (3) Build sparse index
             ↓ (4) Add keys to bloom filter
             └─→ Compaction check
                   ↓
                   Background compaction if needed
```

**Sequence diagram**:
```
Client → Server → Handler → Database → Engine
                                         ↓
                                        WAL (sync)
                                         ↓
                                      Memtable (in-memory)
                                         ↓
                                   [Size check]
                                         ↓
                                   Background: Flush → SSTable
                                                         ↓
                                                     Compaction
```

### Read Path

```
Client
  ↓ GET key
Server (protocol.go)
  ↓ ParseCommand
Handler (handler.go)
  ↓ db.Get(key)
Database (database.go)
  ↓ engine.Get(key)
LSM Engine (lsm.go)
  ↓ (1) Check memtable
  └─→ If found: return value
  ↓ (2) For each SSTable (newest to oldest):
      ├─ Check bloom filter
      │   └─→ If "definitely not present": skip
      ├─ Check SSTable
      │   ↓ Binary search on sparse index
      │   ↓ Scan from offset
      │   └─→ If found: return value
      └─→ Continue to next SSTable
  ↓ (3) Not found: return error
```

**Optimizations**:
- Memtable checked first (most recent data)
- Bloom filter avoids ~90% of disk reads
- Sparse index reduces search space
- Read lock allows concurrent reads

### Compaction Flow

```
Background Goroutine
  ↓ Periodic check: ShouldCompact()
  ↓ If true:
    ├─ Acquire compaction lock
    ├─ Select tables to merge
    ├─ Create merged SSTable:
    │   ↓ Multi-way merge sort
    │   ↓ Keep latest version per key
    │   ↓ Remove deletion markers
    │   └─ Write new SSTable
    ├─ Update engine table list (with write lock)
    ├─ Delete old SSTables
    └─ Release compaction lock
```

## Concurrency Model

### Lock Hierarchy

```
Engine.mu (RWMutex)
  ├─ Protects: tables, mergedTables, closed flag
  ├─ Read lock: Get operations (concurrent)
  └─ Write lock: Put (memtable swap), compaction

Memtable.mu (RWMutex)
  ├─ Protects: tree, size
  ├─ Read lock: Get (concurrent)
  └─ Write lock: Put, Delete

SSTable: Immutable (no lock needed)

MemoryFileIO.mu (Mutex)
  ├─ Protects: files map
  └─ Per-handle pos (no shared state)
```

### Thread Safety Rules

1. **Immutable data doesn't need locks**: SSTables are immutable after creation
2. **Short critical sections**: Minimize time holding locks
3. **No I/O while holding locks**: Release lock before disk operations
4. **Read-heavy optimization**: Use RWMutex, prefer read locks
5. **Atomic for simple flags**: Use `atomic.Bool` for closed flag

### Race Condition Fixes

#### Fixed: Multiple lock acquisitions in Put()

**Before (Buggy)**:
```go
l.LSMLock.Lock()
l.ActiveMemCache.Put(key, value)
l.LSMLock.Unlock()

l.LSMLock.Lock()  // RACE: Another thread can swap memtable here!
if atomic.LoadInt64(&l.MemCacheSize) > utils.TABLE_SIZE {
    // Swap memtable
}
l.LSMLock.Unlock()
```

**After (Fixed)**:
```go
e.mu.Lock()
e.activeMemtable.Put(key, value)
size := e.activeMemtable.Size()
if size > e.config.MemTableSizeBytes {
    oldMemtable := e.activeMemtable
    e.activeMemtable = e.newMemtable()
    e.mu.Unlock()
    go e.flushMemtable(oldMemtable)
    return nil
}
e.mu.Unlock()
```

#### Fixed: Unsafe map access in MemoryFileIO

**Before (Buggy)**:
```go
type memFile struct {
    pos int64  // Shared across all handles!
}
```

**After (Fixed)**:
```go
type memFile struct {
    // No pos here - immutable
}

type memFileHandle struct {
    pos int64  // Per-handle position
    mu  sync.Mutex
}
```

## Storage Format

### SSTable Format

```
File: segments/sstable-{timestamp}.db

Content: key1:value1;key2:value2;key3:value3;

Sparse Index (in-memory):
[
    {Key: "key1", Offset: 0},
    {Key: "key100", Offset: 10485760},  // Every ~100KB
    {Key: "key200", Offset: 20971520},
]
```

### WAL Format

```
File: dummydb-wal

Content: key1:value1;key2:value2;delete:key3:;

Entry types:
- Put: key:value;
- Delete: key:{DELETE};
```

### Directory Structure

```
DummyDB/
├── segments/
│   ├── sstable-1234567890.db         # Regular SSTable
│   ├── sstable-1234567891.db
│   └── merged-1234567892.db          # Merged SSTable
├── dummydb-wal                        # Write-ahead log
└── data/                              # Other data files
```

## Design Decisions

### Why LSM Tree?

**Pros**:
- Fast writes (O(log n) in memory, no random disk I/O)
- Good for write-heavy workloads
- Sequential disk writes (SSD/HDD friendly)
- Natural support for versioning

**Cons**:
- Reads slower than B-tree (check multiple levels)
- Write amplification (compaction rewrites data)
- Background compaction overhead

**Trade-off**: Optimized for write-heavy workloads common in caches, logs, and event stores.

### Why Red-Black Tree for Memtable?

**Alternatives considered**:
- Hash map: No sort order, can't iterate
- Skip list: Good, but less common in Go
- AVL tree: Similar, but RB tree has better insert performance

**Decision**: Red-black tree provides:
- O(log n) insert, search, delete
- Sorted iteration (needed for SSTable flush)
- Mature Go library (emirpasic/gods)

### Why Leveled Compaction?

**Alternatives**:
- Size-tiered: Simple but creates large files
- Time-window: Good for time-series, not general KV
- Universal: Between leveled and size-tiered

**Decision**: Leveled compaction provides:
- Predictable read performance (fewer levels)
- Better space amplification (less duplication)
- Standard in industry (LevelDB, RocksDB)

### Why Text-Based Protocol?

**Alternatives**:
- Binary: More efficient, harder to debug
- JSON: Verbose, slower parsing
- gRPC: Requires proto definitions, complexity

**Decision**: Text protocol is:
- Human-readable (debugging, manual testing)
- Simple to implement
- Adequate performance for demonstration
- Future: Can add binary/gRPC without changing storage

### Why Sparse Index?

**Alternatives**:
- Dense index: One entry per key (too much memory)
- No index: Linear scan (too slow)

**Decision**: Sparse index (1 entry per 100KB):
- ~1% memory overhead
- Binary search reduces scan to ~100KB max
- Good balance of space and speed

### Why Functional Options?

**Alternatives**:
- Builder pattern: More verbose
- Config struct: Can't enforce invariants
- Multiple constructors: Explodes combinations

**Decision**: Functional options provide:
- Clean API: `NewDatabase(WithConfig(...), WithEngine(...))`
- Extensibility: Easy to add new options
- Defaults: Can have sensible defaults
- Type safety: Compile-time checks

## Testing Strategy

### Unit Tests

- **Scope**: Single component in isolation
- **Dependencies**: Mocked via interfaces
- **File I/O**: MemoryFileIO for speed
- **Concurrency**: Run with `-race` detector
- **Parallel**: All tests use `t.Parallel()`

Example:
```go
func TestEngine_Put(t *testing.T) {
    t.Parallel()

    cfg := config.DefaultConfig()
    fileIO := disk.NewMemoryFileIO()
    mockWAL := testutil.NewMockWAL("test.wal")
    mockCompactor := testutil.NewMockCompactor()
    mockFilter := testutil.NewMockFilter()

    engine := lsm.NewEngine(cfg, fileIO, mockWAL, mockCompactor, mockFilter)

    err := engine.Put("key", []byte("value"))
    assert.NoError(t, err)
}
```

### Integration Tests

- **Scope**: Multiple components together
- **Dependencies**: Real implementations
- **File I/O**: MemoryFileIO for speed and isolation
- **Scenarios**: Full lifecycle, crash recovery, concurrent ops

Example:
```go
func TestFullCycle(t *testing.T) {
    // Setup full stack with memory file I/O
    // Write data → trigger flush → trigger compaction → verify reads
    // Tests entire write→compact→read cycle
}
```

### Benchmarks

- **Scope**: Performance measurement
- **Baseline**: Established for comparison
- **Profiling**: CPU and memory profiles
- **Realistic**: Use typical workload patterns

## Future Enhancements

### Planned Features

1. **Range Queries**: Iterate over key ranges
2. **Transactions**: ACID guarantees with MVCC
3. **Snapshots**: Point-in-time consistent reads
4. **Compression**: Reduce SSTable size
5. **Replication**: Multi-node consistency
6. **HTTP/gRPC**: Alternative protocols
7. **TTL**: Auto-expire old keys
8. **Metrics**: Prometheus integration

### Performance Improvements

1. **Block cache**: Cache frequently accessed SSTable blocks
2. **Bloom filter per level**: More granular filtering
3. **Parallel compaction**: Multiple background threads
4. **Memtable size tuning**: Dynamic based on workload
5. **SSTable prefetching**: Async reads

### Operational Features

1. **Live backup**: Hot backup without downtime
2. **Compaction tuning**: Runtime configuration
3. **Monitoring**: Detailed metrics and logging
4. **Admin API**: Trigger manual compaction, view stats
5. **Multi-database**: Multiple DBs per process

## References

- [LevelDB Design](https://github.com/google/leveldb/blob/master/doc/impl.md)
- [RocksDB Architecture](https://github.com/facebook/rocksdb/wiki/RocksDB-Basics)
- [The Log-Structured Merge-Tree (LSM-Tree)](https://www.cs.umb.edu/~poneil/lsmtree.pdf)
- [Clean Architecture](https://blog.cleancoder.com/uncle-bob/2012/08/13/the-clean-architecture.html)
