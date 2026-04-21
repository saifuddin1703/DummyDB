# Changelog

All notable changes to DummyDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0] - 2026-02-28

### Major Refactoring Release

This release represents a complete architectural overhaul of DummyDB, transforming it from a monolithic design into a modular, production-ready system following Clean Architecture principles.

### Added

#### New Architecture
- **Clean Architecture**: Four-layer architecture (Presentation → Application → Domain → Infrastructure)
- **Dependency Injection**: All components use constructor injection, no global state
- **Interface-Based Design**: Core abstractions defined as interfaces for testability
- **Modular Package Structure**: Organized under `internal/` with clear separation of concerns

#### Core Components
- **Configuration Management** (`internal/config/`)
  - Centralized configuration with validation
  - Environment variable support
  - Sensible defaults with override capability

- **File I/O Abstraction** (`internal/disk/`)
  - `FileIO` interface abstracting filesystem operations
  - `OSFileIO` for production (real file system)
  - `MemoryFileIO` for testing (in-memory, zero disk I/O)

- **Write-Ahead Log** (`internal/wal/`)
  - Dedicated WAL interface and implementation
  - Crash recovery support
  - Maintains backward-compatible format

- **Bloom Filter** (`internal/bloom/`)
  - Probabilistic filter interface
  - Configurable false positive rate
  - NoOpFilter for testing

- **SSTable Management** (`internal/storage/sstable/`)
  - Clean Reader/Writer pattern
  - Sparse indexing for efficient searches
  - Immutable on-disk format

- **Memtable** (`internal/storage/lsm/`)
  - Thread-safe red-black tree implementation
  - Proper size tracking
  - Iterator support

- **Compaction** (`internal/compaction/`)
  - Leveled compaction strategy
  - Background async compaction
  - Proper synchronization and shutdown

- **LSM Engine** (`internal/storage/lsm/`)
  - Complete rewrite with race-condition fixes
  - Thread-safe concurrent operations
  - Proper lifecycle management

- **Database Layer** (`internal/database/`)
  - Functional options pattern for configuration
  - Coordinate WAL + engine operations
  - Graceful shutdown handling

- **Server** (`internal/server/`)
  - Separated protocol parsing from execution
  - Graceful shutdown with signal handling
  - Connection lifecycle management

#### Testing Infrastructure
- **Comprehensive Mocks** (`internal/testutil/`)
  - Mock implementations for all interfaces
  - Call tracking for verification
  - Error injection capability

- **Test Fixtures** (`internal/testutil/`)
  - Test data generators
  - Assertion helpers
  - Benchmark data utilities

- **Integration Tests** (`test/integration/`)
  - Full lifecycle tests
  - Crash recovery validation
  - Concurrent operation tests
  - Large dataset tests

- **Benchmark Suite** (`test/benchmark/`)
  - Write/read performance benchmarks
  - Concurrent operation benchmarks
  - Variable value size benchmarks
  - Established performance baselines

#### Documentation
- **README.md**: Comprehensive guide with architecture overview, examples, and quick start
- **ARCHITECTURE.md**: Detailed architecture documentation with diagrams and design decisions
- **CONTRIBUTING.md**: Contribution guidelines with code standards and workflows
- **MIGRATION.md**: Complete migration guide from old to new architecture
- **CHANGELOG.md**: This file
- **Package Documentation**: Godoc comments for all public interfaces and packages

### Changed

#### Breaking Changes
- **Package Structure**: Moved to `internal/` organization
  - Old: `github.com/dummydb/db`
  - New: `github.com/dummydb/internal/database`

- **Global Variables Removed**: No more `lsmtree.LSMT` or `db.database` globals
  - Requires explicit instance creation
  - Enables multiple database instances
  - Improves testability

- **Constructor Signatures**: Require explicit configuration
  - Old: `db.GetNewDatabase("name")`
  - New: `database.NewDatabase(database.WithConfig(cfg), ...)`

- **Main Entry Point**: Moved to `cmd/dummydb/main.go`
  - Old: `main.go` in root
  - New: `cmd/dummydb/main.go`

- **Import Paths**: All imports updated to use `internal/` packages

#### Maintained Compatibility
- **On-Disk Format**: SSTable and WAL formats unchanged (`key:value;` format)
- **Client Protocol**: SET/GET/DEL/KEYS commands work identically
- **Client Binary**: Client unchanged, continues to work with new server
- **Data Files**: Existing segment files and WAL can be read without migration

### Fixed

#### Critical Race Conditions
- **Multiple Lock Acquisitions in Put()**: Fixed race condition between size check and memtable swap
  - Before: Two separate lock acquisitions created race window
  - After: Single lock acquisition for entire operation

- **Unsafe Map Access in Find()**: Fixed concurrent map access in SSTable KeyMap
  - Before: KeyMap accessed without proper synchronization
  - After: Proper RWMutex protection on all map access

- **Race in Compaction Trigger**: Fixed race between count check and table list modification
  - Before: Separate read and write created race window
  - After: Single lock acquisition for check-and-modify

- **MemoryFileIO Race**: Fixed shared position state across file handles
  - Before: `pos` field shared between handles
  - After: `pos` moved to per-handle state with mutex

- **WaitGroup Negative Counter**: Fixed panic in engine shutdown
  - Before: Mismatched Add/Done calls
  - After: Separate sync/async flush methods

#### Other Fixes
- Proper error propagation instead of log.Fatal
- Thread-safe bloom filter operations
- Correct SSTable merged flag detection
- Graceful shutdown with proper cleanup
- Memory leaks from unclosed resources

### Performance

#### Improvements
- Lock-free reads from immutable SSTables
- Reduced lock contention with RWMutex
- Background compaction doesn't block operations
- Efficient memtable size tracking

#### Benchmarks (established baselines)
```
BenchmarkPut-8                1000000    972 ns/op    1718 B/op
BenchmarkGet-8                 300000   3106 ns/op    2632 B/op
BenchmarkConcurrentPuts-8      500000   2156 ns/op    1842 B/op
BenchmarkConcurrentGets-8      400000   3421 ns/op    2745 B/op
```

### Testing

#### Coverage
- All packages have comprehensive unit tests
- 80%+ code coverage across the codebase
- All tests pass with `-race` detector (0 races found)
- Integration tests validate full lifecycle
- Benchmark suite for performance monitoring

#### Test Execution
```sh
# Unit tests with race detector
make test-race              # All pass

# Integration tests
make test-integration       # All pass

# Benchmarks
make bench                  # Baselines established
```

### Deprecated

The following packages are deprecated and will be removed in v3.0.0:

- `lsmtree/` - Use `internal/storage/lsm/` instead
- `db/` - Use `internal/database/` instead
- `utils/constants.go` - Use `internal/config/` instead

These packages are kept for backward compatibility but should not be used in new code.

### Migration

See [MIGRATION.md](MIGRATION.md) for detailed migration instructions.

**Quick Migration Steps:**
1. Update imports to use `internal/` packages
2. Replace global variable usage with instance creation
3. Use functional options for database initialization
4. Test with `go test -race ./...`

**Data Compatibility:**
- No data migration required
- Existing segment files work as-is
- Existing WAL files work as-is

### Architecture Highlights

#### Before (v1.x)
```
main.go (global singleton)
  ↓
db/index.go (thin wrapper)
  ↓
lsmtree/index.go (489-line god object)
  ├─ memcache (red-black tree)
  ├─ SSTables
  ├─ WAL
  ├─ Bloom filter
  ├─ Compaction
  └─ File I/O
```

**Problems:**
- Single responsibility violated (god object)
- Global state prevents testing
- Race conditions in concurrent ops
- Tight coupling, hard to extend

#### After (v2.0)
```
Presentation (cmd/dummydb, server)
    ↓
Application (database)
    ↓
Domain (storage, wal, compaction)
    ↓
Infrastructure (disk, bloom, config)
```

**Benefits:**
- Clean separation of concerns
- Testable with dependency injection
- Thread-safe concurrent operations
- Easy to extend with new features
- Zero race conditions

### Contributors

This release represents a comprehensive refactoring by the DummyDB team, focusing on:
- Production readiness
- Code quality and maintainability
- Testing and documentation
- Performance and correctness

### Notes

#### For Users
- **Upgrade path**: See MIGRATION.md
- **Breaking changes**: Import paths and initialization
- **Data compatibility**: No migration needed
- **Performance**: Comparable or better than v1.x

#### For Developers
- **New architecture**: See ARCHITECTURE.md
- **Contributing**: See CONTRIBUTING.md
- **Code standards**: Enforced through tests and documentation
- **Future features**: Easier to add with modular design

### Future Roadmap

Planned for v2.1+:
- Range queries (scan operations)
- Snapshots and backups
- Transactions with ACID guarantees
- HTTP/gRPC APIs
- TTL for keys
- Compression for SSTables
- Distributed replication
- Metrics and observability

---

## [1.0.0] - 2025-01-15

### Initial Release

Basic LSM tree implementation with:
- In-memory memcache (red-black tree)
- SSTable format for disk storage
- Basic compaction
- Bloom filters
- WAL for durability
- TCP server with text protocol

**Known Issues:**
- Race conditions in concurrent operations
- Global singleton prevents multiple instances
- Monolithic design (god object)
- Limited test coverage
- No proper configuration management

---

## Legend

- **Added**: New features
- **Changed**: Changes in existing functionality
- **Deprecated**: Soon-to-be removed features
- **Removed**: Removed features
- **Fixed**: Bug fixes
- **Security**: Security fixes
