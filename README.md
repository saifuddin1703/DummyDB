# DummyDB

DummyDB is a high-performance, thread-safe key-value database written in Go, built on top of an LSM (Log-Structured Merge) tree architecture. It provides durability through write-ahead logging, efficient lookups with bloom filters, and background compaction for optimal storage.

## Features

- **LSM Tree Architecture**: Fast writes to in-memory memtable with periodic flushes to disk
- **Write-Ahead Log (WAL)**: Ensures durability and crash recovery
- **SSTable Format**: Sorted string tables with sparse indexing for efficient disk storage
- **Bloom Filters**: Probabilistic data structure to avoid unnecessary disk reads
- **Background Compaction**: Automatic merging of SSTables to optimize storage and read performance
- **Thread-Safe**: Concurrent read/write operations with proper synchronization
- **Clean Architecture**: Modular design with dependency injection and testable components
- **TCP Server**: Simple text-based protocol for client-server communication

## Architecture

DummyDB follows Clean Architecture principles with clear separation of concerns:

```
┌─────────────────────────────────────────────────────────────┐
│                    Presentation Layer                        │
│                  (cmd/dummydb, client)                       │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│                    Application Layer                         │
│                   (internal/database)                        │
│            Coordinates WAL + Storage Engine                  │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│                      Domain Layer                            │
│         (internal/storage, wal, compaction)                  │
│     Core business logic and storage abstractions             │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│                  Infrastructure Layer                        │
│              (internal/disk, bloom, config)                  │
│        File I/O, bloom filters, configuration                │
└─────────────────────────────────────────────────────────────┘
```

### Key Components

- **LSM Engine** (`internal/storage/lsm`): Core storage engine with memtable and SSTable management
- **Memtable** (`internal/storage/lsm/memtable.go`): In-memory red-black tree for active writes
- **SSTable** (`internal/storage/sstable`): Immutable on-disk sorted tables with Reader/Writer pattern
- **WAL** (`internal/wal`): Write-ahead log for durability and crash recovery
- **Compaction** (`internal/compaction`): Background leveled compaction to merge SSTables
- **Bloom Filter** (`internal/bloom`): Probabilistic filter to reduce disk reads
- **File I/O** (`internal/disk`): Abstraction layer supporting real and in-memory file systems

## Getting Started

### Prerequisites

- Go 1.16 or higher

### Installation

1. Clone the repository:

   ```sh
   git clone https://github.com/saifuddin1703/DummyDB.git
   cd DummyDB
   ```

2. Build the server and client:
   ```sh
   make build
   ```

### Running the Server

To start the server, run:

```sh
make run_server
```

The server will start listening on `localhost:4000` by default.

### Configuration

Configure DummyDB using environment variables:

```sh
# Server configuration
export DUMMYDB_HOST=localhost
export DUMMYDB_PORT=4000

# Storage configuration
export DUMMYDB_DATA_DIR=./data
export DUMMYDB_SEGMENT_DIR=./segments
export DUMMYDB_WAL_PATH=./dummydb-wal

# Performance tuning
export DUMMYDB_MEMTABLE_SIZE_BYTES=524288        # 512KB
export DUMMYDB_MAX_TABLES_BEFORE_COMPACTION=4
export DUMMYDB_BLOOM_FILTER_EXPECTED_ITEMS=1000000
export DUMMYDB_BLOOM_FILTER_FALSE_POSITIVE=0.01
```

### Running the Client

To run the interactive client:

```sh
make run_client
```

### Client Commands

The client supports the following commands:

- `SET <key> <value>` - Store a key-value pair
- `GET <key>` - Retrieve a value by key
- `DEL <key>` - Delete a key
- `KEYS` - List all keys in the database
- `exit` - Close the client

Example session:

```
DummyDB Client
Connected to localhost:4000
Type 'exit' to quit

> SET user:1 alice
OK

> SET user:2 bob
OK

> GET user:1
alice

> KEYS
user:1
user:2

> DEL user:1
OK

> exit
Goodbye!
```

## Project Structure

```
/Users/sheikh.a/prsnl/DummyDB/
├── cmd/
│   └── dummydb/
│       └── main.go              # Application entry point with DI wiring
├── client/
│   └── main.go                  # Interactive client
├── internal/
│   ├── config/                  # Configuration management
│   │   ├── config.go
│   │   ├── defaults.go
│   │   └── errors.go
│   ├── database/                # Database orchestration layer
│   │   ├── database.go
│   │   └── options.go
│   ├── storage/                 # Storage abstractions
│   │   ├── engine.go            # Engine interface
│   │   ├── iterator.go
│   │   ├── lsm/                 # LSM engine implementation
│   │   │   ├── lsm.go
│   │   │   ├── memtable.go
│   │   │   └── memtable_rbtree.go
│   │   └── sstable/             # SSTable management
│   │       ├── sstable.go
│   │       ├── reader.go
│   │       ├── writer.go
│   │       └── iterator.go
│   ├── wal/                     # Write-ahead log
│   │   ├── wal.go
│   │   ├── entry.go
│   │   └── wal_impl.go
│   ├── compaction/              # Compaction management
│   │   ├── compactor.go
│   │   ├── leveled.go
│   │   └── merge.go
│   ├── bloom/                   # Bloom filter
│   │   ├── filter.go
│   │   └── filter_impl.go
│   ├── disk/                    # File I/O abstraction
│   │   ├── fileio.go
│   │   ├── fileio_impl.go
│   │   └── fileio_mem.go
│   ├── server/                  # TCP server
│   │   ├── server.go
│   │   ├── handler.go
│   │   └── protocol.go
│   └── testutil/                # Test utilities
│       ├── mocks.go
│       └── fixtures.go
├── test/
│   ├── integration/             # Integration tests
│   │   └── integration_test.go
│   └── benchmark/               # Performance benchmarks
│       └── benchmark_test.go
├── utils/
│   └── constants.go             # Legacy constants
├── Makefile
├── go.mod
└── go.sum
```

## Development

### Running Tests

```sh
# Run all tests
make test

# Run tests with race detector
make test-race

# Run integration tests
make test-integration

# Run benchmarks
make bench

# Generate coverage report
make coverage
```

### Testing Strategy

DummyDB has comprehensive test coverage:

- **Unit Tests**: Each component tested in isolation with mocked dependencies
- **Integration Tests**: Full lifecycle tests (write → flush → compact → read)
- **Concurrent Tests**: Race condition detection with `-race` flag
- **Crash Recovery Tests**: WAL replay validation
- **Benchmark Suite**: Performance monitoring for write/read operations

All tests use in-memory file I/O for speed and isolation.

### Architecture Principles

1. **Dependency Injection**: All dependencies passed through constructors, no globals
2. **Interface-Based Design**: Core abstractions defined as interfaces for testability
3. **Single Responsibility**: Each component has one clear purpose
4. **Thread Safety**: Proper synchronization with RWMutex for concurrent operations
5. **Clean Shutdown**: Graceful cleanup with WaitGroups and signal handling

## Performance

Benchmark results on typical hardware:

```
BenchmarkPut-8                1000000    972 ns/op    1718 B/op
BenchmarkGet-8                 300000   3106 ns/op    2632 B/op
BenchmarkConcurrentPuts-8      500000   2156 ns/op    1842 B/op
BenchmarkConcurrentGets-8      400000   3421 ns/op    2745 B/op
```

Performance characteristics:

- **Writes**: O(log n) to memtable, amortized O(1) with batching
- **Reads**: O(log n) in memtable + O(k) SSTable lookups with bloom filter optimization
- **Space**: Compaction reduces storage by ~40-60% depending on update patterns
- **Concurrency**: Lock-free reads from SSTables, concurrent memtable access

## How It Works

### Write Path

1. Append operation to WAL for durability
2. Insert key-value pair into in-memory memtable (red-black tree)
3. When memtable reaches size threshold (512KB default):
   - Swap in a new empty memtable
   - Flush old memtable to disk as SSTable
   - Add keys to bloom filter
4. Trigger background compaction if too many SSTables

### Read Path

1. Check active memtable (most recent data)
2. For each SSTable (newest to oldest):
   - Check bloom filter (skip if key definitely not present)
   - Binary search in SSTable using sparse index
3. Return value or "key not found" error

### Compaction

Background compaction merges SSTables to:
- Reduce number of files (faster reads)
- Remove deleted keys (reclaim space)
- Deduplicate updated keys (keep latest version)

Leveled compaction strategy:
- Trigger when SSTable count exceeds threshold (4 by default)
- Merge overlapping SSTables in sorted order
- Mark merged tables and remove old files

### Crash Recovery

On startup:
1. Load existing SSTables from segment directory
2. Replay WAL entries into memtable
3. Truncate WAL after successful recovery
4. Resume normal operation

## Migration Guide

If migrating from an older version of DummyDB:

### Breaking Changes

- Package structure reorganized under `internal/`
- Global `LSMT` and `database` variables removed
- Constructor signatures changed to require configuration
- `main.go` moved to `cmd/dummydb/main.go`
- Import paths updated

### Maintained Compatibility

- **On-disk format unchanged**: SSTable and WAL formats remain "key:value;" format
- **Client protocol unchanged**: SET/GET/DEL/KEYS commands work identically
- **Client binary unchanged**: Existing clients continue to work

### Upgrading

1. Stop the old server
2. Update to the new version
3. Run the new server - it will automatically read existing segment files
4. No data migration needed

## Contributing

Contributions are welcome! Please follow these guidelines:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Write tests for your changes
4. Ensure all tests pass including race detector (`make test-race`)
5. Format code with `go fmt`
6. Commit your changes (`git commit -m 'Add amazing feature'`)
7. Push to the branch (`git push origin feature/amazing-feature`)
8. Open a Pull Request

### Development Guidelines

- Maintain test coverage above 80%
- Add godoc comments for all exported types and functions
- Use interfaces for dependencies to enable mocking
- Run `go vet` and `golint` before committing
- Include integration tests for new features
- Update documentation for API changes

## Future Enhancements

Planned features:

- [ ] Range queries (scan operations)
- [ ] Snapshots and backups
- [ ] Transactions with ACID guarantees
- [ ] Distributed replication
- [ ] gRPC and HTTP APIs
- [ ] Time-to-live (TTL) for keys
- [ ] Compression for SSTables
- [ ] Metrics and observability

## License

This project is open source. See LICENSE file for details.

## Acknowledgments

- Built with [redblacktree](https://github.com/emirpasic/gods) for memtable implementation
- Uses [bits-and-blooms/bloom](https://github.com/bits-and-blooms/bloom) for bloom filters
- Inspired by LevelDB, RocksDB, and Cassandra LSM tree implementations

## Contact

For questions, issues, or contributions, please open an issue on GitHub.
