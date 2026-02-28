# Contributing to DummyDB

Thank you for your interest in contributing to DummyDB! This document provides guidelines and instructions for contributing to the project.

## Code of Conduct

- Be respectful and inclusive in all interactions
- Provide constructive feedback
- Focus on the technical merit of contributions
- Help others learn and grow

## Getting Started

1. **Fork the repository** on GitHub
2. **Clone your fork** locally:
   ```sh
   git clone https://github.com/YOUR_USERNAME/DummyDB.git
   cd DummyDB
   ```
3. **Add upstream remote**:
   ```sh
   git remote add upstream https://github.com/saifuddin1703/DummyDB.git
   ```
4. **Install dependencies**:
   ```sh
   make deps
   ```

## Development Workflow

### 1. Create a Feature Branch

```sh
git checkout -b feature/your-feature-name
```

Branch naming conventions:
- `feature/` - New features
- `fix/` - Bug fixes
- `refactor/` - Code refactoring
- `docs/` - Documentation updates
- `test/` - Test improvements

### 2. Make Your Changes

Follow the coding guidelines below and ensure:
- Code is properly formatted
- Tests are written and passing
- Documentation is updated

### 3. Run Tests

Before committing, run the full test suite:

```sh
# Run all tests with race detector
make test-race

# Run integration tests
make test-integration

# Check code coverage
make coverage
```

**All tests must pass before submitting a PR.**

### 4. Commit Your Changes

Write clear, concise commit messages:

```sh
git commit -m "Add feature: description of what you added"
```

Good commit message examples:
- `Fix race condition in LSM engine Put() method`
- `Add range query support to storage engine`
- `Update README with configuration examples`
- `Refactor compaction scheduler for better performance`

### 5. Push and Create Pull Request

```sh
git push origin feature/your-feature-name
```

Then create a Pull Request on GitHub with:
- Clear title describing the change
- Description of what the PR does
- Reference to any related issues
- Screenshots/examples if applicable

## Coding Guidelines

### Go Style

Follow standard Go conventions:

```sh
# Format code
make fmt

# Run static analysis
make vet

# Run linter (if installed)
make lint
```

### Code Organization

DummyDB follows Clean Architecture:

```
internal/
├── config/       # Configuration management
├── database/     # Application layer - orchestration
├── storage/      # Domain layer - core storage logic
│   ├── lsm/
│   └── sstable/
├── wal/          # Domain layer - write-ahead log
├── compaction/   # Domain layer - compaction logic
├── bloom/        # Infrastructure - bloom filters
├── disk/         # Infrastructure - file I/O
├── server/       # Presentation - network protocol
└── testutil/     # Test utilities
```

**Key principles:**
- Dependencies point inward (Infrastructure → Domain → Application → Presentation)
- Use interfaces for core abstractions
- Keep business logic separate from infrastructure
- No circular dependencies

### Interface Design

Define interfaces in the package that uses them:

```go
// Good: Define interface where it's used
package database

type StorageEngine interface {
    Put(key string, value []byte) error
    Get(key string) ([]byte, error)
    // ...
}

// Implementation is in another package
package lsm

type Engine struct { ... }

func (e *Engine) Put(key string, value []byte) error { ... }
```

### Error Handling

- Return errors, don't panic (except for programmer errors)
- Wrap errors with context: `fmt.Errorf("failed to write WAL entry: %w", err)`
- Define sentinel errors for known conditions: `var ErrKeyNotFound = errors.New("key not found")`
- Check errors immediately after function calls

```go
// Good
value, err := db.Get("key")
if err != nil {
    return fmt.Errorf("failed to get key: %w", err)
}

// Bad
value, _ := db.Get("key")  // Don't ignore errors
```

### Concurrency

- Use `sync.RWMutex` for read-heavy workloads
- Document locking strategy in comments
- Avoid holding locks during I/O operations
- Use `go test -race` to detect race conditions

```go
type Engine struct {
    mu           sync.RWMutex
    tables       []sstable.SSTable  // protected by mu
    memtable     Memtable           // has its own locking
}

func (e *Engine) Get(key string) ([]byte, error) {
    // Check memtable (it has its own locking)
    if val, found := e.memtable.Get(key); found {
        return val, nil
    }

    // Read from SSTables with read lock
    e.mu.RLock()
    tables := e.tables  // Copy slice, not tables themselves
    e.mu.RUnlock()

    // Search tables without holding lock
    for _, table := range tables {
        // ...
    }
}
```

### Testing

Every contribution must include tests:

#### Unit Tests

- Test each component in isolation
- Use mocks from `internal/testutil` for dependencies
- Run tests in parallel: `t.Parallel()`
- Use table-driven tests for multiple cases

```go
func TestMemtable_Put(t *testing.T) {
    t.Parallel()

    tests := []struct {
        name    string
        key     string
        value   string
        wantErr bool
    }{
        {"valid key", "key1", "value1", false},
        {"empty key", "", "value", true},
        // ...
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            m := NewMemtableRBTree()
            err := m.Put(tt.key, tt.value)
            if (err != nil) != tt.wantErr {
                t.Errorf("Put() error = %v, wantErr %v", err, tt.wantErr)
            }
        })
    }
}
```

#### Integration Tests

- Place in `test/integration/`
- Test full component interactions
- Use MemoryFileIO for speed
- Include crash recovery scenarios

#### Benchmarks

- Place in `test/benchmark/`
- Use `b.ResetTimer()` after setup
- Include `b.ReportAllocs()` for memory tracking
- Test realistic workloads

```go
func BenchmarkPut(b *testing.B) {
    db, cleanup := setupBenchmarkDB(b)
    defer cleanup()

    data := testutil.GenerateBenchmarkData(b.N, 100)

    b.ResetTimer()
    b.ReportAllocs()

    for i := 0; i < b.N; i++ {
        db.Put(data.Keys[i], data.Values[i])
    }
}
```

### Documentation

#### Godoc Comments

All exported types, functions, and constants must have godoc comments:

```go
// Engine implements a log-structured merge tree storage engine.
// It provides thread-safe read and write operations with durability
// guarantees through write-ahead logging.
type Engine struct {
    // ...
}

// Put writes a key-value pair to the storage engine.
// The operation is first logged to the WAL for durability, then
// written to the active memtable. When the memtable reaches the
// configured size threshold, it is flushed to disk as an SSTable.
//
// Returns an error if the engine is closed or if the WAL write fails.
func (e *Engine) Put(key string, value []byte) error {
    // ...
}
```

#### Package Documentation

Add package-level documentation in `doc.go` or the main file:

```go
// Package lsm implements a log-structured merge tree storage engine.
//
// The LSM engine provides high write throughput by buffering writes in
// an in-memory structure (memtable) and periodically flushing to disk
// as immutable sorted files (SSTables). Background compaction merges
// SSTables to maintain read performance and reclaim space.
//
// Example usage:
//
//     cfg := config.DefaultConfig()
//     fileIO := disk.NewOSFileIO()
//     walImpl := wal.NewWAL(cfg.WALPath, fileIO)
//     engine := lsm.NewEngine(cfg, fileIO, walImpl, compactor, filter)
//
//     err := engine.Put("key", []byte("value"))
//     value, err := engine.Get("key")
//
package lsm
```

## Specific Contribution Areas

### Adding New Features

1. **Discuss first**: Open an issue to discuss the feature before implementing
2. **Design review**: For significant features, share design docs for feedback
3. **Incremental PRs**: Break large features into smaller, reviewable PRs
4. **Update docs**: Update README, godoc, and examples

### Fixing Bugs

1. **Reproduction**: Include a test that reproduces the bug
2. **Root cause**: Explain the root cause in the PR description
3. **Regression test**: Ensure the test fails before fix and passes after
4. **Related issues**: Check for and reference related issues

### Performance Improvements

1. **Benchmark**: Provide before/after benchmark results
2. **Profile**: Include CPU/memory profiles if relevant
3. **Trade-offs**: Discuss any trade-offs (complexity vs performance)
4. **Verify correctness**: Ensure all tests still pass

### Documentation

1. **Accuracy**: Ensure technical accuracy
2. **Examples**: Include code examples where helpful
3. **Clarity**: Use clear, concise language
4. **Formatting**: Follow Markdown best practices

## Pull Request Process

### Before Submitting

- [ ] Code is formatted (`make fmt`)
- [ ] All tests pass (`make test-race`)
- [ ] Integration tests pass (`make test-integration`)
- [ ] Coverage is maintained or improved (`make coverage`)
- [ ] Code is well-documented with godoc comments
- [ ] No unnecessary dependencies added
- [ ] Commit messages are clear and descriptive

### Review Process

1. **Automated checks**: CI will run tests and linting
2. **Code review**: Maintainers will review your code
3. **Feedback**: Address review comments
4. **Approval**: PR needs approval from at least one maintainer
5. **Merge**: Maintainer will merge after approval

### Addressing Feedback

- Be responsive to review comments
- Ask questions if feedback is unclear
- Make requested changes or explain why not
- Push new commits (don't force-push during review)
- Mark resolved conversations as resolved

## Common Tasks

### Adding a New Storage Engine

1. Implement the `storage.Engine` interface in a new package
2. Add comprehensive unit tests with mocked dependencies
3. Add integration tests comparing behavior to LSM engine
4. Update documentation with configuration examples
5. Add benchmarks comparing performance

### Adding a New Command

1. Add command parsing in `internal/server/protocol.go`
2. Implement handler in `internal/server/handler.go`
3. Add method to database interface if needed
4. Write tests for parsing and execution
5. Update README with command documentation
6. Update client with example usage

### Improving Compaction

1. Create new compactor in `internal/compaction/`
2. Implement `Compactor` interface
3. Add strategy selection in configuration
4. Benchmark against existing strategy
5. Document trade-offs and use cases

## Release Process

(For maintainers)

1. Update version in `go.mod`
2. Update CHANGELOG.md
3. Create git tag: `git tag v1.x.x`
4. Push tag: `git push origin v1.x.x`
5. Create GitHub release with notes

## Questions?

- Open an issue for bugs or feature requests
- Start a discussion for design questions
- Check existing issues and PRs first

## License

By contributing, you agree that your contributions will be licensed under the same license as the project.

---

Thank you for contributing to DummyDB! 🎉
