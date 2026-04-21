package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/dummydb/internal/bloom"
	"github.com/dummydb/internal/compaction"
	"github.com/dummydb/internal/config"
	"github.com/dummydb/internal/disk"
	"github.com/dummydb/internal/storage/sstable"
	"github.com/dummydb/internal/wal"
)

// Engine implements an LSM tree-based storage engine
type Engine struct {
	config *config.Config

	// Memtable
	activeMemtable Memtable
	mu             sync.RWMutex // Protects activeMemtable, tables, mergedTables

	// SSTables
	tables       []sstable.SSTable // Regular SSTables
	mergedTables []sstable.SSTable // Merged SSTables

	// Dependencies
	fileIO    disk.FileIO
	wal       wal.WAL
	compactor compaction.Compactor
	filter    bloom.Filter

	// Shutdown
	closed    bool
	closeChan chan struct{}
	wg        sync.WaitGroup
}

// NewEngine creates a new LSM storage engine
func NewEngine(
	cfg *config.Config,
	fileIO disk.FileIO,
	walImpl wal.WAL,
	compactor compaction.Compactor,
	filter bloom.Filter,
) (*Engine, error) {
	// Create directories
	if err := fileIO.MkdirAll(cfg.SegmentDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create segment directory: %w", err)
	}

	e := &Engine{
		config:         cfg,
		activeMemtable: NewMemtableRBTree(),
		tables:         make([]sstable.SSTable, 0),
		mergedTables:   make([]sstable.SSTable, 0),
		fileIO:         fileIO,
		wal:            walImpl,
		compactor:      compactor,
		filter:         filter,
		closed:         false,
		closeChan:      make(chan struct{}),
	}

	// Load existing SSTables from disk
	if err := e.loadSSTables(); err != nil {
		return nil, fmt.Errorf("failed to load SSTables: %w", err)
	}

	// Recover from WAL
	if err := e.recoverFromWAL(); err != nil {
		return nil, fmt.Errorf("failed to recover from WAL: %w", err)
	}

	// Start compactor
	if err := compactor.Start(); err != nil {
		return nil, fmt.Errorf("failed to start compactor: %w", err)
	}

	return e, nil
}

// Put inserts or updates a key-value pair
func (e *Engine) Put(key string, value []byte) error {
	e.mu.Lock()

	if e.closed {
		e.mu.Unlock()
		return fmt.Errorf("engine is closed")
	}

	// Write to WAL first
	entry := &wal.Entry{
		Key:       key,
		Value:     string(value),
		Timestamp: time.Now(),
		Type:      wal.EntryTypePut,
	}

	// We need to write to WAL while holding the lock to ensure consistency
	// between memtable and WAL
	if err := e.wal.Append(entry); err != nil {
		e.mu.Unlock()
		return fmt.Errorf("failed to append to WAL: %w", err)
	}

	// Add to bloom filter
	e.filter.Add([]byte(key))

	// Put in active memtable
	if err := e.activeMemtable.Put(key, string(value)); err != nil {
		e.mu.Unlock()
		return fmt.Errorf("failed to put in memtable: %w", err)
	}

	// Check if we need to flush
	size := e.activeMemtable.Size()
	shouldFlush := size >= e.config.MemTableSizeBytes

	if shouldFlush {
		// Swap memtables
		oldMemtable := e.activeMemtable
		e.activeMemtable = NewMemtableRBTree()

		// Unlock before flushing (flush can take time)
		e.mu.Unlock()

		// Flush in background
		e.wg.Add(1)
		go e.flushMemtable(oldMemtable)

		return nil
	}

	e.mu.Unlock()
	return nil
}

// Get retrieves a value by key
func (e *Engine) Get(key string) ([]byte, bool) {
	e.mu.RLock()

	if e.closed {
		e.mu.RUnlock()
		return nil, false
	}

	// Check bloom filter first
	if !e.filter.Test([]byte(key)) {
		e.mu.RUnlock()
		return nil, false
	}

	// Check active memtable
	if val, found := e.activeMemtable.Get(key); found {
		e.mu.RUnlock()

		// Check if it's a deletion marker
		if val == config.DeletedIndicator {
			return nil, false
		}

		return []byte(val), true
	}

	// Search SSTables (newer first)
	// Make copies of table slices to avoid holding lock during I/O
	tablesCopy := make([]sstable.SSTable, len(e.tables))
	copy(tablesCopy, e.tables)

	mergedTablesCopy := make([]sstable.SSTable, len(e.mergedTables))
	copy(mergedTablesCopy, e.mergedTables)

	e.mu.RUnlock()

	// Search regular tables (newest first)
	for i := len(tablesCopy) - 1; i >= 0; i-- {
		val, err := tablesCopy[i].Get(key)
		if err == nil {
			// Check if it's a deletion marker
			if string(val) == config.DeletedIndicator {
				return nil, false
			}
			return val, true
		}
	}

	// Search merged tables (newest first)
	for i := len(mergedTablesCopy) - 1; i >= 0; i-- {
		val, err := mergedTablesCopy[i].Get(key)
		if err == nil {
			// Check if it's a deletion marker
			if string(val) == config.DeletedIndicator {
				return nil, false
			}
			return val, true
		}
	}

	return nil, false
}

// Delete marks a key as deleted
func (e *Engine) Delete(key string) error {
	// Deletion is just a Put with a special marker
	return e.Put(key, []byte(config.DeletedIndicator))
}

// Keys returns all keys in the storage engine
func (e *Engine) Keys() []string {
	e.mu.RLock()

	if e.closed {
		e.mu.RUnlock()
		return []string{}
	}

	keySet := make(map[string]bool)

	// Get keys from active memtable
	iter := e.activeMemtable.Iterator()
	for iter.Next() {
		key := iter.Key()
		val := iter.Value()
		if val != config.DeletedIndicator {
			keySet[key] = true
		} else {
			// Mark as deleted
			delete(keySet, key)
		}
	}
	iter.Close()

	// Make copies of table slices
	tablesCopy := make([]sstable.SSTable, len(e.tables))
	copy(tablesCopy, e.tables)

	mergedTablesCopy := make([]sstable.SSTable, len(e.mergedTables))
	copy(mergedTablesCopy, e.mergedTables)

	e.mu.RUnlock()

	// Get keys from regular tables (oldest first)
	for i := 0; i < len(tablesCopy); i++ {
		iter := tablesCopy[i].Iterator()
		for iter.Next() {
			key := iter.Key()
			val := iter.Value()
			if string(val) != config.DeletedIndicator {
				keySet[key] = true
			} else {
				delete(keySet, key)
			}
		}
		iter.Close()
	}

	// Get keys from merged tables (oldest first)
	for i := 0; i < len(mergedTablesCopy); i++ {
		iter := mergedTablesCopy[i].Iterator()
		for iter.Next() {
			key := iter.Key()
			val := iter.Value()
			if string(val) != config.DeletedIndicator {
				keySet[key] = true
			} else {
				delete(keySet, key)
			}
		}
		iter.Close()
	}

	// Convert to slice
	keys := make([]string, 0, len(keySet))
	for key := range keySet {
		keys = append(keys, key)
	}

	return keys
}

// Close shuts down the engine gracefully
func (e *Engine) Close() error {
	e.mu.Lock()

	if e.closed {
		e.mu.Unlock()
		return nil
	}

	e.closed = true
	close(e.closeChan)

	// Flush active memtable
	if !e.activeMemtable.IsEmpty() {
		oldMemtable := e.activeMemtable
		e.activeMemtable = NewMemtableRBTree()
		e.mu.Unlock()

		// Flush synchronously during close (call directly without goroutine)
		e.flushMemtableSync(oldMemtable)
	} else {
		e.mu.Unlock()
	}

	// Wait for all background operations
	e.wg.Wait()

	// Stop compactor
	if err := e.compactor.Stop(); err != nil {
		return fmt.Errorf("failed to stop compactor: %w", err)
	}

	// Close WAL
	if err := e.wal.Close(); err != nil {
		return fmt.Errorf("failed to close WAL: %w", err)
	}

	// Close all SSTables
	e.mu.Lock()
	defer e.mu.Unlock()

	for _, table := range e.tables {
		table.Close()
	}

	for _, table := range e.mergedTables {
		table.Close()
	}

	return nil
}

// flushMemtable writes a memtable to disk as an SSTable (async)
func (e *Engine) flushMemtable(memtable Memtable) {
	defer e.wg.Done()
	e.flushMemtableSync(memtable)
}

// flushMemtableSync writes a memtable to disk synchronously
func (e *Engine) flushMemtableSync(memtable Memtable) {

	// Create SSTable from memtable
	path := filepath.Join(e.config.SegmentDir, fmt.Sprintf("%d-segment", time.Now().UnixNano()))

	opts := sstable.Options{
		FileIO:        e.fileIO,
		IndexInterval: e.config.SparseIndexIntervalBytes,
		IsMerged:      false,
	}

	writer, err := sstable.NewWriter(path, opts)
	if err != nil {
		fmt.Printf("Error creating SSTable writer: %v\n", err)
		return
	}

	// Write all entries from memtable
	iter := memtable.Iterator()
	for iter.Next() {
		key := iter.Key()
		value := iter.Value()

		if err := writer.Append(key, []byte(value)); err != nil {
			fmt.Printf("Error appending to SSTable: %v\n", err)
			writer.Close()
			return
		}
	}
	iter.Close()

	// Finalize SSTable
	table, err := writer.Finalize()
	if err != nil {
		fmt.Printf("Error finalizing SSTable: %v\n", err)
		return
	}

	// Truncate WAL
	if err := e.wal.Truncate(); err != nil {
		fmt.Printf("Error truncating WAL: %v\n", err)
	}

	// Add to tables list
	e.mu.Lock()
	e.tables = append(e.tables, table)
	tableCount := len(e.tables)
	e.mu.Unlock()

	// Check if compaction is needed
	if e.compactor.ShouldCompact(tableCount) {
		e.triggerCompaction()
	}
}

// triggerCompaction triggers background compaction
func (e *Engine) triggerCompaction() {
	e.mu.Lock()

	if e.closed {
		e.mu.Unlock()
		return
	}

	// Extract tables to compact
	numTables := e.config.MaxTablesBeforeCompaction
	if len(e.tables) < numTables {
		e.mu.Unlock()
		return
	}

	// Take the oldest N tables
	tablesToCompact := make([]sstable.SSTable, numTables)
	copy(tablesToCompact, e.tables[:numTables])

	// Remove them from the list
	e.tables = e.tables[numTables:]

	e.mu.Unlock()

	// Compact in background
	e.wg.Add(1)
	go e.compactTables(tablesToCompact)
}

// compactTables performs compaction
func (e *Engine) compactTables(tables []sstable.SSTable) {
	defer e.wg.Done()

	mergedTable, err := e.compactor.Compact(tables)
	if err != nil {
		fmt.Printf("Error compacting tables: %v\n", err)
		// Re-add tables back
		e.mu.Lock()
		e.tables = append(tables, e.tables...)
		e.mu.Unlock()
		return
	}

	// Add merged table
	e.mu.Lock()
	e.mergedTables = append(e.mergedTables, mergedTable)
	e.mu.Unlock()

	// Delete old tables
	for _, table := range tables {
		if err := table.Delete(); err != nil {
			fmt.Printf("Error deleting old table: %v\n", err)
		}
	}
}

// loadSSTables loads existing SSTables from disk
func (e *Engine) loadSSTables() error {
	// Check if directory exists
	if _, err := e.fileIO.Stat(e.config.SegmentDir); err != nil {
		if os.IsNotExist(err) {
			return nil // Directory doesn't exist yet
		}
		return err
	}

	entries, err := e.fileIO.ReadDir(e.config.SegmentDir)
	if err != nil {
		return fmt.Errorf("failed to read segment directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		path := filepath.Join(e.config.SegmentDir, entry.Name())

		// Determine if merged based on filename
		isMerged := false
		if name := entry.Name(); len(name) > 0 {
			isMerged = filepath.Base(name)[len(name)-1:] == "d" ||
			          len(name) > 6 && name[len(name)-6:] == "merged"
		}

		table, err := sstable.OpenWithOptions(path, e.fileIO, isMerged)
		if err != nil {
			fmt.Printf("Warning: failed to open SSTable %s: %v\n", path, err)
			continue
		}

		if table.IsMerged() {
			e.mergedTables = append(e.mergedTables, table)
		} else {
			e.tables = append(e.tables, table)
		}

		// Rebuild bloom filter
		iter := table.Iterator()
		for iter.Next() {
			e.filter.Add([]byte(iter.Key()))
		}
		iter.Close()
	}

	return nil
}

// recoverFromWAL recovers data from the WAL
func (e *Engine) recoverFromWAL() error {
	entries, err := e.wal.Recover()
	if err != nil {
		return fmt.Errorf("failed to recover WAL: %w", err)
	}

	for _, entry := range entries {
		e.activeMemtable.Put(entry.Key, entry.Value)
		e.filter.Add([]byte(entry.Key))
	}

	// Truncate WAL after recovery
	return e.wal.Truncate()
}
