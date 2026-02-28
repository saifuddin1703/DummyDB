package compaction

import (
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/dummydb/internal/config"
	"github.com/dummydb/internal/storage/sstable"
)

// LeveledCompactor implements leveled compaction strategy
type LeveledCompactor struct {
	config     *config.Config
	sstableOpts sstable.Options

	// Background compaction
	requestChan chan CompactionRequest
	stopChan    chan struct{}
	wg          sync.WaitGroup
	running     bool
	mu          sync.Mutex
}

// NewLeveledCompactor creates a new leveled compaction strategy
func NewLeveledCompactor(cfg *config.Config, sstableOpts sstable.Options) *LeveledCompactor {
	return &LeveledCompactor{
		config:      cfg,
		sstableOpts: sstableOpts,
		requestChan: make(chan CompactionRequest, 10),
		stopChan:    make(chan struct{}),
		running:     false,
	}
}

// Compact merges multiple SSTables into a single merged SSTable
func (c *LeveledCompactor) Compact(tables []sstable.SSTable) (sstable.SSTable, error) {
	if len(tables) == 0 {
		return nil, fmt.Errorf("no tables to compact")
	}

	if len(tables) == 1 {
		return tables[0], nil
	}

	// Read all tables and collect segments
	segments := make([][]string, len(tables))

	for i, table := range tables {
		iter := table.Iterator()
		var entries []string

		for iter.Next() {
			key := iter.Key()
			value := iter.Value()
			entry := fmt.Sprintf("%s:%s", key, string(value))
			entries = append(entries, entry)
		}

		if err := iter.Err(); err != nil {
			iter.Close()
			return nil, fmt.Errorf("failed to iterate table %s: %w", table.Path(), err)
		}
		iter.Close()

		segments[i] = entries
	}

	// Merge all segments
	mergedEntries := MergeMultipleSegments(segments)

	// Create merged SSTable
	mergedPath := filepath.Join(c.config.SegmentDir, fmt.Sprintf("%d-merged-segment", time.Now().UnixNano()))

	writerOpts := sstable.Options{
		FileIO:        c.sstableOpts.FileIO,
		IndexInterval: c.sstableOpts.IndexInterval,
		IsMerged:      true,
	}

	writer, err := sstable.NewWriter(mergedPath, writerOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to create merged SSTable writer: %w", err)
	}

	// Write merged entries
	for _, entry := range mergedEntries {
		if len(entry) == 0 {
			continue
		}

		e := ParseEntry(entry)
		err := writer.Append(e.Key, []byte(e.Value))
		if err != nil {
			writer.Close()
			return nil, fmt.Errorf("failed to write entry: %w", err)
		}
	}

	// Finalize the merged SSTable
	mergedTable, err := writer.Finalize()
	if err != nil {
		return nil, fmt.Errorf("failed to finalize merged SSTable: %w", err)
	}

	return mergedTable, nil
}

// ShouldCompact returns true if compaction should be triggered
func (c *LeveledCompactor) ShouldCompact(tableCount int) bool {
	return tableCount >= c.config.MaxTablesBeforeCompaction
}

// Start begins background compaction
func (c *LeveledCompactor) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.running {
		return fmt.Errorf("compactor already running")
	}

	c.running = true
	c.wg.Add(1)

	go c.compactionWorker()

	return nil
}

// Stop stops background compaction and waits for completion
func (c *LeveledCompactor) Stop() error {
	c.mu.Lock()
	if !c.running {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	close(c.stopChan)
	c.wg.Wait()

	c.mu.Lock()
	c.running = false
	c.mu.Unlock()

	return nil
}

// compactionWorker runs in the background and processes compaction requests
func (c *LeveledCompactor) compactionWorker() {
	defer c.wg.Done()

	for {
		select {
		case <-c.stopChan:
			return

		case req := <-c.requestChan:
			// Process compaction request
			mergedTable, err := c.Compact(req.Tables)

			result := CompactionResult{
				MergedTable: mergedTable,
				Error:       err,
			}

			// Send result back
			select {
			case req.Done <- result:
			case <-c.stopChan:
				return
			}
		}
	}
}

// CompactAsync submits a compaction request and returns immediately
// The result will be sent to the done channel when compaction completes
func (c *LeveledCompactor) CompactAsync(tables []sstable.SSTable, done chan<- CompactionResult) error {
	c.mu.Lock()
	if !c.running {
		c.mu.Unlock()
		return fmt.Errorf("compactor not running")
	}
	c.mu.Unlock()

	req := CompactionRequest{
		Tables: tables,
		Done:   done,
	}

	select {
	case c.requestChan <- req:
		return nil
	default:
		return fmt.Errorf("compaction queue full")
	}
}
