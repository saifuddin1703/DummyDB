package compaction

import (
	"github.com/dummydb/internal/storage/sstable"
)

// Compactor handles SSTable compaction
type Compactor interface {
	// Compact merges multiple SSTables into a single merged SSTable
	// Returns the merged SSTable
	Compact(tables []sstable.SSTable) (sstable.SSTable, error)

	// ShouldCompact returns true if compaction should be triggered
	ShouldCompact(tableCount int) bool

	// Start begins background compaction (if applicable)
	Start() error

	// Stop stops background compaction and waits for completion
	Stop() error
}

// CompactionRequest represents a request to compact tables
type CompactionRequest struct {
	Tables []sstable.SSTable
	Done   chan<- CompactionResult
}

// CompactionResult represents the result of a compaction
type CompactionResult struct {
	MergedTable sstable.SSTable
	Error       error
}
