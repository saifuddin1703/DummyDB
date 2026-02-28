package sstable

import (
	"io"
	"strings"

	"github.com/dummydb/internal/disk"
)

// SSTable represents an immutable sorted string table on disk
type SSTable interface {
	// Get retrieves a value by key
	Get(key string) ([]byte, error)

	// Has checks if a key exists without reading the value
	Has(key string) bool

	// Iterator returns an iterator over all key-value pairs
	Iterator() Iterator

	// Path returns the file path
	Path() string

	// Size returns the size of the SSTable in bytes
	Size() int64

	// IsMerged returns true if this is a merged SSTable
	IsMerged() bool

	// Close closes the SSTable and releases resources
	Close() error

	// Delete removes the SSTable file from disk
	Delete() error
}

// Iterator iterates over key-value pairs in an SSTable
type Iterator interface {
	// Next advances to the next key-value pair
	// Returns false when iteration is complete
	Next() bool

	// Key returns the current key
	Key() string

	// Value returns the current value
	Value() []byte

	// Err returns any error encountered during iteration
	Err() error

	// Close releases iterator resources
	Close() error
}

// Writer writes key-value pairs to create a new SSTable
type Writer interface {
	// Append adds a key-value pair
	// Keys must be added in sorted order
	Append(key string, value []byte) error

	// Finalize completes writing and closes the file
	// Returns the created SSTable
	Finalize() (SSTable, error)

	// Close closes the writer without finalizing (discards partial writes)
	Close() error
}

// Options for creating SSTables
type Options struct {
	// FileIO for file operations
	FileIO disk.FileIO

	// IndexInterval is the byte interval for sparse index entries
	IndexInterval int64

	// IsMerged marks this as a merged SSTable
	IsMerged bool
}

// NewWriter creates a new SSTable writer
func NewWriter(path string, opts Options) (Writer, error) {
	return newWriterImpl(path, opts)
}

// Open opens an existing SSTable for reading
func Open(path string, fileIO disk.FileIO) (SSTable, error) {
	return openSSTableWithMergedFlag(path, fileIO, strings.Contains(path, "merged"))
}

// OpenWithOptions opens an existing SSTable with specific options
func OpenWithOptions(path string, fileIO disk.FileIO, isMerged bool) (SSTable, error) {
	return openSSTableWithMergedFlag(path, fileIO, isMerged)
}

// KeyValue represents a key-value pair
type KeyValue struct {
	Key   string
	Value []byte
}

// SparseIndex maps keys to byte offsets in the SSTable
type SparseIndex struct {
	// entries maps key -> byte offset
	entries map[string]int64
}

// NewSparseIndex creates a new sparse index
func NewSparseIndex() *SparseIndex {
	return &SparseIndex{
		entries: make(map[string]int64),
	}
}

// Put adds an index entry
func (idx *SparseIndex) Put(key string, offset int64) {
	idx.entries[key] = offset
}

// Get retrieves the offset for a key
// Returns the offset and true if found
func (idx *SparseIndex) Get(key string) (int64, bool) {
	offset, ok := idx.entries[key]
	return offset, ok
}

// FindNearestBefore finds the nearest indexed key before or equal to the query key
// Returns the offset to start scanning from
func (idx *SparseIndex) FindNearestBefore(key string) int64 {
	// Find the largest indexed key that is <= query key
	var nearestOffset int64 = 0
	var nearestKey string

	for idxKey, offset := range idx.entries {
		if idxKey <= key && (nearestKey == "" || idxKey > nearestKey) {
			nearestKey = idxKey
			nearestOffset = offset
		}
	}

	return nearestOffset
}

// Len returns the number of index entries
func (idx *SparseIndex) Len() int {
	return len(idx.entries)
}

// WriteTo writes the index to a writer (for persistence if needed)
func (idx *SparseIndex) WriteTo(w io.Writer) (int64, error) {
	// Not implemented yet - can be added for persistent indexes
	return 0, nil
}
