package sstable

import (
	"fmt"
	"strings"

	"github.com/dummydb/internal/disk"
)

// sstableImpl implements SSTable for reading
type sstableImpl struct {
	path     string
	fileIO   disk.FileIO
	index    *SparseIndex
	size     int64
	isMerged bool
}

// openSSTableWithMergedFlag opens an existing SSTable with a specified merged flag
func openSSTableWithMergedFlag(path string, fileIO disk.FileIO, isMerged bool) (*sstableImpl, error) {
	// Get file info
	info, err := fileIO.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("failed to stat SSTable: %w", err)
	}

	// Read entire file to build index
	// In a production system, we'd persist the index separately
	data, err := fileIO.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read SSTable: %w", err)
	}

	// Build sparse index by scanning the file
	index := NewSparseIndex()
	offset := int64(0)
	entries := strings.Split(string(data), ";")

	for i, entry := range entries {
		if len(entry) == 0 {
			continue
		}

		parts := strings.SplitN(entry, ":", 2)
		if len(parts) != 2 {
			continue
		}

		key := parts[0]
		entrySize := int64(len(entry) + 1) // +1 for semicolon

		// Add to index at regular intervals (every 100KB) and first/last entries
		if i == 0 || offset%(100*1024) == 0 || i == len(entries)-2 {
			index.Put(key, offset)
		}

		offset += entrySize
	}

	return &sstableImpl{
		path:     path,
		fileIO:   fileIO,
		index:    index,
		size:     info.Size(),
		isMerged: isMerged,
	}, nil
}

// Get retrieves a value by key
func (s *sstableImpl) Get(key string) ([]byte, error) {
	// Find the offset to start searching from
	startOffset := s.index.FindNearestBefore(key)

	// Open file
	file, err := s.fileIO.Open(s.path)
	if err != nil {
		return nil, fmt.Errorf("failed to open SSTable: %w", err)
	}
	defer file.Close()

	// Seek to the start offset
	_, err = file.Seek(startOffset, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to seek: %w", err)
	}

	// Read from offset to end
	remainingSize := s.size - startOffset
	if remainingSize <= 0 {
		return nil, fmt.Errorf("key not found: %s", key)
	}

	buf := make([]byte, remainingSize)
	n, err := file.Read(buf)
	if err != nil && err.Error() != "EOF" {
		return nil, fmt.Errorf("failed to read: %w", err)
	}

	// Parse entries and search
	segment := string(buf[:n])
	entries := strings.Split(segment, ";")

	// Binary search within the segment
	return binarySearchEntries(entries, key)
}

// binarySearchEntries performs binary search on key-value entries
func binarySearchEntries(entries []string, query string) ([]byte, error) {
	// First filter out empty entries
	validEntries := make([]string, 0, len(entries))
	for _, entry := range entries {
		if len(entry) > 0 {
			validEntries = append(validEntries, entry)
		}
	}

	if len(validEntries) == 0 {
		return nil, fmt.Errorf("key not found: %s", query)
	}

	low := 0
	high := len(validEntries) - 1

	for low <= high {
		mid := (low + high) / 2

		parts := strings.SplitN(validEntries[mid], ":", 2)
		if len(parts) != 2 {
			// Invalid entry - this shouldn't happen with valid data
			// Try linear search as fallback
			for _, entry := range validEntries {
				parts := strings.SplitN(entry, ":", 2)
				if len(parts) == 2 && parts[0] == query {
					return []byte(parts[1]), nil
				}
			}
			return nil, fmt.Errorf("key not found: %s", query)
		}

		key := parts[0]
		value := parts[1]

		if key == query {
			return []byte(value), nil
		}

		if key < query {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	return nil, fmt.Errorf("key not found: %s", query)
}

// Has checks if a key exists
func (s *sstableImpl) Has(key string) bool {
	_, err := s.Get(key)
	return err == nil
}

// Iterator returns an iterator over all key-value pairs
func (s *sstableImpl) Iterator() Iterator {
	return newIterator(s)
}

// Path returns the file path
func (s *sstableImpl) Path() string {
	return s.path
}

// Size returns the size in bytes
func (s *sstableImpl) Size() int64 {
	return s.size
}

// IsMerged returns true if this is a merged SSTable
func (s *sstableImpl) IsMerged() bool {
	return s.isMerged
}

// Close closes the SSTable
func (s *sstableImpl) Close() error {
	// Nothing to close for read-only access
	return nil
}

// Delete removes the SSTable file from disk
func (s *sstableImpl) Delete() error {
	return s.fileIO.Remove(s.path)
}

// iterator implementation

type iterator struct {
	sstable *sstableImpl
	entries []string
	current int
	err     error
}

func newIterator(sst *sstableImpl) *iterator {
	// Read entire file
	data, err := sst.fileIO.ReadFile(sst.path)
	if err != nil {
		return &iterator{
			sstable: sst,
			err:     err,
		}
	}

	entries := strings.Split(string(data), ";")
	// Remove empty last entry
	if len(entries) > 0 && entries[len(entries)-1] == "" {
		entries = entries[:len(entries)-1]
	}

	return &iterator{
		sstable: sst,
		entries: entries,
		current: -1,
	}
}

func (it *iterator) Next() bool {
	if it.err != nil {
		return false
	}

	it.current++
	return it.current < len(it.entries)
}

func (it *iterator) Key() string {
	if it.current < 0 || it.current >= len(it.entries) {
		return ""
	}

	entry := it.entries[it.current]
	parts := strings.SplitN(entry, ":", 2)
	if len(parts) < 1 {
		return ""
	}

	return parts[0]
}

func (it *iterator) Value() []byte {
	if it.current < 0 || it.current >= len(it.entries) {
		return nil
	}

	entry := it.entries[it.current]
	parts := strings.SplitN(entry, ":", 2)
	if len(parts) < 2 {
		return nil
	}

	return []byte(parts[1])
}

func (it *iterator) Err() error {
	return it.err
}

func (it *iterator) Close() error {
	// Nothing to clean up
	return nil
}
