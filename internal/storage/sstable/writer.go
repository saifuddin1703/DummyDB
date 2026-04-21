package sstable

import (
	"fmt"
	"os"

	"github.com/dummydb/internal/disk"
)

type writerImpl struct {
	path          string
	fileIO        disk.FileIO
	file          disk.File
	index         *SparseIndex
	indexInterval int64
	isMerged      bool
	currentOffset int64
	lastKey       string
	multiplier    int64
	closed        bool
}

func newWriterImpl(path string, opts Options) (*writerImpl, error) {
	file, err := opts.FileIO.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create SSTable file: %w", err)
	}

	return &writerImpl{
		path:          path,
		fileIO:        opts.FileIO,
		file:          file,
		index:         NewSparseIndex(),
		indexInterval: opts.IndexInterval,
		isMerged:      opts.IsMerged,
		currentOffset: 0,
		multiplier:    1,
		closed:        false,
	}, nil
}

// Append adds a key-value pair
// Keys must be added in sorted order
func (w *writerImpl) Append(key string, value []byte) error {
	if w.closed {
		return fmt.Errorf("writer is closed")
	}

	// Verify keys are in sorted order
	if w.lastKey != "" && key < w.lastKey {
		return fmt.Errorf("keys must be appended in sorted order: %s < %s", key, w.lastKey)
	}

	// Format: "key:value;"
	data := fmt.Sprintf("%s:%s;", key, string(value))

	prevOffset := w.currentOffset

	// Write data
	n, err := w.file.WriteString(data)
	if err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	w.currentOffset += int64(n)

	// Add to sparse index based on interval
	// Index first key, then every indexInterval bytes, and we'll add last key in Finalize
	if prevOffset == 0 || w.currentOffset > w.indexInterval*w.multiplier {
		w.index.Put(key, prevOffset)
		w.multiplier++
	}

	w.lastKey = key
	return nil
}

// Finalize completes writing and closes the file
func (w *writerImpl) Finalize() (SSTable, error) {
	if w.closed {
		return nil, fmt.Errorf("writer already closed")
	}

	// Sync to disk
	err := w.file.Sync()
	if err != nil {
		return nil, fmt.Errorf("failed to sync: %w", err)
	}

	// Close the file
	err = w.file.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to close file: %w", err)
	}

	w.closed = true

	// Re-open and build proper index by scanning the file
	// This ensures the index is accurate
	return openSSTableWithMergedFlag(w.path, w.fileIO, w.isMerged)
}

// Close closes the writer without finalizing
func (w *writerImpl) Close() error {
	if w.closed {
		return nil
	}

	w.closed = true

	if w.file != nil {
		w.file.Close()
	}

	// Remove partial file
	w.fileIO.Remove(w.path)

	return nil
}
