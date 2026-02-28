package lsm

// Memtable represents an in-memory sorted table
type Memtable interface {
	// Put adds or updates a key-value pair
	Put(key string, value string) error

	// Get retrieves a value by key
	// Returns value, found
	Get(key string) (string, bool)

	// Delete marks a key as deleted
	Delete(key string) error

	// Size returns the approximate size in bytes
	Size() int64

	// Iterator returns an iterator over all entries in sorted order
	Iterator() MemtableIterator

	// Clear removes all entries
	Clear()

	// IsEmpty returns true if the memtable has no entries
	IsEmpty() bool
}

// MemtableIterator iterates over memtable entries in sorted order
type MemtableIterator interface {
	// Next advances to the next entry
	// Returns false when iteration is complete
	Next() bool

	// Key returns the current key
	Key() string

	// Value returns the current value
	Value() string

	// Close releases resources
	Close()
}
