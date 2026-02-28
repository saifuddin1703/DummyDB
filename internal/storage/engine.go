package storage

// Engine defines the storage engine interface
type Engine interface {
	// Put inserts or updates a key-value pair
	Put(key string, value []byte) error

	// Get retrieves a value by key
	// Returns value, found
	Get(key string) ([]byte, bool)

	// Delete marks a key as deleted
	Delete(key string) error

	// Keys returns all keys in the storage engine
	Keys() []string

	// Close shuts down the engine gracefully
	Close() error
}
