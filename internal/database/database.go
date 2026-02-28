package database

import (
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/dummydb/internal/config"
	"github.com/dummydb/internal/storage"
)

// Database defines the database interface
type Database interface {
	// Put inserts or updates a key-value pair
	Put(key string, value []byte) error

	// Get retrieves a value by key
	Get(key string) ([]byte, error)

	// Delete marks a key as deleted
	Delete(key string) error

	// Keys returns all keys in the database
	Keys() ([]byte, error)

	// Close shuts down the database gracefully
	Close() error
}

// DB implements the Database interface
type DB struct {
	config *config.Config
	engine storage.Engine
	closed atomic.Bool
}

// NewDatabase creates a new database instance with the given options
func NewDatabase(opts ...Option) (*DB, error) {
	// Apply options
	options := &dbOptions{}
	for _, opt := range opts {
		opt(options)
	}

	// Validate required options
	if options.engine == nil {
		return nil, fmt.Errorf("storage engine is required")
	}

	db := &DB{
		config: options.config,
		engine: options.engine,
	}

	db.closed.Store(false)

	return db, nil
}

// Put inserts or updates a key-value pair
func (db *DB) Put(key string, value []byte) error {
	if db.closed.Load() {
		return fmt.Errorf("database is closed")
	}

	return db.engine.Put(key, value)
}

// Get retrieves a value by key
func (db *DB) Get(key string) ([]byte, error) {
	if db.closed.Load() {
		return nil, fmt.Errorf("database is closed")
	}

	val, found := db.engine.Get(key)
	if !found {
		return nil, fmt.Errorf("key not found: %s", key)
	}

	return val, nil
}

// Delete marks a key as deleted
func (db *DB) Delete(key string) error {
	if db.closed.Load() {
		return fmt.Errorf("database is closed")
	}

	return db.engine.Delete(key)
}

// Keys returns all keys in the database
func (db *DB) Keys() ([]byte, error) {
	if db.closed.Load() {
		return nil, fmt.Errorf("database is closed")
	}

	keys := db.engine.Keys()

	// Format keys as comma-separated string
	if len(keys) == 0 {
		return []byte(""), nil
	}

	return []byte(strings.Join(keys, ",")), nil
}

// Close shuts down the database gracefully
func (db *DB) Close() error {
	if db.closed.Swap(true) {
		// Already closed
		return nil
	}

	return db.engine.Close()
}
