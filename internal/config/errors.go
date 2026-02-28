package config

import "errors"

var (
	// Configuration validation errors
	ErrInvalidMemTableSize = errors.New("invalid memtable size: must be positive")
	ErrInvalidMaxTables    = errors.New("invalid max tables: must be positive")
	ErrInvalidBloomFilter  = errors.New("invalid bloom filter settings: expected items must be positive")
	ErrInvalidPort         = errors.New("invalid server port: must be between 1 and 65535")
)
