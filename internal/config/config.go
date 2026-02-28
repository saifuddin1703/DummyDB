package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds all configuration settings for DummyDB
type Config struct {
	// Storage paths
	DataDir    string
	SegmentDir string
	WALPath    string

	// LSM Tree settings
	MemTableSizeBytes          int64
	MaxTablesBeforeCompaction  int
	SparseIndexIntervalBytes   int64

	// Bloom filter settings
	BloomFilterExpectedItems uint
	BloomFilterFalsePositive float64

	// Server settings
	ServerHost string
	ServerPort int

	// Performance tuning
	CompactionWorkers int
	FlushWorkers      int

	// Timeouts
	WriteTimeout time.Duration
	ReadTimeout  time.Duration
}

// LoadFromEnv creates a Config from environment variables with fallback to defaults
func LoadFromEnv() *Config {
	cfg := DefaultConfig()

	if dataDir := os.Getenv("DUMMYDB_DATA_DIR"); dataDir != "" {
		cfg.DataDir = dataDir
	}
	if segmentDir := os.Getenv("DUMMYDB_SEGMENT_DIR"); segmentDir != "" {
		cfg.SegmentDir = segmentDir
	}
	if walPath := os.Getenv("DUMMYDB_WAL_PATH"); walPath != "" {
		cfg.WALPath = walPath
	}
	if memTableSize := os.Getenv("DUMMYDB_MEMTABLE_SIZE"); memTableSize != "" {
		if size, err := strconv.ParseInt(memTableSize, 10, 64); err == nil {
			cfg.MemTableSizeBytes = size
		}
	}
	if maxTables := os.Getenv("DUMMYDB_MAX_TABLES"); maxTables != "" {
		if count, err := strconv.Atoi(maxTables); err == nil {
			cfg.MaxTablesBeforeCompaction = count
		}
	}
	if serverHost := os.Getenv("DUMMYDB_HOST"); serverHost != "" {
		cfg.ServerHost = serverHost
	}
	if serverPort := os.Getenv("DUMMYDB_PORT"); serverPort != "" {
		if port, err := strconv.Atoi(serverPort); err == nil {
			cfg.ServerPort = port
		}
	}

	return cfg
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.MemTableSizeBytes <= 0 {
		return ErrInvalidMemTableSize
	}
	if c.MaxTablesBeforeCompaction <= 0 {
		return ErrInvalidMaxTables
	}
	if c.BloomFilterExpectedItems == 0 {
		return ErrInvalidBloomFilter
	}
	if c.ServerPort < 1 || c.ServerPort > 65535 {
		return ErrInvalidPort
	}
	return nil
}
