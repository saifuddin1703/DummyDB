package config

import (
	"time"
)

const (
	// Size constants
	KB = 1024
	MB = 1024 * KB

	// Default storage paths
	DefaultDataDir    = "./data"
	DefaultSegmentDir = "./segments"
	DefaultWALPath    = "dummydb-wal"

	// Default LSM Tree settings
	DefaultMemTableSize          = 512 * KB // 512 KB
	DefaultMaxTables             = 4
	DefaultSparseIndexInterval   = 100 * KB // 100 KB

	// Default bloom filter settings
	DefaultBloomExpectedItems = 1000000
	DefaultBloomFalsePositive = 0.01

	// Default server settings
	DefaultServerHost = "localhost"
	DefaultServerPort = 4000

	// Default performance settings
	DefaultCompactionWorkers = 2
	DefaultFlushWorkers      = 1

	// Default timeouts
	DefaultWriteTimeout = 5 * time.Second
	DefaultReadTimeout  = 3 * time.Second

	// Deleted indicator
	DeletedIndicator = "__DELETED__"
)

// DefaultConfig returns a Config with default values
func DefaultConfig() *Config {
	return &Config{
		DataDir:                    DefaultDataDir,
		SegmentDir:                 DefaultSegmentDir,
		WALPath:                    DefaultWALPath,
		MemTableSizeBytes:          DefaultMemTableSize,
		MaxTablesBeforeCompaction:  DefaultMaxTables,
		SparseIndexIntervalBytes:   DefaultSparseIndexInterval,
		BloomFilterExpectedItems:   DefaultBloomExpectedItems,
		BloomFilterFalsePositive:   DefaultBloomFalsePositive,
		ServerHost:                 DefaultServerHost,
		ServerPort:                 DefaultServerPort,
		CompactionWorkers:          DefaultCompactionWorkers,
		FlushWorkers:               DefaultFlushWorkers,
		WriteTimeout:               DefaultWriteTimeout,
		ReadTimeout:                DefaultReadTimeout,
	}
}
