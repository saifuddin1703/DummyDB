package config

import (
	"os"
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()

	if cfg.MemTableSizeBytes != DefaultMemTableSize {
		t.Errorf("expected memtable size %d, got %d", DefaultMemTableSize, cfg.MemTableSizeBytes)
	}
	if cfg.MaxTablesBeforeCompaction != DefaultMaxTables {
		t.Errorf("expected max tables %d, got %d", DefaultMaxTables, cfg.MaxTablesBeforeCompaction)
	}
	if cfg.ServerPort != DefaultServerPort {
		t.Errorf("expected port %d, got %d", DefaultServerPort, cfg.ServerPort)
	}
	if cfg.BloomFilterExpectedItems != DefaultBloomExpectedItems {
		t.Errorf("expected bloom items %d, got %d", DefaultBloomExpectedItems, cfg.BloomFilterExpectedItems)
	}
}

func TestValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		config    *Config
		expectErr error
	}{
		{
			name:      "valid config",
			config:    DefaultConfig(),
			expectErr: nil,
		},
		{
			name: "invalid memtable size",
			config: &Config{
				MemTableSizeBytes:         0,
				MaxTablesBeforeCompaction: 4,
				BloomFilterExpectedItems:  1000,
				ServerPort:                4000,
			},
			expectErr: ErrInvalidMemTableSize,
		},
		{
			name: "invalid max tables",
			config: &Config{
				MemTableSizeBytes:         512 * KB,
				MaxTablesBeforeCompaction: 0,
				BloomFilterExpectedItems:  1000,
				ServerPort:                4000,
			},
			expectErr: ErrInvalidMaxTables,
		},
		{
			name: "invalid bloom filter",
			config: &Config{
				MemTableSizeBytes:         512 * KB,
				MaxTablesBeforeCompaction: 4,
				BloomFilterExpectedItems:  0,
				ServerPort:                4000,
			},
			expectErr: ErrInvalidBloomFilter,
		},
		{
			name: "invalid port - too low",
			config: &Config{
				MemTableSizeBytes:         512 * KB,
				MaxTablesBeforeCompaction: 4,
				BloomFilterExpectedItems:  1000,
				ServerPort:                0,
			},
			expectErr: ErrInvalidPort,
		},
		{
			name: "invalid port - too high",
			config: &Config{
				MemTableSizeBytes:         512 * KB,
				MaxTablesBeforeCompaction: 4,
				BloomFilterExpectedItems:  1000,
				ServerPort:                70000,
			},
			expectErr: ErrInvalidPort,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if err != tt.expectErr {
				t.Errorf("expected error %v, got %v", tt.expectErr, err)
			}
		})
	}
}

func TestLoadFromEnv(t *testing.T) {
	t.Parallel()

	// Save original env
	origDataDir := os.Getenv("DUMMYDB_DATA_DIR")
	origPort := os.Getenv("DUMMYDB_PORT")
	origMemTableSize := os.Getenv("DUMMYDB_MEMTABLE_SIZE")

	// Cleanup
	defer func() {
		os.Setenv("DUMMYDB_DATA_DIR", origDataDir)
		os.Setenv("DUMMYDB_PORT", origPort)
		os.Setenv("DUMMYDB_MEMTABLE_SIZE", origMemTableSize)
	}()

	// Set test env vars
	os.Setenv("DUMMYDB_DATA_DIR", "/tmp/test")
	os.Setenv("DUMMYDB_PORT", "5000")
	os.Setenv("DUMMYDB_MEMTABLE_SIZE", "1048576")

	cfg := LoadFromEnv()

	if cfg.DataDir != "/tmp/test" {
		t.Errorf("expected data dir /tmp/test, got %s", cfg.DataDir)
	}
	if cfg.ServerPort != 5000 {
		t.Errorf("expected port 5000, got %d", cfg.ServerPort)
	}
	if cfg.MemTableSizeBytes != 1048576 {
		t.Errorf("expected memtable size 1048576, got %d", cfg.MemTableSizeBytes)
	}
}

func TestLoadFromEnv_InvalidValues(t *testing.T) {
	t.Parallel()

	// Save original env
	origPort := os.Getenv("DUMMYDB_PORT")
	defer os.Setenv("DUMMYDB_PORT", origPort)

	// Set invalid port
	os.Setenv("DUMMYDB_PORT", "invalid")

	cfg := LoadFromEnv()

	// Should fall back to default
	if cfg.ServerPort != DefaultServerPort {
		t.Errorf("expected default port %d when invalid, got %d", DefaultServerPort, cfg.ServerPort)
	}
}
