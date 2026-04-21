package compaction

import (
	"fmt"
	"testing"

	"github.com/dummydb/internal/config"
	"github.com/dummydb/internal/disk"
	"github.com/dummydb/internal/storage/sstable"
)

func TestLeveledCompactor_ShouldCompact(t *testing.T) {
	t.Parallel()

	cfg := config.DefaultConfig()
	cfg.MaxTablesBeforeCompaction = 4

	opts := sstable.Options{
		FileIO:        disk.NewMemoryFileIO(),
		IndexInterval: config.DefaultSparseIndexInterval,
	}

	compactor := NewLeveledCompactor(cfg, opts)

	tests := []struct {
		tableCount int
		expected   bool
	}{
		{0, false},
		{3, false},
		{4, true},
		{5, true},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("count=%d", tt.tableCount), func(t *testing.T) {
			result := compactor.ShouldCompact(tt.tableCount)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestLeveledCompactor_CompactBasic(t *testing.T) {
	t.Parallel()

	fs := disk.NewMemoryFileIO()
	cfg := config.DefaultConfig()
	cfg.SegmentDir = "segments"
	fs.MkdirAll(cfg.SegmentDir, 0755)

	opts := sstable.Options{
		FileIO:        fs,
		IndexInterval: config.DefaultSparseIndexInterval,
		IsMerged:      false,
	}

	compactor := NewLeveledCompactor(cfg, opts)

	// Create two test SSTables
	tables := make([]sstable.SSTable, 2)

	// Table 1
	writer1, err := sstable.NewWriter("segments/table1.sst", opts)
	if err != nil {
		t.Fatalf("failed to create writer1: %v", err)
	}
	writer1.Append("a", []byte("value_a_old"))
	writer1.Append("b", []byte("value_b"))
	tables[0], err = writer1.Finalize()
	if err != nil {
		t.Fatalf("failed to finalize writer1: %v", err)
	}

	// Table 2 (newer)
	writer2, err := sstable.NewWriter("segments/table2.sst", opts)
	if err != nil {
		t.Fatalf("failed to create writer2: %v", err)
	}
	writer2.Append("a", []byte("value_a_new"))
	writer2.Append("c", []byte("value_c"))
	tables[1], err = writer2.Finalize()
	if err != nil {
		t.Fatalf("failed to finalize writer2: %v", err)
	}

	// Compact
	merged, err := compactor.Compact(tables)
	if err != nil {
		t.Fatalf("failed to compact: %v", err)
	}
	defer merged.Close()

	// Verify merged table has correct data
	// Should have: a:value_a_new (newer), b:value_b, c:value_c
	expectedData := map[string]string{
		"a": "value_a_new",
		"b": "value_b",
		"c": "value_c",
	}

	for key, expectedVal := range expectedData {
		val, err := merged.Get(key)
		if err != nil {
			t.Errorf("failed to get key %s: %v", key, err)
			continue
		}

		if string(val) != expectedVal {
			t.Errorf("key %s: expected %s, got %s", key, expectedVal, val)
		}
	}

	// Verify it's marked as merged
	if !merged.IsMerged() {
		t.Error("expected merged table to be marked as merged")
	}
}

func TestLeveledCompactor_CompactEmpty(t *testing.T) {
	t.Parallel()

	fs := disk.NewMemoryFileIO()
	cfg := config.DefaultConfig()

	opts := sstable.Options{
		FileIO:        fs,
		IndexInterval: config.DefaultSparseIndexInterval,
	}

	compactor := NewLeveledCompactor(cfg, opts)

	// Try to compact empty list
	_, err := compactor.Compact([]sstable.SSTable{})
	if err == nil {
		t.Error("expected error when compacting empty list")
	}
}

func TestLeveledCompactor_CompactSingle(t *testing.T) {
	t.Parallel()

	fs := disk.NewMemoryFileIO()
	cfg := config.DefaultConfig()
	cfg.SegmentDir = "segments"
	fs.MkdirAll(cfg.SegmentDir, 0755)

	opts := sstable.Options{
		FileIO:        fs,
		IndexInterval: config.DefaultSparseIndexInterval,
	}

	compactor := NewLeveledCompactor(cfg, opts)

	// Create single table
	writer, err := sstable.NewWriter("segments/table1.sst", opts)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}
	writer.Append("a", []byte("value_a"))
	table, err := writer.Finalize()
	if err != nil {
		t.Fatalf("failed to finalize: %v", err)
	}

	// Compact single table should just return it
	merged, err := compactor.Compact([]sstable.SSTable{table})
	if err != nil {
		t.Fatalf("failed to compact: %v", err)
	}

	if merged != table {
		t.Error("expected same table to be returned for single-table compaction")
	}
}

func TestLeveledCompactor_CompactMultiple(t *testing.T) {
	t.Parallel()

	fs := disk.NewMemoryFileIO()
	cfg := config.DefaultConfig()
	cfg.SegmentDir = "segments"
	fs.MkdirAll(cfg.SegmentDir, 0755)

	opts := sstable.Options{
		FileIO:        fs,
		IndexInterval: config.DefaultSparseIndexInterval,
	}

	compactor := NewLeveledCompactor(cfg, opts)

	// Create 4 tables with overlapping keys
	numTables := 4
	tables := make([]sstable.SSTable, numTables)

	for i := 0; i < numTables; i++ {
		path := fmt.Sprintf("segments/table%d.sst", i)
		writer, err := sstable.NewWriter(path, opts)
		if err != nil {
			t.Fatalf("failed to create writer%d: %v", i, err)
		}

		// Each table has overlapping key "shared" with incrementing value
		writer.Append("shared", []byte(fmt.Sprintf("version_%d", i)))
		writer.Append(fmt.Sprintf("unique_%d", i), []byte(fmt.Sprintf("value_%d", i)))

		tables[i], err = writer.Finalize()
		if err != nil {
			t.Fatalf("failed to finalize writer%d: %v", i, err)
		}
	}

	// Compact all tables
	merged, err := compactor.Compact(tables)
	if err != nil {
		t.Fatalf("failed to compact: %v", err)
	}
	defer merged.Close()

	// "shared" key should have the newest value (version_3)
	val, err := merged.Get("shared")
	if err != nil {
		t.Fatalf("failed to get 'shared' key: %v", err)
	}

	if string(val) != "version_3" {
		t.Errorf("expected 'version_3', got %s", val)
	}

	// All unique keys should be present
	for i := 0; i < numTables; i++ {
		key := fmt.Sprintf("unique_%d", i)
		val, err := merged.Get(key)
		if err != nil {
			t.Errorf("failed to get key %s: %v", key, err)
		}

		expectedVal := fmt.Sprintf("value_%d", i)
		if string(val) != expectedVal {
			t.Errorf("key %s: expected %s, got %s", key, expectedVal, val)
		}
	}
}

func TestLeveledCompactor_StartStop(t *testing.T) {
	t.Parallel()

	fs := disk.NewMemoryFileIO()
	cfg := config.DefaultConfig()

	opts := sstable.Options{
		FileIO:        fs,
		IndexInterval: config.DefaultSparseIndexInterval,
	}

	compactor := NewLeveledCompactor(cfg, opts)

	// Start
	err := compactor.Start()
	if err != nil {
		t.Fatalf("failed to start: %v", err)
	}

	// Try to start again - should fail
	err = compactor.Start()
	if err == nil {
		t.Error("expected error when starting already running compactor")
	}

	// Stop
	err = compactor.Stop()
	if err != nil {
		t.Fatalf("failed to stop: %v", err)
	}

	// Stop again - should be fine
	err = compactor.Stop()
	if err != nil {
		t.Fatalf("failed to stop again: %v", err)
	}
}

func TestLeveledCompactor_CompactAsync(t *testing.T) {
	t.Parallel()

	fs := disk.NewMemoryFileIO()
	cfg := config.DefaultConfig()
	cfg.SegmentDir = "segments"
	fs.MkdirAll(cfg.SegmentDir, 0755)

	opts := sstable.Options{
		FileIO:        fs,
		IndexInterval: config.DefaultSparseIndexInterval,
	}

	compactor := NewLeveledCompactor(cfg, opts)

	// Create test tables
	writer1, _ := sstable.NewWriter("segments/async1.sst", opts)
	writer1.Append("a", []byte("value_a"))
	table1, _ := writer1.Finalize()

	writer2, _ := sstable.NewWriter("segments/async2.sst", opts)
	writer2.Append("b", []byte("value_b"))
	table2, _ := writer2.Finalize()

	tables := []sstable.SSTable{table1, table2}

	// Try async without starting - should fail
	done := make(chan CompactionResult, 1)
	err := compactor.CompactAsync(tables, done)
	if err == nil {
		t.Error("expected error when using CompactAsync without starting")
	}

	// Start compactor
	compactor.Start()
	defer compactor.Stop()

	// Submit async compaction
	done = make(chan CompactionResult, 1)
	err = compactor.CompactAsync(tables, done)
	if err != nil {
		t.Fatalf("failed to submit async compaction: %v", err)
	}

	// Wait for result
	result := <-done

	if result.Error != nil {
		t.Fatalf("async compaction failed: %v", result.Error)
	}

	if result.MergedTable == nil {
		t.Error("expected merged table in result")
	}

	// Verify merged table
	val, err := result.MergedTable.Get("a")
	if err != nil {
		t.Errorf("failed to get key 'a': %v", err)
	}
	if string(val) != "value_a" {
		t.Errorf("expected 'value_a', got %s", val)
	}
}

func TestLeveledCompactor_CompactWithDeletions(t *testing.T) {
	t.Parallel()

	fs := disk.NewMemoryFileIO()
	cfg := config.DefaultConfig()
	cfg.SegmentDir = "segments"
	fs.MkdirAll(cfg.SegmentDir, 0755)

	opts := sstable.Options{
		FileIO:        fs,
		IndexInterval: config.DefaultSparseIndexInterval,
	}

	compactor := NewLeveledCompactor(cfg, opts)

	// Table 1: has key "a"
	writer1, _ := sstable.NewWriter("segments/del1.sst", opts)
	writer1.Append("a", []byte("value_a"))
	writer1.Append("b", []byte("value_b"))
	table1, _ := writer1.Finalize()

	// Table 2: marks "a" as deleted
	writer2, _ := sstable.NewWriter("segments/del2.sst", opts)
	writer2.Append("a", []byte(config.DeletedIndicator))
	writer2.Append("c", []byte("value_c"))
	table2, _ := writer2.Finalize()

	// Compact
	merged, err := compactor.Compact([]sstable.SSTable{table1, table2})
	if err != nil {
		t.Fatalf("failed to compact: %v", err)
	}
	defer merged.Close()

	// Key "a" should have deletion marker
	val, err := merged.Get("a")
	if err != nil {
		t.Fatalf("failed to get key 'a': %v", err)
	}

	if string(val) != config.DeletedIndicator {
		t.Errorf("expected deletion marker, got %s", val)
	}

	// Keys "b" and "c" should be present
	val, err = merged.Get("b")
	if err != nil {
		t.Errorf("failed to get key 'b': %v", err)
	}
	if string(val) != "value_b" {
		t.Errorf("expected 'value_b', got %s", val)
	}

	val, err = merged.Get("c")
	if err != nil {
		t.Errorf("failed to get key 'c': %v", err)
	}
	if string(val) != "value_c" {
		t.Errorf("expected 'value_c', got %s", val)
	}
}
