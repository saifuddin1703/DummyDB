package testutil

import (
	"testing"

	"github.com/dummydb/internal/wal"
)

func TestMockEngine(t *testing.T) {
	t.Parallel()

	engine := NewMockEngine()

	// Test Put
	err := engine.Put("key1", []byte("value1"))
	if err != nil {
		t.Errorf("Put failed: %v", err)
	}

	if engine.PutCalls != 1 {
		t.Errorf("expected 1 Put call, got %d", engine.PutCalls)
	}

	// Test Get
	val, found := engine.Get("key1")
	if !found {
		t.Error("expected to find key1")
	}
	if string(val) != "value1" {
		t.Errorf("expected value1, got %s", val)
	}
	if engine.GetCalls != 1 {
		t.Errorf("expected 1 Get call, got %d", engine.GetCalls)
	}

	// Test Delete
	err = engine.Delete("key1")
	if err != nil {
		t.Errorf("Delete failed: %v", err)
	}
	if engine.DelCalls != 1 {
		t.Errorf("expected 1 Delete call, got %d", engine.DelCalls)
	}

	// Test Keys
	engine.Put("key2", []byte("value2"))
	keys := engine.Keys()
	if len(keys) != 1 {
		t.Errorf("expected 1 key, got %d", len(keys))
	}

	// Test Close
	err = engine.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
	if !engine.Closed {
		t.Error("expected engine to be closed")
	}
}

func TestMockWAL(t *testing.T) {
	t.Parallel()

	mockWAL := NewMockWAL("test.wal")

	// Test Append
	entry := &wal.Entry{
		Key:   "key1",
		Value: "value1",
	}
	err := mockWAL.Append(entry)
	if err != nil {
		t.Errorf("Append failed: %v", err)
	}
	if mockWAL.AppendCalls != 1 {
		t.Errorf("expected 1 Append call, got %d", mockWAL.AppendCalls)
	}

	// Test Recover
	entries, err := mockWAL.Recover()
	if err != nil {
		t.Errorf("Recover failed: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("expected 1 entry, got %d", len(entries))
	}
	if mockWAL.RecoverCalls != 1 {
		t.Errorf("expected 1 Recover call, got %d", mockWAL.RecoverCalls)
	}

	// Test Truncate
	err = mockWAL.Truncate()
	if err != nil {
		t.Errorf("Truncate failed: %v", err)
	}

	entries, _ = mockWAL.Recover()
	if len(entries) != 0 {
		t.Errorf("expected 0 entries after truncate, got %d", len(entries))
	}

	// Test Path
	if mockWAL.Path() != "test.wal" {
		t.Errorf("expected test.wal, got %s", mockWAL.Path())
	}
}

func TestMockCompactor(t *testing.T) {
	t.Parallel()

	compactor := NewMockCompactor()

	// Test ShouldCompact
	if !compactor.ShouldCompact(4) {
		t.Error("expected ShouldCompact(4) to be true")
	}

	if compactor.ShouldCompact(3) {
		t.Error("expected ShouldCompact(3) to be false")
	}

	// Test Start
	err := compactor.Start()
	if err != nil {
		t.Errorf("Start failed: %v", err)
	}
	if !compactor.Started {
		t.Error("expected compactor to be started")
	}

	// Test Stop
	err = compactor.Stop()
	if err != nil {
		t.Errorf("Stop failed: %v", err)
	}
	if !compactor.Stopped {
		t.Error("expected compactor to be stopped")
	}
}

func TestMockFilter(t *testing.T) {
	t.Parallel()

	filter := NewMockFilter()

	// Test Add
	filter.Add([]byte("key1"))
	if filter.AddCalls != 1 {
		t.Errorf("expected 1 Add call, got %d", filter.AddCalls)
	}

	// Test Test
	if !filter.Test([]byte("key1")) {
		t.Error("expected key1 to be in filter")
	}
	if filter.TestCalls != 1 {
		t.Errorf("expected 1 Test call, got %d", filter.TestCalls)
	}

	if filter.Test([]byte("key2")) {
		t.Error("expected key2 not to be in filter")
	}

	// Test TestAndAdd
	found := filter.TestAndAdd([]byte("key2"))
	if found {
		t.Error("expected key2 not found on first TestAndAdd")
	}

	found = filter.TestAndAdd([]byte("key2"))
	if !found {
		t.Error("expected key2 found on second TestAndAdd")
	}

	// Test Count
	count := filter.Count()
	if count != 2 {
		t.Errorf("expected count 2, got %d", count)
	}

	// Test Clear
	filter.Clear()
	count = filter.Count()
	if count != 0 {
		t.Errorf("expected count 0 after clear, got %d", count)
	}
}

func TestMockMemtable(t *testing.T) {
	t.Parallel()

	memtable := NewMockMemtable()

	// Test Put
	err := memtable.Put("key1", "value1")
	if err != nil {
		t.Errorf("Put failed: %v", err)
	}
	if memtable.PutCalls != 1 {
		t.Errorf("expected 1 Put call, got %d", memtable.PutCalls)
	}

	// Test Get
	val, found := memtable.Get("key1")
	if !found {
		t.Error("expected to find key1")
	}
	if val != "value1" {
		t.Errorf("expected value1, got %s", val)
	}
	if memtable.GetCalls != 1 {
		t.Errorf("expected 1 Get call, got %d", memtable.GetCalls)
	}

	// Test Size
	size := memtable.Size()
	if size <= 0 {
		t.Errorf("expected positive size, got %d", size)
	}

	// Test Iterator
	memtable.Put("key2", "value2")
	iter := memtable.Iterator()
	count := 0
	for iter.Next() {
		count++
	}
	if count != 2 {
		t.Errorf("expected 2 items from iterator, got %d", count)
	}
	iter.Close()

	// Test Clear
	memtable.Clear()
	if !memtable.IsEmpty() {
		t.Error("expected memtable to be empty after clear")
	}
}
