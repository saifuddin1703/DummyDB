package lsm

import (
	"fmt"
	"sync"
	"testing"
)

func TestMemtableRBTree_PutAndGet(t *testing.T) {
	t.Parallel()

	m := NewMemtableRBTree()

	// Put some values
	err := m.Put("key1", "value1")
	if err != nil {
		t.Fatalf("failed to put: %v", err)
	}

	err = m.Put("key2", "value2")
	if err != nil {
		t.Fatalf("failed to put: %v", err)
	}

	// Get values back
	val, found := m.Get("key1")
	if !found {
		t.Error("expected to find key1")
	}
	if val != "value1" {
		t.Errorf("expected value1, got %s", val)
	}

	val, found = m.Get("key2")
	if !found {
		t.Error("expected to find key2")
	}
	if val != "value2" {
		t.Errorf("expected value2, got %s", val)
	}
}

func TestMemtableRBTree_GetNotFound(t *testing.T) {
	t.Parallel()

	m := NewMemtableRBTree()

	_, found := m.Get("nonexistent")
	if found {
		t.Error("expected not to find nonexistent key")
	}
}

func TestMemtableRBTree_Update(t *testing.T) {
	t.Parallel()

	m := NewMemtableRBTree()

	// Put initial value
	m.Put("key1", "value1")

	// Update value
	m.Put("key1", "value2")

	// Get updated value
	val, found := m.Get("key1")
	if !found {
		t.Error("expected to find key1")
	}
	if val != "value2" {
		t.Errorf("expected value2, got %s", val)
	}
}

func TestMemtableRBTree_Delete(t *testing.T) {
	t.Parallel()

	m := NewMemtableRBTree()

	// Put a value
	m.Put("key1", "value1")

	// Delete it
	err := m.Delete("key1")
	if err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	// Get should return deletion marker
	val, found := m.Get("key1")
	if !found {
		t.Error("expected to find deletion marker")
	}
	if val != "__DELETED__" {
		t.Errorf("expected __DELETED__, got %s", val)
	}
}

func TestMemtableRBTree_Size(t *testing.T) {
	t.Parallel()

	m := NewMemtableRBTree()

	initialSize := m.Size()
	if initialSize != 0 {
		t.Errorf("expected initial size 0, got %d", initialSize)
	}

	// Add entries
	m.Put("key1", "value1")
	m.Put("key2", "value2")

	size := m.Size()
	if size <= 0 {
		t.Errorf("expected positive size, got %d", size)
	}

	// Size should increase after adding more
	prevSize := size
	m.Put("key3", "value3")
	size = m.Size()

	if size <= prevSize {
		t.Errorf("expected size to increase, prev=%d, new=%d", prevSize, size)
	}
}

func TestMemtableRBTree_Iterator(t *testing.T) {
	t.Parallel()

	m := NewMemtableRBTree()

	// Add entries in non-sorted order
	m.Put("c", "value_c")
	m.Put("a", "value_a")
	m.Put("b", "value_b")

	// Iterate and verify sorted order
	iter := m.Iterator()
	defer iter.Close()

	expectedKeys := []string{"a", "b", "c"}
	expectedValues := []string{"value_a", "value_b", "value_c"}

	idx := 0
	for iter.Next() {
		if idx >= len(expectedKeys) {
			t.Error("iterator returned more items than expected")
			break
		}

		key := iter.Key()
		value := iter.Value()

		if key != expectedKeys[idx] {
			t.Errorf("expected key %s, got %s", expectedKeys[idx], key)
		}

		if value != expectedValues[idx] {
			t.Errorf("expected value %s, got %s", expectedValues[idx], value)
		}

		idx++
	}

	if idx != len(expectedKeys) {
		t.Errorf("expected %d items, got %d", len(expectedKeys), idx)
	}
}

func TestMemtableRBTree_Clear(t *testing.T) {
	t.Parallel()

	m := NewMemtableRBTree()

	// Add entries
	m.Put("key1", "value1")
	m.Put("key2", "value2")

	// Verify not empty
	if m.IsEmpty() {
		t.Error("expected memtable not to be empty")
	}

	// Clear
	m.Clear()

	// Verify empty
	if !m.IsEmpty() {
		t.Error("expected memtable to be empty after clear")
	}

	// Verify size is 0
	if m.Size() != 0 {
		t.Errorf("expected size 0 after clear, got %d", m.Size())
	}

	// Verify keys are gone
	_, found := m.Get("key1")
	if found {
		t.Error("expected key1 not to be found after clear")
	}
}

func TestMemtableRBTree_IsEmpty(t *testing.T) {
	t.Parallel()

	m := NewMemtableRBTree()

	// Initially empty
	if !m.IsEmpty() {
		t.Error("expected new memtable to be empty")
	}

	// Add entry
	m.Put("key1", "value1")

	// Not empty
	if m.IsEmpty() {
		t.Error("expected memtable not to be empty")
	}
}

func TestMemtableRBTree_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	m := NewMemtableRBTree()

	// Launch multiple goroutines writing different keys
	var wg sync.WaitGroup
	numGoroutines := 10
	numOpsPerGoroutine := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := 0; j < numOpsPerGoroutine; j++ {
				key := fmt.Sprintf("key_%d_%d", id, j)
				value := fmt.Sprintf("value_%d_%d", id, j)
				m.Put(key, value)
			}
		}(i)
	}

	// Launch goroutines reading
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := 0; j < numOpsPerGoroutine; j++ {
				key := fmt.Sprintf("key_%d_%d", id, j)
				m.Get(key) // Don't care about result, just testing concurrent access
			}
		}(i)
	}

	wg.Wait()

	// Verify some data is present
	if m.IsEmpty() {
		t.Error("expected memtable not to be empty after concurrent writes")
	}

	// Verify a specific key
	val, found := m.Get("key_0_0")
	if !found {
		t.Error("expected to find key_0_0")
	}
	if val != "value_0_0" {
		t.Errorf("expected value_0_0, got %s", val)
	}
}

func TestMemtableRBTree_LargeDataset(t *testing.T) {
	t.Parallel()

	m := NewMemtableRBTree()

	// Add 10000 entries
	numEntries := 10000
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key%05d", i)
		value := fmt.Sprintf("value%05d", i)
		err := m.Put(key, value)
		if err != nil {
			t.Fatalf("failed to put entry %d: %v", i, err)
		}
	}

	// Verify count via iterator
	iter := m.Iterator()
	count := 0
	for iter.Next() {
		count++
	}
	iter.Close()

	if count != numEntries {
		t.Errorf("expected %d entries, got %d", numEntries, count)
	}

	// Verify random reads
	testIndices := []int{0, 100, 5000, 9999}
	for _, i := range testIndices {
		key := fmt.Sprintf("key%05d", i)
		expectedValue := fmt.Sprintf("value%05d", i)

		value, found := m.Get(key)
		if !found {
			t.Errorf("expected to find key %s", key)
			continue
		}

		if value != expectedValue {
			t.Errorf("key %s: expected %s, got %s", key, expectedValue, value)
		}
	}
}

func TestMemtableIterator_Empty(t *testing.T) {
	t.Parallel()

	m := NewMemtableRBTree()

	iter := m.Iterator()
	defer iter.Close()

	if iter.Next() {
		t.Error("expected no items in empty memtable")
	}
}

func TestMemtableRBTree_DeleteNonExistent(t *testing.T) {
	t.Parallel()

	m := NewMemtableRBTree()

	// Delete non-existent key - should still work (creates tombstone)
	err := m.Delete("nonexistent")
	if err != nil {
		t.Fatalf("failed to delete nonexistent key: %v", err)
	}

	// Should find the tombstone
	val, found := m.Get("nonexistent")
	if !found {
		t.Error("expected to find deletion marker")
	}
	if val != "__DELETED__" {
		t.Errorf("expected __DELETED__, got %s", val)
	}
}
