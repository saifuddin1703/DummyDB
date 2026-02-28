package lsm

import (
	"sync"
	"unsafe"

	"github.com/emirpasic/gods/trees/redblacktree"
)

// MemtableRBTree implements Memtable using a Red-Black tree
type MemtableRBTree struct {
	tree *redblacktree.Tree
	size int64
	mu   sync.RWMutex
}

// NewMemtableRBTree creates a new Red-Black tree based memtable
func NewMemtableRBTree() *MemtableRBTree {
	return &MemtableRBTree{
		tree: redblacktree.NewWithStringComparator(),
		size: 0,
	}
}

// Put adds or updates a key-value pair
func (m *MemtableRBTree) Put(key string, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if key exists to calculate size delta
	var sizeDelta int64
	if node := m.tree.GetNode(key); node != nil {
		// Key exists - calculate delta
		oldValue := node.Value.(string)
		sizeDelta = int64(len(value) - len(oldValue))
	} else {
		// New key - add full size
		sizeDelta = int64(unsafe.Sizeof(key)) + int64(len(key)) + int64(len(value))
	}

	m.tree.Put(key, value)
	m.size += sizeDelta

	return nil
}

// Get retrieves a value by key
func (m *MemtableRBTree) Get(key string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	value, found := m.tree.Get(key)
	if !found {
		return "", false
	}

	return value.(string), true
}

// Delete marks a key as deleted
func (m *MemtableRBTree) Delete(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// In LSM trees, we don't actually remove from memtable
	// Instead, we write a tombstone (deletion marker)
	// We'll use a special value to indicate deletion
	deletionMarker := "__DELETED__"

	// Calculate size delta
	var sizeDelta int64
	if node := m.tree.GetNode(key); node != nil {
		oldValue := node.Value.(string)
		sizeDelta = int64(len(deletionMarker) - len(oldValue))
	} else {
		sizeDelta = int64(unsafe.Sizeof(key)) + int64(len(key)) + int64(len(deletionMarker))
	}

	m.tree.Put(key, deletionMarker)
	m.size += sizeDelta

	return nil
}

// Size returns the approximate size in bytes
func (m *MemtableRBTree) Size() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.size
}

// Iterator returns an iterator over all entries
func (m *MemtableRBTree) Iterator() MemtableIterator {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Get a snapshot of keys in sorted order
	keys := m.tree.Keys()
	values := make([]string, len(keys))

	for i, key := range keys {
		if value, found := m.tree.Get(key); found {
			values[i] = value.(string)
		}
	}

	return &rbtreeIterator{
		keys:    keys,
		values:  values,
		current: -1,
	}
}

// Clear removes all entries
func (m *MemtableRBTree) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.tree.Clear()
	m.size = 0
}

// IsEmpty returns true if the memtable has no entries
func (m *MemtableRBTree) IsEmpty() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.tree.Empty()
}

// rbtreeIterator implements MemtableIterator
type rbtreeIterator struct {
	keys    []any
	values  []string
	current int
}

func (it *rbtreeIterator) Next() bool {
	it.current++
	return it.current < len(it.keys)
}

func (it *rbtreeIterator) Key() string {
	if it.current < 0 || it.current >= len(it.keys) {
		return ""
	}
	return it.keys[it.current].(string)
}

func (it *rbtreeIterator) Value() string {
	if it.current < 0 || it.current >= len(it.values) {
		return ""
	}
	return it.values[it.current]
}

func (it *rbtreeIterator) Close() {
	// Nothing to clean up
}
