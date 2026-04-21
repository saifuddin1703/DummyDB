package testutil

import (
	"sync"

	"github.com/dummydb/internal/bloom"
	"github.com/dummydb/internal/compaction"
	"github.com/dummydb/internal/config"
	"github.com/dummydb/internal/storage"
	"github.com/dummydb/internal/storage/lsm"
	"github.com/dummydb/internal/storage/sstable"
	"github.com/dummydb/internal/wal"
)

// MockEngine is a mock implementation of storage.Engine
type MockEngine struct {
	mu      sync.RWMutex
	data    map[string][]byte
	PutErr  error
	GetErr  error
	DelErr  error
	Closed  bool
	PutCalls int
	GetCalls int
	DelCalls int
}

func NewMockEngine() *MockEngine {
	return &MockEngine{
		data: make(map[string][]byte),
	}
}

func (m *MockEngine) Put(key string, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.PutCalls++
	if m.PutErr != nil {
		return m.PutErr
	}
	m.data[key] = append([]byte(nil), value...)
	return nil
}

func (m *MockEngine) Get(key string) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.GetCalls++
	val, found := m.data[key]
	if !found {
		return nil, false
	}
	return append([]byte(nil), val...), true
}

func (m *MockEngine) Delete(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.DelCalls++
	if m.DelErr != nil {
		return m.DelErr
	}
	delete(m.data, key)
	return nil
}

func (m *MockEngine) Keys() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	return keys
}

func (m *MockEngine) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Closed = true
	return nil
}

// MockWAL is a mock implementation of wal.WAL
type MockWAL struct {
	mu           sync.Mutex
	entries      []*wal.Entry
	AppendErr    error
	RecoverErr   error
	TruncateErr  error
	AppendCalls  int
	RecoverCalls int
	path         string
}

func NewMockWAL(path string) *MockWAL {
	return &MockWAL{
		entries: make([]*wal.Entry, 0),
		path:    path,
	}
}

func (m *MockWAL) Append(entry *wal.Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.AppendCalls++
	if m.AppendErr != nil {
		return m.AppendErr
	}
	m.entries = append(m.entries, entry)
	return nil
}

func (m *MockWAL) Sync() error {
	return nil
}

func (m *MockWAL) Recover() ([]*wal.Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.RecoverCalls++
	if m.RecoverErr != nil {
		return nil, m.RecoverErr
	}
	return m.entries, nil
}

func (m *MockWAL) Truncate() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.TruncateErr != nil {
		return m.TruncateErr
	}
	m.entries = make([]*wal.Entry, 0)
	return nil
}

func (m *MockWAL) Close() error {
	return nil
}

func (m *MockWAL) Path() string {
	return m.path
}

// MockCompactor is a mock implementation of compaction.Compactor
type MockCompactor struct {
	mu              sync.Mutex
	CompactErr      error
	ShouldCompactFn func(int) bool
	Started         bool
	Stopped         bool
	CompactCalls    int
}

func NewMockCompactor() *MockCompactor {
	return &MockCompactor{
		ShouldCompactFn: func(count int) bool { return count >= 4 },
	}
}

func (m *MockCompactor) Compact(tables []sstable.SSTable) (sstable.SSTable, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.CompactCalls++
	if m.CompactErr != nil {
		return nil, m.CompactErr
	}
	// Return the first table as a mock merged table
	if len(tables) > 0 {
		return tables[0], nil
	}
	return nil, nil
}

func (m *MockCompactor) ShouldCompact(tableCount int) bool {
	if m.ShouldCompactFn != nil {
		return m.ShouldCompactFn(tableCount)
	}
	return tableCount >= 4
}

func (m *MockCompactor) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Started = true
	return nil
}

func (m *MockCompactor) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Stopped = true
	return nil
}

// MockFilter is a mock implementation of bloom.Filter
type MockFilter struct {
	mu       sync.RWMutex
	items    map[string]bool
	TestFn   func([]byte) bool
	AddCalls int
	TestCalls int
}

func NewMockFilter() *MockFilter {
	return &MockFilter{
		items: make(map[string]bool),
	}
}

func (m *MockFilter) Add(item []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.AddCalls++
	m.items[string(item)] = true
}

func (m *MockFilter) Test(item []byte) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.TestCalls++
	if m.TestFn != nil {
		return m.TestFn(item)
	}
	return m.items[string(item)]
}

func (m *MockFilter) TestAndAdd(item []byte) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	found := m.items[string(item)]
	m.items[string(item)] = true
	return found
}

func (m *MockFilter) EstimatedFPRate() float64 {
	return 0.01
}

func (m *MockFilter) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items = make(map[string]bool)
}

func (m *MockFilter) Count() uint {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return uint(len(m.items))
}

// MockMemtable is a mock implementation of lsm.Memtable
type MockMemtable struct {
	mu       sync.RWMutex
	data     map[string]string
	size     int64
	PutCalls int
	GetCalls int
}

func NewMockMemtable() *MockMemtable {
	return &MockMemtable{
		data: make(map[string]string),
	}
}

func (m *MockMemtable) Put(key string, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.PutCalls++
	m.data[key] = value
	m.size += int64(len(key) + len(value))
	return nil
}

func (m *MockMemtable) Get(key string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.GetCalls++
	val, found := m.data[key]
	return val, found
}

func (m *MockMemtable) Delete(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = config.DeletedIndicator
	return nil
}

func (m *MockMemtable) Size() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

func (m *MockMemtable) Iterator() lsm.MemtableIterator {
	m.mu.RLock()
	defer m.mu.RUnlock()

	keys := make([]string, 0, len(m.data))
	values := make([]string, 0, len(m.data))
	for k, v := range m.data {
		keys = append(keys, k)
		values = append(values, v)
	}

	return &mockMemtableIterator{
		keys:    keys,
		values:  values,
		current: -1,
	}
}

func (m *MockMemtable) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = make(map[string]string)
	m.size = 0
}

func (m *MockMemtable) IsEmpty() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.data) == 0
}

type mockMemtableIterator struct {
	keys    []string
	values  []string
	current int
}

func (it *mockMemtableIterator) Next() bool {
	it.current++
	return it.current < len(it.keys)
}

func (it *mockMemtableIterator) Key() string {
	if it.current < 0 || it.current >= len(it.keys) {
		return ""
	}
	return it.keys[it.current]
}

func (it *mockMemtableIterator) Value() string {
	if it.current < 0 || it.current >= len(it.values) {
		return ""
	}
	return it.values[it.current]
}

func (it *mockMemtableIterator) Close() {}

// Verify interfaces are implemented
var _ storage.Engine = (*MockEngine)(nil)
var _ wal.WAL = (*MockWAL)(nil)
var _ compaction.Compactor = (*MockCompactor)(nil)
var _ bloom.Filter = (*MockFilter)(nil)
var _ lsm.Memtable = (*MockMemtable)(nil)
var _ lsm.MemtableIterator = (*mockMemtableIterator)(nil)
