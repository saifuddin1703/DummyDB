package disk

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	ErrNotExist     = errors.New("file does not exist")
	ErrAlreadyExist = errors.New("file already exists")
	ErrIsDir        = errors.New("is a directory")
	ErrNotDir       = errors.New("not a directory")
	ErrClosed       = errors.New("file is closed")
)

// MemoryFileIO implements FileIO using in-memory storage for testing
type MemoryFileIO struct {
	mu    sync.RWMutex
	files map[string]*memFile
}

// NewMemoryFileIO creates a new in-memory file system
func NewMemoryFileIO() *MemoryFileIO {
	return &MemoryFileIO{
		files: make(map[string]*memFile),
	}
}

type memFile struct {
	name    string
	data    *bytes.Buffer
	mode    os.FileMode
	modTime time.Time
	isDir   bool
	closed  bool
	mu      sync.RWMutex
}

type memFileHandle struct {
	mf  *memFile
	mfs *MemoryFileIO
	pos int64  // Move pos to handle for per-handle tracking
	mu  sync.Mutex
}

func (m *MemoryFileIO) Open(name string) (File, error) {
	return m.OpenFile(name, os.O_RDONLY, 0)
}

func (m *MemoryFileIO) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	name = filepath.Clean(name)

	mf, exists := m.files[name]

	// Handle create flags
	if flag&os.O_CREATE != 0 {
		if !exists {
			mf = &memFile{
				name:    name,
				data:    new(bytes.Buffer),
				mode:    perm,
				modTime: time.Now(),
				isDir:   false,
				closed:  false,
			}
			m.files[name] = mf
		}
	} else if !exists {
		return nil, fmt.Errorf("%s: %w", name, ErrNotExist)
	}

	if mf.isDir {
		return nil, fmt.Errorf("%s: %w", name, ErrIsDir)
	}

	// Handle truncate flag and set closed status while holding lock
	mf.mu.Lock()
	if flag&os.O_TRUNC != 0 {
		mf.data = new(bytes.Buffer)
		mf.modTime = time.Now()
	}
	mf.closed = false
	dataLen := mf.data.Len()
	mf.mu.Unlock()

	// Set initial position based on flags
	pos := int64(0)
	if flag&os.O_APPEND != 0 {
		pos = int64(dataLen)
	}

	return &memFileHandle{mf: mf, mfs: m, pos: pos}, nil
}

func (m *MemoryFileIO) Create(name string) (File, error) {
	return m.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
}

func (m *MemoryFileIO) Remove(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	name = filepath.Clean(name)

	if _, exists := m.files[name]; !exists {
		return fmt.Errorf("%s: %w", name, ErrNotExist)
	}

	delete(m.files, name)
	return nil
}

func (m *MemoryFileIO) RemoveAll(path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	path = filepath.Clean(path)

	// Remove the path itself and all files under it
	for name := range m.files {
		if name == path || strings.HasPrefix(name, path+string(filepath.Separator)) {
			delete(m.files, name)
		}
	}

	return nil
}

func (m *MemoryFileIO) ReadDir(dirname string) ([]os.DirEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	dirname = filepath.Clean(dirname)
	if dirname == "." {
		dirname = ""
	}

	entries := make(map[string]os.DirEntry)

	for name, mf := range m.files {
		// Check if file is in this directory
		if dirname == "" || strings.HasPrefix(name, dirname+string(filepath.Separator)) {
			rel := name
			if dirname != "" {
				rel = strings.TrimPrefix(name, dirname+string(filepath.Separator))
			}

			// Only include direct children
			parts := strings.Split(rel, string(filepath.Separator))
			if len(parts) > 0 && parts[0] != "" {
				childName := parts[0]
				if _, exists := entries[childName]; !exists {
					isDir := len(parts) > 1
					entries[childName] = &memDirEntry{
						name:  childName,
						isDir: isDir,
						mode:  mf.mode,
					}
				}
			}
		}
	}

	result := make([]os.DirEntry, 0, len(entries))
	for _, entry := range entries {
		result = append(result, entry)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name() < result[j].Name()
	})

	return result, nil
}

func (m *MemoryFileIO) ReadFile(name string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	name = filepath.Clean(name)

	mf, exists := m.files[name]
	if !exists {
		return nil, fmt.Errorf("%s: %w", name, ErrNotExist)
	}

	if mf.isDir {
		return nil, fmt.Errorf("%s: %w", name, ErrIsDir)
	}

	return mf.data.Bytes(), nil
}

func (m *MemoryFileIO) WriteFile(name string, data []byte, perm os.FileMode) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	name = filepath.Clean(name)

	mf := &memFile{
		name:    name,
		data:    bytes.NewBuffer(data),
		mode:    perm,
		modTime: time.Now(),
		isDir:   false,
		closed:  false,
	}

	m.files[name] = mf
	return nil
}

func (m *MemoryFileIO) Stat(name string) (os.FileInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	name = filepath.Clean(name)

	mf, exists := m.files[name]
	if !exists {
		return nil, fmt.Errorf("%s: %w", name, ErrNotExist)
	}

	return &memFileInfo{
		name:    filepath.Base(name),
		size:    int64(mf.data.Len()),
		mode:    mf.mode,
		modTime: mf.modTime,
		isDir:   mf.isDir,
	}, nil
}

func (m *MemoryFileIO) MkdirAll(path string, perm os.FileMode) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	path = filepath.Clean(path)

	// Create directory entry
	mf := &memFile{
		name:    path,
		data:    new(bytes.Buffer),
		mode:    perm | os.ModeDir,
		modTime: time.Now(),
		isDir:   true,
		closed:  false,
	}

	m.files[path] = mf
	return nil
}

func (m *MemoryFileIO) Rename(oldpath, newpath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	oldpath = filepath.Clean(oldpath)
	newpath = filepath.Clean(newpath)

	mf, exists := m.files[oldpath]
	if !exists {
		return fmt.Errorf("%s: %w", oldpath, ErrNotExist)
	}

	mf.name = newpath
	m.files[newpath] = mf
	delete(m.files, oldpath)

	return nil
}

// memFileHandle methods

func (h *memFileHandle) Read(p []byte) (n int, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.mf.mu.RLock()
	defer h.mf.mu.RUnlock()

	if h.mf.closed {
		return 0, ErrClosed
	}

	data := h.mf.data.Bytes()
	if h.pos >= int64(len(data)) {
		return 0, io.EOF
	}

	n = copy(p, data[h.pos:])
	h.pos += int64(n)
	return n, nil
}

func (h *memFileHandle) Write(p []byte) (n int, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.mf.mu.Lock()
	defer h.mf.mu.Unlock()

	if h.mf.closed {
		return 0, ErrClosed
	}

	// For append mode or writing at end
	if h.pos >= int64(h.mf.data.Len()) {
		n, err = h.mf.data.Write(p)
		h.pos = int64(h.mf.data.Len())
	} else {
		// Writing in the middle - need to handle this
		data := h.mf.data.Bytes()
		newData := make([]byte, len(data))
		copy(newData, data)

		// Extend if necessary
		needed := int(h.pos) + len(p)
		if needed > len(newData) {
			newData = append(newData, make([]byte, needed-len(newData))...)
		}

		copy(newData[h.pos:], p)
		h.mf.data = bytes.NewBuffer(newData)
		n = len(p)
		h.pos += int64(n)
	}

	h.mf.modTime = time.Now()
	return n, err
}

func (h *memFileHandle) WriteString(s string) (int, error) {
	return h.Write([]byte(s))
}

func (h *memFileHandle) Seek(offset int64, whence int) (int64, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.mf.mu.RLock()
	defer h.mf.mu.RUnlock()

	if h.mf.closed {
		return 0, ErrClosed
	}

	var newPos int64
	switch whence {
	case io.SeekStart:
		newPos = offset
	case io.SeekCurrent:
		newPos = h.pos + offset
	case io.SeekEnd:
		newPos = int64(h.mf.data.Len()) + offset
	default:
		return 0, errors.New("invalid whence")
	}

	if newPos < 0 {
		return 0, errors.New("negative position")
	}

	h.pos = newPos
	return newPos, nil
}

func (h *memFileHandle) Close() error {
	h.mf.mu.Lock()
	defer h.mf.mu.Unlock()

	h.mf.closed = true
	return nil
}

func (h *memFileHandle) Sync() error {
	// No-op for in-memory
	return nil
}

func (h *memFileHandle) Name() string {
	return h.mf.name
}

func (h *memFileHandle) Stat() (os.FileInfo, error) {
	h.mf.mu.RLock()
	defer h.mf.mu.RUnlock()

	return &memFileInfo{
		name:    filepath.Base(h.mf.name),
		size:    int64(h.mf.data.Len()),
		mode:    h.mf.mode,
		modTime: h.mf.modTime,
		isDir:   h.mf.isDir,
	}, nil
}

func (h *memFileHandle) ReadDir(n int) ([]fs.DirEntry, error) {
	if !h.mf.isDir {
		return nil, ErrNotDir
	}
	return h.mfs.ReadDir(h.mf.name)
}

// memFileInfo implements os.FileInfo

type memFileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
}

func (fi *memFileInfo) Name() string       { return fi.name }
func (fi *memFileInfo) Size() int64        { return fi.size }
func (fi *memFileInfo) Mode() os.FileMode  { return fi.mode }
func (fi *memFileInfo) ModTime() time.Time { return fi.modTime }
func (fi *memFileInfo) IsDir() bool        { return fi.isDir }
func (fi *memFileInfo) Sys() any   { return nil }

// memDirEntry implements os.DirEntry

type memDirEntry struct {
	name  string
	isDir bool
	mode  os.FileMode
}

func (de *memDirEntry) Name() string               { return de.name }
func (de *memDirEntry) IsDir() bool                { return de.isDir }
func (de *memDirEntry) Type() fs.FileMode          { return de.mode.Type() }
func (de *memDirEntry) Info() (fs.FileInfo, error) {
	return &memFileInfo{
		name:  de.name,
		mode:  de.mode,
		isDir: de.isDir,
	}, nil
}
