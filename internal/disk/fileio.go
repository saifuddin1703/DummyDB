package disk

import (
	"io"
	"io/fs"
	"os"
)

// FileIO abstracts file system operations for testability
type FileIO interface {
	// Open opens a file for reading
	Open(name string) (File, error)

	// OpenFile opens a file with specific flags and permissions
	OpenFile(name string, flag int, perm os.FileMode) (File, error)

	// Create creates or truncates a file
	Create(name string) (File, error)

	// Remove removes a file
	Remove(name string) error

	// RemoveAll removes a directory and all its contents
	RemoveAll(path string) error

	// ReadDir reads the directory and returns directory entries
	ReadDir(dirname string) ([]os.DirEntry, error)

	// ReadFile reads the entire file
	ReadFile(name string) ([]byte, error)

	// WriteFile writes data to a file
	WriteFile(name string, data []byte, perm os.FileMode) error

	// Stat returns file info
	Stat(name string) (os.FileInfo, error)

	// MkdirAll creates a directory and all parent directories
	MkdirAll(path string, perm os.FileMode) error

	// Rename renames a file
	Rename(oldpath, newpath string) error
}

// File abstracts file operations
type File interface {
	io.Reader
	io.Writer
	io.Closer
	io.Seeker

	// Sync commits the current contents of the file
	Sync() error

	// Name returns the name of the file
	Name() string

	// Stat returns file info
	Stat() (os.FileInfo, error)

	// ReadDir reads the directory (for directories opened with Open)
	ReadDir(n int) ([]fs.DirEntry, error)

	// WriteString writes a string to the file
	WriteString(s string) (int, error)
}
