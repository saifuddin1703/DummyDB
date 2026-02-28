package disk

import (
	"io/fs"
	"os"
)

// OSFileIO implements FileIO using the os package
type OSFileIO struct{}

// NewOSFileIO creates a new OSFileIO instance
func NewOSFileIO() *OSFileIO {
	return &OSFileIO{}
}

func (o *OSFileIO) Open(name string) (File, error) {
	return os.Open(name)
}

func (o *OSFileIO) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	return os.OpenFile(name, flag, perm)
}

func (o *OSFileIO) Create(name string) (File, error) {
	return os.Create(name)
}

func (o *OSFileIO) Remove(name string) error {
	return os.Remove(name)
}

func (o *OSFileIO) RemoveAll(path string) error {
	return os.RemoveAll(path)
}

func (o *OSFileIO) ReadDir(dirname string) ([]os.DirEntry, error) {
	return os.ReadDir(dirname)
}

func (o *OSFileIO) ReadFile(name string) ([]byte, error) {
	return os.ReadFile(name)
}

func (o *OSFileIO) WriteFile(name string, data []byte, perm os.FileMode) error {
	return os.WriteFile(name, data, perm)
}

func (o *OSFileIO) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

func (o *OSFileIO) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

func (o *OSFileIO) Rename(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

// osFile wraps os.File to implement our File interface
type osFile struct {
	*os.File
}

func (f *osFile) ReadDir(n int) ([]fs.DirEntry, error) {
	return f.File.ReadDir(n)
}
