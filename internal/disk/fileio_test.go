package disk

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// TestFileIOImplementations tests both OS and Memory implementations
func TestFileIOImplementations(t *testing.T) {
	implementations := map[string]func() (FileIO, func()){
		"Memory": func() (FileIO, func()) {
			return NewMemoryFileIO(), func() {}
		},
		"OS": func() (FileIO, func()) {
			tmpDir := t.TempDir()
			fs := NewOSFileIO()
			return fs, func() {
				os.RemoveAll(tmpDir)
			}
		},
	}

	for name, setupFn := range implementations {
		t.Run(name, func(t *testing.T) {
			fs, cleanup := setupFn()
			defer cleanup()

			testFileIO(t, fs)
		})
	}
}

func testFileIO(t *testing.T, fs FileIO) {
	t.Run("WriteAndReadFile", func(t *testing.T) {
		testData := []byte("hello world")
		filename := "test.txt"

		err := fs.WriteFile(filename, testData, 0644)
		if err != nil {
			t.Fatalf("WriteFile failed: %v", err)
		}

		data, err := fs.ReadFile(filename)
		if err != nil {
			t.Fatalf("ReadFile failed: %v", err)
		}

		if !bytes.Equal(data, testData) {
			t.Errorf("expected %s, got %s", testData, data)
		}
	})

	t.Run("OpenCreateWrite", func(t *testing.T) {
		filename := "test2.txt"

		file, err := fs.Create(filename)
		if err != nil {
			t.Fatalf("Create failed: %v", err)
		}

		testData := []byte("test data")
		n, err := file.Write(testData)
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
		if n != len(testData) {
			t.Errorf("expected to write %d bytes, wrote %d", len(testData), n)
		}

		err = file.Close()
		if err != nil {
			t.Fatalf("Close failed: %v", err)
		}

		// Read it back
		data, err := fs.ReadFile(filename)
		if err != nil {
			t.Fatalf("ReadFile failed: %v", err)
		}

		if !bytes.Equal(data, testData) {
			t.Errorf("expected %s, got %s", testData, data)
		}
	})

	t.Run("AppendMode", func(t *testing.T) {
		filename := "append.txt"

		// Write initial data
		err := fs.WriteFile(filename, []byte("first"), 0644)
		if err != nil {
			t.Fatalf("WriteFile failed: %v", err)
		}

		// Append more data
		file, err := fs.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			t.Fatalf("OpenFile failed: %v", err)
		}

		_, err = file.WriteString(" second")
		if err != nil {
			t.Fatalf("WriteString failed: %v", err)
		}
		file.Close()

		// Read back
		data, err := fs.ReadFile(filename)
		if err != nil {
			t.Fatalf("ReadFile failed: %v", err)
		}

		expected := "first second"
		if string(data) != expected {
			t.Errorf("expected %s, got %s", expected, string(data))
		}
	})

	t.Run("Remove", func(t *testing.T) {
		filename := "remove.txt"

		err := fs.WriteFile(filename, []byte("delete me"), 0644)
		if err != nil {
			t.Fatalf("WriteFile failed: %v", err)
		}

		err = fs.Remove(filename)
		if err != nil {
			t.Fatalf("Remove failed: %v", err)
		}

		_, err = fs.ReadFile(filename)
		if err == nil {
			t.Error("expected error reading removed file, got nil")
		}
	})

	t.Run("Stat", func(t *testing.T) {
		filename := "stat.txt"
		testData := []byte("test data for stat")

		err := fs.WriteFile(filename, testData, 0644)
		if err != nil {
			t.Fatalf("WriteFile failed: %v", err)
		}

		info, err := fs.Stat(filename)
		if err != nil {
			t.Fatalf("Stat failed: %v", err)
		}

		if info.Name() != filename && info.Name() != filepath.Base(filename) {
			t.Errorf("expected name %s, got %s", filename, info.Name())
		}

		if info.Size() != int64(len(testData)) {
			t.Errorf("expected size %d, got %d", len(testData), info.Size())
		}

		if info.IsDir() {
			t.Error("expected file, got directory")
		}
	})

	t.Run("ReadWriteSeek", func(t *testing.T) {
		filename := "seek.txt"

		// Create and write
		file, err := fs.Create(filename)
		if err != nil {
			t.Fatalf("Create failed: %v", err)
		}

		testData := []byte("0123456789")
		file.Write(testData)
		file.Close()

		// Open for reading and seek
		file, err = fs.Open(filename)
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		defer file.Close()

		// Seek to position 5
		pos, err := file.Seek(5, io.SeekStart)
		if err != nil {
			t.Fatalf("Seek failed: %v", err)
		}
		if pos != 5 {
			t.Errorf("expected position 5, got %d", pos)
		}

		// Read from position 5
		buf := make([]byte, 3)
		n, err := file.Read(buf)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		if n != 3 {
			t.Errorf("expected to read 3 bytes, read %d", n)
		}
		if string(buf) != "567" {
			t.Errorf("expected '567', got '%s'", string(buf))
		}
	})
}

func TestMemoryFileIO_MkdirAll(t *testing.T) {
	t.Parallel()

	fs := NewMemoryFileIO()

	err := fs.MkdirAll("dir1/dir2/dir3", 0755)
	if err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	// Write a file in the directory
	err = fs.WriteFile("dir1/dir2/dir3/test.txt", []byte("test"), 0644)
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Read it back
	data, err := fs.ReadFile("dir1/dir2/dir3/test.txt")
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	if string(data) != "test" {
		t.Errorf("expected 'test', got %s", string(data))
	}
}

func TestMemoryFileIO_ReadDir(t *testing.T) {
	t.Parallel()

	fs := NewMemoryFileIO()

	// Create some files
	fs.WriteFile("file1.txt", []byte("1"), 0644)
	fs.WriteFile("file2.txt", []byte("2"), 0644)
	fs.WriteFile("dir/file3.txt", []byte("3"), 0644)

	entries, err := fs.ReadDir(".")
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	expectedNames := map[string]bool{
		"file1.txt": true,
		"file2.txt": true,
		"dir":       true,
	}

	if len(entries) != len(expectedNames) {
		t.Errorf("expected %d entries, got %d", len(expectedNames), len(entries))
	}

	for _, entry := range entries {
		if !expectedNames[entry.Name()] {
			t.Errorf("unexpected entry: %s", entry.Name())
		}
	}
}

func TestMemoryFileIO_RemoveAll(t *testing.T) {
	t.Parallel()

	fs := NewMemoryFileIO()

	// Create directory structure
	fs.WriteFile("dir/subdir/file1.txt", []byte("1"), 0644)
	fs.WriteFile("dir/subdir/file2.txt", []byte("2"), 0644)
	fs.WriteFile("dir/file3.txt", []byte("3"), 0644)

	err := fs.RemoveAll("dir")
	if err != nil {
		t.Fatalf("RemoveAll failed: %v", err)
	}

	// Try to read files - should fail
	_, err = fs.ReadFile("dir/subdir/file1.txt")
	if err == nil {
		t.Error("expected error reading removed file")
	}
}

func TestMemoryFileIO_Rename(t *testing.T) {
	t.Parallel()

	fs := NewMemoryFileIO()

	testData := []byte("test data")
	err := fs.WriteFile("old.txt", testData, 0644)
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	err = fs.Rename("old.txt", "new.txt")
	if err != nil {
		t.Fatalf("Rename failed: %v", err)
	}

	// Old file should not exist
	_, err = fs.ReadFile("old.txt")
	if err == nil {
		t.Error("expected error reading old file")
	}

	// New file should exist with same content
	data, err := fs.ReadFile("new.txt")
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	if !bytes.Equal(data, testData) {
		t.Errorf("expected %s, got %s", testData, data)
	}
}
