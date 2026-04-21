package server

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/dummydb/internal/bloom"
	"github.com/dummydb/internal/compaction"
	"github.com/dummydb/internal/config"
	"github.com/dummydb/internal/database"
	"github.com/dummydb/internal/disk"
	"github.com/dummydb/internal/storage/lsm"
	"github.com/dummydb/internal/storage/sstable"
	"github.com/dummydb/internal/wal"
)

func setupTestServer(t *testing.T) (*Server, func()) {
	t.Helper()

	cfg := config.DefaultConfig()
	cfg.ServerHost = "localhost"
	cfg.ServerPort = 0 // Use random port
	cfg.SegmentDir = fmt.Sprintf("test-segments-%d", time.Now().UnixNano())
	cfg.WALPath = fmt.Sprintf("test-wal-%d", time.Now().UnixNano())
	cfg.MemTableSizeBytes = 10 * 1024 * 1024

	fs := disk.NewMemoryFileIO()
	fs.MkdirAll(cfg.SegmentDir, 0755)

	walImpl, _ := wal.NewWAL(cfg.WALPath, fs)
	filter := bloom.NewFilter(10000, 0.01)

	sstableOpts := sstable.Options{
		FileIO:        fs,
		IndexInterval: cfg.SparseIndexIntervalBytes,
	}

	compactor := compaction.NewLeveledCompactor(cfg, sstableOpts)
	engine, _ := lsm.NewEngine(cfg, fs, walImpl, compactor, filter)

	db, _ := database.NewDatabase(
		database.WithConfig(cfg),
		database.WithStorageEngine(engine),
	)

	server := NewServer(cfg, db)

	cleanup := func() {
		server.Stop()
		db.Close()
	}

	return server, cleanup
}

func TestParseCommand_Set(t *testing.T) {
	t.Parallel()

	cmd, err := ParseCommand("SET key1 value1")
	if err != nil {
		t.Fatalf("failed to parse: %v", err)
	}

	if cmd.Type != CommandSet {
		t.Errorf("expected CommandSet, got %v", cmd.Type)
	}
	if cmd.Key != "key1" {
		t.Errorf("expected key1, got %s", cmd.Key)
	}
	if cmd.Value != "value1" {
		t.Errorf("expected value1, got %s", cmd.Value)
	}
}

func TestParseCommand_Get(t *testing.T) {
	t.Parallel()

	cmd, err := ParseCommand("GET key1")
	if err != nil {
		t.Fatalf("failed to parse: %v", err)
	}

	if cmd.Type != CommandGet {
		t.Errorf("expected CommandGet, got %v", cmd.Type)
	}
	if cmd.Key != "key1" {
		t.Errorf("expected key1, got %s", cmd.Key)
	}
}

func TestParseCommand_Delete(t *testing.T) {
	t.Parallel()

	tests := []string{"DEL key1", "DELETE key1"}

	for _, cmdStr := range tests {
		cmd, err := ParseCommand(cmdStr)
		if err != nil {
			t.Fatalf("failed to parse %s: %v", cmdStr, err)
		}

		if cmd.Type != CommandDelete {
			t.Errorf("expected CommandDelete, got %v", cmd.Type)
		}
		if cmd.Key != "key1" {
			t.Errorf("expected key1, got %s", cmd.Key)
		}
	}
}

func TestParseCommand_Keys(t *testing.T) {
	t.Parallel()

	cmd, err := ParseCommand("KEYS")
	if err != nil {
		t.Fatalf("failed to parse: %v", err)
	}

	if cmd.Type != CommandKeys {
		t.Errorf("expected CommandKeys, got %v", cmd.Type)
	}
}

func TestParseCommand_Invalid(t *testing.T) {
	t.Parallel()

	invalid := []string{
		"",
		"   ",
		"UNKNOWN",
		"SET",            // Missing arguments
		"SET key",        // Missing value
		"GET",            // Missing key
		"GET key1 extra", // Too many arguments
		"DEL",            // Missing key
		"KEYS extra",     // Too many arguments
	}

	for _, cmdStr := range invalid {
		_, err := ParseCommand(cmdStr)
		if err == nil {
			t.Errorf("expected error for command: %s", cmdStr)
		}
	}
}

func TestParseCommand_CaseInsensitive(t *testing.T) {
	t.Parallel()

	commands := []string{"set", "SET", "Set", "SeT"}

	for _, cmdStr := range commands {
		cmd, err := ParseCommand(cmdStr + " key1 value1")
		if err != nil {
			t.Errorf("failed to parse %s: %v", cmdStr, err)
		}

		if cmd.Type != CommandSet {
			t.Errorf("expected CommandSet for %s, got %v", cmdStr, cmd.Type)
		}
	}
}

func TestHandler_Execute(t *testing.T) {
	t.Parallel()

	_, cleanup := setupTestServer(t)
	defer cleanup()

	cfg := config.DefaultConfig()
	cfg.SegmentDir = fmt.Sprintf("test-segments-%d", time.Now().UnixNano())
	cfg.WALPath = fmt.Sprintf("test-wal-%d", time.Now().UnixNano())

	fs := disk.NewMemoryFileIO()
	fs.MkdirAll(cfg.SegmentDir, 0755)

	walImpl, _ := wal.NewWAL(cfg.WALPath, fs)
	filter := bloom.NewFilter(10000, 0.01)

	sstableOpts := sstable.Options{
		FileIO:        fs,
		IndexInterval: cfg.SparseIndexIntervalBytes,
	}

	compactor := compaction.NewLeveledCompactor(cfg, sstableOpts)
	engine, _ := lsm.NewEngine(cfg, fs, walImpl, compactor, filter)

	db, _ := database.NewDatabase(
		database.WithConfig(cfg),
		database.WithStorageEngine(engine),
	)
	defer db.Close()

	handler := NewHandler(db)

	// Test SET
	cmd, _ := ParseCommand("SET key1 value1")
	result, err := handler.Execute(cmd)
	if err != nil {
		t.Errorf("failed to execute SET: %v", err)
	}
	if string(result) != "value1" {
		t.Errorf("expected value1, got %s", result)
	}

	// Test GET
	cmd, _ = ParseCommand("GET key1")
	result, err = handler.Execute(cmd)
	if err != nil {
		t.Errorf("failed to execute GET: %v", err)
	}
	if string(result) != "value1" {
		t.Errorf("expected value1, got %s", result)
	}

	// Test DELETE
	cmd, _ = ParseCommand("DEL key1")
	result, err = handler.Execute(cmd)
	if err != nil {
		t.Errorf("failed to execute DELETE: %v", err)
	}

	// Test KEYS
	db.Put("key2", []byte("value2"))
	cmd, _ = ParseCommand("KEYS")
	result, err = handler.Execute(cmd)
	if err != nil {
		t.Errorf("failed to execute KEYS: %v", err)
	}
	if !strings.Contains(string(result), "key2") {
		t.Errorf("expected key2 in result, got %s", result)
	}
}

func TestServer_StartStop(t *testing.T) {
	t.Parallel()

	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Start server
	err := server.Start()
	if err != nil {
		t.Fatalf("failed to start server: %v", err)
	}

	// Give it a moment to start
	time.Sleep(10 * time.Millisecond)

	// Stop server
	err = server.Stop()
	if err != nil {
		t.Fatalf("failed to stop server: %v", err)
	}

	// Stop again - should be fine
	err = server.Stop()
	if err != nil {
		t.Fatalf("failed to stop again: %v", err)
	}
}

func TestServer_ClientConnection(t *testing.T) {
	t.Parallel()

	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Start server
	err := server.Start()
	if err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer server.Stop()

	// Get the actual port
	addr := server.listener.Addr().String()

	// Connect as client
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	// Send SET command
	_, err = conn.Write([]byte("SET mykey myvalue\n"))
	if err != nil {
		t.Fatalf("failed to write: %v", err)
	}

	// Read response
	response, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}

	response = strings.TrimSpace(response)
	if response != "myvalue" {
		t.Errorf("expected myvalue, got %s", response)
	}

	// Send GET command
	_, err = conn.Write([]byte("GET mykey\n"))
	if err != nil {
		t.Fatalf("failed to write: %v", err)
	}

	// Read response
	response, err = reader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}

	response = strings.TrimSpace(response)
	if response != "myvalue" {
		t.Errorf("expected myvalue, got %s", response)
	}
}

func TestServer_InvalidCommand(t *testing.T) {
	t.Parallel()

	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Start server
	server.Start()
	defer server.Stop()

	addr := server.listener.Addr().String()

	// Connect as client
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	// Send invalid command
	conn.Write([]byte("INVALID\n"))

	// Read response
	response, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}

	if !strings.Contains(response, "ERROR") {
		t.Errorf("expected ERROR in response, got %s", response)
	}
}
