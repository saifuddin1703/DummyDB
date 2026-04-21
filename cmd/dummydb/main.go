package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/dummydb/internal/bloom"
	"github.com/dummydb/internal/compaction"
	"github.com/dummydb/internal/config"
	"github.com/dummydb/internal/database"
	"github.com/dummydb/internal/disk"
	"github.com/dummydb/internal/server"
	"github.com/dummydb/internal/storage/lsm"
	"github.com/dummydb/internal/storage/sstable"
	"github.com/dummydb/internal/wal"
)

func main() {
	// Load configuration
	cfg := config.LoadFromEnv()

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		log.Fatalf("Invalid configuration: %v", err)
	}

	fmt.Println("Starting DummyDB...")
	fmt.Printf("Data directory: %s\n", cfg.SegmentDir)
	fmt.Printf("WAL path: %s\n", cfg.WALPath)
	fmt.Printf("Server: %s:%d\n", cfg.ServerHost, cfg.ServerPort)

	// Create file I/O
	fileIO := disk.NewOSFileIO()

	// Create directories
	if err := fileIO.MkdirAll(cfg.SegmentDir, 0755); err != nil {
		log.Fatalf("Failed to create segment directory: %v", err)
	}

	// Create WAL
	walImpl, err := wal.NewWAL(cfg.WALPath, fileIO)
	if err != nil {
		log.Fatalf("Failed to create WAL: %v", err)
	}

	// Create bloom filter
	filter := bloom.NewFilter(cfg.BloomFilterExpectedItems, cfg.BloomFilterFalsePositive)

	// Create SSTable options
	sstableOpts := sstable.Options{
		FileIO:        fileIO,
		IndexInterval: cfg.SparseIndexIntervalBytes,
		IsMerged:      false,
	}

	// Create compactor
	compactor := compaction.NewLeveledCompactor(cfg, sstableOpts)

	// Create LSM engine
	engine, err := lsm.NewEngine(cfg, fileIO, walImpl, compactor, filter)
	if err != nil {
		log.Fatalf("Failed to create storage engine: %v", err)
	}

	// Create database
	db, err := database.NewDatabase(
		database.WithConfig(cfg),
		database.WithStorageEngine(engine),
	)
	if err != nil {
		log.Fatalf("Failed to create database: %v", err)
	}

	// Create server
	srv := server.NewServer(cfg, db)

	// Start server
	if err := srv.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan

	fmt.Println("\nShutting down...")

	// Graceful shutdown
	if err := srv.Stop(); err != nil {
		log.Printf("Error stopping server: %v", err)
	}

	if err := db.Close(); err != nil {
		log.Printf("Error closing database: %v", err)
	}

	fmt.Println("Goodbye!")
}
