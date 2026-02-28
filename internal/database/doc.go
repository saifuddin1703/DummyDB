// Package database provides the application-level database orchestration layer.
//
// The Database interface coordinates between the storage engine, write-ahead log,
// and compaction to provide a unified API for database operations. It handles
// lifecycle management, error handling, and ensures consistency across components.
//
// Example usage:
//
//	db, err := database.NewDatabase(
//	    database.WithConfig(cfg),
//	    database.WithStorageEngine(engine),
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer db.Close()
//
//	// Write
//	err = db.Put("user:1", []byte("alice"))
//
//	// Read
//	value, err := db.Get("user:1")
//
//	// Delete
//	err = db.Delete("user:1")
//
//	// List keys
//	keys, err := db.Keys()
//
// The database uses functional options for configuration, allowing flexible
// setup while maintaining sensible defaults.
package database
