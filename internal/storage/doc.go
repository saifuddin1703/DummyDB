// Package storage defines the core storage engine interface for DummyDB.
//
// The storage engine is responsible for persisting and retrieving key-value pairs.
// Different storage engines can be implemented to provide different performance
// characteristics or storage backends.
//
// The primary implementation is the LSM (Log-Structured Merge) tree engine,
// which provides fast writes by buffering in memory and periodically flushing
// to immutable on-disk files.
//
// Example usage:
//
//	engine := lsm.NewEngine(cfg, fileIO, wal, compactor, filter)
//	defer engine.Close()
//
//	err := engine.Put("key", []byte("value"))
//	value, found := engine.Get("key")
//	err = engine.Delete("key")
//	keys := engine.Keys()
//
package storage
