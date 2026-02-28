package utils

// DEPRECATED: These constants are kept for backward compatibility.
// New code should use github.com/dummydb/internal/config package instead.

const (
	KB                = 1024
	MB                = 1024 * KB
	MAX_TABLE_COUNT   = 4      // Use config.DefaultMaxTables
	TABLE_SIZE        = 512 * KB // Use config.DefaultMemTableSize
	DELETED_INDICATOR = "__DELETED__" // Use config.DeletedIndicator
)
