package bloom

// Filter defines the bloom filter interface
type Filter interface {
	// Add adds an item to the filter
	Add(item []byte)

	// Test checks if an item might be in the filter
	// Returns true if the item might be present (could be false positive)
	// Returns false if the item is definitely not present
	Test(item []byte) bool

	// TestAndAdd tests if an item is in the filter and adds it if not
	TestAndAdd(item []byte) bool

	// EstimatedFPRate returns the estimated false positive rate
	EstimatedFPRate() float64

	// Clear resets the filter
	Clear()

	// Count returns the approximate number of items added
	Count() uint
}
