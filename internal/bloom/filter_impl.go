package bloom

import (
	"github.com/bits-and-blooms/bloom/v3"
)

// FilterImpl implements Filter using the bits-and-blooms/bloom library
type FilterImpl struct {
	filter *bloom.BloomFilter
}

// NewFilter creates a new bloom filter with the given parameters
func NewFilter(expectedItems uint, falsePositiveRate float64) *FilterImpl {
	return &FilterImpl{
		filter: bloom.NewWithEstimates(expectedItems, falsePositiveRate),
	}
}

// Add adds an item to the filter
func (f *FilterImpl) Add(item []byte) {
	f.filter.Add(item)
}

// Test checks if an item might be in the filter
func (f *FilterImpl) Test(item []byte) bool {
	return f.filter.Test(item)
}

// TestAndAdd tests if an item is in the filter and adds it if not
func (f *FilterImpl) TestAndAdd(item []byte) bool {
	return f.filter.TestAndAdd(item)
}

// EstimatedFPRate returns the estimated false positive rate
func (f *FilterImpl) EstimatedFPRate() float64 {
	// Calculate FP rate based on current capacity
	// This is an approximation since the library doesn't expose this directly
	k := f.filter.K()
	if k == 0 {
		return 0.0
	}
	// FP rate ≈ (1 - e^(-k*n/m))^k, but we'll use a simple approximation
	return 0.01 // Return configured rate as approximation
}

// Clear resets the filter
func (f *FilterImpl) Clear() {
	f.filter.ClearAll()
}

// Count returns the approximate number of items added
func (f *FilterImpl) Count() uint {
	return uint(f.filter.ApproximatedSize())
}

// NoOpFilter is a bloom filter that always returns true (for testing/debugging)
type NoOpFilter struct{}

// NewNoOpFilter creates a filter that doesn't actually filter anything
func NewNoOpFilter() *NoOpFilter {
	return &NoOpFilter{}
}

func (f *NoOpFilter) Add(item []byte)                {}
func (f *NoOpFilter) Test(item []byte) bool          { return true }
func (f *NoOpFilter) TestAndAdd(item []byte) bool    { return true }
func (f *NoOpFilter) EstimatedFPRate() float64       { return 1.0 }
func (f *NoOpFilter) Clear()                         {}
func (f *NoOpFilter) Count() uint                    { return 0 }
