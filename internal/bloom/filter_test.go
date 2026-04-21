package bloom

import (
	"fmt"
	"testing"
)

func TestFilterImpl_AddAndTest(t *testing.T) {
	t.Parallel()

	filter := NewFilter(1000, 0.01)

	// Add some items
	items := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
	}

	for _, item := range items {
		filter.Add(item)
	}

	// Test for added items - should all return true
	for _, item := range items {
		if !filter.Test(item) {
			t.Errorf("expected item %s to be in filter", string(item))
		}
	}

	// Test for non-added item - might return false (or true if false positive)
	notAdded := []byte("key999")
	result := filter.Test(notAdded)
	// We can't assert false here because bloom filters can have false positives
	// But we can check that it doesn't crash
	_ = result
}

func TestFilterImpl_TestAndAdd(t *testing.T) {
	t.Parallel()

	filter := NewFilter(1000, 0.01)

	item := []byte("testkey")

	// First TestAndAdd should return false (not in filter)
	if filter.TestAndAdd(item) {
		t.Error("expected first TestAndAdd to return false")
	}

	// Second TestAndAdd should return true (now in filter)
	if !filter.TestAndAdd(item) {
		t.Error("expected second TestAndAdd to return true")
	}
}

func TestFilterImpl_Clear(t *testing.T) {
	t.Parallel()

	filter := NewFilter(1000, 0.01)

	// Add items
	items := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
	}

	for _, item := range items {
		filter.Add(item)
	}

	// All items should be in filter
	for _, item := range items {
		if !filter.Test(item) {
			t.Errorf("expected item %s to be in filter before clear", string(item))
		}
	}

	// Clear the filter
	filter.Clear()

	// After clear, items should not be in filter
	// Note: We need to be careful here - bloom filters might still have false positives
	// But typically after clear, the probability should be very low
	foundAfterClear := 0
	for _, item := range items {
		if filter.Test(item) {
			foundAfterClear++
		}
	}

	// Allow for some false positives, but not all items should be found
	if foundAfterClear == len(items) {
		t.Error("all items still found after clear - filter not properly cleared")
	}
}

func TestFilterImpl_EstimatedFPRate(t *testing.T) {
	t.Parallel()

	filter := NewFilter(1000, 0.01)

	fpRate := filter.EstimatedFPRate()

	// FP rate should be positive
	if fpRate <= 0 {
		t.Errorf("expected positive FP rate, got %f", fpRate)
	}

	// FP rate should be reasonable (less than 1.0)
	if fpRate >= 1.0 {
		t.Errorf("expected FP rate < 1.0, got %f", fpRate)
	}
}

func TestFilterImpl_Count(t *testing.T) {
	t.Parallel()

	filter := NewFilter(1000, 0.01)

	// Add 10 items
	for i := 0; i < 10; i++ {
		filter.Add([]byte(fmt.Sprintf("key%d", i)))
	}

	count := filter.Count()

	// Count should be approximately 10 (allow some error)
	if count < 5 || count > 15 {
		t.Errorf("expected count around 10, got %d", count)
	}
}

func TestNoOpFilter(t *testing.T) {
	t.Parallel()

	filter := NewNoOpFilter()

	// NoOp filter should always return true for Test
	if !filter.Test([]byte("anything")) {
		t.Error("NoOpFilter should always return true")
	}

	// TestAndAdd should also return true
	if !filter.TestAndAdd([]byte("anything")) {
		t.Error("NoOpFilter TestAndAdd should always return true")
	}

	// FP rate should be 1.0
	if filter.EstimatedFPRate() != 1.0 {
		t.Errorf("expected FP rate 1.0, got %f", filter.EstimatedFPRate())
	}

	// Count should be 0
	if filter.Count() != 0 {
		t.Errorf("expected count 0, got %d", filter.Count())
	}

	// Add and Clear should not panic
	filter.Add([]byte("test"))
	filter.Clear()
}

func TestFilterImpl_FalsePositiveRate(t *testing.T) {
	t.Parallel()

	// Test with different parameters
	testCases := []struct {
		name              string
		expectedItems     uint
		falsePositiveRate float64
		itemsToAdd        int
		itemsToTest       int
	}{
		{
			name:              "small filter",
			expectedItems:     100,
			falsePositiveRate: 0.01,
			itemsToAdd:        100,
			itemsToTest:       1000,
		},
		{
			name:              "large filter",
			expectedItems:     10000,
			falsePositiveRate: 0.001,
			itemsToAdd:        10000,
			itemsToTest:       10000,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			filter := NewFilter(tc.expectedItems, tc.falsePositiveRate)

			// Add items
			for i := 0; i < tc.itemsToAdd; i++ {
				filter.Add([]byte(fmt.Sprintf("added_%d", i)))
			}

			// Test for false positives
			falsePositives := 0
			for i := 0; i < tc.itemsToTest; i++ {
				if filter.Test([]byte(fmt.Sprintf("not_added_%d", i))) {
					falsePositives++
				}
			}

			// Calculate actual FP rate
			actualFPRate := float64(falsePositives) / float64(tc.itemsToTest)

			// Actual FP rate should be reasonably close to expected
			// Allow 10x tolerance for small sample sizes
			maxAllowed := tc.falsePositiveRate * 10
			if actualFPRate > maxAllowed {
				t.Errorf("FP rate too high: expected ~%f, got %f (max allowed: %f)",
					tc.falsePositiveRate, actualFPRate, maxAllowed)
			}
		})
	}
}
