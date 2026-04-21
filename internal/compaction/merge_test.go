package compaction

import (
	"reflect"
	"testing"
)

func TestMergeTwoSegments_Basic(t *testing.T) {
	t.Parallel()

	segment1 := []string{"a:value1", "c:value3"}
	segment2 := []string{"b:value2", "d:value4"}

	merged := MergeTwoSegments(segment1, segment2)

	expected := []string{"a:value1", "b:value2", "c:value3", "d:value4"}

	if !reflect.DeepEqual(merged, expected) {
		t.Errorf("expected %v, got %v", expected, merged)
	}
}

func TestMergeTwoSegments_Overlap(t *testing.T) {
	t.Parallel()

	// segment1 is older, segment2 is newer
	segment1 := []string{"a:old_value", "b:old_value"}
	segment2 := []string{"a:new_value", "c:new_value"}

	merged := MergeTwoSegments(segment1, segment2)

	// Expected: 'a' should have new_value (from segment2)
	expected := []string{"a:new_value", "b:old_value", "c:new_value"}

	if !reflect.DeepEqual(merged, expected) {
		t.Errorf("expected %v, got %v", expected, merged)
	}
}

func TestMergeTwoSegments_EmptyFirst(t *testing.T) {
	t.Parallel()

	segment1 := []string{}
	segment2 := []string{"a:value1", "b:value2"}

	merged := MergeTwoSegments(segment1, segment2)

	expected := []string{"a:value1", "b:value2"}

	if !reflect.DeepEqual(merged, expected) {
		t.Errorf("expected %v, got %v", expected, merged)
	}
}

func TestMergeTwoSegments_EmptySecond(t *testing.T) {
	t.Parallel()

	segment1 := []string{"a:value1", "b:value2"}
	segment2 := []string{}

	merged := MergeTwoSegments(segment1, segment2)

	expected := []string{"a:value1", "b:value2"}

	if !reflect.DeepEqual(merged, expected) {
		t.Errorf("expected %v, got %v", expected, merged)
	}
}

func TestMergeTwoSegments_BothEmpty(t *testing.T) {
	t.Parallel()

	segment1 := []string{}
	segment2 := []string{}

	merged := MergeTwoSegments(segment1, segment2)

	if len(merged) != 0 {
		t.Errorf("expected empty result, got %v", merged)
	}
}

func TestMergeTwoSegments_WithEmptyStrings(t *testing.T) {
	t.Parallel()

	segment1 := []string{"a:value1", "", "c:value3"}
	segment2 := []string{"b:value2", "", "d:value4"}

	merged := MergeTwoSegments(segment1, segment2)

	// Empty strings should be filtered out
	for _, entry := range merged {
		if entry == "" {
			t.Error("merged result should not contain empty strings")
		}
	}
}

func TestMergeTwoSegments_AllSameKeys(t *testing.T) {
	t.Parallel()

	segment1 := []string{"a:old1", "b:old2", "c:old3"}
	segment2 := []string{"a:new1", "b:new2", "c:new3"}

	merged := MergeTwoSegments(segment1, segment2)

	// All values should be from segment2 (newer)
	expected := []string{"a:new1", "b:new2", "c:new3"}

	if !reflect.DeepEqual(merged, expected) {
		t.Errorf("expected %v, got %v", expected, merged)
	}
}

func TestMergeMultipleSegments_Empty(t *testing.T) {
	t.Parallel()

	segments := [][]string{}
	merged := MergeMultipleSegments(segments)

	if len(merged) != 0 {
		t.Errorf("expected empty result, got %v", merged)
	}
}

func TestMergeMultipleSegments_Single(t *testing.T) {
	t.Parallel()

	segments := [][]string{
		{"a:value1", "b:value2"},
	}

	merged := MergeMultipleSegments(segments)

	expected := []string{"a:value1", "b:value2"}

	if !reflect.DeepEqual(merged, expected) {
		t.Errorf("expected %v, got %v", expected, merged)
	}
}

func TestMergeMultipleSegments_Three(t *testing.T) {
	t.Parallel()

	// Oldest to newest
	segments := [][]string{
		{"a:old", "b:old"},       // oldest
		{"a:middle", "c:middle"}, // middle
		{"a:new", "d:new"},       // newest
	}

	merged := MergeMultipleSegments(segments)

	// 'a' should have newest value
	expected := []string{"a:new", "b:old", "c:middle", "d:new"}

	if !reflect.DeepEqual(merged, expected) {
		t.Errorf("expected %v, got %v", expected, merged)
	}
}

func TestMergeMultipleSegments_Complex(t *testing.T) {
	t.Parallel()

	segments := [][]string{
		{"a:1", "c:1", "e:1"},
		{"b:2", "d:2", "e:2"},
		{"a:3", "f:3"},
	}

	merged := MergeMultipleSegments(segments)

	// Expected: a:3 (newest), b:2, c:1, d:2, e:2, f:3
	expected := []string{"a:3", "b:2", "c:1", "d:2", "e:2", "f:3"}

	if !reflect.DeepEqual(merged, expected) {
		t.Errorf("expected %v, got %v", expected, merged)
	}
}

func TestParseEntry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input       string
		expectedKey string
		expectedVal string
	}{
		{"key:value", "key", "value"},
		{"key:value:with:colons", "key", "value:with:colons"},
		{"key:", "key", ""},
		{"key", "key", ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			entry := ParseEntry(tt.input)

			if entry.Key != tt.expectedKey {
				t.Errorf("expected key %s, got %s", tt.expectedKey, entry.Key)
			}

			if entry.Value != tt.expectedVal {
				t.Errorf("expected value %s, got %s", tt.expectedVal, entry.Value)
			}
		})
	}
}

func TestMergeTwoSegments_DeletionMarker(t *testing.T) {
	t.Parallel()

	// Simulate deletion markers
	segment1 := []string{"a:value1", "b:value2"}
	segment2 := []string{"a:__DELETED__"}

	merged := MergeTwoSegments(segment1, segment2)

	// 'a' should have deletion marker
	expected := []string{"a:__DELETED__", "b:value2"}

	if !reflect.DeepEqual(merged, expected) {
		t.Errorf("expected %v, got %v", expected, merged)
	}
}

func TestMergeTwoSegments_LargeDataset(t *testing.T) {
	t.Parallel()

	// Create two large segments
	segment1 := make([]string, 1000)
	segment2 := make([]string, 1000)

	for i := 0; i < 1000; i++ {
		// Even keys in segment1
		if i%2 == 0 {
			segment1[i/2] = string(rune('a'+i%26)) + ":value1"
		}
		// Odd keys in segment2
		if i%2 == 1 {
			segment2[i/2] = string(rune('a'+i%26)) + ":value2"
		}
	}

	// Remove trailing empty entries
	segment1 = segment1[:500]
	segment2 = segment2[:500]

	merged := MergeTwoSegments(segment1, segment2)

	// Result should be sorted and have entries from both
	if len(merged) != 1000 {
		t.Errorf("expected 1000 entries, got %d", len(merged))
	}
}
