package compaction

import (
	"strings"
)

// Entry represents a key-value entry
type Entry struct {
	Key   string
	Value string
}

// ParseEntry parses a "key:value" string into an Entry
func ParseEntry(s string) Entry {
	parts := strings.SplitN(s, ":", 2)
	if len(parts) != 2 {
		return Entry{Key: parts[0], Value: ""}
	}
	return Entry{Key: parts[0], Value: parts[1]}
}

// MergeTwoSegments merges two sorted segments
// segment1 is older than segment2 - newer values take precedence
func MergeTwoSegments(segment1, segment2 []string) []string {
	i := 0
	j := 0
	n := len(segment1)
	m := len(segment2)

	sortedSegment := make([]string, 0, n+m)

	for i < n && j < m {
		entry1 := segment1[i]
		entry2 := segment2[j]

		if len(entry1) == 0 {
			i++
			continue
		}
		if len(entry2) == 0 {
			j++
			continue
		}

		e1 := ParseEntry(entry1)
		e2 := ParseEntry(entry2)

		if e1.Key == e2.Key {
			// Same key - take newer value (from segment2)
			sortedSegment = append(sortedSegment, entry2)
			i++
			j++
		} else if e1.Key < e2.Key {
			sortedSegment = append(sortedSegment, entry1)
			i++
		} else {
			sortedSegment = append(sortedSegment, entry2)
			j++
		}
	}

	// Append remaining entries from segment1
	for i < n {
		if len(segment1[i]) > 0 {
			sortedSegment = append(sortedSegment, segment1[i])
		}
		i++
	}

	// Append remaining entries from segment2
	for j < m {
		if len(segment2[j]) > 0 {
			sortedSegment = append(sortedSegment, segment2[j])
		}
		j++
	}

	return sortedSegment
}

// MergeMultipleSegments merges multiple segments
// Earlier segments in the array are older
func MergeMultipleSegments(segments [][]string) []string {
	if len(segments) == 0 {
		return []string{}
	}

	if len(segments) == 1 {
		return segments[0]
	}

	// Start with the oldest segment
	result := segments[len(segments)-1]

	// Merge from second-newest to oldest
	for i := len(segments) - 2; i >= 0; i-- {
		result = MergeTwoSegments(segments[i], result)
	}

	return result
}
