package testutil

import (
	"fmt"
	"math/rand"
	"time"
)

// GenerateRandomKey generates a random key with the given prefix
func GenerateRandomKey(prefix string) string {
	return fmt.Sprintf("%s_%d_%d", prefix, time.Now().UnixNano(), rand.Int())
}

// GenerateRandomValue generates a random value of the given size
func GenerateRandomValue(size int) []byte {
	value := make([]byte, size)
	for i := range value {
		value[i] = byte(rand.Intn(256))
	}
	return value
}

// GenerateKeyValuePairs generates n key-value pairs with the given prefix
func GenerateKeyValuePairs(n int, prefix string, valueSize int) map[string][]byte {
	pairs := make(map[string][]byte, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("%s_key_%05d", prefix, i)
		value := []byte(fmt.Sprintf("%s_value_%05d", prefix, i))
		if valueSize > 0 {
			value = GenerateRandomValue(valueSize)
		}
		pairs[key] = value
	}
	return pairs
}

// GenerateSequentialKeys generates n sequential keys with the given prefix
func GenerateSequentialKeys(n int, prefix string) []string {
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = fmt.Sprintf("%s_%05d", prefix, i)
	}
	return keys
}

// BenchmarkData represents data for benchmarking
type BenchmarkData struct {
	Keys   []string
	Values [][]byte
}

// GenerateBenchmarkData generates benchmark data
func GenerateBenchmarkData(numKeys int, valueSize int) *BenchmarkData {
	data := &BenchmarkData{
		Keys:   make([]string, numKeys),
		Values: make([][]byte, numKeys),
	}

	for i := 0; i < numKeys; i++ {
		data.Keys[i] = fmt.Sprintf("bench_key_%08d", i)
		data.Values[i] = GenerateRandomValue(valueSize)
	}

	return data
}

// AssertKeyExists checks if a key exists with the expected value
type AssertFunc func(t TestingT, condition bool, msgAndArgs ...any)

// TestingT is an interface that wraps testing.T methods we need
type TestingT interface {
	Errorf(format string, args ...any)
	Fatalf(format string, args ...any)
	Helper()
}

// AssertNoError asserts that an error is nil
func AssertNoError(t TestingT, err error, msgAndArgs ...any) {
	t.Helper()
	if err != nil {
		msg := "expected no error"
		if len(msgAndArgs) > 0 {
			msg = fmt.Sprintf(msgAndArgs[0].(string), msgAndArgs[1:]...)
		}
		t.Fatalf("%s, got: %v", msg, err)
	}
}

// AssertError asserts that an error is not nil
func AssertError(t TestingT, err error, msgAndArgs ...any) {
	t.Helper()
	if err == nil {
		msg := "expected an error"
		if len(msgAndArgs) > 0 {
			msg = fmt.Sprintf(msgAndArgs[0].(string), msgAndArgs[1:]...)
		}
		t.Fatalf("%s, but got nil", msg)
	}
}

// AssertEqual asserts that two values are equal
func AssertEqual(t TestingT, expected, actual any, msgAndArgs ...any) {
	t.Helper()
	if expected != actual {
		msg := "values not equal"
		if len(msgAndArgs) > 0 {
			msg = fmt.Sprintf(msgAndArgs[0].(string), msgAndArgs[1:]...)
		}
		t.Errorf("%s: expected %v, got %v", msg, expected, actual)
	}
}

// AssertTrue asserts that a condition is true
func AssertTrue(t TestingT, condition bool, msgAndArgs ...any) {
	t.Helper()
	if !condition {
		msg := "expected condition to be true"
		if len(msgAndArgs) > 0 {
			msg = fmt.Sprintf(msgAndArgs[0].(string), msgAndArgs[1:]...)
		}
		t.Errorf("%s", msg)
	}
}

// AssertFalse asserts that a condition is false
func AssertFalse(t TestingT, condition bool, msgAndArgs ...any) {
	t.Helper()
	if condition {
		msg := "expected condition to be false"
		if len(msgAndArgs) > 0 {
			msg = fmt.Sprintf(msgAndArgs[0].(string), msgAndArgs[1:]...)
		}
		t.Errorf("%s", msg)
	}
}

// AssertBytesEqual asserts that two byte slices are equal
func AssertBytesEqual(t TestingT, expected, actual []byte, msgAndArgs ...any) {
	t.Helper()
	if len(expected) != len(actual) {
		msg := "byte slices have different lengths"
		if len(msgAndArgs) > 0 {
			msg = fmt.Sprintf(msgAndArgs[0].(string), msgAndArgs[1:]...)
		}
		t.Errorf("%s: expected %d bytes, got %d bytes", msg, len(expected), len(actual))
		return
	}

	for i := range expected {
		if expected[i] != actual[i] {
			msg := "byte slices differ"
			if len(msgAndArgs) > 0 {
				msg = fmt.Sprintf(msgAndArgs[0].(string), msgAndArgs[1:]...)
			}
			t.Errorf("%s at index %d: expected %d, got %d", msg, i, expected[i], actual[i])
			return
		}
	}
}
