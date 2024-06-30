package lsmtree

import (
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"

	"github.com/emirpasic/gods/trees/redblacktree"
)

func TestLSMTree_PutAndFind(t *testing.T) {
	LSMT := &LSMTree{
		MemCache:        redblacktree.NewWithStringComparator(),
		SizeChannel:     make(chan int),
		Tables:          make([]*SSTable, 0),
		LSMLock:         &sync.RWMutex{},
		MergerSemaphore: make(chan int, 1),
	}

	key := "key1"
	value := "value1"
	LSMT.Put(key, value)

	foundValue, err := LSMT.Find(key)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if string(foundValue) != value {
		t.Fatalf("expected %s, got %s", value, string(foundValue))
	}
}

func TestLSMTree_ConvertMemCacheToSSTable(t *testing.T) {
	LSMT := &LSMTree{
		MemCache:        redblacktree.NewWithStringComparator(),
		SizeChannel:     make(chan int),
		Tables:          make([]*SSTable, 0),
		LSMLock:         &sync.RWMutex{},
		MergerSemaphore: make(chan int, 1),
	}

	key := "key1"
	value := "value1"
	walLocation := "test_wal.log"
	os.WriteFile(walLocation, []byte(key+":"+value+";"), 0644)

	LSMT.BuildMemCacheFromWAL(walLocation)
	LSMT.ConvertMemCacheToSSTable(walLocation)

	foundValue, err := LSMT.Find(key)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if string(foundValue) != value {
		t.Fatalf("expected %s, got %s", value, string(foundValue))
	}

	// Clean up
	os.Remove(walLocation)
	os.Remove(LSMT.Tables[0].filePath)
}

func TestLSMTree_MergeSSTables(t *testing.T) {
	LSMT := &LSMTree{
		MemCache:        redblacktree.NewWithStringComparator(),
		SizeChannel:     make(chan int),
		Tables:          make([]*SSTable, 0),
		LSMLock:         &sync.RWMutex{},
		MergerSemaphore: make(chan int, 1),
	}

	// Create SSTables for testing
	sst1 := &SSTable{
		KeyMap:      map[string]int64{"key1": 0, "key2": 10},
		filePath:    "test_segment1.sst",
		SegmentSize: 20,
	}
	sst2 := &SSTable{
		KeyMap:      map[string]int64{"key2": 0, "key3": 10},
		filePath:    "test_segment2.sst",
		SegmentSize: 20,
	}
	os.WriteFile(sst1.filePath, []byte("key1:value1;key2:value2;"), 0644)
	os.WriteFile(sst2.filePath, []byte("key2:value2_new;key3:value3;"), 0644)

	LSMT.Tables = append(LSMT.Tables, sst1, sst2)

	err := LSMT.MergeSSTables()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	fmt.Println("talbes ", LSMT.Tables)
	foundValue, err := LSMT.Find("key2")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if string(foundValue) != "value2_new" {
		t.Fatalf("expected value2_new, got %s", string(foundValue))
	}

	// Clean up
	os.Remove(sst1.filePath)
	os.Remove(sst2.filePath)
	if len(LSMT.Tables) > 0 {
		os.Remove(LSMT.Tables[0].filePath)
	}
}

func TestLSMTree_GetAllKeys(t *testing.T) {
	LSMT := &LSMTree{
		MemCache:        redblacktree.NewWithStringComparator(),
		SizeChannel:     make(chan int),
		Tables:          make([]*SSTable, 0),
		LSMLock:         &sync.RWMutex{},
		MergerSemaphore: make(chan int, 1),
	}

	// Add some keys to MemCache
	LSMT.Put("key1", "value1")
	LSMT.Put("key2", "value2")

	// Create SSTables for testing
	sst := &SSTable{
		KeyMap:      map[string]int64{"key3": 0, "key4": 10},
		filePath:    "test_segment.sst",
		SegmentSize: 20,
	}
	os.WriteFile(sst.filePath, []byte("key3:value3;key4:value4;"), 0644)

	LSMT.Tables = append(LSMT.Tables, sst)

	keys, err := LSMT.GetAllKeys()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expectedKeys := map[string]bool{"key1": true, "key2": true, "key3": true, "key4": true}
	for _, key := range keys {
		if !expectedKeys[key] {
			t.Fatalf("unexpected key %s found", key)
		}
	}

	// Clean up
	os.Remove(sst.filePath)
}

func TestLSMTree_BuildMemCacheFromWAL(t *testing.T) {
	LSMT := &LSMTree{
		MemCache:        redblacktree.NewWithStringComparator(),
		SizeChannel:     make(chan int),
		Tables:          make([]*SSTable, 0),
		LSMLock:         &sync.RWMutex{},
		MergerSemaphore: make(chan int, 1),
	}

	walLocation := "test_wal.log"
	os.WriteFile(walLocation, []byte("key1:value1;key2:value2;"), 0644)

	LSMT.BuildMemCacheFromWAL(walLocation)

	value1, found1 := LSMT.MemCache.Get("key1")
	if !found1 || value1.(string) != "value1" {
		t.Fatalf("expected key1:value1, got key1:%v", value1)
	}

	value2, found2 := LSMT.MemCache.Get("key2")
	if !found2 || value2.(string) != "value2" {
		t.Fatalf("expected key2:value2, got key2:%v", value2)
	}

	// Clean up
	os.Remove(walLocation)
}

func Test_mergeTwoSegments(t *testing.T) {
	type args struct {
		segment1 []string
		segment2 []string
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		// TODO: Add test cases.
		{name: "pass", args: args{
			segment1: []string{"test:key"},
			segment2: []string{"tes2:2"},
		}, want: []string{"test:key", "tes2:2"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mergeTwoSegments(tt.args.segment1, tt.args.segment2); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mergeTwoSegments() = %v, want %v", got, tt.want)
			}
		})
	}
}
