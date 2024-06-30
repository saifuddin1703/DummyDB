package lsmtree

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/dummydb/utils"
	"github.com/emirpasic/gods/trees/redblacktree"
)

var (
	LSMT *LSMTree
)

func init() {
	rbtree := redblacktree.NewWithStringComparator()
	filter := bloom.NewWithEstimates(1000000, 0.01)

	LSMT = &LSMTree{
		MemCache:        rbtree,
		SizeChannel:     make(chan int),
		Tables:          make([]*SSTable, 0),
		LSMLock:         &sync.RWMutex{},
		MergerSemaphore: make(chan int, 1),
		Filter:          filter,
	}
	LSMT.LoadSSTable()
	tables, err := json.MarshalIndent(LSMT.Tables, " ", " ")
	if err != nil {
		log.Fatal("Unable to marshal tables")
	}

	fmt.Println("tables : ", string(tables))
}

type LSMTree struct {
	MemCache        *redblacktree.Tree
	MemCacheSize    uint64
	SizeChannel     chan int // for testing only will be removed in future
	Tables          []*SSTable
	LSMLock         *sync.RWMutex
	MergerSemaphore chan int
	Filter          *bloom.BloomFilter
}

func (l *LSMTree) LoadSSTable() {
	log.Println("Loading SSTables ...")
	dirs, err := os.ReadDir("./segments")
	if err != nil {
		log.Fatal("Error listing all the segments : ", err)
	}

	for _, dir := range dirs {
		segmentFile := "./segments/" + dir.Name()
		file, err := os.OpenFile(segmentFile, os.O_RDWR, 0644)
		if err != nil {
			log.Fatal("error opening segment file : ", err)
		}

		segmentStat, _ := file.Stat()
		segmentSize := segmentStat.Size()

		segmentContent := make([]byte, segmentSize)
		_, err = file.Read(segmentContent)
		if err != nil {
			log.Fatal("error reading segment")
		}

		segmentString := string(segmentContent)
		kvs := strings.Split(segmentString, ";")
		size := 0
		var prevFileSize int64 = 0
		isMerged := false
		if segmentSize >= utils.MAX_TABLE_COUNT*utils.TABLE_SIZE {
			isMerged = true
		}
		ssTable := SSTable{
			KeyMap:      make(map[string]int64),
			filePath:    segmentFile,
			SegmentSize: segmentSize,
			IsMerged:    isMerged,
		}
		for idx, kv := range kvs {
			if len(kv) > 0 {
				kvpair := strings.Split(kv, ":")
				key := kvpair[0]
				currentFilesize := len(kv) + 1
				size += currentFilesize
				if err != nil {
					log.Fatal("error appending segment")
				}
				if prevFileSize == 0 || size > 100*utils.KB || idx == len(kvs)-2 {
					ssTable.KeyMap[key] = prevFileSize
				}
				prevFileSize = int64(size)
			}
		}
		l.Tables = append(l.Tables, &ssTable)
	}
}

func (l *LSMTree) BuildMemCacheFromWAL(walLocation string) {
	walContents, err := os.ReadFile(walLocation)

	if err != nil {
		log.Println("Error opening wal file")
	}

	kvs := strings.Split(string(walContents), ";")
	fmt.Println("wal contents : ", kvs)
	for _, kv := range kvs {
		if len(kv) > 0 {
			kvpair := strings.Split(kv, ":")
			key := kvpair[0]
			value := kvpair[1]
			l.MemCache.Put(key, value)
			size := unsafe.Sizeof(*l.MemCache.GetNode(key))
			l.MemCacheSize += uint64(size)
		}
	}

}

func (l *LSMTree) MergeSSTables() error {
	fmt.Println("merging tables...")
	tables := l.Tables
	newSegmentContent, err := tables[len(tables)-1].GetSegment()
	if err != nil {
		return err
	}
	lastIndex := -1
	newSegment := strings.Split(string(newSegmentContent), ";")
	for i := len(tables) - 2; i >= 0; i-- {
		if tables[i].IsMerged {
			lastIndex = i
			break
		}
		olderSegmentContent, err := tables[i].GetSegment()
		if err != nil {
			return err
		}
		olderSegment := strings.Split(string(olderSegmentContent), ";")
		fmt.Println("oldersegment : ", olderSegment)
		fmt.Println("newersegment : ", newSegment)
		newSegment = mergeTwoSegments(olderSegment[:len(olderSegment)-1], newSegment[:len(newSegment)-1])
		tables[i].ToDelete = true
		// a, _ := json.Marshal(tables[i])
		// fmt.Println("to deleted : ", string(a))
	}

	tables[len(tables)-1].ToDelete = true
	ssTable := SSTable{
		KeyMap:   make(map[string]int64),
		filePath: fmt.Sprintf("./segments/%v-segment", time.Now().Unix()),
		IsMerged: true,
	}

	var size int64 = 0
	// var prevFileSize int64 = 0
	for idx, kv := range newSegment {
		if len(kv) > 0 {
			kvpair := strings.Split(kv, ":")
			key := kvpair[0]
			val := kvpair[1]
			// currentFilesize := len(kv) + 1
			data := key + ":" + val + ";"
			prevFileSize, currentFilesize, err := ssTable.AppendIntoFile([]byte(data))
			if err != nil {
				log.Fatal("error appending segment")
			}

			size += currentFilesize
			if prevFileSize == 0 || size > 100*utils.KB || idx == len(newSegment)-2 {
				ssTable.KeyMap[key] = prevFileSize
			}
			prevFileSize = int64(size)
		}
	}

	ssTable.SegmentSize = int64(size)
	l.LSMLock.Lock()

	if lastIndex == -1 {
		lastIndex = len(tables) - 1
	}
	toDeleted := tables[lastIndex+1:]

	// delete all the merges segments

	for _, deleted := range toDeleted {
		if !deleted.ToDelete {
			fmt.Println("someting wrong")
			// log.Fatal(err)
			// a, _ := json.Marshal(deleted)
			// fmt.Println("to deleted : ", string(a))
		}
		err := deleted.Destroy()
		if err != nil {
			fmt.Println("someting wrong")
			log.Fatal(err)
		}
	}
	if lastIndex != -1 {
		l.Tables = tables[:lastIndex+1]
	}

	// ssTable.AppendIntoFile()
	l.Tables = append(l.Tables, &ssTable)
	fmt.Println("after merges tables : ", l.Tables)
	l.LSMLock.Unlock()
	return nil
}

func mergeTwoSegments(segment1 []string, segment2 []string) []string {
	// segment1 is older than segment2
	// each string of segment should be in this format key:value

	i := 0
	j := 0
	n := len(segment1)
	m := len(segment2)

	sortedSegment := make([]string, 0)
	for i < n && j < m {
		entry1 := segment1[i]
		entry2 := segment2[j]

		key1 := strings.Split(entry1, ":")[0]
		key2 := strings.Split(entry2, ":")[0]

		if key1 == key2 {
			fmt.Println("entry : ", entry2)
			sortedSegment = append(sortedSegment, entry2)
			i++
			j++
		} else if key1 < key2 {
			fmt.Println("entry : ", entry1)
			sortedSegment = append(sortedSegment, entry1)
			i++
		} else {
			fmt.Println("entry : ", entry2)
			sortedSegment = append(sortedSegment, entry2)
			j++
		}
	}

	if j < m {
		entry2 := segment2[j]
		sortedSegment = append(sortedSegment, entry2)
	}

	if i < n {
		entry1 := segment1[i]
		sortedSegment = append(sortedSegment, entry1)
	}

	fmt.Println("sortedSegment: ", sortedSegment)

	return sortedSegment
}

func (l *LSMTree) StartMerger() {
	go func() {
		fmt.Println("Segment merger is running...")
		for range l.MergerSemaphore {
			fmt.Println("Starting the merger")
			err := l.MergeSSTables()
			if err != nil {
				log.Fatal("Error merging")
			}
		}
	}()
}
func (l *LSMTree) StartConverter(walLocation string) {
	ticker := time.NewTicker(10 * time.Second) // for testing purposes only
	go func() {
		fmt.Println("MemCache converter is running...")
		for {
			select {
			case <-l.SizeChannel:
				l.ConvertMemCacheToSSTable(walLocation)
			case <-ticker.C:
				if l.MemCacheSize > 0 {
					l.ConvertMemCacheToSSTable(walLocation)
				}
			}
		}

		// for range l.SizeChannel {
		// 	l.ConvertMemCacheToSSTable(walLocation)
		// }
		// fmt.Println("MemCache converter exited...")
	}()
}

func (l *LSMTree) truncateWAL(walLocation string) {
	file, err := os.OpenFile(walLocation, os.O_RDWR, 0644)
	if err != nil {
		log.Fatal("Error truncating wal location : ", err)
	}

	err = file.Truncate(0)
	if err != nil {
		log.Fatal("Error truncating wal location : ", err)
	}
	fmt.Println("WAL is truncated")
}

func (l *LSMTree) ConvertMemCacheToSSTable(walLocation string) {
	fmt.Println("convertimg mem cache")
	l.LSMLock.Lock()
	defer func() {
		l.MemCacheSize = 0
		fmt.Println("truncate")
		l.truncateWAL(walLocation)
	}()
	defer l.LSMLock.Unlock()
	ssTable := SSTable{
		KeyMap: make(map[string]int64),
	}
	ssTable.HandleConstruction(l.MemCache)
	l.Tables = append(l.Tables, &ssTable)
	if len(l.Tables) > utils.MAX_TABLE_COUNT {
		l.MergerSemaphore <- 1
	}
}

func (l *LSMTree) Put(key string, value string) {
	l.LSMLock.Lock()
	defer l.LSMLock.Unlock()
	l.MemCache.Put(key, value)
	size := unsafe.Sizeof(*l.MemCache.GetNode(key))
	l.MemCacheSize += uint64(size)
	if l.MemCacheSize > utils.TABLE_SIZE {
		l.SizeChannel <- 1
	}
	l.Filter.Add([]byte(key))
}

func (l *LSMTree) Remove(key string) {
	l.Put(key, utils.DELETED_INDICATOR)
}

func (l *LSMTree) Find(query string) ([]byte, error) {
	present := l.Filter.Test([]byte(query))
	if !present {
		fmt.Println("value is not present the bloom filter")
		return nil, fmt.Errorf("key not found")
	}
	value, found := l.MemCache.Get(query)
	if found {
		if value == utils.DELETED_INDICATOR {
			return nil, fmt.Errorf("key not found")
		}
		return ([]byte)(value.(string)), nil
	} else {
		// find in the sstables
		// for now doing a linear search of all the hash table
		// in future need to select a hash table
		// do a binary search on the table and then
	tableloop:
		// loop through the l.Tables in reverse order
		for i := len(l.Tables) - 1; i >= 0; i-- {
			table := l.Tables[i]
			fmt.Println("table : ", table.filePath)
			hashtable := table.KeyMap
			prevKey := ""
			var offset int64 = 0
			found := false
			for key := range hashtable {
				if query <= key {
					offset = hashtable[prevKey]
					found = true
					break
				}
				prevKey = key
			}
			if found {
				fmt.Println("byte offset: ", offset)
				value, err := table.Search(query, offset)
				if err != nil {
					// key not found
					continue tableloop
				} else {
					return value, nil
				}
			} else {
				continue tableloop
			}
		}
		// value not found
		return nil, fmt.Errorf("key not found")
	}
}

func (l *LSMTree) GetAllKeys() ([]string, error) {
	// loop through the l.Tables in reverse order
	keys := make([]string, 0)
	uniqueKeys := make(map[string]int)

	iterator := l.MemCache.Iterator()

	for iterator.Next() {
		key := iterator.Key().(string)
		val := iterator.Value().(string)
		if _, ok := uniqueKeys[key]; !ok {
			if val != utils.DELETED_INDICATOR {
				keys = append(keys, key)
			}
			uniqueKeys[key] = 1
		}
	}
	for i := len(l.Tables) - 1; i >= 0; i-- {
		table := l.Tables[i]
		tableContent, err := os.ReadFile(table.filePath)
		if err != nil {
			return nil, err
		}

		kvs := strings.Split(string(tableContent), ";")

		for _, kv := range kvs {
			if kv != "" {
				kvpair := strings.Split(kv, ":")
				key := kvpair[0]
				val := kvpair[1]
				// fmt.Println("key : ", key)
				// _, pre := uniqueKeys[key]
				// fmt.Printf("%s is %v\n", key, val)
				if _, ok := uniqueKeys[key]; !ok {
					if val != utils.DELETED_INDICATOR {
						// fmt.Println("key and val : ", key, val)
						keys = append(keys, key)
					}
					uniqueKeys[key] = 1
				}
			}
		}
	}

	return keys, nil
}
