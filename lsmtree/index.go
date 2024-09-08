package lsmtree

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
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
		ActiveMemCache:  rbtree,
		PassiveMemCache: redblacktree.NewWithStringComparator(),
		SizeChannel:     make(chan int, 1),
		Tables:          make([]*SSTable, 0),
		MergedTables:    make([]*SSTable, 0),
		TableCount:      0,
		LSMLock:         &sync.RWMutex{},
		MergerSemaphore: make(chan int, 1),
		Filter:          filter,
		WalLocation:     "dummydb-wal",
	}
	LSMT.LoadSSTable()
	tables, err := json.MarshalIndent(LSMT.Tables, " ", " ")
	if err != nil {
		log.Fatal("Unable to marshal tables")
	}

	fmt.Println("tables : ", string(tables))
}

type LSMTree struct {
	ActiveMemCache  *redblacktree.Tree
	PassiveMemCache *redblacktree.Tree
	MemCacheSize    int64
	SizeChannel     chan int // for testing only will be removed in future
	Tables          []*SSTable
	MergedTables    []*SSTable
	TableCount      int64
	LSMLock         *sync.RWMutex
	MergerSemaphore chan int
	Filter          *bloom.BloomFilter
	WalLocation     string
}

func (l *LSMTree) LoadSSTable() {
	log.Println("Loading SSTables ...")
	dir := "./segments"

	// Check if the directory exists
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		// Create the directory if it does not exist
		err := os.Mkdir(dir, os.ModePerm)
		if err != nil {
			fmt.Println("Error creating directory:", err)
			return
		}
		fmt.Println("Directory created:", dir)
	}
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
		isMerged := strings.Contains(segmentFile, "merged")
		ssTable := SSTable{
			KeyMap:      make(map[string]int64),
			FilePath:    segmentFile,
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
				l.Filter.Add([]byte(key))
			}
		}
		if isMerged {
			l.MergedTables = append(l.MergedTables, &ssTable)
		} else {
			l.Tables = append(l.Tables, &ssTable)
		}
		atomic.AddInt64(&l.TableCount, 1)
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
			l.ActiveMemCache.Put(key, value)
			size := unsafe.Sizeof(*l.ActiveMemCache.GetNode(key))
			// l.MemCacheSize = uint64(size)
			atomic.AddInt64(&l.MemCacheSize, int64(size))
			l.Filter.Add([]byte(key))
		}
	}
	l.truncateWAL(walLocation)
}

func (l *LSMTree) MergeSSTables(tables []*SSTable) error {

	mergedTableContent, _ := tables[len(tables)-1].GetSegment()
	mergedKvs := strings.Split(string(mergedTableContent), ";")
	mergedKvs = mergedKvs[:len(mergedKvs)-1]
	for i := len(tables) - 2; i >= 0; i-- {
		currentTableContent, _ := tables[i].GetSegment()
		currentKVs := strings.Split(string(currentTableContent), ";")
		currentKVs = currentKVs[:len(currentKVs)-1]
		mergedKvs = mergeTwoSegments(currentKVs, mergedKvs)
	}

	mergedTable := SSTable{
		KeyMap:   make(map[string]int64),
		FilePath: fmt.Sprintf("./segments/%v-merged-segment", time.Now().Nanosecond()),
		IsMerged: true,
	}

	var size int64 = 0
	for idx, kv := range mergedKvs {
		if len(kv) > 0 {
			kvpair := strings.Split(kv, ":")
			key := kvpair[0]
			val := kvpair[1]
			// currentFilesize := len(kv) + 1
			data := key + ":" + val + ";"
			prevFileSize, currentFilesize, err := mergedTable.AppendIntoFile([]byte(data))
			if err != nil {
				log.Fatal("error appending segment")
			}

			size = currentFilesize
			if prevFileSize == 0 || size > 100*utils.KB || idx == len(mergedKvs)-1 {
				mergedTable.KeyMap[key] = prevFileSize
			}
		}
	}

	l.LSMLock.Lock()
	mergedTable.SegmentSize = size
	l.MergedTables = append(l.MergedTables, &mergedTable)

	// delete the tables
	for _, table := range tables {
		table.Destroy()
	}
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
			sortedSegment = append(sortedSegment, entry2)
			i++
			j++
		} else if key1 < key2 {
			sortedSegment = append(sortedSegment, entry1)
			i++
		} else {
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

	return sortedSegment
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

func (l *LSMTree) ConvertMemCacheToSSTable(walLocation string, memtable *redblacktree.Tree) {
	fmt.Println("convertimg mem cache")
	ssTable := SSTable{
		KeyMap: make(map[string]int64),
	}
	l.LSMLock.Lock()
	l.truncateWAL(walLocation)
	l.LSMLock.Unlock()

	fmt.Println("truncate")
	ssTable.HandleConstruction(memtable)

	l.LSMLock.Lock()
	l.Tables = append(l.Tables, &ssTable)

	l.LSMLock.Unlock()


	l.LSMLock.Lock()
	if atomic.LoadInt64(&l.TableCount) >= utils.MAX_TABLE_COUNT {
		atomic.AddInt64(&l.TableCount, utils.MAX_TABLE_COUNT*-1)

		// extract the tables
		endIndex := len(l.Tables)
		startIndex := endIndex - utils.MAX_TABLE_COUNT

		originalTables := l.Tables

		tables := l.Tables[:startIndex]
		l.Tables = tables
		go l.MergeSSTables(originalTables[startIndex:endIndex])

		l.LSMLock.Unlock()
	} else {
		l.LSMLock.Unlock()
	}
}

func (l *LSMTree) Put(key string, value string) {
	l.LSMLock.Lock()
	fmt.Printf("put lock accquired for %s\n", key)

	defer func() {
		fmt.Printf("put lock released for %s\n", key)
	}()
	l.ActiveMemCache.Put(key, value)
	size := fmt.Sprintf("%s:%s;", key, value)
	atomic.AddInt64(&l.MemCacheSize, int64(len(size)))
	l.LSMLock.Unlock()

	l.LSMLock.Lock()
	if atomic.LoadInt64(&l.MemCacheSize) > utils.TABLE_SIZE {
		memtable := l.ActiveMemCache
		l.ActiveMemCache = redblacktree.NewWith(l.ActiveMemCache.Comparator)
		atomic.StoreInt64(&l.MemCacheSize, 0)
		l.LSMLock.Unlock()

		go l.ConvertMemCacheToSSTable(l.WalLocation, memtable)
	} else {
		l.LSMLock.Unlock()
	}

	l.Filter.Add([]byte(key))
}

func (l *LSMTree) Remove(key string) {
	l.LSMLock.Lock()
	defer l.LSMLock.Unlock()
	l.Put(key, utils.DELETED_INDICATOR)
}

func (l *LSMTree) Find(query string) ([]byte, error) {
	present := l.Filter.Test([]byte(query))
	if !present {
		fmt.Println("value is not present the bloom filter")
		return nil, fmt.Errorf("key not found")
	}
	value, found := l.ActiveMemCache.Get(query)
	if found {
		if value == utils.DELETED_INDICATOR {
			fmt.Println("key deleted")
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
			fmt.Println("table : ", table.FilePath)
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
	mergedTableloop:
		// loop through the Merged tables in reverse order
		for i := len(l.MergedTables) - 1; i >= 0; i-- {
			table := l.MergedTables[i]
			fmt.Println("table : ", table.FilePath)
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
					continue mergedTableloop
				} else {
					return value, nil
				}
			} else {
				continue mergedTableloop
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

	iterator := l.ActiveMemCache.Iterator()

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
		tableContent, err := os.ReadFile(table.FilePath)
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
