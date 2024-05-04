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

	"github.com/dummydb/utils"
	"github.com/emirpasic/gods/trees/redblacktree"
)

var (
	LSMT *LSMTree
)

func init() {
	rbtree := redblacktree.NewWithStringComparator()
	LSMT = &LSMTree{
		MemCache:    rbtree,
		SizeChannel: make(chan int),
		Tables:      make([]*SSTable, 0),
		LSMLock:     &sync.RWMutex{},
	}
	LSMT.LoadSSTable()
	tables, err := json.MarshalIndent(LSMT.Tables, " ", " ")
	if err != nil {
		log.Fatal("Unable to marshal tables")
	}

	fmt.Println("tables : ", string(tables))
}

type LSMTree struct {
	MemCache     *redblacktree.Tree
	MemCacheSize uint64
	SizeChannel  chan int // for testing only will be removed in future
	Tables       []*SSTable
	LSMLock      *sync.RWMutex
}

func (l *LSMTree) LoadSSTable() {
	log.Println("Loading SSTables ...")
	dirs, err := os.ReadDir("segments")
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
		ssTable := SSTable{
			KeyMap:      make(map[string]int64),
			filePath:    segmentFile,
			SegmentSize: segmentSize,
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
	file, err := os.OpenFile(walLocation, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		log.Println("Error opening wal file")
	}

	walState, _ := file.Stat()
	walSize := walState.Size()

	if walSize > 0 {
		walContents := make([]byte, walSize)
		file.Read(walContents)
		kvs := strings.Split(string(walContents), ";")

		for _, kv := range kvs {
			if len(kv) > 0 {
				kvpair := strings.Split(kv, ":")
				key := kvpair[0]
				value := kvpair[1]
				l.MemCache.Put(key, value)
			}
		}
	}
}
func (l *LSMTree) Put(key string, value string) {
	l.LSMLock.Lock()
	defer l.LSMLock.Unlock()
	l.MemCache.Put(key, value)
	size := unsafe.Sizeof(*l.MemCache.GetNode(key))
	l.MemCacheSize += uint64(size)
	if l.MemCacheSize > 2*utils.MB {
		l.SizeChannel <- 1
	}
}

func (l *LSMTree) StartConverter(walLocation string) {
	ticker := time.NewTicker(1 * time.Minute) // for testing purposes only
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
}

func (l *LSMTree) ConvertMemCacheToSSTable(walLocation string) {
	l.LSMLock.Lock()
	defer func() {
		l.MemCacheSize = 0
		l.truncateWAL(walLocation)
	}()
	defer l.LSMLock.Unlock()
	ssTable := SSTable{
		KeyMap: make(map[string]int64),
	}
	ssTable.HandleConstruction(l.MemCache)
	l.Tables = append(l.Tables, &ssTable)
}

func (l *LSMTree) Find(query string) ([]byte, error) {
	value, found := l.MemCache.Get(query)
	if found {
		return ([]byte)(value.(string)), nil
	} else {
		// find in the sstables
		// for now doing a linear search of all the hash table
		// in future need to select a hash table
		// do a binary search on the table and then
	tableloop:
		for _, table := range l.Tables {
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
