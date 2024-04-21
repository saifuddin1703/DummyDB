package lsmtree

import (
	"fmt"
	"sync"
	"time"
	"unsafe"

	"github.com/dummydb/db"
	"github.com/dummydb/utils"
	"github.com/emirpasic/gods/trees/redblacktree"
)

type LSMTree struct {
	MemCache     *redblacktree.Tree
	MemCacheSize uint64
	SizeChannel  chan int // for testing only will be removed in future
	Tables       []*db.SSTable
	LSMLock      *sync.RWMutex
}

func (l *LSMTree) Put(key string, value interface{}) {
	l.MemCache.Put(key, value)
	size := unsafe.Sizeof(*l.MemCache.GetNode(key))
	l.MemCacheSize += uint64(size)
	if l.MemCacheSize > 2*utils.MB {
		l.SizeChannel <- 1
	}
}

func (l *LSMTree) StartConverter() {
	ticker := time.NewTicker(5 * time.Second) // for testing purposes only
	go func() {
		fmt.Println("MemCache converter is running...")
		for {
			select {
			case <-l.SizeChannel:
				l.ConvertMemCacheToSSTable()
			case <-ticker.C:
				l.ConvertMemCacheToSSTable()
			}
		}
	}()
}

func (l *LSMTree) ConvertMemCacheToSSTable() {
	l.LSMLock.Lock()
	defer l.LSMLock.Unlock()
	ssTable := db.SSTable{
		KeyMap: make(map[string]int64),
	}
	ssTable.HandleConstruction(l.MemCache)
	l.Tables = append(l.Tables, &ssTable)
}
