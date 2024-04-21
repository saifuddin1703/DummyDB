package db

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/dummydb/utils"
	"github.com/emirpasic/gods/trees/redblacktree"
)

type SSTable struct {
	KeyMap map[string]int64
}

func (s *SSTable) HandleConstruction(MemCache *redblacktree.Tree) {
	iterator := MemCache.Iterator()

	now := time.Now().Unix()
	for iterator.Next() {
		key := iterator.Key().(string)
		value := iterator.Value().(string)

		data := key + ":" + value + ";"
		prevFileSize, currenFilesize, err := s.AppendIntoFile(fmt.Sprintf("%v-segment", now), []byte(data))
		if err != nil {
			log.Fatal("error appending segment")
		}
		if prevFileSize == 0 || currenFilesize > 100*utils.KB {
			s.KeyMap[key] = prevFileSize
		}
	}
}

func (s *SSTable) AppendIntoFile(filePath string, data []byte) (int64, int64, error) {
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return 0, 0, err
	}
	stat, err := file.Stat()
	if err != nil {
		log.Fatal(err)
	}
	fileSize := stat.Size()

	n, err := file.Write(data)
	if err != nil {
		return 0, 0, err
	}
	return fileSize, fileSize + int64(n), nil
}
