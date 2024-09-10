package lsmtree

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/dummydb/utils"
	"github.com/emirpasic/gods/trees/redblacktree"
)

type SSTable struct {
	KeyMap      map[string]int64
	FilePath    string
	SegmentSize int64
	IsMerged    bool
	ToDelete    bool
}

func (s *SSTable) GetSegment() ([]byte, error) {
	segment, err := os.ReadFile(s.FilePath)
	if err != nil {
		return nil, err
	}

	return segment, nil
}

func (s *SSTable) HandleConstruction(MemCache *redblacktree.Tree) {
	defer func() {
		MemCache.Clear()
	}()
	fmt.Printf("Inserting %d keys \n", len(MemCache.Keys()))
	fmt.Println("Inserting keys : ", MemCache.Keys())
	// iterator := MemCache.Iterator()
	now := time.Now().Unix()
	s.FilePath = fmt.Sprintf("./segments/%v-segment", now)
	size := 0
	// count := 0
	// keys := []string{}
	multiplier := int64(1)
	for idx, key := range MemCache.Keys() {
		nkey, ok := key.(string)
		// fmt.Println("value : ", string(iterator.Value().([]byte)))
		if !ok {
			fmt.Println("error getting key ", key)
		}
		value, ok := MemCache.Get(key)
		if !ok {
			fmt.Println("error getting value ", key)
		}
		nvalue := value.(string)
		data := nkey + ":" + nvalue + ";"
		prevFileSize, currenFilesize, err := s.AppendIntoFile([]byte(data))
		size = int(currenFilesize)
		fmt.Println("size : ", currenFilesize)
		if err != nil {
			log.Fatal("error appending segment", err)
		}
		if prevFileSize == 0 || currenFilesize > (100*utils.KB)*multiplier || idx == MemCache.Size()-1 {
			s.KeyMap[nkey] = prevFileSize
			fmt.Println("key and offset : ", nkey, prevFileSize)
			multiplier++
		}

	}
	// fmt.Println("size : ", size)
	// fmt.Printf("Inserted %d keys \n", count)
	// fmt.Println("Inserted keys", keys)
	s.SegmentSize = int64(size)
}

func (s *SSTable) AppendIntoFile(data []byte) (int64, int64, error) {
	file, err := os.OpenFile(s.FilePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return 0, 0, err
	}
	stat, err := file.Stat()
	if err != nil {
		log.Fatal("Error getting file stats", err)
	}
	fileSize := stat.Size()

	n, err := file.Write(data)
	if err != nil {
		return 0, 0, err
	}
	return fileSize, fileSize + int64(n), nil
}

func binarySearchKV(kvpairs []string, query string) ([]byte, error) {
	low := 0
	high := len(kvpairs) - 1

	for low <= high {
		mid := (low + high) / 2
		kv := strings.Split(kvpairs[mid], ":")
		key := kv[0]

		if key == query {
			return []byte(kv[1]), nil
		}

		if key < query {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	return nil, errors.New("key not found during the search")
}

func (s *SSTable) Search(query string, byteoffset int64) ([]byte, error) {
	file, err := os.OpenFile(s.FilePath, os.O_RDWR, 0644)
	if err != nil {
		// return 0, 0, err
		return nil, err
	}

	segment := make([]byte, (s.SegmentSize - byteoffset))
	// fmt.Println("segment size: ", s.SegmentSize-1)
	n, err := file.ReadAt(segment, byteoffset)
	fmt.Printf("readed %v bytes \n", n)
	// info, _ := file.Stat()
	// fmt.Println("file size : ", info.Size())
	// fmt.Println("segment : ", string(segment))
	if err != nil {
		fmt.Println("err : ", err)
		return nil, err
	}

	segmentString := string(segment)
	kvpairs := strings.Split(segmentString, ";")

	// fmt.Println("segment string : ", segmentString)
	// for _, kvpair := range kvpairs {
	// 	if len(kvpair) > 0 {
	// 		kv := strings.Split(kvpair, ":")
	// 		key := kv[0]
	// 		value := kv[1]
	// 		if key == query {
	// 			return []byte(value), nil
	// 		}
	// 	}
	// }

	return binarySearchKV(kvpairs, query)
}

func (s *SSTable) Destroy() error {
	FilePath := s.FilePath

	err := os.Remove(FilePath)
	if err != nil {
		return err
	}
	s = nil
	return nil
}
