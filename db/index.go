package db

import (
	"fmt"
	"log"
	"os"

	"github.com/dummydb/lsmtree"
)

type DB interface {
	Put(key string, value []byte) error
	Get(key string) ([]byte, error)
	Keys() ([]byte, error)
}

var database DB

type Database struct {
	Name    string
	LSMTree *lsmtree.LSMTree
	WALFile string
}

func (db *Database) Put(key string, value []byte) error {
	// alogn with lsm tree put the write to wal too
	db.AppendToWAL(key, value)
	db.LSMTree.Put(key, string(value))
	return nil
}
func (db *Database) AppendToWAL(key string, value []byte) error {
	file, err := os.OpenFile(db.WALFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal("Error opening wal file: ", err)
	}

	writeString := fmt.Sprintf("%v:%v;", key, string(value))
	_, err = file.WriteString(writeString)
	if err != nil {
		log.Fatal("Error appeding to WAL: ", err)
	}
	return nil
}
func (db *Database) Get(key string) ([]byte, error) {
	val, err := db.LSMTree.Find(key)
	fmt.Println("key, val , ok = ", key, val, err)
	if err != nil {
		return nil, fmt.Errorf("key not found")
	}
	return val, nil
}

// func (db *Database) Delete(key string) error {

// 	// 
// 	return nil
// }
func (db *Database) Keys() ([]byte, error) {
	keys, err := db.LSMTree.GetAllKeys()
	fmt.Println("keys : ", keys)
	if err != nil {
		return nil, err
	}

	toRet := ""

	for i, key := range keys {
		if i == len(keys)-1 {
			toRet += key
		} else {
			toRet += fmt.Sprintf("%s,", key)
		}
	}
	// keyBytes, err := json.Marshal(keys)
	// if err != nil {
	// 	return nil, err
	// }

	return []byte(toRet), nil
}

func GetNewDatabase(name string) (DB, error) {
	if database == nil {
		// lsmtree.LSMT.
		dbInstance := &Database{
			Name:    name,
			LSMTree: lsmtree.LSMT,
			WALFile: "dummydb-wal",
		}
		database = dbInstance // Correctly assign to interface
		lsmtree.LSMT.BuildMemCacheFromWAL(dbInstance.WALFile)
		lsmtree.LSMT.StartConverter(dbInstance.WALFile)
		lsmtree.LSMT.StartMerger()
	}
	return database, nil
}
