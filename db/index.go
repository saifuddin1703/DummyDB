package db

import (
	"fmt"

	"github.com/emirpasic/gods/trees/redblacktree"
)

type DB interface {
	Put(key string, value []byte) error
	Get(key string) ([]byte, error)
}

var database DB

type Database struct {
	Name         string
	TreeInstance *redblacktree.Tree
}

func (db *Database) Put(key string, value []byte) error {
	db.TreeInstance.Put(key, value)
	return nil
}

func (db *Database) Get(key string) ([]byte, error) {
	val, ok := db.TreeInstance.Get(key)
	fmt.Println("key, val , ok = ", key, val, ok)
	if !ok {
		return nil, fmt.Errorf("key not found")
	}
	return val.([]byte), nil
}

func initialzeRBTree() *redblacktree.Tree {
	//TODO : intialize a red black tree
	return redblacktree.NewWithStringComparator()
}

func GetNewDatabase(name string) (DB, error) {
	if database == nil {
		tree := initialzeRBTree()
		dbInstance := &Database{
			Name:         name,
			TreeInstance: tree,
		}
		database = dbInstance // Correctly assign to interface
	}
	return database, nil
}
