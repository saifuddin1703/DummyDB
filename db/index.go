package db

import "github.com/emirpasic/gods/trees/redblacktree"

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
	return nil
}

func (db *Database) Get(key string) ([]byte, error) {
	return nil, nil
}

func initialzeRBTree() *redblacktree.Tree {
	//TODO : intialize a red black tree
	return redblacktree.NewWithStringComparator()
}
func GetNewDatabase(name string) (*DB, error) {
	if database == nil {
		tree := initialzeRBTree()
		dbInstance := &Database{
			Name:         name,
			TreeInstance: tree,
		}
		database = dbInstance // Correctly assign to interface
	}
	return &database, nil
}
