package db

type DB interface {
	Put(key string, value []byte) error
	Get(key string) ([]byte, error)
}

type Database struct {
	Name string
}

func initialzeRBTree(){
	//TODO : intialize a red black tree
}
func GetNewDatabase(name string) (*Database, error) {
	initialzeRBTree()
	
	return &Database{
		Name: name,
	}, nil
}
