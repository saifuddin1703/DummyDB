package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strings"
)

type DB struct {
	Name        string
	KeyFileName string
	lastoffset  int64
}

func (db *DB) Initialize() {
	go func() {
		file, err := os.OpenFile(db.Name+"-key.txt", os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal("Could not open file ")
		}
		defer file.Close()
	}()
}

func (db *DB) StoreKeyAndOffset(key string, offset int64) error {
	file, err := os.OpenFile(db.Name+"-key.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.Write([]byte(fmt.Sprintf("%s:%d;", key, offset)))
	if err != nil {
		return err
	}
	return nil
}

func (db *DB) Set(key string, value []byte) error {
	fmt.Println("pre offset : ", db.lastoffset)
	file, err := os.OpenFile(db.Name+".txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("err : ", err)
		return fmt.Errorf("error creating file ")
	}
	defer file.Close()
	n, err := file.Write([]byte(fmt.Sprintf("%s:%s;", key, string(value))))
	if err != nil {
		return err
	}
	err = db.StoreKeyAndOffset(key, db.lastoffset)
	if err != nil {
		return err
	}
	db.lastoffset += int64(n)
	fmt.Println("offset : ", db.lastoffset)
	return nil
}

func (db *DB) Get(key string) ([]byte, error) {
	fmt.Println("pre offset : ", db.lastoffset)
	file, err := os.ReadFile(db.Name + ".txt")
	if err != nil {
		fmt.Println("err : ", err)
		return nil, fmt.Errorf("error creating file ")
	}

	fileString := string(file)
	fileString = fileString[:len(fileString)-1]
	// fmt.Println("fiel : ", fileString)
	keyvalues := strings.Split(fileString, ";")

	// fmt.Println("keyvalues : ", keyvalues)
	for i := len(keyvalues) - 2; i >= 0; i-- {
		// fmt.Println("keyvalue : ", keyvalues[i])
		keyvalue := strings.Split(keyvalues[i], ":")
		// fmt.Println("keyvalue : ", keyvalue)
		_key := keyvalue[0]
		value := keyvalue[1]
		if _key == key {
			return []byte(value), nil
		}
	}
	// fmt.Println("data : ", string(file))
	// db.file = file
	return nil, nil
}

func main() {
	// db := DB{
	server, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatal("Error listening")
	}

	fmt.Println("Listening on port 8080")

	connectionCount := 0
	for {
		conn, err := server.Accept()
		if err != nil {
			log.Fatal("Error accepting connection")
		}
		connectionCount++
		fmt.Println("Received connection : ", connectionCount)

		conn.Write([]byte("recieved connection"))
	}
}
