package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/dummydb/db"
)

type Server struct {
	Host       string
	Port       int
	DBInstance db.DB
	Listener   net.Listener
}

func (s *Server) Start() {
	listener, err := net.Listen("tcp", fmt.Sprintf("%v:%v", s.Host, s.Port))
	if err != nil {
		log.Fatal("Error listening")
	}
	s.Listener = listener
	fmt.Println("Listening on port 8080")

	dbInstance, _ := db.GetNewDatabase("MyDB")
	if dbInstance != nil {
		fmt.Println("Connection to database is established")
		s.DBInstance = dbInstance
	}
}

func (s *Server) HandleConnection(conn net.Conn) {
	// connectionCount++
	fmt.Println("Received connection : ")
	reader := bufio.NewReader(conn)
	connId := fmt.Sprint(time.Now().Unix())

readerLoop:
	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading:", err.Error())
			break
		}
		// Strip newline or return characters
		message = strings.TrimRight(message, "\r\n")

		val, err := s.handleOperations([]byte(message))
		if err != nil {
			conn.Write([]byte(err.Error()))
			conn.Write([]byte("\r\n"))
			continue readerLoop
		}
		log.Printf("%v I am writing %v ", connId, string(val))
		conn.Write(val)
		conn.Write([]byte("\r\n"))
	}
}

func (s *Server) handleOperations(data []byte) ([]byte, error) {
	dataString := string(data)
	ops := strings.Split(dataString, " ")

	fmt.Println("ops : ", len(ops))
	if len(ops) > 3 || len(ops) < 2 {
		// for now returning error but need to handle accordingly in future
		return nil, fmt.Errorf("Invalid operation")
	}

	if len(ops) == 3 {
		// set operations
		if strings.ToLower(ops[0]) == "set" {
			key := ops[1]
			value := ops[2]

			err := s.DBInstance.Put(key, []byte(value))
			if err != nil {
				return nil, fmt.Errorf("error setting key %s: %v", key, err)
			}
			return []byte(value), nil
		}
	}
	if len(ops) == 2 {
		// set operations
		if strings.ToLower(ops[0]) == "get" {
			key := strings.TrimSuffix(ops[1], "\r\n")
			val, err := s.DBInstance.Get(key)
			if err != nil {
				return nil, fmt.Errorf("error getting value for key %s: %v", key, err)
			}
			return val, nil
		}
	}
	return nil, fmt.Errorf("Invalide operation")
}

func main() {
	server := Server{
		Host: "localhost",
		Port: 4000,
	}
	server.Start()
	for {
		conn, err := server.Listener.Accept()
		if err != nil {
			log.Fatal("Error accepting connection")
		}
		go server.HandleConnection(conn)
	}
}
