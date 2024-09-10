package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
)

func createConnectionPool(address string) (net.Conn, error) {
	// pool := make(chan net.Conn, poolSize)
	// for i := 0; i < poolSize; i++ {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func Client(wg *sync.WaitGroup, id int, commands []string) {
	// conn := <-pool // Retrieve a connection from the pool
	// defer func() {
	// 	conn.Close()
	// 	// pool <- conn // Return the connection to the pool
	// 	defer wg.Done()
	// }()
	conn, _ := createConnectionPool("localhost:4000")
	defer conn.Close()
	// defer wg.Done()

	reader := bufio.NewReader(conn)
	for _, cmd := range commands {
		if cmd != " " {
			fmt.Fprint(conn, cmd)
			message, err := reader.ReadString('\n')
			if err != nil {
				fmt.Printf("Client %d: Error reading: %s\n", id, err.Error())
				continue
			}
			if strings.Contains(message, "not found") {
				fmt.Printf("Client %d: Received: %s", id, message)
			}
		}
	}
}

var batches = 1000
var clientCount = 50

func ReadTest(wg *sync.WaitGroup) {
	prev := time.Now()

	for client := 0; client < clientCount; client++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			for batch := 0; batch < batches; batch++ {
				fmt.Println("client : ", c)
				Client(wg, c, []string{fmt.Sprintf("get key_%d_%d\n", c, batch)})
			}
		}(client)
	}
	// for j := 0; j < batches; j++ {
	// 	fmt.Printf("batch %d\n", j)
	// 	for i := 0; i < clientCount; i++ {
	// 		wg.Add(1)
	// 		go
	// 	}
	wg.Wait()
	// }

	fmt.Println("time taken for writes : ", time.Since(prev))
}
func WriteTest(wg *sync.WaitGroup) {
	prev := time.Now()

	for client := 0; client < clientCount; client++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			for batch := 0; batch < batches; batch++ {
				fmt.Println("client : ", c)
				fmt.Println("sending : ", fmt.Sprintf("key_%d_%d", c, batch))
				Client(wg, c, []string{fmt.Sprintf("set key_%d_%d value_%d_%d\n", c, batch, c, batch)})
			}
		}(client)
	}
	// for j := 0; j < batches; j++ {
	// 	fmt.Printf("batch %d\n", j)
	// 	for i := 0; i < clientCount; i++ {
	// 		wg.Add(1)
	// 		go
	// 	}
	wg.Wait()
	// }

	fmt.Println("time taken for writes : ", time.Since(prev))
}
func main() {
	var wg sync.WaitGroup
	// address := "localhost:4000"
	// poolSize := 500 // Define the size of the connection pool

	// // Create a pool of TCP connections
	// // pool, err := createConnectionPool(poolSize, address)
	// if err != nil {
	// 	fmt.Println("Failed to create connection pool:", err)
	// 	return
	// }

	// _, _ = wg, pool
	// WriteTest(&wg)

	ReadTest(&wg)

}
