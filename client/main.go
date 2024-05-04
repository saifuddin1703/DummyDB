package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func main() {
	// Connect to the server
	conn, err := net.Dial("tcp", "localhost:4000")
	if err != nil {
		fmt.Println("Error connecting:", err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Println("Connected to server. Enter text to send:")
	// Send data to server and receive response
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("> ")
		input, _ := reader.ReadString('\n')

		// Send the input
		_, err = fmt.Fprintf(conn, input+"\n")
		if err != nil {
			fmt.Println("Error sending data:", err)
			break
		}

		// Receive the response
		response, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			fmt.Println("Error reading response:", err)
			continue
		}
		fmt.Print("Received:", response)
	}
}
