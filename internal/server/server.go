package server

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/dummydb/internal/config"
	"github.com/dummydb/internal/database"
)

// Server represents a TCP server for DummyDB
type Server struct {
	config   *config.Config
	handler  *Handler
	listener net.Listener
	running  atomic.Bool
	wg       sync.WaitGroup
	stopChan chan struct{}
}

// NewServer creates a new server instance
func NewServer(cfg *config.Config, db database.Database) *Server {
	return &Server{
		config:   cfg,
		handler:  NewHandler(db),
		stopChan: make(chan struct{}),
	}
}

// Start starts the server
func (s *Server) Start() error {
	addr := fmt.Sprintf("%s:%d", s.config.ServerHost, s.config.ServerPort)

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	s.listener = listener
	s.running.Store(true)

	fmt.Printf("DummyDB server listening on %s\n", addr)

	// Accept connections in a goroutine
	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

// Stop gracefully stops the server
func (s *Server) Stop() error {
	if !s.running.Swap(false) {
		// Already stopped
		return nil
	}

	fmt.Println("Stopping server...")

	// Close stop channel to signal goroutines
	close(s.stopChan)

	// Close listener to stop accepting new connections
	if s.listener != nil {
		s.listener.Close()
	}

	// Wait for all connections to finish
	s.wg.Wait()

	fmt.Println("Server stopped")
	return nil
}

// acceptLoop accepts incoming connections
func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.running.Load() {
				log.Printf("Error accepting connection: %v\n", err)
			}
			return
		}

		// Handle connection in a separate goroutine
		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// handleConnection handles a single client connection
func (s *Server) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	fmt.Printf("Client connected: %s\n", conn.RemoteAddr())

	reader := bufio.NewReader(conn)

	for {
		select {
		case <-s.stopChan:
			return

		default:
			// Read command
			message, err := reader.ReadString('\n')
			if err != nil {
				if err.Error() != "EOF" && !strings.Contains(err.Error(), "use of closed") {
					log.Printf("Error reading from client: %v\n", err)
				}
				return
			}

			// Process command
			response := s.processCommand(message)

			// Send response
			_, err = conn.Write(response)
			if err != nil {
				log.Printf("Error writing to client: %v\n", err)
				return
			}

			// Add newline
			conn.Write([]byte("\r\n"))
		}
	}
}

// processCommand processes a command and returns the response
func (s *Server) processCommand(message string) []byte {
	message = strings.TrimRight(message, "\r\n")

	// Parse command
	cmd, err := ParseCommand(message)
	if err != nil {
		return []byte(fmt.Sprintf("ERROR: %v", err))
	}

	// Execute command
	result, err := s.handler.Execute(cmd)
	if err != nil {
		return []byte(fmt.Sprintf("ERROR: %v", err))
	}

	return result
}
