.PHONY: build build_server build_client run_server run_client test test-race test-integration bench coverage clean help

# Build targets
build: build_server build_client

build_server:
	@echo "Building server..."
	@mkdir -p bin
	@env GOARCH=amd64 GO111MODULE=on go build -ldflags="-s -w" -o bin/server ./cmd/dummydb && chmod +x bin/server
	@echo "Server built successfully at bin/server"

build_client:
	@echo "Building client..."
	@mkdir -p bin
	@env GOARCH=amd64 GO111MODULE=on go build -ldflags="-s -w" -o bin/client ./client/main.go && chmod +x bin/client
	@echo "Client built successfully at bin/client"

# Run targets
run_server: build_server
	@echo "Starting DummyDB server..."
	@./bin/server

run_client: build_client
	@echo "Starting DummyDB client..."
	@./bin/client

# Test targets
test:
	@echo "Running unit tests..."
	@go test -v ./internal/...

test-race:
	@echo "Running tests with race detector..."
	@go test -race -v ./internal/...

test-integration:
	@echo "Running integration tests..."
	@go test -v ./test/integration/...

test-all: test test-integration
	@echo "All tests completed"

# Benchmark targets
bench:
	@echo "Running benchmarks..."
	@go test -bench=. -benchmem ./test/benchmark/...

bench-cpu:
	@echo "Running benchmarks with CPU profiling..."
	@go test -bench=. -benchmem -cpuprofile=cpu.prof ./test/benchmark/...
	@echo "View profile with: go tool pprof cpu.prof"

bench-mem:
	@echo "Running benchmarks with memory profiling..."
	@go test -bench=. -benchmem -memprofile=mem.prof ./test/benchmark/...
	@echo "View profile with: go tool pprof mem.prof"

# Coverage targets
coverage:
	@echo "Generating coverage report..."
	@go test -cover -coverprofile=coverage.out ./internal/...
	@go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated at coverage.html"

coverage-func:
	@echo "Function-level coverage:"
	@go test -cover -coverprofile=coverage.out ./internal/... > /dev/null 2>&1
	@go tool cover -func=coverage.out

# Code quality targets
fmt:
	@echo "Formatting code..."
	@go fmt ./...

vet:
	@echo "Running go vet..."
	@go vet ./...

lint:
	@echo "Running golint..."
	@golint ./...

# Utility targets
clean:
	@echo "Cleaning build artifacts..."
	@rm -rf bin/
	@rm -rf segments/
	@rm -rf dummydb-wal
	@rm -rf *.prof
	@rm -rf coverage.out coverage.html
	@echo "Clean completed"

clean-data:
	@echo "Cleaning data files..."
	@rm -rf segments/
	@rm -rf dummydb-wal
	@echo "Data cleaned"

deps:
	@echo "Downloading dependencies..."
	@go mod download
	@go mod tidy
	@echo "Dependencies updated"

# Help target
help:
	@echo "DummyDB Makefile Commands:"
	@echo ""
	@echo "Build Commands:"
	@echo "  make build              - Build both server and client"
	@echo "  make build_server       - Build server only"
	@echo "  make build_client       - Build client only"
	@echo ""
	@echo "Run Commands:"
	@echo "  make run_server         - Build and run the server"
	@echo "  make run_client         - Build and run the client"
	@echo ""
	@echo "Test Commands:"
	@echo "  make test               - Run unit tests"
	@echo "  make test-race          - Run tests with race detector"
	@echo "  make test-integration   - Run integration tests"
	@echo "  make test-all           - Run all tests"
	@echo ""
	@echo "Benchmark Commands:"
	@echo "  make bench              - Run performance benchmarks"
	@echo "  make bench-cpu          - Run benchmarks with CPU profiling"
	@echo "  make bench-mem          - Run benchmarks with memory profiling"
	@echo ""
	@echo "Coverage Commands:"
	@echo "  make coverage           - Generate HTML coverage report"
	@echo "  make coverage-func      - Show function-level coverage"
	@echo ""
	@echo "Code Quality Commands:"
	@echo "  make fmt                - Format code with go fmt"
	@echo "  make vet                - Run go vet"
	@echo "  make lint               - Run golint"
	@echo ""
	@echo "Utility Commands:"
	@echo "  make clean              - Remove build artifacts and data"
	@echo "  make clean-data         - Remove data files only"
	@echo "  make deps               - Download and tidy dependencies"
	@echo "  make help               - Show this help message"
