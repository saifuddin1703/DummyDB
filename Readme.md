# DummyDB

DummyDB is a simple database server written in Go. It allows for basic database operations and includes a client for interacting with the server.

## Features

- Establishes a connection to a database and listens for client connections.
- Handles basic CRUD operations.
- Uses an in-memory cache with a red-black tree for efficient data access.
- Periodically converts the in-memory cache to a sorted string table (SSTable) on disk.
- Merges SSTables to optimize storage.
- Includes additional features like a bloom filter, key removal, and listing all keys.

## Getting Started

### Prerequisites

- Go 1.16 or higher

### Installation

1. Clone the repository:

   ```sh
   git clone https://github.com/saifuddin1703/DummyDB.git
   cd DummyDB
   ```

2. Build the server and client:
   ```sh
   make build_server
   make build_client
   ```

### Running the Server

To start the server, run:

```sh
make run_server
```

The server will start listening on port 4000.

### Running the Client

To run the client, use:

```sh
make run_client
```

## Project Structure

- `main.go`: Main entry point for the server.
- `client/`: Contains the client code.
- `db/`: Database implementation.
- `utils/`: Utility functions and helpers.
- `bin/`: Directory for the compiled binaries.
- `test/`: Contains test files.
- `todo.md`: List of tasks and future enhancements.
- `makefile`: Script for building and running the server and client.
- `.gitignore`: Specifies files and directories to ignore in git.
- `go.mod` and `go.sum`: Manage project dependencies.

## Future Enhancements

- Implement range queries.
- Add key count functionality.
- Optimize SSTable merging logic.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.
