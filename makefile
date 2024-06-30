build_server:
	env GOARCH=amd64 GO111MODULE=on go build -ldflags="-s -w" -o bin/server main.go && chmod +x bin/server
build_client:
	env GOARCH=amd64 GO111MODULE=on go build -ldflags="-s -w" -o bin/client ./client/main.go && chmod +x bin/client

run_server: build_server
	./bin/server
	
run_client: build_client
	./bin/client
