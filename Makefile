# 'run' builds and starts our program without creating an extra file
run:
	go run cmd/vortexkv/main.go

# 'build' creates a runnable program inside a "bin" folder
build:
	go build -o bin/vortexkv cmd/vortexkv/main.go

# 'test' runs all our tests to make sure things aren't broken
test:
	go test ./...

# 'clean' throws away the built programs so we can start fresh
clean:
	rm -rf bin/
