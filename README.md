# VortexKV

![Go Version](https://img.shields.io/badge/go-1.22%2B-blue.svg)
![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)

**VortexKV** is a high-performance, embedded Key-Value store written in Go, built around the Log-Structured Merge-Tree (LSM-Tree) architecture. It is designed to handle high write throughput and provide fast reads using modern storage engine techniques.

## Features

- **LSM-Tree Architecture**: Built from the ground up for write-heavy workloads.
- **In-Memory MemTable**: Fast writes and recent reads using a custom SkipList implementation.
- **SSTable Flushes & Compaction**: Automatic background rotation of MemTables and hierarchical compaction of SSTables on disk to reclaim space and optimize read performance.
- **Bloom Filters**: Highly optimized point-lookups to quickly reject non-existent keys before reading from disk.
- **Tombstones**: Efficient deletions without immediate expensive data reorganization.

## Installation

Ensure you have Go installed (1.22+ recommended).

```bash
# Clone the repository
git clone https://github.com/yourusername/VortexKV.git
cd VortexKV

# Download dependencies
go mod tidy
```

## Quick Start

You can embed VortexKV into your own applications or run the provided example binary to see it in action.

### Running the Example

```bash
# Run in development mode (pretty console logging)
APP_ENV=development go run cmd/vortexkv/main.go

# Run in production mode (default JSON logging)
go run cmd/vortexkv/main.go
```

### Embedding in Your Go Project

```go
package main

import (
	"fmt"
	"vortexkv/internal/storage"
)

func main() {
	// Initialize a new MemTable store with a capacity constraint
	store := storage.NewMemTable(500)
	defer store.Close()

	// Put a key-value pair
	err := store.Put([]byte("my_key"), []byte("my_value"))
	if err != nil {
		panic(err)
	}

	// Get a value
	val, err := store.Get([]byte("my_key"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("Retrieved: %s\n", val)

	// Delete a key
	err = store.Delete([]byte("my_key"))
	if err != nil {
		panic(err)
	}
}
```

## Architecture

VortexKV uses a classic LSM-Tree approach to ensure blazing-fast operations:
1. **Writes**: All writes go to an in-memory `MemTable` (a SkipList). If the MemTable hits its capacity constraint, it is rotated to an immutable state and flushed to disk as an `SSTable` asynchronously.
2. **Reads**: Reads consult the active `MemTable` first, then the immutable MemTables, and finally traverse the on-disk `SSTables` from newest to oldest. `Bloom Filters` are utilized to skip unnecessary disk I/O.
3. **Compaction**: As SSTables accumulate, a background compaction worker merges them, purges deleted keys (tombstones), and reduces read amplification.

## Contributing

We welcome contributions! Please follow these steps:
1. Fork the repository.
2. Create your feature branch (`git checkout -b feature/amazing-feature`).
3. Commit your changes (`git commit -m 'Add some amazing feature'`).
4. Push to the branch (`git push origin feature/amazing-feature`).
5. Open a Pull Request.
