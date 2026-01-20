# NATS Chunked Message Transport

A Go library for sending large messages over NATS by automatically chunking them into smaller pieces that fit within NATS payload limits.

## Problem

NATS has a default maximum payload size of 1MB. When you need to send larger data (files, images, serialized objects), you either need to:
1. Increase the server limit (requires server config changes)
2. Use NATS JetStream Object Store (adds complexity)
3. Chunk the data yourself

This library provides option 3 with a simple API and high performance.

## Features

- Automatic chunking of large messages
- Transparent reassembly on the receiver side
- Sequential and concurrent sending modes
- Header-based metadata for reliable reassembly
- Throughput up to 1300+ MB/s

## Installation

```bash
go get github.com/your-org/natsChunked
```

## Quick Start

### Sending Large Data

```go
package main

import (
    "log"
    "os"

    "github.com/nats-io/nats.go"
)

func main() {
    nc, _ := nats.Connect(nats.DefaultURL)
    defer nc.Close()

    // Read a large file
    data, _ := os.ReadFile("large-file.bin")

    // Create sender with 512KB chunk size
    sender := NewChunkedSender(nc, 512*1024)

    // Send the data
    result, err := sender.Send("my.subject", data)
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("Sent %d bytes in %d chunks, took %v",
        result.DataSize, result.TotalParts, result.Duration)
}
```

### Receiving Large Data

```go
package main

import (
    "log"
    "time"

    "github.com/nats-io/nats.go"
)

func main() {
    nc, _ := nats.Connect(nats.DefaultURL)
    defer nc.Close()

    receiver := NewChunkedReceiver(nc)

    result, err := receiver.Receive("my.subject", 30*time.Second)
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("Received %d bytes in %d chunks",
        result.DataSize, result.TotalParts)

    // result.Data contains the reassembled message
}
```

### Concurrent Sending (Higher Throughput)

```go
// Use multiple goroutines to send chunks in parallel
result, err := sender.SendConcurrent("my.subject", data, 8) // 8 workers
```

## Benchmark Tool

Run comprehensive benchmarks with CSV output:

```bash
# Run with default settings (100 iterations)
go run ./cmd/benchmark -output=results.csv

# Run with custom iterations
go run ./cmd/benchmark -iterations=50 -output=results.csv

# Run Go native benchmarks
go test -bench=. -benchmem
```

### CLI Options

| Flag | Default | Description |
|------|---------|-------------|
| `-nats` | `nats://127.0.0.1:4222` | NATS server URL |
| `-iterations` | `100` | Iterations per benchmark |
| `-output` | stdout | Output CSV file path |

## Benchmark Results

Tested on Intel Core i7-1185G7 @ 3.00GHz with local NATS server.

### Sequential Send Performance

| Data Size | Chunk Size | Chunks | Throughput (MB/s) | Avg Latency (ms) |
|-----------|------------|--------|-------------------|------------------|
| 64 KB | 64 KB | 1 | 1,783 | 0.03 |
| 256 KB | 256 KB | 1 | 2,862 | 0.07 |
| 1 MB | 128 KB | 8 | 624 | 1.64 |
| 4 MB | 256 KB | 16 | 1,057 | 3.75 |
| 8 MB | 256 KB | 32 | 1,090 | 7.48 |
| 16 MB | 256 KB | 64 | 1,078 | 14.47 |
| 32 MB | 768 KB | 43 | 1,059 | 28.97 |

### Concurrent Send Performance (8 Workers)

| Data Size | Chunk Size | Chunks | Throughput (MB/s) | Avg Latency (ms) |
|-----------|------------|--------|-------------------|------------------|
| 1 MB | 64 KB | 16 | 711 | 1.38 |
| 4 MB | 256 KB | 16 | 1,056 | 3.51 |
| 8 MB | 768 KB | 11 | 1,151 | 6.99 |
| 16 MB | 256 KB | 64 | 1,232 | 13.12 |
| 32 MB | 512 KB | 64 | 1,352 | 23.75 |

### Direct Publish Baseline (No Chunking)

| Data Size | Throughput (MB/s) |
|-----------|-------------------|
| 1 KB | 331 |
| 4 KB | 389 |
| 16 KB | 455 |
| 64 KB | 499 |
| 256 KB | 701 |
| 512 KB | 880 |

### Chunked vs Direct Comparison (512 KB payload)

| Method | Chunk Size | Throughput (MB/s) | Overhead |
|--------|------------|-------------------|----------|
| Direct Publish | - | 880 | baseline |
| Chunked | 64 KB | 461 | -48% |
| Chunked | 128 KB | 559 | -36% |
| Chunked | 256 KB | 551 | -37% |
| Chunked | 512 KB | 807 | -8% |

## Key Findings

1. **Optimal Chunk Size**: 256KB-512KB provides the best balance of throughput and overhead for most workloads.

2. **Throughput Scales Well**: Large files (8MB+) achieve 1000-1350 MB/s throughput.

3. **Concurrency Benefits**: Concurrent sending with 8 workers improves throughput by 10-25% for large files (16MB+).

4. **Chunking Overhead**: For data that fits in a single message, direct publish is faster. Chunking adds ~8-48% overhead depending on chunk size.

5. **Memory Efficiency**: Larger chunk sizes reduce allocations. 768KB chunks use ~4x less memory than 64KB chunks for the same data.

## Recommendations

| Data Size | Recommended Approach |
|-----------|---------------------|
| < 768 KB | Direct publish (no chunking needed) |
| 768 KB - 4 MB | Sequential send, 512KB chunks |
| 4 MB - 16 MB | Sequential send, 256KB chunks |
| > 16 MB | Concurrent send (4-8 workers), 256-512KB chunks |

## Project Structure

```
natsChunked/
├── main.go              # Example usage
├── chunker.go           # Core chunking logic
├── benchmark_test.go    # Go native benchmarks
├── cmd/
│   └── benchmark/
│       └── main.go      # CLI benchmark tool
└── resources/           # Test files
    ├── 1MB.html
    ├── 8Mb.html
    ├── 16MB.bin
    └── 32MB.bin
```

## API Reference

### ChunkedSender

```go
// Create a new sender with specified chunk size
sender := NewChunkedSender(nc *nats.Conn, chunkSize int)

// Send data synchronously
result, err := sender.Send(subject string, data []byte) (*SendResult, error)

// Send data with multiple workers
result, err := sender.SendConcurrent(subject string, data []byte, workers int) (*SendResult, error)
```

### ChunkedReceiver

```go
// Create a new receiver
receiver := NewChunkedReceiver(nc *nats.Conn)

// Receive and reassemble a chunked message
result, err := receiver.Receive(subject string, timeout time.Duration) (*ReceiveResult, error)
```

### SendResult

```go
type SendResult struct {
    MsgID      string        // Unique message identifier
    TotalParts int           // Number of chunks sent
    ChunkSize  int           // Size of each chunk
    DataSize   int           // Total data size
    Duration   time.Duration // Time taken to send
}
```

### ReceiveResult

```go
type ReceiveResult struct {
    MsgID      string        // Unique message identifier
    TotalParts int           // Number of chunks received
    DataSize   int           // Total data size
    Duration   time.Duration // Time taken to receive
    Data       []byte        // Reassembled data
}
```

## Requirements

- Go 1.21+
- NATS Server 2.x
- github.com/nats-io/nats.go

## License

MIT
