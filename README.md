# NATS Chunks

Minimal, fast chunked message protocol for NATS. Send payloads larger than `max_payload` without increasing server memory pressure.

## Features

- **No mutex** - channel actor pattern
- **Offset-based** - no header message, self-describing chunks
- **Late allocation** - buffer only allocated on complete message
- **Single subject** - no subscription churn
- **Optional digest** - SHA-256 verification

## Usage

```go
nc, _ := nats.Connect(nats.DefaultURL)

// Receiver
asm := chunks.NewAssembler(nc, 30*time.Second, func(id string, data []byte) {
    fmt.Printf("Received %s: %d bytes\n", id, len(data))
})
defer asm.Close()
sub, _ := asm.Subscribe("events.large")
defer sub.Unsubscribe()

// Publisher
pub := chunks.NewPublisher(nc, 512*1024, chunks.WithDigest())
id, _ := pub.PublishBytes("events.large", largeData)
```

## API

```go
// Publisher
func NewPublisher(nc *nats.Conn, chunkSize int, opts ...PublisherOption) *Publisher
func (p *Publisher) Publish(subject string, r io.Reader) (string, error)
func (p *Publisher) PublishBytes(subject string, data []byte) (string, error)
func WithDigest() PublisherOption

// Assembler
func NewAssembler(nc *nats.Conn, timeout time.Duration, handler func(string, []byte)) *Assembler
func (a *Assembler) Subscribe(subject string) (*nats.Subscription, error)
func (a *Assembler) Close()
```

## Wire Format

Each chunk is a standard NATS message with headers:

| Header | Type | Description |
|--------|------|-------------|
| `X-Msg-Id` | string | UUID identifying the logical message |
| `X-Offset` | uint64 | Byte offset in final payload |
| `X-Flags` | uint8 | `0x01`=LAST, `0x02`=HAS_DIGEST |
| `X-Total-Size` | uint64 | Total payload size (LAST only) |
| `X-Digest` | string | SHA-256 hex (LAST + HAS_DIGEST only) |

Body: raw binary chunk data.

## Benchmarks

### Publisher Only (no receiver wait)

```
Size        Throughput      Allocs/op
────────────────────────────────────────
1MB         2466 MB/s       17
4MB         2733 MB/s       59
10MB        2747 MB/s       143
1MB+digest  1112 MB/s       21
```

### Full Round Trip (publish + reassemble)

```
Size    Chunked (1MB server)    Direct (8MB server)     Overhead
──────────────────────────────────────────────────────────────────
1MB     974 MB/s, 3.2MB mem     1210 MB/s, 1.0MB mem    0.8x, 3x
4MB     1070 MB/s, 12.7MB mem   1366 MB/s, 4.2MB mem    0.8x, 3x
8MB     1058 MB/s, 25.3MB mem   1320 MB/s, 8.4MB mem    0.8x, 3x
```

### v1 (req/reply) vs v2 (pub/sub offset)

```
Size    v1 req/reply    v2 pub/sub      Improvement
────────────────────────────────────────────────────
1MB     299 MB/s        974 MB/s        3.3x faster
4MB     985 MB/s        1070 MB/s       1.1x faster
8MB     1085 MB/s       1058 MB/s       ~same
```

## When to Use

| Scenario | Recommendation |
|----------|----------------|
| Messages < 1MB | Use NATS directly |
| Messages 1-4MB | Consider increasing `max_payload` |
| Messages > 4MB | Use this library |
| Need persistence | Use JetStream Object Store |

## Memory Overhead

~3x vs direct NATS due to:
1. Each chunk copied to assembler buffer
2. Final reassembly allocation

This is inherent to any chunking solution. Incomplete messages are discarded on timeout without large allocation.
