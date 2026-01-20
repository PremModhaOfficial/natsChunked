package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

var testDataSizes = []int{
	64 * 1024,
	256 * 1024,
	1 * 1024 * 1024,
	4 * 1024 * 1024,
	8 * 1024 * 1024,
	16 * 1024 * 1024,
	32 * 1024 * 1024,
}

var testChunkSizes = []int{
	64 * 1024,
	128 * 1024,
	256 * 1024,
	512 * 1024,
	768 * 1024,
}

var testWorkerCounts = []int{1, 2, 4, 8}

func generateTestData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}
	return data
}

func setupNATSConnection(b *testing.B) *nats.Conn {
	b.Helper()
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		b.Fatalf("Failed to connect to NATS: %v", err)
	}
	return nc
}

func BenchmarkSend(b *testing.B) {
	nc := setupNATSConnection(b)
	defer nc.Close()

	for _, dataSize := range testDataSizes {
		for _, chunkSize := range testChunkSizes {
			if chunkSize > dataSize {
				continue
			}
			name := fmt.Sprintf("data_%dKB_chunk_%dKB", dataSize/1024, chunkSize/1024)
			b.Run(name, func(b *testing.B) {
				data := generateTestData(dataSize)
				sender := NewChunkedSender(nc, chunkSize)
				subject := fmt.Sprintf("bench.send.%d.%d", dataSize, chunkSize)

				b.ResetTimer()
				b.SetBytes(int64(dataSize))

				for i := 0; i < b.N; i++ {
					_, err := sender.Send(subject, data)
					if err != nil {
						b.Fatalf("Send failed: %v", err)
					}
				}
			})
		}
	}
}

func BenchmarkSendConcurrent(b *testing.B) {
	nc := setupNATSConnection(b)
	defer nc.Close()

	for _, dataSize := range testDataSizes {
		for _, chunkSize := range testChunkSizes {
			if chunkSize > dataSize {
				continue
			}
			for _, workers := range testWorkerCounts {
				name := fmt.Sprintf("data_%dKB_chunk_%dKB_workers_%d", dataSize/1024, chunkSize/1024, workers)
				b.Run(name, func(b *testing.B) {
					data := generateTestData(dataSize)
					sender := NewChunkedSender(nc, chunkSize)
					subject := fmt.Sprintf("bench.concurrent.%d.%d.%d", dataSize, chunkSize, workers)

					b.ResetTimer()
					b.SetBytes(int64(dataSize))

					for i := 0; i < b.N; i++ {
						_, err := sender.SendConcurrent(subject, data, workers)
						if err != nil {
							b.Fatalf("SendConcurrent failed: %v", err)
						}
					}
				})
			}
		}
	}
}

func BenchmarkEndToEnd(b *testing.B) {
	nc := setupNATSConnection(b)
	defer nc.Close()

	for _, dataSize := range testDataSizes[:5] {
		for _, chunkSize := range testChunkSizes {
			if chunkSize > dataSize {
				continue
			}
			name := fmt.Sprintf("data_%dKB_chunk_%dKB", dataSize/1024, chunkSize/1024)
			b.Run(name, func(b *testing.B) {
				data := generateTestData(dataSize)
				sender := NewChunkedSender(nc, chunkSize)
				receiver := NewChunkedReceiver(nc)
				subject := fmt.Sprintf("bench.e2e.%d.%d.%d", dataSize, chunkSize, b.N)

				b.ResetTimer()
				b.SetBytes(int64(dataSize))

				for i := 0; i < b.N; i++ {
					iterSubject := fmt.Sprintf("%s.%d", subject, i)

					done := make(chan struct{})
					var recvErr error

					go func() {
						_, recvErr = receiver.Receive(iterSubject, 30*time.Second)
						close(done)
					}()

					time.Sleep(10 * time.Millisecond)

					_, err := sender.Send(iterSubject, data)
					if err != nil {
						b.Fatalf("Send failed: %v", err)
					}

					<-done
					if recvErr != nil {
						b.Fatalf("Receive failed: %v", recvErr)
					}
				}
			})
		}
	}
}

func BenchmarkDirectPublish(b *testing.B) {
	nc := setupNATSConnection(b)
	defer nc.Close()

	smallSizes := []int{1024, 4096, 16384, 65536}

	for _, dataSize := range smallSizes {
		name := fmt.Sprintf("data_%dB", dataSize)
		b.Run(name, func(b *testing.B) {
			data := generateTestData(dataSize)
			subject := fmt.Sprintf("bench.direct.%d", dataSize)

			b.ResetTimer()
			b.SetBytes(int64(dataSize))

			for i := 0; i < b.N; i++ {
				err := nc.Publish(subject, data)
				if err != nil {
					b.Fatalf("Publish failed: %v", err)
				}
			}
			nc.Flush()
		})
	}
}

func BenchmarkChunkedVsDirect(b *testing.B) {
	nc := setupNATSConnection(b)
	defer nc.Close()

	dataSize := 512 * 1024
	data := generateTestData(dataSize)

	b.Run("direct_512KB", func(b *testing.B) {
		subject := "bench.compare.direct"
		b.ResetTimer()
		b.SetBytes(int64(dataSize))

		for i := 0; i < b.N; i++ {
			err := nc.Publish(subject, data)
			if err != nil {
				b.Fatalf("Publish failed: %v", err)
			}
		}
		nc.Flush()
	})

	for _, chunkSize := range testChunkSizes {
		if chunkSize > dataSize {
			continue
		}
		name := fmt.Sprintf("chunked_512KB_chunk_%dKB", chunkSize/1024)
		b.Run(name, func(b *testing.B) {
			sender := NewChunkedSender(nc, chunkSize)
			subject := fmt.Sprintf("bench.compare.chunked.%d", chunkSize)

			b.ResetTimer()
			b.SetBytes(int64(dataSize))

			for i := 0; i < b.N; i++ {
				_, err := sender.Send(subject, data)
				if err != nil {
					b.Fatalf("Send failed: %v", err)
				}
			}
		})
	}
}
