package chunks_test

import (
	"bytes"
	"strconv"
	"sync"
	"testing"
	"time"

	"natsChunked/pkg/chunks"

	"github.com/nats-io/nats.go"
)

func setupNATS(t *testing.T) *nats.Conn {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}
	return nc
}

func TestRoundTrip(t *testing.T) {
	nc := setupNATS(t)
	defer nc.Close()

	sizes := []int{
		64 * 1024,
		512 * 1024,
		1 * 1024 * 1024,
		4 * 1024 * 1024,
	}

	for _, size := range sizes {
		t.Run(formatSize(size), func(t *testing.T) {
			testData := make([]byte, size)
			for i := range testData {
				testData[i] = byte(i % 256)
			}

			var wg sync.WaitGroup
			var received []byte
			var receivedID string

			wg.Add(1)
			asm := chunks.NewAssembler(nc, 10*time.Second, func(id string, data []byte) {
				receivedID = id
				received = data
				wg.Done()
			})
			defer asm.Close()

			sub, err := asm.Subscribe("test.roundtrip")
			if err != nil {
				t.Fatal(err)
			}
			defer sub.Unsubscribe()

			pub := chunks.NewPublisher(nc, 512*1024, chunks.WithDigest())
			sentID, err := pub.PublishBytes("test.roundtrip", testData)
			if err != nil {
				t.Fatal(err)
			}

			done := make(chan struct{})
			go func() {
				wg.Wait()
				close(done)
			}()

			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("timeout waiting for message")
			}

			if sentID != receivedID {
				t.Errorf("ID mismatch: sent %s, got %s", sentID, receivedID)
			}
			if !bytes.Equal(testData, received) {
				t.Errorf("data mismatch: sent %d bytes, got %d bytes", len(testData), len(received))
			}
		})
	}
}

func BenchmarkPublisher(b *testing.B) {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		b.Skipf("NATS not available: %v", err)
	}
	defer nc.Close()

	sizes := []int{
		1 * 1024 * 1024,
		4 * 1024 * 1024,
		10 * 1024 * 1024,
	}

	for _, size := range sizes {
		data := make([]byte, size)

		b.Run(formatSize(size), func(b *testing.B) {
			pub := chunks.NewPublisher(nc, 512*1024)
			b.SetBytes(int64(size))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				pub.PublishBytes("bench.pub", data)
			}
		})

		b.Run(formatSize(size)+"_digest", func(b *testing.B) {
			pub := chunks.NewPublisher(nc, 512*1024, chunks.WithDigest())
			b.SetBytes(int64(size))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				pub.PublishBytes("bench.pub.digest", data)
			}
		})
	}
}

func BenchmarkRoundTrip(b *testing.B) {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		b.Skipf("NATS not available: %v", err)
	}
	defer nc.Close()

	sizes := []int{
		1 * 1024 * 1024,
		4 * 1024 * 1024,
		8 * 1024 * 1024,
	}

	for _, size := range sizes {
		data := make([]byte, size)

		b.Run(formatSize(size), func(b *testing.B) {
			var wg sync.WaitGroup
			wg.Add(b.N)

			asm := chunks.NewAssembler(nc, 30*time.Second, func(id string, data []byte) {
				wg.Done()
			})
			defer asm.Close()

			sub, _ := asm.Subscribe("bench.rt." + formatSize(size))
			defer sub.Unsubscribe()

			pub := chunks.NewPublisher(nc, 512*1024)

			b.SetBytes(int64(size))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				pub.PublishBytes("bench.rt."+formatSize(size), data)
			}

			wg.Wait()
		})
	}
}

func BenchmarkDirect(b *testing.B) {
	nc, err := nats.Connect("nats://localhost:4333")
	if err != nil {
		b.Skipf("8MB NATS server not available on :4333: %v", err)
	}
	defer nc.Close()

	sizes := []int{
		1 * 1024 * 1024,
		4 * 1024 * 1024,
		8 * 1024 * 1024,
	}

	for _, size := range sizes {
		data := make([]byte, size)

		b.Run(formatSize(size), func(b *testing.B) {
			var wg sync.WaitGroup
			wg.Add(b.N)

			sub, _ := nc.Subscribe("bench.direct."+formatSize(size), func(msg *nats.Msg) {
				wg.Done()
			})
			defer sub.Unsubscribe()

			b.SetBytes(int64(size))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				nc.Publish("bench.direct."+formatSize(size), data)
			}

			wg.Wait()
		})
	}
}

func formatSize(bytes int) string {
	switch {
	case bytes >= 1024*1024:
		mb := bytes / (1024 * 1024)
		return strconv.Itoa(mb) + "MB"
	case bytes >= 1024:
		kb := bytes / 1024
		return strconv.Itoa(kb) + "KB"
	default:
		return strconv.Itoa(bytes) + "B"
	}
}
