package main

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"time"

	"natsChunked/pkg/chunks"

	"github.com/nats-io/nats.go"
)

func main() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	data := make([]byte, 4*1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	var wg sync.WaitGroup
	var received []byte

	wg.Add(1)
	asm := chunks.NewAssembler(nc, 30*time.Second, func(id string, d []byte) {
		received = d
		fmt.Printf("Received message %s: %d bytes\n", id, len(d))
		wg.Done()
	})
	defer asm.Close()

	sub, err := asm.Subscribe("demo.chunks")
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Unsubscribe()

	pub := chunks.NewPublisher(nc, 512*1024, chunks.WithDigest())
	id, err := pub.PublishBytes("demo.chunks", data)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Published message %s: %d bytes\n", id, len(data))

	wg.Wait()

	if bytes.Equal(data, received) {
		fmt.Println("OK - data matches")
	} else {
		fmt.Println("MISMATCH")
	}
}
