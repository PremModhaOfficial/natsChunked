package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"time"

	"natsChunked/pkg/chunker"

	"github.com/nats-io/nats.go"
)

func main() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	data, err := os.ReadFile("./resources/8Mb.html")
	if err != nil {
		log.Fatal(err)
	}

	sender := chunker.NewSender(nc, 512*1024)
	receiver := chunker.NewReceiver(nc)

	subject := "test.chunked"
	done := make(chan []byte)

	go func() {
		received, err := receiver.Receive(subject, 30*time.Second)
		if err != nil {
			log.Printf("Receive error: %v", err)
			done <- nil
			return
		}
		done <- received
	}()

	time.Sleep(100 * time.Millisecond)

	if err := sender.Send(subject, data); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Sent %d bytes\n", len(data))

	received := <-done
	if received == nil {
		log.Fatal("Failed to receive")
	}

	fmt.Printf("Received %d bytes\n", len(received))

	if bytes.Equal(data, received) {
		fmt.Println("OK")
	} else {
		fmt.Println("MISMATCH")
	}
}
