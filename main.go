package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	bytes, err := os.ReadFile("./resources/8Mb.html")
	if err != nil {
		panic(err)
	}

	sender := NewChunkedSender(nc, 1*1024*1024)

	result, err := sender.Send("subject.a", bytes)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Sent %d bytes in %d parts (chunk size: %d) - took %v\n",
		result.DataSize, result.TotalParts, result.ChunkSize, result.Duration)

	receiver := NewChunkedReceiver(nc)

	go func() {
		recvResult, err := receiver.Receive("subject.a", 30*time.Second)
		if err != nil {
			log.Printf("Receive error: %v", err)
			return
		}
		fmt.Printf("Received %d bytes in %d parts - took %v\n",
			recvResult.DataSize, recvResult.TotalParts, recvResult.Duration)
	}()

	time.Sleep(5 * time.Second)
	fmt.Println("Done.")
}
