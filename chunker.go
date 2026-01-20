package main

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

const (
	PartPostFix   = "_parts"
	MsgIDHeader   = "MSG_ID"
	TotalPartsHdr = "TOTAL_PARTS"
	PartSizeHdr   = "PART_SIZE"
	PartIndexHdr  = "PART_INDEX"
	MultipartHdr  = "Multipart"
	DataSizeHdr   = "DATA_SIZE"
)

type ChunkedSender struct {
	nc        *nats.Conn
	chunkSize int
}

type ChunkedReceiver struct {
	nc *nats.Conn
}

type SendResult struct {
	MsgID      string
	TotalParts int
	ChunkSize  int
	DataSize   int
	Duration   time.Duration
}

type ReceiveResult struct {
	MsgID      string
	TotalParts int
	DataSize   int
	Duration   time.Duration
	Data       []byte
}

func NewChunkedSender(nc *nats.Conn, chunkSize int) *ChunkedSender {
	return &ChunkedSender{
		nc:        nc,
		chunkSize: chunkSize,
	}
}

func NewChunkedReceiver(nc *nats.Conn) *ChunkedReceiver {
	return &ChunkedReceiver{
		nc: nc,
	}
}

func (cs *ChunkedSender) Send(subject string, data []byte) (*SendResult, error) {
	start := time.Now()
	dataSize := len(data)
	msgID := uuid.NewString()

	parts := dataSize / cs.chunkSize
	if dataSize%cs.chunkSize != 0 {
		parts++
	}

	if dataSize <= cs.chunkSize {
		header := nats.Header{}
		header.Set(MultipartHdr, "false")
		header.Set(MsgIDHeader, msgID)
		header.Set(DataSizeHdr, strconv.Itoa(dataSize))

		msg := &nats.Msg{
			Subject: subject,
			Header:  header,
			Data:    data,
		}
		if err := cs.nc.PublishMsg(msg); err != nil {
			return nil, fmt.Errorf("failed to publish single message: %w", err)
		}

		return &SendResult{
			MsgID:      msgID,
			TotalParts: 1,
			ChunkSize:  dataSize,
			DataSize:   dataSize,
			Duration:   time.Since(start),
		}, nil
	}

	header := nats.Header{}
	header.Set(MultipartHdr, "true")
	header.Set(MsgIDHeader, msgID)
	header.Set(TotalPartsHdr, strconv.Itoa(parts))
	header.Set(PartSizeHdr, strconv.Itoa(cs.chunkSize))
	header.Set(DataSizeHdr, strconv.Itoa(dataSize))

	headerMsg := &nats.Msg{
		Subject: subject,
		Header:  header,
		Data:    []byte{},
	}
	if err := cs.nc.PublishMsg(headerMsg); err != nil {
		return nil, fmt.Errorf("failed to publish header message: %w", err)
	}

	for i := range parts {
		startIdx := i * cs.chunkSize
		endIdx := min(startIdx+cs.chunkSize, dataSize)

		chunkHeader := nats.Header{}
		chunkHeader.Set(MsgIDHeader, msgID)
		chunkHeader.Set(PartIndexHdr, strconv.Itoa(i))
		chunkHeader.Set(TotalPartsHdr, strconv.Itoa(parts))

		msg := &nats.Msg{
			Subject: subject + PartPostFix,
			Header:  chunkHeader,
			Data:    data[startIdx:endIdx],
		}
		if err := cs.nc.PublishMsg(msg); err != nil {
			return nil, fmt.Errorf("failed to publish chunk %d: %w", i, err)
		}
	}

	if err := cs.nc.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush: %w", err)
	}

	return &SendResult{
		MsgID:      msgID,
		TotalParts: parts,
		ChunkSize:  cs.chunkSize,
		DataSize:   dataSize,
		Duration:   time.Since(start),
	}, nil
}

func (cs *ChunkedSender) SendConcurrent(subject string, data []byte, workers int) (*SendResult, error) {
	start := time.Now()
	dataSize := len(data)
	msgID := uuid.NewString()

	parts := dataSize / cs.chunkSize
	if dataSize%cs.chunkSize != 0 {
		parts++
	}

	if dataSize <= cs.chunkSize {
		return cs.Send(subject, data)
	}

	header := nats.Header{}
	header.Set(MultipartHdr, "true")
	header.Set(MsgIDHeader, msgID)
	header.Set(TotalPartsHdr, strconv.Itoa(parts))
	header.Set(PartSizeHdr, strconv.Itoa(cs.chunkSize))
	header.Set(DataSizeHdr, strconv.Itoa(dataSize))

	headerMsg := &nats.Msg{
		Subject: subject,
		Header:  header,
		Data:    []byte{},
	}
	if err := cs.nc.PublishMsg(headerMsg); err != nil {
		return nil, fmt.Errorf("failed to publish header message: %w", err)
	}

	type chunk struct {
		index int
		data  []byte
	}
	chunks := make(chan chunk, parts)

	for i := range parts {
		startIdx := i * cs.chunkSize
		endIdx := min(startIdx+cs.chunkSize, dataSize)
		chunks <- chunk{index: i, data: data[startIdx:endIdx]}
	}
	close(chunks)

	var wg sync.WaitGroup
	errChan := make(chan error, workers)

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := range chunks {
				chunkHeader := nats.Header{}
				chunkHeader.Set(MsgIDHeader, msgID)
				chunkHeader.Set(PartIndexHdr, strconv.Itoa(c.index))
				chunkHeader.Set(TotalPartsHdr, strconv.Itoa(parts))

				msg := &nats.Msg{
					Subject: subject + PartPostFix,
					Header:  chunkHeader,
					Data:    c.data,
				}
				if err := cs.nc.PublishMsg(msg); err != nil {
					errChan <- fmt.Errorf("failed to publish chunk %d: %w", c.index, err)
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errChan)

	if err := <-errChan; err != nil {
		return nil, err
	}

	if err := cs.nc.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush: %w", err)
	}

	return &SendResult{
		MsgID:      msgID,
		TotalParts: parts,
		ChunkSize:  cs.chunkSize,
		DataSize:   dataSize,
		Duration:   time.Since(start),
	}, nil
}

func (cr *ChunkedReceiver) Receive(subject string, timeout time.Duration) (*ReceiveResult, error) {
	start := time.Now()

	sub, err := cr.nc.SubscribeSync(subject)
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe: %w", err)
	}
	defer sub.Unsubscribe()

	headerMsg, err := sub.NextMsg(timeout)
	if err != nil {
		return nil, fmt.Errorf("failed to receive header: %w", err)
	}

	msgID := headerMsg.Header.Get(MsgIDHeader)
	isMultipart := headerMsg.Header.Get(MultipartHdr) == "true"

	if !isMultipart {
		dataSize, _ := strconv.Atoi(headerMsg.Header.Get(DataSizeHdr))
		return &ReceiveResult{
			MsgID:      msgID,
			TotalParts: 1,
			DataSize:   dataSize,
			Duration:   time.Since(start),
			Data:       headerMsg.Data,
		}, nil
	}

	totalParts, err := strconv.Atoi(headerMsg.Header.Get(TotalPartsHdr))
	if err != nil {
		return nil, fmt.Errorf("invalid total parts: %w", err)
	}
	partSize, err := strconv.Atoi(headerMsg.Header.Get(PartSizeHdr))
	if err != nil {
		return nil, fmt.Errorf("invalid part size: %w", err)
	}
	dataSize, err := strconv.Atoi(headerMsg.Header.Get(DataSizeHdr))
	if err != nil {
		return nil, fmt.Errorf("invalid data size: %w", err)
	}

	partsSub, err := cr.nc.SubscribeSync(subject + PartPostFix)
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to parts: %w", err)
	}
	defer partsSub.Unsubscribe()

	buffer := make([]byte, dataSize)
	received := make([]bool, totalParts)
	receivedCount := 0

	for receivedCount < totalParts {
		msg, err := partsSub.NextMsg(timeout)
		if err != nil {
			return nil, fmt.Errorf("failed to receive part: %w", err)
		}

		if msg.Header.Get(MsgIDHeader) != msgID {
			continue
		}

		partIndex, err := strconv.Atoi(msg.Header.Get(PartIndexHdr))
		if err != nil {
			return nil, fmt.Errorf("invalid part index: %w", err)
		}

		if received[partIndex] {
			continue
		}

		startIdx := partIndex * partSize
		copy(buffer[startIdx:], msg.Data)

		received[partIndex] = true
		receivedCount++
	}

	return &ReceiveResult{
		MsgID:      msgID,
		TotalParts: totalParts,
		DataSize:   dataSize,
		Duration:   time.Since(start),
		Data:       buffer,
	}, nil
}

func (cr *ChunkedReceiver) ReceiveWithCallback(subject string, timeout time.Duration, cb func(*ReceiveResult)) error {
	result, err := cr.Receive(subject, timeout)
	if err != nil {
		return err
	}
	cb(result)
	return nil
}

func GetTotalPartsFromHeader(msg *nats.Msg) int {
	parts, err := strconv.Atoi(msg.Header.Get(TotalPartsHdr))
	if err != nil {
		log.Printf("Warning: could not parse total parts: %v", err)
		return 0
	}
	return parts
}

func GetPartSizeFromHeader(msg *nats.Msg) int {
	size, err := strconv.Atoi(msg.Header.Get(PartSizeHdr))
	if err != nil {
		log.Printf("Warning: could not parse part size: %v", err)
		return 0
	}
	return size
}

func GetDataSizeFromHeader(msg *nats.Msg) int {
	size, err := strconv.Atoi(msg.Header.Get(DataSizeHdr))
	if err != nil {
		log.Printf("Warning: could not parse data size: %v", err)
		return 0
	}
	return size
}

func IsMultipart(msg *nats.Msg) bool {
	return msg.Header.Get(MultipartHdr) == "true"
}
