package chunker

import (
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

const (
	partsSuffix      = "_parts"
	headerMsgID      = "MSG_ID"
	headerTotalParts = "TOTAL_PARTS"
	headerPartSize   = "PART_SIZE"
	headerPartIndex  = "PART_INDEX"
	headerMultipart  = "Multipart"
	headerDataSize   = "DATA_SIZE"
)

type Sender struct {
	conn      *nats.Conn
	chunkSize int
}

type Receiver struct {
	conn *nats.Conn
}

func NewSender(conn *nats.Conn, chunkSize int) *Sender {
	return &Sender{conn: conn, chunkSize: chunkSize}
}

func NewReceiver(conn *nats.Conn) *Receiver {
	return &Receiver{conn: conn}
}

func (s *Sender) Send(subject string, data []byte) error {
	dataSize := len(data)
	msgID := uuid.NewString()

	if dataSize <= s.chunkSize {
		header := nats.Header{}
		header.Set(headerMultipart, "false")
		header.Set(headerMsgID, msgID)
		header.Set(headerDataSize, strconv.Itoa(dataSize))
		return s.conn.PublishMsg(&nats.Msg{Subject: subject, Header: header, Data: data})
	}

	totalParts := (dataSize + s.chunkSize - 1) / s.chunkSize

	header := nats.Header{}
	header.Set(headerMultipart, "true")
	header.Set(headerMsgID, msgID)
	header.Set(headerTotalParts, strconv.Itoa(totalParts))
	header.Set(headerPartSize, strconv.Itoa(s.chunkSize))
	header.Set(headerDataSize, strconv.Itoa(dataSize))

	if err := s.conn.PublishMsg(&nats.Msg{Subject: subject, Header: header}); err != nil {
		return err
	}

	partsSubject := subject + partsSuffix
	for i := range totalParts {
		start := i * s.chunkSize
		end := min(start+s.chunkSize, dataSize)

		chunkHeader := nats.Header{}
		chunkHeader.Set(headerMsgID, msgID)
		chunkHeader.Set(headerPartIndex, strconv.Itoa(i))

		if err := s.conn.PublishMsg(&nats.Msg{Subject: partsSubject, Header: chunkHeader, Data: data[start:end]}); err != nil {
			return err
		}
	}

	return s.conn.Flush()
}

func (r *Receiver) Receive(subject string, timeout time.Duration) ([]byte, error) {
	sub, err := r.conn.SubscribeSync(subject)
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe()

	msg, err := sub.NextMsg(timeout)
	if err != nil {
		return nil, err
	}

	if msg.Header.Get(headerMultipart) != "true" {
		return msg.Data, nil
	}

	msgID := msg.Header.Get(headerMsgID)
	totalParts, _ := strconv.Atoi(msg.Header.Get(headerTotalParts))
	partSize, _ := strconv.Atoi(msg.Header.Get(headerPartSize))
	dataSize, _ := strconv.Atoi(msg.Header.Get(headerDataSize))

	partsSub, err := r.conn.SubscribeSync(subject + partsSuffix)
	if err != nil {
		return nil, err
	}
	defer partsSub.Unsubscribe()

	buffer := make([]byte, dataSize)
	received := make([]bool, totalParts)
	count := 0

	for count < totalParts {
		partMsg, err := partsSub.NextMsg(timeout)
		if err != nil {
			return nil, fmt.Errorf("failed to receive part: %w", err)
		}

		if partMsg.Header.Get(headerMsgID) != msgID {
			continue
		}

		idx, _ := strconv.Atoi(partMsg.Header.Get(headerPartIndex))
		if received[idx] {
			continue
		}

		copy(buffer[idx*partSize:], partMsg.Data)
		received[idx] = true
		count++
	}

	return buffer, nil
}
