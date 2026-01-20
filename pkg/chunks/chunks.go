package chunks

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

const (
	HeaderMsgID     = "X-Msg-Id"
	HeaderOffset    = "X-Offset"
	HeaderFlags     = "X-Flags"
	HeaderTotalSize = "X-Total-Size"
	HeaderDigest    = "X-Digest"

	FlagLast      = 0x01
	FlagHasDigest = 0x02
)

type Publisher struct {
	nc        *nats.Conn
	chunkSize int
	digest    bool
}

type PublisherOption func(*Publisher)

func WithDigest() PublisherOption {
	return func(p *Publisher) { p.digest = true }
}

func NewPublisher(nc *nats.Conn, chunkSize int, opts ...PublisherOption) *Publisher {
	p := &Publisher{nc: nc, chunkSize: chunkSize}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

func (p *Publisher) Publish(subject string, r io.Reader) (string, error) {
	msgID := uuid.NewString()
	buf := make([]byte, p.chunkSize)
	var offset uint64
	var h *sha256Hasher
	if p.digest {
		h = newSHA256Hasher()
	}

	for {
		n, err := r.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			if h != nil {
				h.Write(chunk)
			}

			isLast := err == io.EOF
			flags := uint8(0)
			if isLast {
				flags |= FlagLast
				if h != nil {
					flags |= FlagHasDigest
				}
			}

			hdr := nats.Header{
				HeaderMsgID:  {msgID},
				HeaderOffset: {strconv.FormatUint(offset, 10)},
				HeaderFlags:  {strconv.Itoa(int(flags))},
			}

			if isLast {
				totalSize := offset + uint64(n)
				hdr.Set(HeaderTotalSize, strconv.FormatUint(totalSize, 10))
				if h != nil {
					hdr.Set(HeaderDigest, h.Sum())
				}
			}

			if err := p.nc.PublishMsg(&nats.Msg{
				Subject: subject,
				Header:  hdr,
				Data:    chunk,
			}); err != nil {
				return "", err
			}

			offset += uint64(n)
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
	}

	return msgID, nil
}

func (p *Publisher) PublishBytes(subject string, data []byte) (string, error) {
	msgID := uuid.NewString()
	total := len(data)
	var offset uint64
	var digestHex string

	if p.digest {
		h := sha256.Sum256(data)
		digestHex = hex.EncodeToString(h[:])
	}

	for offset < uint64(total) {
		end := offset + uint64(p.chunkSize)
		if end > uint64(total) {
			end = uint64(total)
		}
		chunk := data[offset:end]
		isLast := end == uint64(total)

		flags := uint8(0)
		if isLast {
			flags |= FlagLast
			if p.digest {
				flags |= FlagHasDigest
			}
		}

		hdr := nats.Header{
			HeaderMsgID:  {msgID},
			HeaderOffset: {strconv.FormatUint(offset, 10)},
			HeaderFlags:  {strconv.Itoa(int(flags))},
		}

		if isLast {
			hdr.Set(HeaderTotalSize, strconv.FormatUint(uint64(total), 10))
			if p.digest {
				hdr.Set(HeaderDigest, digestHex)
			}
		}

		if err := p.nc.PublishMsg(&nats.Msg{
			Subject: subject,
			Header:  hdr,
			Data:    chunk,
		}); err != nil {
			return "", err
		}

		offset = end
	}

	return msgID, nil
}

type sha256Hasher struct {
	data []byte
}

func newSHA256Hasher() *sha256Hasher {
	return &sha256Hasher{}
}

func (h *sha256Hasher) Write(p []byte) {
	h.data = append(h.data, p...)
}

func (h *sha256Hasher) Sum() string {
	sum := sha256.Sum256(h.data)
	return hex.EncodeToString(sum[:])
}

type assembly struct {
	chunks   map[uint64][]byte
	received uint64
	total    uint64
	lastSeen bool
	digest   string
	deadline time.Time
}

type msgType int

const (
	msgChunk msgType = iota
	msgTimeout
)

type internalMsg struct {
	typ    msgType
	id     string
	offset uint64
	flags  uint8
	total  uint64
	digest string
	data   []byte
}

type Assembler struct {
	nc      *nats.Conn
	timeout time.Duration
	handler func(id string, data []byte)
	inbox   chan internalMsg
	done    chan struct{}
}

func NewAssembler(nc *nats.Conn, timeout time.Duration, handler func(string, []byte)) *Assembler {
	a := &Assembler{
		nc:      nc,
		timeout: timeout,
		handler: handler,
		inbox:   make(chan internalMsg, 4096),
		done:    make(chan struct{}),
	}
	go a.run()
	return a
}

func (a *Assembler) Subscribe(subject string) (*nats.Subscription, error) {
	return a.nc.Subscribe(subject, func(msg *nats.Msg) {
		id := msg.Header.Get(HeaderMsgID)
		if id == "" {
			return
		}

		offset, _ := strconv.ParseUint(msg.Header.Get(HeaderOffset), 10, 64)
		flags, _ := strconv.Atoi(msg.Header.Get(HeaderFlags))
		total, _ := strconv.ParseUint(msg.Header.Get(HeaderTotalSize), 10, 64)
		digest := msg.Header.Get(HeaderDigest)

		dataCopy := make([]byte, len(msg.Data))
		copy(dataCopy, msg.Data)

		select {
		case a.inbox <- internalMsg{
			typ:    msgChunk,
			id:     id,
			offset: offset,
			flags:  uint8(flags),
			total:  total,
			digest: digest,
			data:   dataCopy,
		}:
		case <-a.done:
		}
	})
}

func (a *Assembler) run() {
	pending := make(map[string]*assembly)

	for {
		select {
		case <-a.done:
			return
		case m := <-a.inbox:
			switch m.typ {
			case msgChunk:
				asm, ok := pending[m.id]
				if !ok {
					asm = &assembly{
						chunks:   make(map[uint64][]byte),
						deadline: time.Now().Add(a.timeout),
					}
					pending[m.id] = asm

					id := m.id
					time.AfterFunc(a.timeout, func() {
						select {
						case a.inbox <- internalMsg{typ: msgTimeout, id: id}:
						case <-a.done:
						}
					})
				}

				if _, exists := asm.chunks[m.offset]; exists {
					continue
				}

				asm.chunks[m.offset] = m.data
				asm.received += uint64(len(m.data))

				if m.flags&FlagLast != 0 {
					asm.lastSeen = true
					asm.total = m.total
					if m.flags&FlagHasDigest != 0 {
						asm.digest = m.digest
					}
				}

				if asm.lastSeen && asm.received == asm.total {
					data := a.reassemble(asm)
					if asm.digest != "" {
						h := sha256.Sum256(data)
						if hex.EncodeToString(h[:]) != asm.digest {
							delete(pending, m.id)
							continue
						}
					}
					delete(pending, m.id)
					a.handler(m.id, data)
				}

			case msgTimeout:
				delete(pending, m.id)
			}
		}
	}
}

func (a *Assembler) reassemble(asm *assembly) []byte {
	buf := make([]byte, asm.total)
	for offset, chunk := range asm.chunks {
		copy(buf[offset:], chunk)
	}
	return buf
}

func (a *Assembler) Close() {
	close(a.done)
}
