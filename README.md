# NATS Chunked

Send large messages over NATS by chunking them.

## Usage

```go
nc, _ := nats.Connect(nats.DefaultURL)

sender := chunker.NewSender(nc, 512*1024)
receiver := chunker.NewReceiver(nc)

sender.Send("subject", data)
received, _ := receiver.Receive("subject", 30*time.Second)
```

## API

```go
func NewSender(conn *nats.Conn, chunkSize int) *Sender
func (s *Sender) Send(subject string, data []byte) error

func NewReceiver(conn *nats.Conn) *Receiver
func (r *Receiver) Receive(subject string, timeout time.Duration) ([]byte, error)
```
