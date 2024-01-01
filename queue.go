package gosignal

type QueueDriver interface {
	Send([]byte) error
	Subscribe(chan QueueMessage) error
}

type QueueMessage interface {
	Attempts() int
	Message() []byte
	Ack() error
	Nack() error
	Retry() error //TODO: handle backoff logic.
}
