package gosignal

type Queue interface {
	Send(messageType string, message []byte) error
	Subscribe(messageType string, ch chan QueueMessage) error
}

type QueueMessage interface {
	Attempts() int
	Message() []byte
	Ack() error
	Nack() error
	Retry() error //TODO: handle backoff logic.
}
