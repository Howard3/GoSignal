package gosignal

import "time"

type Queue interface {
	Send(messageType string, message []byte) error
	Subscribe(messageType string, ch chan QueueMessage) error
}

type QueueMessage interface {
	Attempts() int
	Message() []byte
	Ack() error
	Nack() error
	Retry(RetryParams) error
	Type() string // Type returns the message type, this is the same value in Send() and Subscribe()
}

type RetryParams struct {
	BackoffUntil time.Time
}
