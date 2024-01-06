package gosignal

import (
	"fmt"
	"time"
)

type HandlerResult int

const (
	HandlerResultSuccess HandlerResult = iota
	HandlerResultRetry
	HandlerResultFail
)

type SerDe[T any] interface {
	Serialize(Message[T]) ([]byte, error)
	Deserialize([]byte, *Message[T]) error
}

type IDer interface {
	GetID() string
	SetID(string) error
}

type Message[T any] struct {
	IDer      `json:"-"`
	Type      string    `json:"type"`
	ID        string    `json:"id"`
	CreatedAt time.Time `json:"created_at"`
	Attempts  int       `json:"-"`
	Payload   T         `json:"payload"`
}

func (m *Message[T]) serialize(s SerDe[T]) ([]byte, error) {
	m.ID = m.GetID() // copy ID from IDer to ID field so it gets serialized
	return s.Serialize(*m)
}

func (m *Message[T]) deserialize(qm QueueMessage, s SerDe[T]) error {
	if err := s.Deserialize(qm.Message(), m); err != nil {
		return err
	}

	m.SetID(m.ID)              // copy ID from ID field to IDer
	m.Attempts = qm.Attempts() // copy attempts from queue message to message
	return nil
}

type MessageReceiver[T any] interface {
	Receive(Message[T]) HandlerResult
}

type MessageStream[T any] struct {
	Queue  Queue
	Logger Logger
	SerDe  SerDe[T]
}

// Send sends a message to the queue.
func (ms MessageStream[T]) Send(m Message[T]) error {
	serialized, err := m.serialize(ms.SerDe)
	if err != nil {
		return fmt.Errorf("failed to serialize message: %w", err)
	}

	return ms.Queue.Send(m.Type, serialized)
}

func (ms MessageStream[T]) RegisterReceiver(messageType string, mr MessageReceiver[T]) error {
	var err error
	switch {
	case ms.Queue == nil:
		err = fmt.Errorf("queue driver not set on message stream")
	case ms.SerDe == nil:
		err = fmt.Errorf("serializer/deserializer not set on message stream")
	case mr == nil:
		err = fmt.Errorf("message receiver not set")
	}

	if err != nil {
		return fmt.Errorf("failed to register receiver: %w", err)
	}

	ch := make(chan QueueMessage)
	if err := ms.Queue.Subscribe(messageType, ch); err != nil {
		return fmt.Errorf("failed to subscribe to queue: %w", err)
	}

	go ms.queueMessageIterator(ch, mr)
	return nil
}

func (ms MessageStream[T]) queueMessageIterator(ch chan QueueMessage, mr MessageReceiver[T]) {
	for m := range ch {
		ms.processQueueMessage(m, mr)
	}
}

func (ms MessageStream[T]) processQueueMessage(qm QueueMessage, mr MessageReceiver[T]) {
	var result HandlerResult
	var m Message[T]
	var err error

	if err := m.deserialize(qm, ms.SerDe); err != nil {
		logIfErrWithMsg(ms.Logger, err, "failed to deserialize message")
		result = HandlerResultFail
	} else {
		result = mr.Receive(m)
	}

	switch result {
	case HandlerResultSuccess:
		err = qm.Ack()
		logIfErrWithMsg(ms.Logger, err, "failed to ack message")
	case HandlerResultRetry:
		err = qm.Retry(RetryParams{
			BackoffUntil: time.Now().Add(time.Second), // TODO: make this configurable
		})
		logIfErrWithMsg(ms.Logger, err, "failed to retry message")
	case HandlerResultFail:
		err = qm.Nack()
		logIfErrWithMsg(ms.Logger, err, "failed to nack message")
	default:
		err = qm.Nack()
		if err == nil {
			logError(ms.Logger, fmt.Errorf("received unknown handler result"))
		}

		logIfErrWithMsg(ms.Logger, err, "failed to nack message with unknown result")
	}
}
