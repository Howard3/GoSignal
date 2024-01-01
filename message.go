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
	Deserialize([]byte) (Message[T], error)
}

type IDer interface {
	GetID() string
	SetID(string)
}

type Metadata interface {
	GetMetadata() *MessageMetadata
}

type Message[T any] interface {
	IDer
	Metadata
	Set(T) error
	Get() (T, error)
	Type() string
}

type MessageMetadata struct {
	CreatedAt time.Time
	Attempts  int
}

type MessageReceiver[T any] interface {
	MessageStream() MessageStream[T]
	Receive(Message[T]) HandlerResult
}

type MessageStream[T any] struct {
	QueueDriver QueueDriver
	Logger      Logger
	SerDe       SerDe[T]
}

// Send sends a message to the queue.
func (ms MessageStream[T]) Send(m Message[T]) error {
	serialized, err := ms.SerDe.Serialize(m)
	if err != nil {
		return fmt.Errorf("failed to serialize message: %w", err)
	}

	return ms.QueueDriver.Send(serialized)
}

func (ms MessageStream[T]) RegisterReceiver(mr MessageReceiver[T]) error {
	var err error
	switch {
	case ms.QueueDriver == nil:
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
	if err := ms.QueueDriver.Subscribe(ch); err != nil {
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

	m, err := ms.SerDe.Deserialize(qm.Message())
	if err != nil {
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
		//TODO: increment attempts?
		err = qm.Retry()
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
