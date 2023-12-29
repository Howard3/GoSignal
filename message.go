package gosignal

import "time"

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
	Process(Message[T]) HandlerResult
}

type MessageSender[T any] interface {
	MessageStream() MessageStream[T]
	Send(Message[T]) error
}

type MessageStream[T any] interface {
	QueueDriver() QueueDriver
	Logger() Logger
	SerDe() SerDe[T]
}
