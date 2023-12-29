package gosignal

import "fmt"

type QueueDriver interface {
	Init() error
	Send(Message[any]) error
	Subscribe(chan QueueMessage) error
}

type QueueMessage interface {
	Message() []byte
	Ack() error
	Nack() error
	Retry() error //TODO: handle backoff logic.
}

// RegisterMessageSender registers a messageReceiver with the message stream to process
// messages from the event queue.
func RegisterMessageReceiver[T any](h MessageReceiver[T]) error {
	mp := &messageProcessor[T]{
		logger: h.MessageStream().Logger(),
		qd:     h.MessageStream().QueueDriver(),
		serde:  h.MessageStream().SerDe(),
		mr:     h,
	}

	if err := mp.init(); err != nil {
		return fmt.Errorf("failed to initialize message processor: %w", err)
	}

	ch := make(chan QueueMessage)
	if err := mp.qd.Subscribe(ch); err != nil {
		return fmt.Errorf("failed to subscribe to queue: %w", err)
	}

	go mp.processChannelMessages(ch)

	return nil
}

type messageProcessor[T any] struct {
	logger Logger
	qd     QueueDriver
	serde  SerDe[T]
	mr     MessageReceiver[T]
}

func (mp *messageProcessor[T]) init() error {
	if mp.qd == nil {
		return fmt.Errorf("queue driver not set")
	}

	if mp.serde == nil {
		return fmt.Errorf("serializer/deserializer not set")
	}

	if mp.mr == nil {
		return fmt.Errorf("message receiver not set")
	}

	if err := mp.qd.Init(); err != nil {
		return fmt.Errorf("failed to initialize queue driver: %w", err)
	}

	return nil
}

func (mp *messageProcessor[T]) processChannelMessages(ch chan QueueMessage) {
	for m := range ch {
		mp.processQueueMessage(m)
	}
}

func (mp *messageProcessor[T]) processQueueMessage(qm QueueMessage) {
	var result HandlerResult

	m, err := mp.serde.Deserialize(qm.Message())
	if err != nil {
		logIfErrWithMsg(mp.logger, err, "failed to deserialize message")
		result = HandlerResultFail
	} else {
		result = mp.mr.Process(m)
	}

	switch result {
	case HandlerResultSuccess:
		err = qm.Ack()
		logIfErrWithMsg(mp.logger, err, "failed to ack message")
	case HandlerResultRetry:
		//TODO: increment attempts?
		err = qm.Retry()
		logIfErrWithMsg(mp.logger, err, "failed to retry message")
	case HandlerResultFail:
		err = qm.Nack()
		logIfErrWithMsg(mp.logger, err, "failed to nack message")
	default:
		err = qm.Nack()
		if err == nil {
			logError(mp.logger, fmt.Errorf("received unknown handler result"))
		}
		logIfErrWithMsg(mp.logger, err, "failed to nack message with unknown result")
	}
}
