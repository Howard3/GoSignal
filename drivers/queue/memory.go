package queue

import (
	"fmt"
	"strconv"
	"time"

	"github.com/Howard3/gosignal"
)

type MemoryQueue struct {
	Queue map[string]map[uint]chan gosignal.QueueMessage
}

func (mq *MemoryQueue) Send(messageType string, message []byte) error {
	if _, ok := mq.Queue[messageType]; !ok {
		return fmt.Errorf("message type %s not found", messageType)
	}

	for _, ch := range mq.Queue[messageType] {
		// TODO: make non-blocking on multiple.
		ch <- &MemoryQueueMessage{
			message: message,
			mType:   messageType,
		}
	}

	return nil
}

func (mq *MemoryQueue) Subscribe(messageType string) (string, chan gosignal.QueueMessage, error) {
	if mq.Queue == nil {
		mq.Queue = make(map[string]map[uint]chan gosignal.QueueMessage, 0)
	}

	if _, ok := mq.Queue[messageType]; !ok {
		mq.Queue[messageType] = make(map[uint]chan gosignal.QueueMessage, 0)
	}

	ch := make(chan gosignal.QueueMessage)
	id := uint(time.Now().UnixMicro())

	mq.Queue[messageType][id] = ch

	return fmt.Sprintf("%d", id), ch, nil
}

func (mq *MemoryQueue) Unsubscribe(messageType, sid string) error {
	if _, ok := mq.Queue[messageType]; !ok {
		return fmt.Errorf("message type %s not found", messageType)
	}

	id, err := strconv.Atoi(sid)
	if err != nil {
		return fmt.Errorf("id %s is not a valid id", sid)
	}

	uid := uint(id)

	if _, ok := mq.Queue[messageType][uid]; !ok {
		return fmt.Errorf("id %d not found", id)
	}

	delete(mq.Queue[messageType], uid)

	return nil
}

type MemoryQueueMessage struct {
	message []byte
	mType   string
}

func (MemoryQueueMessage) Attempts() int {
	return 0
}
func (mqm MemoryQueueMessage) Message() []byte {
	return mqm.message
}
func (MemoryQueueMessage) Ack() error {
	panic("not implemented") // TODO: Implement
}
func (MemoryQueueMessage) Nack() error {
	panic("not implemented") // TODO: Implement
}
func (MemoryQueueMessage) Retry(gosignal.RetryParams) error {
	panic("not implemented") // TODO: Implement
}
func (mqm MemoryQueueMessage) Type() string {
	return mqm.mType
}
