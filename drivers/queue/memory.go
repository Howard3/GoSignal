package queue

import (
	"errors"
	"fmt"
	"time"

	"github.com/Howard3/gosignal"
)

var ErrQueueDoesNotExist = errors.New("queue does not exist")

type MemoryQueue struct {
	channels map[string][]chan gosignal.QueueMessage
}

func (q *MemoryQueue) hasChannelForType(messageType string) bool {
	if q.channels == nil {
		return false
	}

	_, ok := q.channels[messageType]
	return ok
}

func (q *MemoryQueue) getChannelsForType(messageType string) []chan gosignal.QueueMessage {
	if !q.hasChannelForType(messageType) {
		return nil
	}

	return q.channels[messageType]
}

func (q *MemoryQueue) Send(messageType string, b []byte) error {
	channels := q.getChannelsForType(messageType)
	if channels == nil {
		return fmt.Errorf("%w: %s", ErrQueueDoesNotExist, messageType)
	}

	m := &MemoryQueueMessage{
		attempts:    0,
		message:     b,
		queue:       q,
		messageType: messageType,
	}

	go q.sendToChannels(channels, m)

	return nil
}

func (q *MemoryQueue) sendToChannels(channels []chan gosignal.QueueMessage, m gosignal.QueueMessage) {
	for _, ch := range channels {
		// TODO: potentially blocking on one send if a channel is blocked, also could cause a panic.
		ch <- m
	}
}

func (q *MemoryQueue) Subscribe(messageType string, ch chan gosignal.QueueMessage) error {
	q.channels[messageType] = append(q.channels[messageType], ch)
	return nil
}

type MemoryQueueMessage struct {
	attempts    int
	message     []byte
	queue       *MemoryQueue
	messageType string
}

func (mqm *MemoryQueueMessage) Type() string {
	return mqm.messageType
}

func (mqm *MemoryQueueMessage) Attempts() int {
	return mqm.attempts
}
func (mqm *MemoryQueueMessage) Message() []byte {
	return mqm.message
}
func (mqm *MemoryQueueMessage) Ack() error {
	return nil
}
func (mqm *MemoryQueueMessage) Nack() error {
	return nil
}
func (mqm *MemoryQueueMessage) Retry(rp gosignal.RetryParams) error {
	time.Sleep(rp.BackoffUntil.Sub(time.Now()))
	mqm.attempts++
	return mqm.queue.Send(mqm.messageType, mqm.message)
}
