package queue

import (
	"testing"

	"github.com/Howard3/gosignal"
	"github.com/stretchr/testify/assert"
)

func TestSubscribeNewMessageType(t *testing.T) {
	mq := &queue.MemoryQueue{Queue: make(map[string]map[uint]chan gosignal.QueueMessage)}
	messageType := "testType"

	_, ch, err := mq.Subscribe(messageType)
	assert.NoError(t, err)
	assert.NotNil(t, ch)
	assert.Contains(t, mq.Queue, messageType)
}
