package queue

import (
	"testing"
	"time"

	"github.com/Howard3/gosignal"
	"github.com/stretchr/testify/assert"
)

func TestSubscribeNewMessageType(t *testing.T) {
	mq := &MemoryQueue{Queue: make(map[string]map[uint]chan gosignal.QueueMessage)}
	messageType := "testType"

	_, ch, err := mq.Subscribe(messageType)
	assert.NoError(t, err)
	assert.NotNil(t, ch)
	assert.Contains(t, mq.Queue, messageType)
}

func TestSendToExistingMessageType(t *testing.T) {
	mq := &MemoryQueue{Queue: make(map[string]map[uint]chan gosignal.QueueMessage)}
	messageType := "existingType"
	_, ch, _ := mq.Subscribe(messageType)

	go func() {
		err := mq.Send(messageType, []byte("message"))
		assert.NoError(t, err)
	}()

	select {
	case msg := <-ch:
		assert.NotNil(t, msg)
	case <-time.After(1 * time.Second):
		t.Fatal("Expected message was not received")
	}
}

func TestSendToNonExistingMessageType(t *testing.T) {
	mq := &MemoryQueue{Queue: make(map[string]map[uint]chan gosignal.QueueMessage)}
	err := mq.Send("nonExistingType", []byte("message"))
	assert.Error(t, err)
}

func TestUnsubscribe(t *testing.T) {
	mq := &MemoryQueue{Queue: make(map[string]map[uint]chan gosignal.QueueMessage)}
	messageType := "typeToUnsubscribe"
	id, _, _ := mq.Subscribe(messageType)

	err := mq.Unsubscribe(messageType, id)
	assert.NoError(t, err)
	assert.NotContains(t, mq.Queue[messageType], id)

	// Test for non-existing messageType
	err = mq.Unsubscribe("nonExistingType", "123")
	assert.Error(t, err)

	// Test for non-existing ID
	err = mq.Unsubscribe(messageType, "nonExistingID")
	assert.Error(t, err)
}
