package queue

import (
	"errors"
	"testing"
	"time"

	"github.com/Howard3/gosignal"
)

// TestSubscribeAndSend tests subscribing to a message type and sending a message.
func TestSubscribeAndSend(t *testing.T) {
	q := &MemoryQueue{channels: make(map[string][]chan gosignal.QueueMessage)}
	ch := make(chan gosignal.QueueMessage, 1)
	messageType := "testType"

	err := q.Subscribe(messageType, ch)
	if err != nil {
		t.Errorf("Error subscribing to message type: %v", err)
	}

	err = q.Send(messageType, []byte("test message"))
	if err != nil {
		t.Errorf("Error sending message: %v", err)
	}

	select {
	case msg := <-ch:
		if msg.Type() != messageType {
			t.Errorf("Expected message type %s, got %s", messageType, msg.Type())
		}
	case <-time.After(1 * time.Second):
		t.Errorf("Expected message was not received")
	}
}

// TestSendToNonExistentQueue tests sending to a non-existent queue.
func TestSendToNonExistentQueue(t *testing.T) {
	q := &MemoryQueue{channels: make(map[string][]chan gosignal.QueueMessage)}
	err := q.Send("nonexistentType", []byte("test message"))

	if err == nil {
		t.Errorf("Expected error sending to non-existent queue")
	}

	if err != nil && !errors.Is(err, ErrQueueDoesNotExist) {
		t.Errorf("Expected ErrQueueDoesNotExist, got %v", err)
	}
}
