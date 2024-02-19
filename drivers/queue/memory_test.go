package queue

import (
	"strconv"
	"testing"
	"time"

	"github.com/Howard3/gosignal"
)

func TestSubscribeNewMessageType(t *testing.T) {
	mq := &MemoryQueue{Queue: make(map[string]map[uint]chan gosignal.QueueMessage)}
	messageType := "testType"

	_, ch, err := mq.Subscribe(messageType)
	if err != nil {
		t.Fatal(err)
	}
	if ch == nil {
		t.Fatal("Channel is nil")
	}
	if _, ok := mq.Queue[messageType]; !ok {
		t.Fatal("Message type was not added to the queue")
	}
}

func TestSendToExistingMessageType(t *testing.T) {
	mq := &MemoryQueue{Queue: make(map[string]map[uint]chan gosignal.QueueMessage)}
	messageType := "existingType"
	_, ch, _ := mq.Subscribe(messageType)

	go func() {
		err := mq.Send(messageType, []byte("message"))
		if err != nil {
			t.Error(err)
			t.Fail()
		}
	}()

	select {
	case msg := <-ch:
		if msg == nil {
			t.Fatal("Received message is nil")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Expected message was not received")
	}
}

func TestSendToNonExistingMessageType(t *testing.T) {
	mq := &MemoryQueue{Queue: make(map[string]map[uint]chan gosignal.QueueMessage)}
	err := mq.Send("nonExistingType", []byte("message"))
	if err == nil {
		t.Fatal("Expected error was not received")
	}
}

func TestUnsubscribe(t *testing.T) {
	mq := &MemoryQueue{Queue: make(map[string]map[uint]chan gosignal.QueueMessage)}
	messageType := "typeToUnsubscribe"
	id, _, _ := mq.Subscribe(messageType)

	err := mq.Unsubscribe(messageType, id)
	if err != nil {
		t.Fatal(err)
	}
	// check if the messageType has the ID removed
	iid, err := strconv.Atoi(id)
	if err != nil {
		t.Log("failed to convert id to string")
		t.Fatal(err)
	}
	if _, ok := mq.Queue[messageType][uint(iid)]; ok {
		t.Fatal("ID was not removed from the messageType")
	}

	// Test for non-existing messageType
	err = mq.Unsubscribe("nonExistingType", "123")
	if err == nil {
		t.Fatal("Expected error was not received")
	}

	// Test for non-existing ID
	err = mq.Unsubscribe(messageType, "nonExistingID")
	if err == nil {
		t.Fatal("Expected error was not received")
	}
}
