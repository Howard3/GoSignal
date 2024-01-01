package serde

import (
	"testing"
	"time"

	"github.com/Howard3/gosignal"
)

// CustomStruct is our custom type for testing.
type CustomStruct struct {
	Field1 string
	Field2 int
}

// TestJSON_Serialize tests the Serialize function for different types.
func TestJSON_SerDeString(t *testing.T) {
	currentTime := time.Now()

	message := gosignal.Message[string]{
		Type:      "string",
		ID:        "123",
		CreatedAt: currentTime,
		Payload:   "test message",
	}

	serializer := JSON[string]{}
	got, err := serializer.Serialize(message)

	if err != nil {
		t.Errorf("JSON.Serialize() error = %v", err)
		return
	}

	deserialized, err := serializer.Deserialize(got)

	if err != nil {
		t.Errorf("JSON.Deserialize() error = %v", err)
		return
	}

	// check fields one by one
	if deserialized.Type != message.Type {
		t.Errorf("JSON.Deserialize() Type = %v, want %v", deserialized.Type, message.Type)
	}

	if deserialized.ID != message.ID {
		t.Errorf("JSON.Deserialize() ID = %v, want %v", deserialized.ID, message.ID)
	}

	// permit difference of up to 1 second
	if deserialized.CreatedAt.Sub(message.CreatedAt) > time.Second {
		t.Errorf("JSON.Deserialize() CreatedAt = %v, want %v", deserialized.CreatedAt, message.CreatedAt)
	}

	if deserialized.Payload != message.Payload {
		t.Errorf("JSON.Deserialize() Payload = %v, want %v", deserialized.Payload, message.Payload)
	}
}

func TestJSON_SerDeInt(t *testing.T) {
	currentTime := time.Now()

	message := gosignal.Message[int]{}
	message.Type = "int"
	message.ID = "123"
	message.CreatedAt = currentTime
	message.Payload = 123

	serializer := JSON[int]{}
	got, err := serializer.Serialize(message)

	if err != nil {
		t.Errorf("JSON.Serialize() error = %v", err)
		return
	}

	deserialized, err := serializer.Deserialize(got)

	if err != nil {
		t.Errorf("JSON.Deserialize() error = %v", err)
		return
	}

	// check fields one by one
	if deserialized.Type != message.Type {
		t.Errorf("JSON.Deserialize() Type = %v, want %v", deserialized.Type, message.Type)
	}

	if deserialized.ID != message.ID {
		t.Errorf("JSON.Deserialize() ID = %v, want %v", deserialized.ID, message.ID)
	}

	// permit difference of up to 1 Second
	if deserialized.CreatedAt.Sub(message.CreatedAt) > time.Second {
		t.Errorf("JSON.Deserialize() CreatedAt = %v, want %v", deserialized.CreatedAt, message.CreatedAt)
	}

	if deserialized.Payload != message.Payload {
		t.Errorf("JSON.Deserialize() Payload = %v, want %v", deserialized.Payload, message.Payload)
	}

}

func TestJSON_SerDeCustomStruct(t *testing.T) {
	currentTime := time.Now()

	message := gosignal.Message[CustomStruct]{}
	message.Type = "CustomStruct"
	message.ID = "123"
	message.CreatedAt = currentTime
	message.Payload = CustomStruct{
		Field1: "test",
		Field2: 123,
	}

	serializer := JSON[CustomStruct]{}
	got, err := serializer.Serialize(message)

	if err != nil {
		t.Errorf("JSON.Serialize() error = %v", err)
		return
	}

	deserialized, err := serializer.Deserialize(got)

	if err != nil {
		t.Errorf("JSON.Deserialize() error = %v", err)
		return
	}

	// check fields one by one
	if deserialized.Type != message.Type {
		t.Errorf("JSON.Deserialize() Type = %v, want %v", deserialized.Type, message.Type)
	}

	if deserialized.ID != message.ID {
		t.Errorf("JSON.Deserialize() ID = %v, want %v", deserialized.ID, message.ID)
	}

	// permit difference of up to 1 second
	if deserialized.CreatedAt.Sub(message.CreatedAt) > time.Second {
		t.Errorf("JSON.Deserialize() CreatedAt = %v, want %v", deserialized.CreatedAt, message.CreatedAt)
	}

	if deserialized.Payload != message.Payload {
		t.Errorf("JSON.Deserialize() Payload = %v, want %v", deserialized.Payload, message.Payload)
	}

}
