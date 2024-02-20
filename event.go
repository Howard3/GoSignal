package gosignal

import "time"

// Event is a struct that represents an event in the system
type Event struct {
	Type        string    // Type of event
	Data        []byte    // Data of the event
	Version     uint      // Version of the event
	Timestamp   time.Time // Timestamp of the event
	AggregateID string    // AggregateID of the event
}
