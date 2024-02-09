package gosignal

// Event is a struct that represents an event in the system
type Event struct {
	Type        string // Type of event
	Data        []byte // Data of the event
	Version     uint   // Version of the event
	Timestamp   int64  // Timestamp of the event
	AggregateID string // AggregateID of the event
}

// ToNotificationProto converts the event to a notification protobuf byte slice
func (e Event) ToNotificationProto() []byte {
	// TODO: implement
	panic("not implemented")
}
