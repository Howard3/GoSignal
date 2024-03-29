package sourcing

import (
	"context"
	"time"

	"github.com/Howard3/gosignal"
)

// EventStore is the interface that wraps the basic event store operations
// it reperents some form of storage for your event sourcing solution.
type EventStore interface {
	// Store stores a list of events for a given aggregate id
	Store(ctx context.Context, events []gosignal.Event) error
	// Load loads all events for a given aggregate id
	Load(ctx context.Context, aggID string, options LoadEventsOptions) ([]gosignal.Event, error)
	// Replace replaces an event with a new version, this mostly exists for legal compliance
	// purposes, your event store should be append-only
	Replace(ctx context.Context, id string, version uint64, event gosignal.Event) error
}

// LoadEventsOptions represents the options that can be passed to the Load method
type LoadEventsOptions struct {
	MinVersion *uint64    // the minimum version of the aggregate to load
	MaxVersion *uint64    // the maximum version of the aggregate to load
	EventTypes []string   // the types of events to load, if empty all events are loaded
	FromTime   *time.Time // the time from which to load events
	ToTime     *time.Time // the time to which to load events
}
