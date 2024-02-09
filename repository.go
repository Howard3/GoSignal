package gosignal

import (
	"errors"
	"time"
)

// ErrLoadingEvents is the error returned when an error occurs while loading events
// it is joined with the underlying error
var ErrLoadingEvents = errors.New("error loading events")

// ErrApplyingEvent is the error returned when an error occurs while applying an event
// it is joined with the underlying error
var ErrApplyingEvent = errors.New("error applying event")

// ErrReplacingVersion is the error returned when an error occurs while replacing the version of an aggregate
// it is joined with the underlying error
var ErrReplacingVersion = errors.New("error replacing version")

// ErrNoEvents is the error returned when no events are found
var ErrNoEvents = errors.New("no events found")

// ErrVersionNotFound is the error returned when a version is not found
var ErrVersionNotFound = errors.New("version not found")

type loadOptions struct {
	lev          *LoadEventsOptions
	skipSnapshot bool
}

type loadOption func(*loadOptions)

// WithMinVersion sets the minimum version of the aggregate to load
func WithMinVersion(minVersion int) loadOption {
	return func(opts *loadOptions) {
		opts.lev.MinVersion = &minVersion
	}
}

// WithMaxVersion sets the maximum version of the aggregate to load
func WithMaxVersion(maxVersion int) loadOption {
	return func(opts *loadOptions) {
		opts.lev.MaxVersion = &maxVersion
	}
}

// WithEventTypes sets the event types to load
func WithEventTypes(eventTypes ...string) loadOption {
	return func(opts *loadOptions) {
		opts.lev.EventTypes = eventTypes
	}
}

// WithFromTime sets the time from which to load events
func WithFromTime(fromTime time.Time) loadOption {
	return func(opts *loadOptions) {
		opts.lev.FromTime = &fromTime
	}
}

// WithToTime sets the time to which to load events
func WithToTime(toTime time.Time) loadOption {
	return func(opts *loadOptions) {
		opts.lev.ToTime = &toTime
	}
}

// WithSkipSnapshot skips loading the
func WithSkipSnapshot() loadOption {
	return func(opts *loadOptions) {
		opts.skipSnapshot = true
	}
}

func applyLoadOptions(options []loadOption) loadOptions {
	opts := loadOptions{lev: &LoadEventsOptions{}}
	for _, opt := range options {
		opt(&opts)
	}

	return opts
}

// Repository is a struct that interacts with the event store, snapshot store, and aggregate
type Repository struct {
	eventStore EventStore
}

// NewRepository creates a new repository
func NewRepository(eventStore EventStore) *Repository {
	return &Repository{eventStore: eventStore}
}

// Store stores events in the event store
func (r *Repository) Store(aggregateID string, events []Event) error {
	return r.eventStore.Store(aggregateID, events)
}

// Load loads an aggregate from the event store, reconstructing it from its events and snapshot
func (r *Repository) Load(aggregateID string, aggregate Aggregate, opts ...loadOption) error {
	// TODO: load snapshot, allow modifying options based on snapshot

	events, err := r.LoadEvents(aggregateID, opts...)
	if err != nil {
		return err
	}

	return r.applyEvents(aggregate, events)
}

func (r *Repository) applyEvents(aggregate Aggregate, events []Event) error {
	for _, event := range events {
		if err := aggregate.Apply(event); err != nil {
			return errors.Join(ErrApplyingEvent, err)
		}
	}
	return nil
}

// LoadEvents loads events from the event store
func (r *Repository) LoadEvents(aggregateID string, opts ...loadOption) ([]Event, error) {
	options := applyLoadOptions(opts)
	event, err := r.eventStore.Load(aggregateID, *options.lev)
	if err != nil {
		return nil, errors.Join(ErrLoadingEvents, err)
	}

	return event, nil
}

// ReplaceVersion replaces the version of an aggregate in the event store
// Note: this is a dangerous operation and should be used with caution, it exists largely for
// legal compliance reasons, otherwise your event store should be append-only
// NOTE: Pass an empty aggregate in, as this function will replay all events on the aggregate
// and apply the new event, this is to ensure that the aggregate is in the correct state
func (r *Repository) ReplaceVersion(id string, agg Aggregate, v uint, e Event) error {
	events, err := r.LoadEvents(id, WithMaxVersion(int(v)))
	if err != nil {
		return errors.Join(ErrReplacingVersion, err)
	}

	if len(events) == 0 {
		return ErrNoEvents
	}

	events, err = r.replaceVersionInEventSlice(events, v, e)
	if err != nil {
		return errors.Join(ErrReplacingVersion, err)
	}

	// attempt to replay all events on the aggregate
	if err := r.applyEvents(agg, events); err != nil {
		return errors.Join(ErrReplacingVersion, err)
	}

	if err := r.eventStore.Replace(id, v, e); err != nil {
		return errors.Join(ErrReplacingVersion, err)
	}

	return nil
}

func (r *Repository) replaceVersionInEventSlice(events []Event, v uint, e Event) ([]Event, error) {
	for i, event := range events {
		if event.Version == v {
			events[i] = e
			return events, nil
		}
	}
	return nil, ErrVersionNotFound
}
