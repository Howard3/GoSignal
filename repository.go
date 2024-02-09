package gosignal

import (
	"context"
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
// NOTE: USE WITH CAUTION. Using incorrectly could result in an inconsistent state for your aggregate
func WithMinVersion(minVersion uint) loadOption {
	return func(opts *loadOptions) {
		opts.lev.MinVersion = &minVersion
	}
}

// WithMaxVersion sets the maximum version of the aggregate to load
// can be used to load a specific version of the aggregate
func WithMaxVersion(maxVersion uint) loadOption {
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
	eventStore       EventStore
	snapshotStrategy SnapshotStrategy
}

// NewRepository creates a new repository
func NewRepository(eventStore EventStore) *Repository {
	return &Repository{eventStore: eventStore}
}

// Store stores events in the event store
func (r *Repository) Store(ctx context.Context, aggID string, events []Event) error {
	// TODO: publish events to event bus when it's successfully stored
	return r.eventStore.Store(ctx, aggID, events)
}

// Load loads an aggregate from the event store, reconstructing it from its events and snapshot
func (r *Repository) Load(ctx context.Context, aggID string, aggregate Aggregate, opts ...loadOption) error {
	snapshot, err := r.snapshotLoader(ctx, aggID)
	if err != nil {
		return err
	}

	if snapshot != nil {
		opts = append(opts, WithMinVersion(snapshot.Version+1)) // load events after the snapshot
	}

	events, err := r.LoadEvents(ctx, aggID, opts...)
	if err != nil {
		return errors.Join(ErrLoadingEvents, err)
	}

	if err := r.applyEvents(aggregate, events); err != nil {
		return errors.Join(ErrApplyingEvent, err)
	}

	if r.snapshotStrategy.ShouldSnapshot(snapshot, events) {
		if err := r.generateSnapshot(ctx, aggID, aggregate); err != nil {
			return errors.Join(ErrSnapshotFailed, err)
		}
	}

	return nil
}

func (r *Repository) generateSnapshot(ctx context.Context, aggID string, agg Aggregate) error {
	if r.snapshotStrategy == nil || r.snapshotStrategy.GetStore() == nil {
		return nil // nothing to do
	}

	state, err := agg.ExportState()
	if err != nil {
		return errors.Join(ErrFailedToExportState, err)
	}

	ss := Snapshot{
		Data:      state,
		Timestamp: time.Now(),
		Version:   agg.Version(),
	}

	if err := r.snapshotStrategy.GetStore().Store(ctx, aggID, ss); err != nil {
		return errors.Join(ErrFailedToExportState, err)
	}

	return nil
}

func (r *Repository) snapshotLoader(ctx context.Context, aggID string) (*Snapshot, error) {
	if r.snapshotStrategy == nil || r.snapshotStrategy.GetStore() == nil {
		return nil, nil // nothing to do
	}

	snapshot, err := r.snapshotStrategy.GetStore().Load(ctx, aggID)
	if err != nil {
		return nil, errors.Join(ErrFailedToLoadSnapshot, err)
	}

	return &snapshot, nil
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
func (r *Repository) LoadEvents(ctx context.Context, aggregateID string, opts ...loadOption) ([]Event, error) {
	options := applyLoadOptions(opts)
	event, err := r.eventStore.Load(ctx, aggregateID, *options.lev)
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
func (r *Repository) ReplaceVersion(ctx context.Context, string, aggID string, agg Aggregate, v uint, e Event) error {
	events, err := r.LoadEvents(ctx, aggID, WithMaxVersion(v))
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

	if err := r.eventStore.Replace(ctx, aggID, v, e); err != nil {
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
