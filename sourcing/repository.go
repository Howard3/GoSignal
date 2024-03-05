package sourcing

import (
	"context"
	"errors"
	"time"

	"github.com/Howard3/gosignal"
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

// ErrStoringEvents is the error returned when an error occurs while storing events
// it is joined with the underlying error
var ErrStoringEvents = errors.New("error storing events")

// ErrNoQueueDefined is the error returned when no queue is defined
// This occurs when trying to store events but you don't have a queue attached
var ErrNoQueueDefined = errors.New("no queue defined")

// ErrSendingEvent is the error returned when an error occurs while sending an event to the Queue
// it is joined with the underlying error
var ErrSendingEvent = errors.New("error sending event")

// Repository is a struct that interacts with the event store, snapshot store, and aggregate
type Repository struct {
	eventStore       EventStore
	snapshotStrategy SnapshotStrategy
	queue            gosignal.Queue
}

type NewRepoOptions func(*Repository)

func WithEventStore(es EventStore) func(*Repository) {
	return func(r *Repository) {
		r.eventStore = es
	}
}

func WithSnapshotStrategy(ss SnapshotStrategy) func(*Repository) {
	return func(r *Repository) {
		r.snapshotStrategy = ss
	}
}

func WithQueue(q gosignal.Queue) func(*Repository) {
	return func(r *Repository) {
		r.queue = q
	}
}

// NewRepository creates a new repository
func NewRepository(options ...NewRepoOptions) *Repository {
	r := &Repository{}
	for _, option := range options {
		option(r)
	}
	return r
}

// Store stores events in the event store
func (r *Repository) Store(ctx context.Context, events []gosignal.Event) error {
	if r.queue == nil {
		return ErrNoQueueDefined
	}

	if err := r.eventStore.Store(ctx, events); err != nil {
		return errors.Join(ErrStoringEvents, err)
	}

	for _, event := range events {
		if err := r.queue.Send(event.Type, event.Data); err != nil {
			// NOTE: this should probably roll back the stored event. TBD on preferred behavior here.
			return errors.Join(ErrSendingEvent, err)
		}
	}

	return nil
}

// Load loads an aggregate from the event store, reconstructing it from its events and snapshot
func (r *Repository) Load(ctx context.Context, agg Aggregate, opts *RepoLoadOptions) error {
	var err error
	var snapshot *Snapshot

	if opts == nil {
		opts = NewRepoLoaderConfigurator().Build()
	}

	if !opts.skipSnapshot {
		snapshot, err = r.snapshotLoader(ctx, agg.GetID())
		if err != nil {
			return err
		}
	}

	// we need to not use the snapshot, and skip snapshot generation if the max version is lower
	// than the snapshot
	maxVerLowerThanSnapshot := opts.lev.MaxVersion != nil && snapshot.Version > *opts.lev.MaxVersion
	opts.skipSnapshot = opts.skipSnapshot || maxVerLowerThanSnapshot

	if !maxVerLowerThanSnapshot && snapshot != nil {
		newMinVersion := snapshot.Version
		opts.lev.MinVersion = &newMinVersion

		if err = r.importState(ctx, agg, snapshot); err != nil {
			return errors.Join(ErrFailedToLoadSnapshot, err)
		}
	}

	events, err := r.LoadEvents(ctx, agg.GetID(), opts)
	if err != nil {
		return errors.Join(ErrLoadingEvents, err)
	}

	if len(events) == 0 && snapshot == nil {
		return ErrNoEvents
	}

	if err := r.ApplyEvents(agg, events); err != nil {
		return errors.Join(ErrApplyingEvent, err)
	}

	skipSnapshot := opts.skipSnapshot || r.snapshotStrategy == nil
	if !skipSnapshot && r.snapshotStrategy.ShouldSnapshot(snapshot, events) {
		if err := r.generateSnapshot(ctx, agg.GetID(), agg); err != nil {
			return errors.Join(ErrSnapshotFailed, err)
		}
	}

	return nil
}

func (r *Repository) importState(ctx context.Context, agg Aggregate, snapshot *Snapshot) error {
	if err := agg.ImportState(snapshot.Data); err != nil {
		return errors.Join(ErrFailedToLoadSnapshot, err)
	}

	agg.SetVersion(snapshot.Version)
	agg.SetID(snapshot.ID)

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
		Version:   agg.GetVersion(),
		ID:        agg.GetID(),
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

	return snapshot, nil
}

// ApplyEvents iteratively applies events to an aggregate
func (r *Repository) ApplyEvents(agg Aggregate, events []gosignal.Event) error {
	for _, event := range events {
		if err := agg.Apply(event); err != nil {
			return errors.Join(ErrApplyingEvent, err)
		}
	}
	return nil
}

// LoadEvents loads events from the event store
func (r *Repository) LoadEvents(ctx context.Context, aggregateID string, opts *RepoLoadOptions) ([]gosignal.Event, error) {
	if opts == nil {
		opts = NewRepoLoaderConfigurator().Build()
	}

	event, err := r.eventStore.Load(ctx, aggregateID, *opts.lev)
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
func (r *Repository) ReplaceVersion(ctx context.Context, aggID string, agg Aggregate, ver uint64, e gosignal.Event) error {
	opts := NewRepoLoaderConfigurator().MaxVersion(ver).Build()
	events, err := r.LoadEvents(ctx, aggID, opts)
	if err != nil {
		return errors.Join(ErrReplacingVersion, err)
	}

	if len(events) == 0 {
		return ErrNoEvents
	}

	events, err = r.replaceVersionInEventSlice(events, ver, e)
	if err != nil {
		return errors.Join(ErrReplacingVersion, err)
	}

	// attempt to replay all events on the aggregate
	if err := r.ApplyEvents(agg, events); err != nil {
		return errors.Join(ErrReplacingVersion, err)
	}

	if err := r.eventStore.Replace(ctx, aggID, ver, e); err != nil {
		return errors.Join(ErrReplacingVersion, err)
	}

	return nil
}

func (r *Repository) replaceVersionInEventSlice(events []gosignal.Event, v uint64, e gosignal.Event) ([]gosignal.Event, error) {
	for i, event := range events {
		if event.Version == v {
			events[i] = e
			return events, nil
		}
	}
	return nil, ErrVersionNotFound
}
