package gosignal

import (
	"context"
	"errors"
	"time"
)

// ErrFailedToLoadSnapshot is the error returned when an error occurs while loading a snapshot
// it is joined with the underlying error
var ErrFailedToLoadSnapshot = errors.New("failed to load snapshot")

// ErrSnapshotFailed is the error returned when a snapshot fails to be created
// it is joined with the underlying error
var ErrSnapshotFailed = errors.New("snapshot failed")

// ErrFailedToExportState is the error returned when an error occurs while exporting the state
// it is joined with the underlying error
var ErrFailedToExportState = errors.New("failed to export state")

type Snapshot struct {
	Data      []byte
	Timestamp time.Time
	Version   uint
}

type SnapshotStrategy interface {
	ShouldSnapshot(snapshot *Snapshot, events []Event) bool
	RegisterStore(store SnapshotStore)
	GetStore() SnapshotStore
}

type SnapshotStore interface {
	Load(ctx context.Context, id string) (Snapshot, error)
	Store(ctx context.Context, aggregateID string, snapshot Snapshot) error
	Delete(ctx context.Context, aggregateID string) error
}
