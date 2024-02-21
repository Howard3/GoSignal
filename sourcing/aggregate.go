package sourcing

import (
	"errors"
	"fmt"

	"github.com/Howard3/gosignal"
)

// ErrEventVersionNE is the error returned when an event version is not equal to the aggregate version
// called from SafeApply
var ErrEventVersionNE = errors.New("event version is not equal to the aggregate version")

// ErrApplyFailed is the error returned when an error occurs while applying an event to an aggregate
// it is joined with the underlying error
var ErrApplyFailed = fmt.Errorf("failed to apply event")

// Aggregate is the interface for all aggregates
// For ID and Version, the DefaultAggregate struct can be embedded to provide default implementations
type Aggregate interface {
	SetID(string)
	SetVersion(uint)
	GetID() string
	GetVersion() uint
	Apply(gosignal.Event) error
	ImportState([]byte) error
	ExportState() ([]byte, error)
}

// DefaultAggregate is a struct that can be embedded in an aggregate to provide default implementations
type DefaultAggregate struct {
	ID      string // ID of the aggregate
	Version uint   // Version of the aggregate
}

// SetID sets the id of the aggregate
func (a *DefaultAggregate) SetID(id string) {
	a.ID = id
}

// SetVersion sets the version of the aggregate
func (a *DefaultAggregate) SetVersion(version uint) {
	a.Version = version
}

// GetID returns the id of the aggregate
func (a *DefaultAggregate) GetID() string {
	return a.ID
}

// GetVersion returns the version of the aggregate
func (a *DefaultAggregate) GetVersion() uint {
	return a.Version
}

// SafeApply applies an event to an aggregate, ensuring that the event is applied in to the correct
// version of the aggregate.
//
// - It returns an error if the event version is not equal to the aggregate version.
// - It will increment the aggregate version if the event is applied successfully.
// - It returns an error if the apply function returns an error and can recover from a panic
func SafeApply(event gosignal.Event, agg Aggregate, applyFn func(gosignal.Event) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}

		if err != nil {
			err = errors.Join(ErrApplyFailed, err)
		}
	}()

	canApplyEvent := event.Version == agg.GetVersion()
	if !canApplyEvent {
		return errors.Join(
			ErrEventVersionNE,
			fmt.Errorf("event version: %d, aggregate version: %d", event.Version, agg.GetVersion()),
		)
	}

	if err := applyFn(event); err != nil {
		return err
	}

	agg.SetVersion(event.Version + 1)

	return nil
}
