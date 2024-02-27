package sourcing

import (
	"errors"
	"fmt"
	"strconv"

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
	SetVersion(uint64)
	GetID() string
	GetVersion() uint64
	Apply(gosignal.Event) error
	ImportState([]byte) error
	ExportState() ([]byte, error)
}

// DefaultAggregate is a struct that can be embedded in an aggregate to provide default implementations
type DefaultAggregate struct {
	DefaultAggregateVersionManager
	ID string // ID of the aggregate
}

// SetID sets the id of the aggregate
func (a *DefaultAggregate) SetID(id string) {
	a.ID = id
}

// GetID returns the id of the aggregate
func (a *DefaultAggregate) GetID() string {
	return a.ID
}

// DefaultAggregateUint64 is a struct that can be embedded in an aggregate to provide default implementations
// for the ID field as a uint64
type DefaultAggregateUint64 struct {
	DefaultAggregateVersionManager
	ID uint64 // ID of the aggregate
}

// SetID sets the id of the aggregate
func (a *DefaultAggregateUint64) SetID(id string) {
	s, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		// NOTE: Determine if this is the best way to handle this error
		panic(fmt.Errorf("DefaultAggregateUint64 failed to parse id: %s", id))
	}

	a.ID = s
}

// GetID returns the id of the aggregate
func (a *DefaultAggregateUint64) GetID() string {
	return strconv.FormatUint(a.ID, 10)
}

func (a *DefaultAggregateUint64) GetIDUint64() uint64 {
	return a.ID
}

func (a *DefaultAggregateUint64) SetIDUint64(id uint64) {
	a.ID = id
}

type DefaultAggregateVersionManager struct {
	Version uint64
}

func (a *DefaultAggregateVersionManager) SetVersion(version uint64) {
	a.Version = version
}

func (a *DefaultAggregateVersionManager) GetVersion() uint64 {
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
			err = errors.Join(fmt.Errorf("event: %q", event.Type), ErrApplyFailed, err)
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
