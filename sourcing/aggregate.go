package sourcing

import "github.com/Howard3/gosignal"

// Aggregate is the interface that wraps the basic aggregate operations
type Aggregate interface {
	Apply(gosignal.Event) error
	ImportState(*Snapshot) error
	ExportState() ([]byte, error)
	ID() string
	Version() uint
}
