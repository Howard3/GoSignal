package snapshots

import (
	"github.com/Howard3/gosignal"
	"github.com/Howard3/gosignal/sourcing"
)

type VersionIntervalStrategy struct {
	EveryNth int
	Store    sourcing.SnapshotStore
}

func (versionintervalstrategy *VersionIntervalStrategy) ShouldSnapshot(snapshot *sourcing.Snapshot, events []gosignal.Event) bool {
	return len(events) > versionintervalstrategy.EveryNth
}
func (versionintervalstrategy *VersionIntervalStrategy) GetStore() sourcing.SnapshotStore {
	return versionintervalstrategy.Store
}
