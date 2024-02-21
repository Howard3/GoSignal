package sourcing

import "time"

// RepoLoaderConfigurator is responsible for setting up load options for the repository.
type RepoLoaderConfigurator struct {
	loadOptions RepoLoadOptions
}

// RepoLoadOptions holds the configuration options for loading from the repository.
type RepoLoadOptions struct {
	lev          *LoadEventsOptions
	skipSnapshot bool
}

// NewRepoLoaderConfigurator creates a new instance of RepoLoaderConfigurator with default settings.
func NewRepoLoaderConfigurator() *RepoLoaderConfigurator {
	return &RepoLoaderConfigurator{
		loadOptions: RepoLoadOptions{
			lev: &LoadEventsOptions{},
		},
	}
}

// MinVersion sets the minimum version of the aggregate to load and returns the configurator.
// NOTE: this will lead to inconsistent state if you're loading against an aggregate. Only use
// this if you're directly querying events.
func (c *RepoLoaderConfigurator) MinVersion(version uint64) *RepoLoaderConfigurator {
	c.loadOptions.lev.MinVersion = &version
	return c
}

// MaxVersion sets the maximum version of the aggregate to load and returns the configurator.
// can be used to load a specific version of the aggregate
func (c *RepoLoaderConfigurator) MaxVersion(version uint64) *RepoLoaderConfigurator {
	c.loadOptions.lev.MaxVersion = &version
	return c
}

// EventTypes sets the event types to load and returns the configurator.
// NOTE: this will lead to inconsistent state if you're loading against an aggregate. Only use
// this if you're directly querying events.
func (c *RepoLoaderConfigurator) EventTypes(types ...string) *RepoLoaderConfigurator {
	c.loadOptions.lev.EventTypes = types
	return c
}

// FromTime sets the starting time from which to load events and returns the configurator.
// NOTE: this will lead to inconsistent state if you're loading against an aggregate. Only use
// this if you're directly querying events.
func (c *RepoLoaderConfigurator) FromTime(time time.Time) *RepoLoaderConfigurator {
	c.loadOptions.lev.FromTime = &time
	return c
}

// ToTime sets the ending time to which to load events and returns the configurator.
func (c *RepoLoaderConfigurator) ToTime(time time.Time) *RepoLoaderConfigurator {
	c.loadOptions.lev.ToTime = &time
	return c
}

// SkipSnapshot indicates to skip loading the snapshot during the operation and returns the configurator.
func (c *RepoLoaderConfigurator) SkipSnapshot(skip bool) *RepoLoaderConfigurator {
	c.loadOptions.skipSnapshot = skip
	return c
}

// Build finalizes the configuration and returns the configured RepoLoadOptions.
func (c *RepoLoaderConfigurator) Build() *RepoLoadOptions {
	return &c.loadOptions
}
