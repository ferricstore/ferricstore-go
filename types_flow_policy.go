package ferricstore

// PolicySnapshot is the effective Flow policy returned by FerricStore.
// Generation is monotonic for one Flow type and can be supplied through
// PolicyOptions.ExpectedGeneration for compare-and-swap updates.
type PolicySnapshot struct {
	Type              string
	State             string
	Generation        int64
	Version           any
	Mode              FlowStateMode
	MaxActiveMS       *int64
	Retry             map[string]any
	Retention         map[string]any
	IndexedAttributes []string
	IndexedStateMeta  string
	Governance        map[string]any
	States            map[string]PolicyStateSnapshot
	Raw               map[string]any
}

// PolicyStateSnapshot is the effective policy for one Flow state.
type PolicyStateSnapshot struct {
	Mode        FlowStateMode
	MaxActiveMS *int64
	Retry       map[string]any
	Retention   map[string]any
	Governance  map[string]any
	Raw         map[string]any
}
