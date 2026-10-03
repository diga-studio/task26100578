package workflow

// NodeStatus is the runtime status of a node instance.
type NodeStatus int

const (
	// StatusPending: not yet determined reachable or impossible.
	StatusPending NodeStatus = iota
	// StatusActive: reachable, waiting to execute (or executing).
	StatusActive
	// StatusDone: executed.
	StatusDone
	// StatusDead: determined impossible to reach; deactivated.
	StatusDead
)

// WorkItem is a ready unit of external work (a Task awaiting completion,
// or a Decision awaiting an edge selection).
type WorkItem struct {
	ID         string
	RunID      string
	InstanceID string
	DefID      string
	NodeID     string
	Kind       NodeKind
}

// NodeState is the inspect view of one node instance.
type NodeState struct {
	InstanceID string
	DefID      string
	NodeID     string
	Kind       NodeKind
	Status     NodeStatus
}

// Run is a handle to a running (root) workflow instance.
type Run struct {
	id  string
	eng *engine
}

// ID returns the run identifier.
func (r *Run) ID() string { return r.id }

// instance is one activation of a definition (the root run or one Call).
type instance struct {
	id     string
	def    *compiledDef
	status map[string]NodeStatus // per node ID
	fired  map[int]bool          // per edge index: edge has delivered an arrival
	// parentCall identifies the Call node (in the parent instance) that
	// created this instance; nil for the root instance.
	parentCall *nodeRef
}

// nodeRef points to a node inside a specific instance.
type nodeRef struct {
	inst *instance
	node string
}
