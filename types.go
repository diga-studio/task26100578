package workflow

// NodeKind enumerates the kinds of workflow nodes.
type NodeKind int

const (
	KindStart NodeKind = iota
	KindEnd
	KindTask
	KindDecision
	KindFork
	KindJoin
	KindCall
)

// Node is a single node in a workflow definition.
type Node struct {
	ID      string
	Kind    NodeKind
	Out     []string // outgoing edge IDs
	CallDef string   // referenced definition ID, Call nodes only
}

// Edge connects two nodes inside a definition.
type Edge struct {
	ID   string
	From string
	To   string
}

// Definition is an immutable workflow graph (a DAG).
type Definition struct {
	ID    string
	Nodes map[string]*Node
	Edges map[string]*Edge
	In    map[string][]string // node ID -> incoming edge IDs
}

// NewDefinition builds a Definition and derives the incoming-edge index.
func NewDefinition(id string, nodes []*Node, edges []*Edge) *Definition {
	d := &Definition{
		ID:    id,
		Nodes: make(map[string]*Node, len(nodes)),
		Edges: make(map[string]*Edge, len(edges)),
		In:    make(map[string][]string, len(nodes)),
	}
	for _, n := range nodes {
		d.Nodes[n.ID] = n
	}
	for _, e := range edges {
		d.Edges[e.ID] = e
		d.In[e.To] = append(d.In[e.To], e.ID)
	}
	return d
}

// nodeState is the runtime state of a node within one call scope.
type nodeState int

const (
	statePending nodeState = iota
	stateReady             // Task/Decision waiting for external resolve
	stateRunning           // Call waiting for its sub-flow
	stateDone
	stateDead // 失活: impossible to reach
)

// instance is one activation of a definition (root or via a Call).
type instance struct {
	def        *Definition
	states     map[string]nodeState
	fired      map[string]bool      // edge ID -> actually arrived
	children   map[string]*instance // Call node ID -> sub-flow instance
	parent     *instance
	parentCall string
}

// WorkItem is a ready Task or Decision awaiting resolve.
type WorkItem struct {
	ID       string
	NodeID   string
	Kind     NodeKind
	Path     []string // Call-node path identifying the activation
	DefID    string
	OutEdges []string // selectable edges for Decision
}

// Run is the execution handle returned by Start.
type Run struct {
	defs  map[string]*Definition
	root  *instance
	ready map[string]*WorkItem
	seq   int
	done  bool
}

// InstanceSnapshot reports the state of one activated definition.
type InstanceSnapshot struct {
	Path       []string
	DefID      string
	NodeStates map[string]nodeState
}

// Snapshot is the result of Inspect.
type Snapshot struct {
	Done      bool
	Ready     []WorkItem
	Instances []InstanceSnapshot
}
