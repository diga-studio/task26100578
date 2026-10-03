package workflow

// NodeKind enumerates the kinds of workflow nodes.
type NodeKind int

const (
	NodeStart NodeKind = iota
	NodeEnd
	NodeTask
	NodeDecision
	NodeFork
	NodeJoin
	NodeCall
)

// Edge is a directed edge between two nodes within one definition.
type Edge struct {
	From string
	To   string
}

// Node is a node in a workflow definition.
type Node struct {
	ID   string
	Kind NodeKind
	// DefID is only meaningful for Call nodes: the referenced definition.
	DefID string
}

// Definition is an immutable workflow graph (a DAG).
type Definition struct {
	ID    string
	Nodes []Node
	Edges []Edge
}

// compiledDef is the indexed, immutable view of a Definition.
type compiledDef struct {
	def     *Definition
	kind    map[string]NodeKind
	callDef map[string]string // Call node ID -> referenced definition ID
	out     map[string][]int  // node ID -> outgoing edge indices
	in      map[string][]int  // node ID -> incoming edge indices
	start   string
	end     string
}

func compileDefinition(d *Definition) (*compiledDef, error) {
	c := &compiledDef{
		def:     d,
		kind:    make(map[string]NodeKind),
		callDef: make(map[string]string),
		out:     make(map[string][]int),
		in:      make(map[string][]int),
	}
	for _, n := range d.Nodes {
		c.kind[n.ID] = n.Kind
		if n.Kind == NodeCall {
			c.callDef[n.ID] = n.DefID
		}
		if n.Kind == NodeStart {
			c.start = n.ID
		}
		if n.Kind == NodeEnd {
			c.end = n.ID
		}
	}
	for i, e := range d.Edges {
		c.out[e.From] = append(c.out[e.From], i)
		c.in[e.To] = append(c.in[e.To], i)
	}
	return c, nil
}
