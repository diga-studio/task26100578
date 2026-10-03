package workflow

import (
	"errors"
	"fmt"
)

// engine holds all runtime state for one run.
type engine struct {
	defs      map[string]*compiledDef
	instances []*instance
	works     map[string]*WorkItem // ready work items by ID
	workSeq   int
	instSeq   int
	runID     string
	done      bool

	pending  []edgeRef  // edges fired but not yet delivered
	newWorks []WorkItem // work items created during the current operation
}

type edgeRef struct {
	inst *instance
	edge int
}

// Start launches the root workflow defined by entry and returns the run
// handle plus the first batch of ready work items.
func Start(definitions []Definition, entry string) (*Run, []WorkItem, error) {
	eng := &engine{
		defs:  make(map[string]*compiledDef),
		works: make(map[string]*WorkItem),
		runID: "run1",
	}
	for i := range definitions {
		d := &definitions[i]
		if _, dup := eng.defs[d.ID]; dup {
			return nil, nil, fmt.Errorf("duplicate definition %q", d.ID)
		}
		c, err := compileDefinition(d)
		if err != nil {
			return nil, nil, err
		}
		eng.defs[d.ID] = c
	}
	for _, c := range eng.defs {
		for _, target := range c.callDef {
			if _, ok := eng.defs[target]; !ok {
				return nil, nil, fmt.Errorf("call references unknown definition %q", target)
			}
		}
	}
	entryDef, ok := eng.defs[entry]
	if !ok {
		return nil, nil, fmt.Errorf("unknown entry definition %q", entry)
	}

	root := eng.newInstance(entryDef, nil)
	eng.execStart(root)
	eng.settle()

	run := &Run{id: eng.runID, eng: eng}
	return run, eng.newWorks, nil
}

// Resolve completes a ready Task (selectedEdge ignored) or selects an
// outgoing edge for a ready Decision. The selected edge is identified by
// the ID of the edge's target node. It returns newly ready work items.
func Resolve(run *Run, workID string, selectedEdge string) ([]WorkItem, error) {
	if run == nil || run.eng == nil {
		return nil, errors.New("invalid run")
	}
	eng := run.eng
	w, ok := eng.works[workID]
	if !ok {
		return nil, fmt.Errorf("work item %q is not ready", workID)
	}
	inst := eng.findInstance(w.InstanceID)
	if inst == nil {
		return nil, fmt.Errorf("instance %q not found", w.InstanceID)
	}

	switch w.Kind {
	case NodeTask:
		delete(eng.works, workID)
		inst.status[w.NodeID] = StatusDone
		eng.fireOutEdges(inst, w.NodeID)
	case NodeDecision:
		edge, err := findOutEdgeByTarget(inst.def, w.NodeID, selectedEdge)
		if err != nil {
			return nil, err
		}
		delete(eng.works, workID)
		inst.status[w.NodeID] = StatusDone
		eng.fireEdge(inst, edge)
	default:
		return nil, fmt.Errorf("work item %q has unresolvable kind", workID)
	}

	eng.settle()
	return eng.newWorks, nil
}

// Inspect reports the status of every node instance in the run.
func Inspect(run *Run) []NodeState {
	if run == nil || run.eng == nil {
		return nil
	}
	eng := run.eng
	var out []NodeState
	for _, inst := range eng.instances {
		for _, n := range inst.def.def.Nodes {
			out = append(out, NodeState{
				InstanceID: inst.id,
				DefID:      inst.def.def.ID,
				NodeID:     n.ID,
				Kind:       n.Kind,
				Status:     inst.status[n.ID],
			})
		}
	}
	return out
}

// Done reports whether the root End node has executed.
func Done(run *Run) bool {
	return run != nil && run.eng != nil && run.eng.done
}

func (e *engine) newInstance(def *compiledDef, parent *nodeRef) *instance {
	e.instSeq++
	inst := &instance{
		id:         fmt.Sprintf("i%d", e.instSeq),
		def:        def,
		status:     make(map[string]NodeStatus),
		fired:      make(map[int]bool),
		parentCall: parent,
	}
	e.instances = append(e.instances, inst)
	return inst
}

func (e *engine) findInstance(id string) *instance {
	for _, inst := range e.instances {
		if inst.id == id {
			return inst
		}
	}
	return nil
}

// settle propagates arrivals, deactivations and join firings to fixpoint.
func (e *engine) settle() {
	e.newWorks = nil
	for {
		for len(e.pending) > 0 {
			ref := e.pending[0]
			e.pending = e.pending[1:]
			e.arrive(ref.inst, ref.edge)
		}
		if !e.scanDeactivationAndJoins() {
			break
		}
	}
}

// arrive delivers a fired edge to its target node.
func (e *engine) arrive(inst *instance, edgeIdx int) {
	edge := inst.def.def.Edges[edgeIdx]
	target := edge.To
	if inst.status[target] != StatusPending {
		return
	}
	switch inst.def.kind[target] {
	case NodeTask, NodeDecision:
		inst.status[target] = StatusActive
		e.workSeq++
		w := WorkItem{
			ID:         fmt.Sprintf("w%d", e.workSeq),
			RunID:      e.runID,
			InstanceID: inst.id,
			DefID:      inst.def.def.ID,
			NodeID:     target,
			Kind:       inst.def.kind[target],
		}
		e.works[w.ID] = &w
		e.newWorks = append(e.newWorks, w)
	case NodeFork:
		inst.status[target] = StatusDone
		e.fireOutEdges(inst, target)
	case NodeCall:
		inst.status[target] = StatusActive
		childDef := e.defs[inst.def.callDef[target]]
		child := e.newInstance(childDef, &nodeRef{inst: inst, node: target})
		e.execStart(child)
	case NodeEnd:
		inst.status[target] = StatusDone
		if inst.parentCall != nil {
			p := inst.parentCall
			p.inst.status[p.node] = StatusDone
			e.fireOutEdges(p.inst, p.node)
		} else {
			e.done = true
		}
	}
}

// scanDeactivationAndJoins deactivates unreachable nodes and fires ready
// joins. It reports whether anything changed.
func (e *engine) scanDeactivationAndJoins() bool {
	changed := false
	for _, inst := range e.instances {
		for _, n := range inst.def.def.Nodes {
			if inst.status[n.ID] != StatusPending || n.Kind == NodeStart {
				continue
			}
			inEdges := inst.def.in[n.ID]
			if n.Kind == NodeJoin && e.joinReady(inst, inEdges) {
				inst.status[n.ID] = StatusDone
				e.fireOutEdges(inst, n.ID)
				changed = true
				continue
			}
			if len(inEdges) > 0 && e.allImpossible(inst, inEdges) {
				inst.status[n.ID] = StatusDead
				changed = true
			}
		}
	}
	return changed
}

func (e *engine) joinReady(inst *instance, inEdges []int) bool {
	anyArrived := false
	for _, ei := range inEdges {
		if inst.fired[ei] {
			anyArrived = true
			continue
		}
		if !e.edgeImpossible(inst, ei) {
			return false
		}
	}
	return anyArrived
}

func (e *engine) allImpossible(inst *instance, edges []int) bool {
	for _, ei := range edges {
		if !e.edgeImpossible(inst, ei) {
			return false
		}
	}
	return true
}

// edgeImpossible reports whether an edge can never deliver an arrival:
// its source is dead, or its source finished without firing this edge.
func (e *engine) edgeImpossible(inst *instance, edgeIdx int) bool {
	if inst.fired[edgeIdx] {
		return false
	}
	edge := inst.def.def.Edges[edgeIdx]
	switch inst.status[edge.From] {
	case StatusDead:
		return true
	case StatusDone:
		return true
	}
	return false
}

func (e *engine) execStart(inst *instance) {
	inst.status[inst.def.start] = StatusDone
	e.fireOutEdges(inst, inst.def.start)
}

func (e *engine) fireOutEdges(inst *instance, node string) {
	for _, ei := range inst.def.out[node] {
		e.fireEdge(inst, ei)
	}
}

func (e *engine) fireEdge(inst *instance, edgeIdx int) {
	if inst.fired[edgeIdx] {
		return
	}
	inst.fired[edgeIdx] = true
	e.pending = append(e.pending, edgeRef{inst: inst, edge: edgeIdx})
}

func findOutEdgeByTarget(def *compiledDef, node, target string) (int, error) {
	for _, ei := range def.out[node] {
		if def.def.Edges[ei].To == target {
			return ei, nil
		}
	}
	return -1, fmt.Errorf("node %q has no outgoing edge to %q", node, target)
}
