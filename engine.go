package workflow

import "fmt"

// Start launches the root flow and returns the handle plus first ready work items.
func Start(defs []*Definition, entry string) (*Run, []WorkItem) {
	run := &Run{
		defs:  make(map[string]*Definition, len(defs)),
		ready: make(map[string]*WorkItem),
	}
	for _, d := range defs {
		run.defs[d.ID] = d
	}
	root := run.newInstance(run.defs[entry], nil, "")
	run.root = root
	run.propagate()
	return run, run.readyList()
}

// Resolve completes a ready Task or picks an outgoing edge of a ready Decision.
func Resolve(run *Run, workID string, selectedEdge string) ([]WorkItem, error) {
	item, ok := run.ready[workID]
	if !ok {
		return nil, fmt.Errorf("workflow: unknown work item %q", workID)
	}
	inst := run.findInstance(item.Path)
	if inst == nil {
		return nil, fmt.Errorf("workflow: instance for %q no longer exists", workID)
	}
	if item.Kind == KindDecision {
		valid := false
		for _, e := range inst.def.Nodes[item.NodeID].Out {
			if e == selectedEdge {
				valid = true
				break
			}
		}
		if !valid {
			return nil, fmt.Errorf("workflow: edge %q is not an outgoing edge of %q", selectedEdge, item.NodeID)
		}
	}
	delete(run.ready, workID)
	inst.states[item.NodeID] = stateDone
	switch item.Kind {
	case KindTask:
		inst.fire(inst.def.Nodes[item.NodeID].Out[0])
	case KindDecision:
		inst.fire(selectedEdge)
	}
	run.propagate()
	return run.readyList(), nil
}

// Inspect reports the current run state.
func Inspect(run *Run) Snapshot {
	snap := Snapshot{Done: run.done, Ready: run.readyList()}
	var walk func(inst *instance)
	walk = func(inst *instance) {
		is := InstanceSnapshot{
			Path:       inst.path(),
			DefID:      inst.def.ID,
			NodeStates: make(map[string]nodeState, len(inst.states)),
		}
		for id, st := range inst.states {
			is.NodeStates[id] = st
		}
		snap.Instances = append(snap.Instances, is)
		for _, child := range inst.children {
			walk(child)
		}
	}
	walk(run.root)
	return snap
}

func (run *Run) newInstance(def *Definition, parent *instance, parentCall string) *instance {
	inst := &instance{
		def:        def,
		states:     make(map[string]nodeState, len(def.Nodes)),
		fired:      make(map[string]bool),
		children:   make(map[string]*instance),
		parent:     parent,
		parentCall: parentCall,
	}
	for id := range def.Nodes {
		inst.states[id] = statePending
	}
	return inst
}

func (inst *instance) path() []string {
	var rev []string
	for cur := inst; cur.parent != nil; cur = cur.parent {
		rev = append(rev, cur.parentCall)
	}
	path := make([]string, len(rev))
	for i, p := range rev {
		path[len(rev)-1-i] = p
	}
	return path
}

func (run *Run) findInstance(path []string) *instance {
	inst := run.root
	for _, callNode := range path {
		inst = inst.children[callNode]
		if inst == nil {
			return nil
		}
	}
	return inst
}

func (inst *instance) fire(edgeID string) {
	inst.fired[edgeID] = true
}

// edgeImpossible reports whether the edge can never carry an arrival:
// its source is dead, or its source finished without firing it.
func (inst *instance) edgeImpossible(edgeID string) bool {
	if inst.fired[edgeID] {
		return false
	}
	src := inst.def.Edges[edgeID].From
	switch inst.states[src] {
	case stateDead, stateDone:
		return true
	}
	return false
}

// propagate advances everything that needs no external input, to a fixpoint.
func (run *Run) propagate() {
	changed := true
	for changed {
		changed = false
		var walk func(inst *instance)
		walk = func(inst *instance) {
			for id, node := range inst.def.Nodes {
				if inst.states[id] != statePending {
					continue
				}
				if inst.tryDeactivate(inst.def.In[id]) {
					inst.states[id] = stateDead
					changed = true
					continue
				}
				if inst.tryActivate(inst.def.In[id], node.Kind) {
					run.execute(inst, node)
					changed = true
				}
			}
			for _, child := range inst.children {
				walk(child)
			}
		}
		walk(run.root)
	}
}

// tryDeactivate: a non-Start node with every incoming edge impossible dies.
func (inst *instance) tryDeactivate(in []string) bool {
	if len(in) == 0 {
		return false // Start has no incoming edges
	}
	for _, e := range in {
		if !inst.edgeImpossible(e) {
			return false
		}
	}
	return true
}

// tryActivate: Join needs every incoming edge resolved and at least one
// arrival; every other node needs a single arrival.
func (inst *instance) tryActivate(in []string, kind NodeKind) bool {
	if kind == KindStart {
		return true // Start has no incoming edges and fires immediately
	}
	arrived := false
	for _, e := range in {
		if inst.fired[e] {
			arrived = true
			continue
		}
		if kind == KindJoin {
			if !inst.edgeImpossible(e) {
				return false
			}
		} else {
			return false
		}
	}
	return arrived
}

// execute runs an auto node or enqueues a Task/Decision work item.
func (run *Run) execute(inst *instance, node *Node) {
	switch node.Kind {
	case KindTask, KindDecision:
		inst.states[node.ID] = stateReady
		run.seq++
		run.ready[fmt.Sprintf("w%d", run.seq)] = &WorkItem{
			ID:       fmt.Sprintf("w%d", run.seq),
			NodeID:   node.ID,
			Kind:     node.Kind,
			Path:     inst.path(),
			DefID:    inst.def.ID,
			OutEdges: append([]string(nil), node.Out...),
		}
	case KindStart, KindFork, KindJoin:
		inst.states[node.ID] = stateDone
		for _, e := range node.Out {
			inst.fire(e)
		}
	case KindEnd:
		inst.states[node.ID] = stateDone
		if inst.parent == nil {
			run.done = true
			return
		}
		call := inst.parentCall
		inst.parent.states[call] = stateDone
		for _, e := range inst.parent.def.Nodes[call].Out {
			inst.parent.fire(e)
		}
	case KindCall:
		inst.states[node.ID] = stateRunning
		inst.children[node.ID] = run.newInstance(run.defs[node.CallDef], inst, node.ID)
	}
}

func (run *Run) readyList() []WorkItem {
	items := make([]WorkItem, 0, len(run.ready))
	for _, item := range run.ready {
		items = append(items, *item)
	}
	return items
}
