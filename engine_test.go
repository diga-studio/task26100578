package workflow

import "testing"

// findWork locates a ready work item by node ID within a work list.
func findWork(t *testing.T, works []WorkItem, nodeID string) WorkItem {
	t.Helper()
	for _, w := range works {
		if w.NodeID == nodeID {
			return w
		}
	}
	t.Fatalf("no ready work item for node %q (have %v)", nodeID, works)
	return WorkItem{}
}

// stateOf returns the status of one node in one instance.
func stateOf(t *testing.T, run *Run, instanceID, nodeID string) NodeStatus {
	t.Helper()
	for _, s := range Inspect(run) {
		if s.InstanceID == instanceID && s.NodeID == nodeID {
			return s.Status
		}
	}
	t.Fatalf("no state for %q in instance %q", nodeID, instanceID)
	return StatusPending
}

// crossGraph builds: S->F; F->D; F->T; D->X->Y->J2; D->J1; T->J1; J1->J2; J2->E.
func crossGraph() []Definition {
	return []Definition{{
		ID: "main",
		Nodes: []Node{
			{ID: "S", Kind: NodeStart},
			{ID: "F", Kind: NodeFork},
			{ID: "D", Kind: NodeDecision},
			{ID: "T", Kind: NodeTask},
			{ID: "X", Kind: NodeTask},
			{ID: "Y", Kind: NodeTask},
			{ID: "J1", Kind: NodeJoin},
			{ID: "J2", Kind: NodeJoin},
			{ID: "E", Kind: NodeEnd},
		},
		Edges: []Edge{
			{From: "S", To: "F"},
			{From: "F", To: "D"},
			{From: "F", To: "T"},
			{From: "D", To: "X"},
			{From: "X", To: "Y"},
			{From: "Y", To: "J2"},
			{From: "D", To: "J1"},
			{From: "T", To: "J1"},
			{From: "J1", To: "J2"},
			{From: "J2", To: "E"},
		},
	}}
}

// TestCrossJoinSelectDirectPath selects D->X (the direct-to-end-join path)
// and verifies the run cannot finish before the slow task T completes.
func TestCrossJoinSelectDirectPath(t *testing.T) {
	run, works, err := Start(crossGraph(), "main")
	if err != nil {
		t.Fatal(err)
	}
	// Fork activates Decision D and slow Task T.
	findWork(t, works, "D")
	wT := findWork(t, works, "T")

	// Choose the direct path D->X->Y->J2.
	works, err = Resolve(run, findWork(t, works, "D").ID, "X")
	if err != nil {
		t.Fatal(err)
	}
	// X and Y become ready in sequence; T is still pending.
	works, err = Resolve(run, findWork(t, works, "X").ID, "")
	if err != nil {
		t.Fatal(err)
	}
	works, err = Resolve(run, findWork(t, works, "Y").ID, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(works) != 0 {
		t.Fatalf("expected no new work, got %v", works)
	}
	// Y->J2 arrived but J1->J2 is still possible (J1 pending), so the run
	// must not finish before slow task T completes.
	if Done(run) {
		t.Fatal("run finished before slow task T completed")
	}
	if got := stateOf(t, run, "i1", "J2"); got != StatusPending {
		t.Fatalf("J2 should be pending, got %v", got)
	}

	// Complete slow task T: J1 fires (D->J1 impossible, T->J1 arrived),
	// then J2 fires and the run ends.
	works, err = Resolve(run, wT.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	if !Done(run) {
		t.Fatal("run should be done after T completes")
	}
	if got := stateOf(t, run, "i1", "J1"); got != StatusDone {
		t.Fatalf("J1 should be done, got %v", got)
	}
}

// TestCrossJoinSelectMergePath selects D->J1 and verifies the unselected
// path deactivates (a chain of two consecutive dead tasks) without
// blocking the joins forever.
func TestCrossJoinSelectMergePath(t *testing.T) {
	run, works, err := Start(crossGraph(), "main")
	if err != nil {
		t.Fatal(err)
	}
	wT := findWork(t, works, "T")

	// Choose the merge path D->J1; X and Y become unreachable.
	works, err = Resolve(run, findWork(t, works, "D").ID, "J1")
	if err != nil {
		t.Fatal(err)
	}
	// Consecutive deactivation: X dead -> X->Y impossible -> Y dead.
	if got := stateOf(t, run, "i1", "X"); got != StatusDead {
		t.Fatalf("X should be dead, got %v", got)
	}
	if got := stateOf(t, run, "i1", "Y"); got != StatusDead {
		t.Fatalf("Y should be dead, got %v", got)
	}
	// Deactivated tasks must not produce work items.
	for _, w := range works {
		if w.NodeID == "X" || w.NodeID == "Y" {
			t.Fatalf("dead task %q produced a work item", w.NodeID)
		}
	}
	// J1 waits for T; J2 waits for J1. The unselected Y->J2 edge must not
	// hold J2 forever, but nothing may finish before T completes.
	if Done(run) {
		t.Fatal("run finished before slow task T completed")
	}
	if got := stateOf(t, run, "i1", "J2"); got != StatusPending {
		t.Fatalf("J2 should be pending, got %v", got)
	}

	if _, err = Resolve(run, wT.ID, ""); err != nil {
		t.Fatal(err)
	}
	if !Done(run) {
		t.Fatal("run should be done after T completes")
	}
	for _, n := range []string{"J1", "J2", "E"} {
		if got := stateOf(t, run, "i1", n); got != StatusDone {
			t.Fatalf("%s should be done, got %v", n, got)
		}
	}
}

// subflowDefs builds a main flow that calls the same sub definition twice
// in parallel; the sub flow has a decision with two task branches.
func subflowDefs() []Definition {
	return []Definition{
		{
			ID: "main",
			Nodes: []Node{
				{ID: "S", Kind: NodeStart},
				{ID: "F", Kind: NodeFork},
				{ID: "C1", Kind: NodeCall, DefID: "sub"},
				{ID: "C2", Kind: NodeCall, DefID: "sub"},
				{ID: "J", Kind: NodeJoin},
				{ID: "E", Kind: NodeEnd},
			},
			Edges: []Edge{
				{From: "S", To: "F"},
				{From: "F", To: "C1"},
				{From: "F", To: "C2"},
				{From: "C1", To: "J"},
				{From: "C2", To: "J"},
				{From: "J", To: "E"},
			},
		},
		{
			ID: "sub",
			Nodes: []Node{
				{ID: "SS", Kind: NodeStart},
				{ID: "SD", Kind: NodeDecision},
				{ID: "ST1", Kind: NodeTask},
				{ID: "ST2", Kind: NodeTask},
				{ID: "SE", Kind: NodeEnd},
			},
			Edges: []Edge{
				{From: "SS", To: "SD"},
				{From: "SD", To: "ST1"},
				{From: "SD", To: "ST2"},
				{From: "ST1", To: "SE"},
				{From: "ST2", To: "SE"},
			},
		},
	}
}

// findWorkIn locates a ready work item by node within one instance.
func findWorkIn(t *testing.T, works []WorkItem, instanceID, nodeID string) WorkItem {
	t.Helper()
	for _, w := range works {
		if w.InstanceID == instanceID && w.NodeID == nodeID {
			return w
		}
	}
	t.Fatalf("no ready work for %q in instance %q (have %v)", nodeID, instanceID, works)
	return WorkItem{}
}

// TestConcurrentSubflowIsolation activates the same sub definition twice
// concurrently, picks different branches, and interleaves completion.
func TestConcurrentSubflowIsolation(t *testing.T) {
	run, works, err := Start(subflowDefs(), "main")
	if err != nil {
		t.Fatal(err)
	}
	// Both calls fire: two sub instances, each with its own SD decision.
	if len(works) != 2 {
		t.Fatalf("expected 2 decision work items, got %v", works)
	}
	instA, instB := works[0].InstanceID, works[1].InstanceID
	if instA == instB {
		t.Fatalf("work items must belong to distinct instances: %v", works)
	}
	wDecA := findWorkIn(t, works, instA, "SD")
	wDecB := findWorkIn(t, works, instB, "SD")
	for _, w := range works {
		if w.NodeID != "SD" || w.DefID != "sub" {
			t.Fatalf("unexpected work item %+v", w)
		}
	}

	// Instance A takes branch ST1; instance B takes branch ST2.
	works, err = Resolve(run, wDecA.ID, "ST1")
	if err != nil {
		t.Fatal(err)
	}
	wA := findWorkIn(t, works, instA, "ST1")
	works, err = Resolve(run, wDecB.ID, "ST2")
	if err != nil {
		t.Fatal(err)
	}
	wB := findWorkIn(t, works, instB, "ST2")
	if wA.ID == wB.ID {
		t.Fatal("work IDs must be unique across instances")
	}

	// Unselected branches die independently per instance.
	if got := stateOf(t, run, instA, "ST2"); got != StatusDead {
		t.Fatalf("ST2 in %s should be dead, got %v", instA, got)
	}
	if got := stateOf(t, run, instB, "ST1"); got != StatusDead {
		t.Fatalf("ST1 in %s should be dead, got %v", instB, got)
	}
	if got := stateOf(t, run, instA, "ST1"); got != StatusActive {
		t.Fatalf("ST1 in %s should be active, got %v", instA, got)
	}

	// Complete A's task first: A's sub End releases only C1's call.
	callOf := func(instID string) string {
		if instID == instA {
			return "C1"
		}
		return "C2"
	}
	if _, err = Resolve(run, wA.ID, ""); err != nil {
		t.Fatal(err)
	}
	if got := stateOf(t, run, instA, "SE"); got != StatusDone {
		t.Fatalf("SE in %s should be done, got %v", instA, got)
	}
	if got := stateOf(t, run, "i1", callOf(instA)); got != StatusDone {
		t.Fatalf("%s should be released, got %v", callOf(instA), got)
	}
	// The other call is untouched and the join still waits.
	if got := stateOf(t, run, "i1", callOf(instB)); got != StatusActive {
		t.Fatalf("%s should still be active, got %v", callOf(instB), got)
	}
	if got := stateOf(t, run, "i1", "J"); got != StatusPending {
		t.Fatalf("J should be pending, got %v", got)
	}
	if Done(run) {
		t.Fatal("run finished while the second call is still active")
	}

	// Complete B's task: the join fires and the run ends.
	if _, err = Resolve(run, wB.ID, ""); err != nil {
		t.Fatal(err)
	}
	if !Done(run) {
		t.Fatal("run should be done after both calls complete")
	}
	if got := stateOf(t, run, "i1", "J"); got != StatusDone {
		t.Fatalf("J should be done, got %v", got)
	}
}

// TestResolveRejectsUnknownWork verifies error handling for bad input.
func TestResolveRejectsUnknownWork(t *testing.T) {
	run, works, err := Start(crossGraph(), "main")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Resolve(run, "nope", ""); err == nil {
		t.Fatal("expected error for unknown work item")
	}
	d := findWork(t, works, "D")
	if _, err := Resolve(run, d.ID, "NOWHERE"); err == nil {
		t.Fatal("expected error for unknown selected edge")
	}
	// The failed resolve must not consume the work item.
	if _, err := Resolve(run, d.ID, "J1"); err != nil {
		t.Fatalf("decision should still be resolvable: %v", err)
	}
}

// doneSet returns the set of node IDs (per instance) that executed.
func doneSet(run *Run) map[string]bool {
	out := make(map[string]bool)
	for _, s := range Inspect(run) {
		if s.Status == StatusDone {
			out[s.InstanceID+"/"+s.NodeID] = true
		}
	}
	return out
}

// TestOrderIndependence runs the cross graph with the same decision but
// different work-completion orders and expects identical executed sets.
func TestOrderIndependence(t *testing.T) {
	// Order 1: decision first, then slow task.
	run1, works, err := Start(crossGraph(), "main")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Resolve(run1, findWork(t, works, "D").ID, "J1"); err != nil {
		t.Fatal(err)
	}
	if _, err = Resolve(run1, findWork(t, works, "T").ID, ""); err != nil {
		t.Fatal(err)
	}

	// Order 2: slow task first, then decision.
	run2, works, err := Start(crossGraph(), "main")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Resolve(run2, findWork(t, works, "T").ID, ""); err != nil {
		t.Fatal(err)
	}
	if _, err = Resolve(run2, findWork(t, works, "D").ID, "J1"); err != nil {
		t.Fatal(err)
	}

	s1, s2 := doneSet(run1), doneSet(run2)
	if len(s1) != len(s2) {
		t.Fatalf("executed sets differ: %v vs %v", s1, s2)
	}
	for k := range s1 {
		if !s2[k] {
			t.Fatalf("executed sets differ: %v vs %v", s1, s2)
		}
	}
	if !Done(run1) || !Done(run2) {
		t.Fatal("both runs should be done")
	}
}
