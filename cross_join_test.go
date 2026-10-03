package workflow

import "testing"

// Graph under test (definition "main"):
//
//	s(Start) -> f(Fork)
//	f --e1--> d(Decision)
//	f --e2--> slow(Task)
//	d --e3--> j2(Join)            direct path
//	d --e4--> j1(Join)            path via middle join
//	d --e7--> a(Task) --e8--> b(Task) --e9--> j2   dead-chain branch
//	slow --e5--> j1
//	j1 --e6--> j2
//	j2 --e10--> t(End)
func crossDef() *Definition {
	return NewDefinition("main",
		[]*Node{
			{ID: "s", Kind: KindStart, Out: []string{"e0"}},
			{ID: "t", Kind: KindEnd},
			{ID: "slow", Kind: KindTask, Out: []string{"e5"}},
			{ID: "a", Kind: KindTask, Out: []string{"e8"}},
			{ID: "b", Kind: KindTask, Out: []string{"e9"}},
			{ID: "d", Kind: KindDecision, Out: []string{"e3", "e4", "e7"}},
			{ID: "f", Kind: KindFork, Out: []string{"e1", "e2"}},
			{ID: "j1", Kind: KindJoin, Out: []string{"e6"}},
			{ID: "j2", Kind: KindJoin, Out: []string{"e10"}},
		},
		[]*Edge{
			{ID: "e0", From: "s", To: "f"},
			{ID: "e1", From: "f", To: "d"},
			{ID: "e2", From: "f", To: "slow"},
			{ID: "e3", From: "d", To: "j2"},
			{ID: "e4", From: "d", To: "j1"},
			{ID: "e5", From: "slow", To: "j1"},
			{ID: "e6", From: "j1", To: "j2"},
			{ID: "e7", From: "d", To: "a"},
			{ID: "e8", From: "a", To: "b"},
			{ID: "e9", From: "b", To: "j2"},
			{ID: "e10", From: "j2", To: "t"},
		},
	)
}

func readyByNode(items []WorkItem) map[string]WorkItem {
	m := make(map[string]WorkItem, len(items))
	for _, it := range items {
		m[it.NodeID] = it
	}
	return m
}

func stateOf(t *testing.T, run *Run, nodeID string) nodeState {
	t.Helper()
	snap := Inspect(run)
	for _, inst := range snap.Instances {
		if st, ok := inst.NodeStates[nodeID]; ok {
			return st
		}
	}
	t.Fatalf("node %q not found in any instance", nodeID)
	return statePending
}

func TestCrossJoinDirectPath(t *testing.T) {
	run, items := Start([]*Definition{crossDef()}, "main")
	ready := readyByNode(items)
	if len(ready) != 2 {
		t.Fatalf("after start want 2 ready items (slow, d), got %v", items)
	}
	if _, ok := ready["slow"]; !ok {
		t.Fatalf("slow task must be ready, got %v", items)
	}
	d := ready["d"]

	// Choose the direct path into the terminal join.
	items, err := Resolve(run, d.ID, "e3")
	if err != nil {
		t.Fatal(err)
	}
	ready = readyByNode(items)
	if _, ok := ready["slow"]; !ok {
		t.Fatalf("slow must still be ready, got %v", items)
	}
	if len(ready) != 1 {
		t.Fatalf("dead chain must not emit work items, got %v", items)
	}
	// Consecutive deactivation: a and b are dead without producing work.
	if st := stateOf(t, run, "a"); st != stateDead {
		t.Fatalf("a must be dead, got %v", st)
	}
	if st := stateOf(t, run, "b"); st != stateDead {
		t.Fatalf("b must be dead, got %v", st)
	}
	// The run must not finish before the slow task completes.
	if Inspect(run).Done {
		t.Fatal("run must not finish before slow completes")
	}
	if st := stateOf(t, run, "j1"); st != statePending {
		t.Fatalf("j1 must wait for slow, got %v", st)
	}
	if st := stateOf(t, run, "j2"); st != statePending {
		t.Fatalf("j2 must wait for j1, got %v", st)
	}

	// Complete the slow task; everything else must drain automatically.
	items, err = Resolve(run, ready["slow"].ID, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 0 {
		t.Fatalf("no work items expected after slow, got %v", items)
	}
	if !Inspect(run).Done {
		t.Fatal("run must be done after slow completes")
	}
	for _, id := range []string{"j1", "j2", "t"} {
		if st := stateOf(t, run, id); st != stateDone {
			t.Fatalf("%s must be done, got %v", id, st)
		}
	}
}

func TestCrossJoinMiddlePath(t *testing.T) {
	run, items := Start([]*Definition{crossDef()}, "main")
	d := readyByNode(items)["d"]

	// Choose the path through the middle join.
	items, err := Resolve(run, d.ID, "e4")
	if err != nil {
		t.Fatal(err)
	}
	ready := readyByNode(items)
	if len(ready) != 1 {
		t.Fatalf("only slow may remain ready, got %v", items)
	}
	// The unchosen direct edge e3 must not block j2 forever, but the
	// run still cannot finish while slow is outstanding.
	if Inspect(run).Done {
		t.Fatal("run must not finish before slow completes")
	}
	if st := stateOf(t, run, "j1"); st != statePending {
		t.Fatalf("j1 must wait for slow, got %v", st)
	}

	items, err = Resolve(run, ready["slow"].ID, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 0 {
		t.Fatalf("no work items expected after slow, got %v", items)
	}
	if !Inspect(run).Done {
		t.Fatal("run must be done after slow completes")
	}
	if st := stateOf(t, run, "a"); st != stateDead {
		t.Fatalf("a must be dead, got %v", st)
	}
	if st := stateOf(t, run, "b"); st != stateDead {
		t.Fatalf("b must be dead, got %v", st)
	}
}

func TestDecisionRejectsForeignEdge(t *testing.T) {
	run, items := Start([]*Definition{crossDef()}, "main")
	d := readyByNode(items)["d"]
	if _, err := Resolve(run, d.ID, "e5"); err == nil {
		t.Fatal("selecting a foreign edge must fail")
	}
	// The failed resolve must not consume the work item.
	if st := stateOf(t, run, "d"); st != stateReady {
		t.Fatalf("d must still be ready after failed resolve, got %v", st)
	}
}
