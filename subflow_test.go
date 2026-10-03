package workflow

import "testing"

// Sub-flow "sub":
//
//	ss(Start) -> sd(Decision)
//	sd --se1--> st1(Task) --se3--> sj(Join)
//	sd --se2--> st2(Task) --se4--> sj
//	sj --se5--> se(End)
func subDef() *Definition {
	return NewDefinition("sub",
		[]*Node{
			{ID: "ss", Kind: KindStart, Out: []string{"se0"}},
			{ID: "se", Kind: KindEnd},
			{ID: "st1", Kind: KindTask, Out: []string{"se3"}},
			{ID: "st2", Kind: KindTask, Out: []string{"se4"}},
			{ID: "sd", Kind: KindDecision, Out: []string{"se1", "se2"}},
			{ID: "sj", Kind: KindJoin, Out: []string{"se5"}},
		},
		[]*Edge{
			{ID: "se0", From: "ss", To: "sd"},
			{ID: "se1", From: "sd", To: "st1"},
			{ID: "se2", From: "sd", To: "st2"},
			{ID: "se3", From: "st1", To: "sj"},
			{ID: "se4", From: "st2", To: "sj"},
			{ID: "se5", From: "sj", To: "se"},
		},
	)
}

// Root flow activates the same sub-flow twice in parallel:
//
//	rs(Start) -> rf(Fork)
//	rf --re1--> c1(Call sub) --re3--> rj(Join)
//	rf --re2--> c2(Call sub) --re4--> rj
//	rj --re5--> re(End)
func rootDef() *Definition {
	return NewDefinition("root",
		[]*Node{
			{ID: "rs", Kind: KindStart, Out: []string{"re0"}},
			{ID: "re", Kind: KindEnd},
			{ID: "c1", Kind: KindCall, Out: []string{"re3"}, CallDef: "sub"},
			{ID: "c2", Kind: KindCall, Out: []string{"re4"}, CallDef: "sub"},
			{ID: "rf", Kind: KindFork, Out: []string{"re1", "re2"}},
			{ID: "rj", Kind: KindJoin, Out: []string{"re5"}},
		},
		[]*Edge{
			{ID: "re0", From: "rs", To: "rf"},
			{ID: "re1", From: "rf", To: "c1"},
			{ID: "re2", From: "rf", To: "c2"},
			{ID: "re3", From: "c1", To: "rj"},
			{ID: "re4", From: "c2", To: "rj"},
			{ID: "re5", From: "rj", To: "re"},
		},
	)
}

func findByPath(t *testing.T, items []WorkItem, nodeID string, path ...string) WorkItem {
	t.Helper()
	for _, it := range items {
		if it.NodeID != nodeID || len(it.Path) != len(path) {
			continue
		}
		match := true
		for i, p := range path {
			if it.Path[i] != p {
				match = false
				break
			}
		}
		if match {
			return it
		}
	}
	t.Fatalf("no ready item for node %q at path %v; got %v", nodeID, path, items)
	return WorkItem{}
}

func instState(t *testing.T, run *Run, callNode, nodeID string) nodeState {
	t.Helper()
	for _, inst := range Inspect(run).Instances {
		if len(inst.Path) == 1 && inst.Path[0] == callNode {
			if st, ok := inst.NodeStates[nodeID]; ok {
				return st
			}
		}
	}
	t.Fatalf("no instance at call %q or node %q missing", callNode, nodeID)
	return statePending
}

func TestConcurrentSubflowActivations(t *testing.T) {
	run, items := Start([]*Definition{rootDef(), subDef()}, "root")

	// Both Calls fire: two independent Decision work items for node sd.
	if len(items) != 2 {
		t.Fatalf("want 2 ready decisions, got %v", items)
	}
	sd1 := findByPath(t, items, "sd", "c1")
	sd2 := findByPath(t, items, "sd", "c2")
	if sd1.ID == sd2.ID {
		t.Fatalf("work items of different activations must differ: %v", items)
	}

	// Pick opposite branches in the two sub-flow instances.
	items, err := Resolve(run, sd1.ID, "se1") // c1 -> st1
	if err != nil {
		t.Fatal(err)
	}
	items, err = Resolve(run, sd2.ID, "se2") // c2 -> st2
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("want 2 ready tasks, got %v", items)
	}
	st1 := findByPath(t, items, "st1", "c1")
	st2 := findByPath(t, items, "st2", "c2")

	// Interleave: finish c2's task first. Only c2 may be released.
	items, err = Resolve(run, st2.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	if Inspect(run).Done {
		t.Fatal("root must wait for c1")
	}
	if st := stateOf(t, run, "c2"); st != stateDone {
		t.Fatalf("c2 must be done, got %v", st)
	}
	if st := stateOf(t, run, "c1"); st != stateRunning {
		t.Fatalf("c1 must still be running, got %v", st)
	}
	if st := stateOf(t, run, "rj"); st != statePending {
		t.Fatalf("rj must wait for c1, got %v", st)
	}
	// c2's completion must not disturb c1's instance.
	if st := instState(t, run, "c1", "st1"); st != stateReady {
		t.Fatalf("c1/st1 must still be ready, got %v", st)
	}
	if st := instState(t, run, "c2", "st1"); st != stateDead {
		t.Fatalf("c2/st1 must be dead (unchosen), got %v", st)
	}
	if st := instState(t, run, "c1", "st2"); st != stateDead {
		t.Fatalf("c1/st2 must be dead (unchosen), got %v", st)
	}
	// c1's outstanding work item must still be resolvable afterwards.
	if len(items) != 1 || items[0].ID != st1.ID {
		t.Fatalf("only c1/st1 may remain ready, got %v", items)
	}

	// Finish c1's task; the whole run drains.
	items, err = Resolve(run, st1.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 0 {
		t.Fatalf("no work items expected, got %v", items)
	}
	if !Inspect(run).Done {
		t.Fatal("run must be done")
	}
	for _, id := range []string{"c1", "c2", "rj", "re"} {
		if st := stateOf(t, run, id); st != stateDone {
			t.Fatalf("%s must be done, got %v", id, st)
		}
	}
	// Each sub-flow instance kept its own branch choices.
	if st := instState(t, run, "c1", "st1"); st != stateDone {
		t.Fatalf("c1/st1 must be done, got %v", st)
	}
	if st := instState(t, run, "c2", "st2"); st != stateDone {
		t.Fatalf("c2/st2 must be done, got %v", st)
	}
	if got := len(Inspect(run).Instances); got != 3 {
		t.Fatalf("want 3 instances (root + 2 sub), got %d", got)
	}
}

func TestCompletionOrderDoesNotChangeExecutedSet(t *testing.T) {
	run := func(order []string) map[string]nodeState {
		r, items := Start([]*Definition{rootDef(), subDef()}, "root")
		sd1 := findByPath(t, items, "sd", "c1")
		sd2 := findByPath(t, items, "sd", "c2")
		var err error
		if items, err = Resolve(r, sd1.ID, "se1"); err != nil {
			t.Fatal(err)
		}
		if items, err = Resolve(r, sd2.ID, "se2"); err != nil {
			t.Fatal(err)
		}
		st1 := findByPath(t, items, "st1", "c1")
		st2 := findByPath(t, items, "st2", "c2")
		ids := map[string]string{"st1": st1.ID, "st2": st2.ID}
		for _, o := range order {
			if _, err = Resolve(r, ids[o], ""); err != nil {
				t.Fatal(err)
			}
		}
		merged := make(map[string]nodeState)
		for _, inst := range Inspect(r).Instances {
			key := inst.DefID + "@"
			for _, p := range inst.Path {
				key += p + "/"
			}
			for n, st := range inst.NodeStates {
				merged[key+n] = st
			}
		}
		return merged
	}
	first := run([]string{"st1", "st2"})
	second := run([]string{"st2", "st1"})
	if len(first) != len(second) {
		t.Fatalf("executed sets differ: %v vs %v", first, second)
	}
	for k, v := range first {
		if second[k] != v {
			t.Fatalf("state of %s differs: %v vs %v", k, v, second[k])
		}
	}
}
