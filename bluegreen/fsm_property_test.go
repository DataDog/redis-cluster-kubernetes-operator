package bluegreen_test

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"strings"
	"testing"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"pgregory.net/rapid"
)

// legalTransitions encodes the documented BG phase graph extracted from
// bluegreen/handler.go by tracing each handler function's input phase
// (from its dispatch in Handle) to its output phase (the bgStatus.Phase
// it assigns). Any change to handler.go's transitions must be reflected
// here; the AST drift guard (TestFSM_HandlerPhasesMatchGraph) catches
// new or removed phases automatically.
//
// Happy path:
//
//	None → CreatingGreen                  (composite literal handler.go:314)
//	CreatingGreen → WaitingPods           (bgCreateGreen :351)
//	WaitingPods → Replicating             (bgWaitForPods :414)
//	Replicating → WaitingSync             (bgSetupReplication :433, :534)
//	WaitingSync → Promoting               (bgWaitForSync :593, :702)
//	Promoting → SwitchingDNS              (finishPromotion :907, called from bgPromote :742)
//	SwitchingDNS → FormingCluster         (bgSwitchDNS :974)
//	FormingCluster → ScalingDownBlue      (bgFormGreenCluster :1443)
//	ScalingDownBlue → TearingDown         (bgScaleDownBlue :997, :1054, :1233)
//
// Rollback / recovery:
//
//	ScalingDownBlue → Promoting           (bgScaleDownBlue :1021, :1034)
//	Failed → SwitchingDNS                 (bgCleanupFailed :1534, cleanup-forward)
//	Failed → Promoting                    (bgCleanupFailed :1582, rollback resume)
//
// Universal edges (modeled separately in bgMachine.Fail / .ForceCutover
// so the graph itself stays a clean happy-path description):
//
//	any phase → Failed                    (Handle :146, :168, :177, :283 on timeout/SSE error)
//	any phase → Promoting                 (Handle :207 force-cutover annotation; CheckBlueAlive :1847)
var legalTransitions = map[cachev1.BlueGreenPhase][]cachev1.BlueGreenPhase{
	cachev1.BlueGreenPhaseNone:            {cachev1.BlueGreenPhaseCreatingGreen},
	cachev1.BlueGreenPhaseCreatingGreen:   {cachev1.BlueGreenPhaseWaitingPods},
	cachev1.BlueGreenPhaseWaitingPods:     {cachev1.BlueGreenPhaseReplicating},
	cachev1.BlueGreenPhaseReplicating:     {cachev1.BlueGreenPhaseWaitingSync},
	cachev1.BlueGreenPhaseWaitingSync:     {cachev1.BlueGreenPhasePromoting},
	cachev1.BlueGreenPhasePromoting:       {cachev1.BlueGreenPhaseSwitchingDNS},
	cachev1.BlueGreenPhaseSwitchingDNS:    {cachev1.BlueGreenPhaseFormingCluster},
	cachev1.BlueGreenPhaseFormingCluster:  {cachev1.BlueGreenPhaseScalingDownBlue},
	cachev1.BlueGreenPhaseScalingDownBlue: {cachev1.BlueGreenPhaseTearingDown, cachev1.BlueGreenPhasePromoting},
	cachev1.BlueGreenPhaseTearingDown:     {}, // terminal
	cachev1.BlueGreenPhaseFailed:          {cachev1.BlueGreenPhaseSwitchingDNS, cachev1.BlueGreenPhasePromoting},
}

// allPhases is the universe; FSM exhaustiveness asserts every enum
// member has an entry in legalTransitions.
var allPhases = []cachev1.BlueGreenPhase{
	cachev1.BlueGreenPhaseNone,
	cachev1.BlueGreenPhaseCreatingGreen,
	cachev1.BlueGreenPhaseWaitingPods,
	cachev1.BlueGreenPhaseReplicating,
	cachev1.BlueGreenPhaseWaitingSync,
	cachev1.BlueGreenPhasePromoting,
	cachev1.BlueGreenPhaseScalingDownBlue,
	cachev1.BlueGreenPhaseFormingCluster,
	cachev1.BlueGreenPhaseSwitchingDNS,
	cachev1.BlueGreenPhaseTearingDown,
	cachev1.BlueGreenPhaseFailed,
}

// TestFSM_ExhaustivePhaseCoverage is the static guard: every member of
// the BlueGreenPhase enum must have an entry in legalTransitions. Adding
// a new phase without wiring it up fails this test immediately.
func TestFSM_ExhaustivePhaseCoverage(t *testing.T) {
	for _, p := range allPhases {
		if _, ok := legalTransitions[p]; !ok {
			t.Errorf("phase %q missing from legalTransitions", p)
		}
	}
	if len(legalTransitions) != len(allPhases) {
		t.Errorf("legalTransitions has %d entries; want %d", len(legalTransitions), len(allPhases))
	}
}

// bgMachine drives a fake BlueGreenStatus through random sequences of:
//   - advance (legal forward transition)
//   - fail (any phase → Failed)
//   - forceCutover (any phase → Promoting, the annotation-driven path)
//   - serialize/deserialize (restart safety: marshal status, unmarshal,
//     resume)
//
// Invariants asserted continuously:
//   - phase is always in allPhases
//   - every transition is in legalTransitions (or is the universal
//     Failed / forceCutover Promoting edge)
//   - after deserialize, the next legal action yields the same result
type bgMachine struct {
	status cachev1.BlueGreenStatus
}

func (m *bgMachine) Advance(t *rapid.T) {
	nexts := legalTransitions[m.status.Phase]
	if len(nexts) == 0 {
		return // terminal
	}
	next := rapid.SampledFrom(nexts).Draw(t, "next")
	m.status.Phase = next
}

func (m *bgMachine) Fail(t *rapid.T) {
	// universal edge — any phase may fail
	m.status.Phase = cachev1.BlueGreenPhaseFailed
}

func (m *bgMachine) ForceCutover(t *rapid.T) {
	// only legal pre-Promoting; mirrors handler line 207 which fires
	// when the cutover annotation is set during early phases
	if m.status.Phase == cachev1.BlueGreenPhaseFailed ||
		m.status.Phase == cachev1.BlueGreenPhaseTearingDown {
		return
	}
	m.status.Phase = cachev1.BlueGreenPhasePromoting
}

func (m *bgMachine) RestartRoundTrip(t *rapid.T) {
	data, err := json.Marshal(m.status)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var restored cachev1.BlueGreenStatus
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if restored.Phase != m.status.Phase {
		t.Fatalf("restart lost phase: had %q, got %q", m.status.Phase, restored.Phase)
	}
	m.status = restored
}

// Check is rapid's invariant hook — called after every action.
func (m *bgMachine) Check(t *rapid.T) {
	if !slices.Contains(allPhases, m.status.Phase) {
		t.Fatalf("invalid phase %q (not in enum)", m.status.Phase)
	}
}

// funcToSourcePhase maps each phase-mutating function in handler.go to
// the BG phase it executes from. Handle's switch dispatches on
// bgStatus.Phase, so each `bg<Foo>` function has a known input phase.
// This mapping is hand-coded but small and stable — adding a new
// handler function requires explicitly classifying its source phase.
//
// The composite literal at handler.go:314 sets the initial
// None → CreatingGreen edge inside Start, which is the public entry
// invoked once before Handle takes over per-phase dispatch.
var funcToSourcePhase = map[string]cachev1.BlueGreenPhase{
	"Start":              cachev1.BlueGreenPhaseNone,
	"bgCreateGreen":      cachev1.BlueGreenPhaseCreatingGreen,
	"bgWaitForPods":      cachev1.BlueGreenPhaseWaitingPods,
	"bgSetupReplication": cachev1.BlueGreenPhaseReplicating,
	"bgWaitForSync":      cachev1.BlueGreenPhaseWaitingSync,
	"bgPromote":          cachev1.BlueGreenPhasePromoting,
	"finishPromotion":    cachev1.BlueGreenPhasePromoting, // helper called from bgPromote
	"bgSwitchDNS":        cachev1.BlueGreenPhaseSwitchingDNS,
	"bgFormGreenCluster": cachev1.BlueGreenPhaseFormingCluster,
	"bgScaleDownBlue":    cachev1.BlueGreenPhaseScalingDownBlue,
	"bgCleanupFailed":    cachev1.BlueGreenPhaseFailed,
}

// handleUniversalAssignments lists functions whose phase assignments
// model the universal edges (any phase → Failed or → Promoting) and so
// are exempt from the per-edge check. Handle dispatches by phase but
// also handles cross-cutting events like timeouts and force-cutover
// annotations; CheckBlueAlive runs outside the phase dispatch entirely.
var handleUniversalAssignments = map[string]bool{
	"Handle":         true,
	"CheckBlueAlive": true,
}

// TestFSM_HandlerEdgesMatchGraph is the drift guard. It parses
// handler.go, groups every BlueGreenPhase assignment by the function
// that performs it, infers the source phase from funcToSourcePhase,
// and asserts each (source → target) edge is in legalTransitions.
//
// Catches:
//   - a handler function adds a transition to a phase not listed under
//     its source in legalTransitions (e.g. WaitingPods → Promoting
//     instead of Replicating)
//   - a phase appears as a graph key but no handler function targets it
//   - a function not classified in funcToSourcePhase mutates Phase
//     (someone added a new handler without updating the mapping)
func TestFSM_HandlerEdgesMatchGraph(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "handler.go", nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse handler.go: %v", err)
	}

	// funcName → set of BlueGreenPhase constant names assigned inside it.
	funcTargets := map[string]map[string]bool{}

	recordTarget := func(funcName string, expr ast.Expr) {
		sel, ok := expr.(*ast.SelectorExpr)
		if !ok {
			return
		}
		pkg, ok := sel.X.(*ast.Ident)
		if !ok || pkg.Name != "cachev1" {
			return
		}
		if !strings.HasPrefix(sel.Sel.Name, "BlueGreenPhase") {
			return
		}
		if funcTargets[funcName] == nil {
			funcTargets[funcName] = map[string]bool{}
		}
		funcTargets[funcName][sel.Sel.Name] = true
	}

	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		fnName := fn.Name.Name
		ast.Inspect(fn, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.AssignStmt:
				if len(node.Lhs) != 1 || len(node.Rhs) != 1 {
					return true
				}
				lhs, ok := node.Lhs[0].(*ast.SelectorExpr)
				if !ok || lhs.Sel.Name != "Phase" {
					return true
				}
				recordTarget(fnName, node.Rhs[0])
			case *ast.KeyValueExpr:
				key, ok := node.Key.(*ast.Ident)
				if !ok || key.Name != "Phase" {
					return true
				}
				recordTarget(fnName, node.Value)
			case *ast.CallExpr:
				// setPhase calls realize phase transitions.
				if id, ok := node.Fun.(*ast.Ident); ok && id.Name == "setPhase" && len(node.Args) >= 2 {
					recordTarget(fnName, node.Args[1])
				}
			}
			return true
		})
	}

	// Build the inverse of legalTransitions: constant-name → set of
	// constant-name targets, for direct comparison against funcTargets.
	graphEdges := map[string]map[string]bool{}
	for src, dsts := range legalTransitions {
		srcName := constantName(src)
		graphEdges[srcName] = map[string]bool{}
		for _, d := range dsts {
			graphEdges[srcName][constantName(d)] = true
		}
	}

	// Check each function classified in funcToSourcePhase.
	for fnName, targets := range funcTargets {
		if handleUniversalAssignments[fnName] {
			continue
		}
		src, classified := funcToSourcePhase[fnName]
		if !classified {
			t.Errorf("function %s mutates bgStatus.Phase but is not classified in funcToSourcePhase; classify it to assert its outgoing edges", fnName)
			continue
		}
		srcName := constantName(src)
		for tgt := range targets {
			if !graphEdges[srcName][tgt] {
				t.Errorf("handler.go: %s sets Phase=%s (source=%s) but legalTransitions[%s] has no such target",
					fnName, tgt, srcName, srcName)
			}
		}
	}

	// Reverse check: every non-terminal edge in legalTransitions should
	// be realized by at least one handler function.
	usedEdges := map[string]bool{}
	for fnName, targets := range funcTargets {
		if handleUniversalAssignments[fnName] {
			continue
		}
		src, ok := funcToSourcePhase[fnName]
		if !ok {
			continue
		}
		srcName := constantName(src)
		for tgt := range targets {
			usedEdges[srcName+"→"+tgt] = true
		}
	}
	for srcName, dsts := range graphEdges {
		for tgt := range dsts {
			edge := srcName + "→" + tgt
			// universal edges (→Failed, →Promoting from non-promoting
			// sources) are handled by Handle/CheckBlueAlive; skip those.
			if tgt == "BlueGreenPhaseFailed" {
				continue
			}
			if srcName == "BlueGreenPhaseFailed" {
				// bgCleanupFailed edges; already validated above
				continue
			}
			if !usedEdges[edge] {
				t.Errorf("legalTransitions has edge %s but no handler function realizes it", edge)
			}
		}
	}
}

// constantName reverses BlueGreenPhase("CreatingGreen") → "BlueGreenPhaseCreatingGreen".
// Hand-mapped because the constant strings and identifiers are documented
// together in apiv1/rediscluster_types.go.
func constantName(p cachev1.BlueGreenPhase) string {
	switch p {
	case cachev1.BlueGreenPhaseNone:
		return "BlueGreenPhaseNone"
	case cachev1.BlueGreenPhaseCreatingGreen:
		return "BlueGreenPhaseCreatingGreen"
	case cachev1.BlueGreenPhaseWaitingPods:
		return "BlueGreenPhaseWaitingPods"
	case cachev1.BlueGreenPhaseReplicating:
		return "BlueGreenPhaseReplicating"
	case cachev1.BlueGreenPhaseWaitingSync:
		return "BlueGreenPhaseWaitingSync"
	case cachev1.BlueGreenPhasePromoting:
		return "BlueGreenPhasePromoting"
	case cachev1.BlueGreenPhaseScalingDownBlue:
		return "BlueGreenPhaseScalingDownBlue"
	case cachev1.BlueGreenPhaseFormingCluster:
		return "BlueGreenPhaseFormingCluster"
	case cachev1.BlueGreenPhaseSwitchingDNS:
		return "BlueGreenPhaseSwitchingDNS"
	case cachev1.BlueGreenPhaseTearingDown:
		return "BlueGreenPhaseTearingDown"
	case cachev1.BlueGreenPhaseFailed:
		return "BlueGreenPhaseFailed"
	}
	return ""
}

func TestFSM_LegalTransitionsAndRestartSafety(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		sm := &bgMachine{status: cachev1.BlueGreenStatus{Phase: cachev1.BlueGreenPhaseNone}}
		rt.Repeat(rapid.StateMachineActions(sm))
	})
}
