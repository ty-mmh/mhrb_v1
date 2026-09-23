package main

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/runtimegate"
)

func TestM7ServeStartupExactOrderingKeepsAllActorsAtZeroUntilGateOpen(t *testing.T) {
	state := newServeStartupEvidenceState()
	operations := state.operations(nil, "")
	if err := runServeStartupSequence(operations); err != nil {
		t.Fatal(err)
	}
	state.waitActors(t)

	wantOrder := []string{
		"tts_gated", "mandatory_preflight", "running_recovery", "integrity_scan",
		"mandatory_recovery", "post_recovery_integrity_scan", "preflight_eligibility",
		"writer_pause", "content_references_preflight", "projection_preflight", "final_readiness", "http_bind",
		"projection_start", "projection_ready", "workers_gated", "http_gated",
		"head_verify", "writer_resume", "gate_open",
	}
	if got := state.phaseSnapshot(); !reflect.DeepEqual(got, wantOrder) {
		t.Fatalf("startup order =\n%v\nwant\n%v", got, wantOrder)
	}
	if state.beforeOpenProvider != 0 || state.beforeOpenHTTP != 0 || state.beforeOpenTTS != 0 {
		t.Fatalf("actors before gate open = provider:%d HTTP:%d TTS:%d",
			state.beforeOpenProvider, state.beforeOpenHTTP, state.beforeOpenTTS)
	}
	if state.providerSubmits.Load() != 1 || state.httpAccepts.Load() != 1 || state.ttsOutputs.Load() != 1 {
		t.Fatalf("actors after gate open = provider:%d HTTP:%d TTS:%d",
			state.providerSubmits.Load(), state.httpAccepts.Load(), state.ttsOutputs.Load())
	}
	if state.resumeCalls.Load() != 1 || state.openCalls.Load() != 1 || state.failCalls.Load() != 0 {
		t.Fatalf("activation transitions = resume:%d open:%d fail:%d",
			state.resumeCalls.Load(), state.openCalls.Load(), state.failCalls.Load())
	}
}

func TestM7ServeStartupBindCoordinatorAndFinalHeadFailuresStayClosed(t *testing.T) {
	for _, failurePhase := range []string{
		"content_references_preflight", "http_bind", "projection_start", "projection_ready", "head_verify",
	} {
		t.Run(failurePhase, func(t *testing.T) {
			state := newServeStartupEvidenceState()
			injected := errors.New("injected " + failurePhase + " failure")
			err := runServeStartupSequence(state.operations(injected, failurePhase))
			if !errors.Is(err, injected) {
				t.Fatalf("startup failure = %v, want %v", err, injected)
			}
			state.waitActors(t)
			if waitErr := state.gate.Wait(context.Background()); !errors.Is(waitErr, injected) {
				t.Fatalf("failed RuntimeStartGate = %v, want %v", waitErr, injected)
			}
			if state.providerSubmits.Load() != 0 || state.httpAccepts.Load() != 0 || state.ttsOutputs.Load() != 0 {
				t.Fatalf("failed startup leaked actor progress = provider:%d HTTP:%d TTS:%d",
					state.providerSubmits.Load(), state.httpAccepts.Load(), state.ttsOutputs.Load())
			}
			if state.resumeCalls.Load() != 0 || state.openCalls.Load() != 0 || state.failCalls.Load() != 1 {
				t.Fatalf("failed activation transitions = resume:%d open:%d fail:%d",
					state.resumeCalls.Load(), state.openCalls.Load(), state.failCalls.Load())
			}
			phases := state.phaseSnapshot()
			if slicesContainString(phases, "writer_resume") || slicesContainString(phases, "gate_open") {
				t.Fatalf("failed startup reached activation: %v", phases)
			}
		})
	}
}

type serveStartupEvidenceState struct {
	gate *runtimegate.Gate

	mu     sync.Mutex
	phases []string
	actors sync.WaitGroup

	providerSubmits atomic.Int64
	httpAccepts     atomic.Int64
	ttsOutputs      atomic.Int64
	resumeCalls     atomic.Int64
	openCalls       atomic.Int64
	failCalls       atomic.Int64

	beforeOpenProvider int64
	beforeOpenHTTP     int64
	beforeOpenTTS      int64
}

func newServeStartupEvidenceState() *serveStartupEvidenceState {
	return &serveStartupEvidenceState{gate: runtimegate.New()}
}

func (state *serveStartupEvidenceState) operations(
	injected error, failurePhase string,
) serveStartupOperations {
	phase := func(name string) error {
		state.record(name)
		if failurePhase == name {
			return injected
		}
		return nil
	}
	register := func(name string, counter *atomic.Int64) error {
		if err := phase(name); err != nil {
			return err
		}
		state.actors.Add(1)
		go func() {
			defer state.actors.Done()
			if err := state.gate.Wait(context.Background()); err == nil {
				counter.Add(1)
			}
		}()
		return nil
	}
	return serveStartupOperations{
		StartTTSGated:      func() error { return register("tts_gated", &state.ttsOutputs) },
		PreflightMandatory: func() error { return phase("mandatory_preflight") },
		TerminalizeRunning: func() error { return phase("running_recovery") },
		ScanIntegrity: func(postRecovery bool) error {
			if postRecovery {
				return phase("post_recovery_integrity_scan")
			}
			return phase("integrity_scan")
		},
		TerminalizeMandatory: func() (bool, error) {
			return true, phase("mandatory_recovery")
		},
		PreflightEligibility:   func() error { return phase("preflight_eligibility") },
		PauseAdmissions:        func() error { return phase("writer_pause") },
		PreflightContentRefs:   func() error { return phase("content_references_preflight") },
		PreflightProjection:    func() error { return phase("projection_preflight") },
		EvaluateFinalReadiness: func() error { return phase("final_readiness") },
		BindHTTP:               func() error { return phase("http_bind") },
		StartProjection:        func() error { return phase("projection_start") },
		WaitProjectionReady:    func() error { return phase("projection_ready") },
		StartWorkersGated: func() error {
			return register("workers_gated", &state.providerSubmits)
		},
		StartHTTPGated:       func() error { return register("http_gated", &state.httpAccepts) },
		RequireCanonicalHead: func() error { return phase("head_verify") },
		ResumeAdmissions: func() error {
			if err := phase("writer_resume"); err != nil {
				return err
			}
			state.resumeCalls.Add(1)
			return nil
		},
		OpenGate: func() error {
			if err := phase("gate_open"); err != nil {
				return err
			}
			state.beforeOpenProvider = state.providerSubmits.Load()
			state.beforeOpenHTTP = state.httpAccepts.Load()
			state.beforeOpenTTS = state.ttsOutputs.Load()
			if state.beforeOpenProvider != 0 || state.beforeOpenHTTP != 0 || state.beforeOpenTTS != 0 {
				return errors.New("actor progressed before RuntimeStartGate opened")
			}
			state.openCalls.Add(1)
			return state.gate.Open()
		},
		FailGate: func(cause error) {
			state.failCalls.Add(1)
			_ = state.gate.Fail(cause)
		},
	}
}

func (state *serveStartupEvidenceState) record(phase string) {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.phases = append(state.phases, phase)
}

func (state *serveStartupEvidenceState) phaseSnapshot() []string {
	state.mu.Lock()
	defer state.mu.Unlock()
	return append([]string(nil), state.phases...)
}

func (state *serveStartupEvidenceState) waitActors(t *testing.T) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		state.actors.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("gated startup actor did not terminate")
	}
}

func slicesContainString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

// Application.Start is retained for compatibility with existing tests and
// embedders. The production composition roots must use the explicit
// Preflight/Prepare/StartWorkers path whose activation is guarded above.
func TestM7LegacyApplicationStartHasNoProductionCaller(t *testing.T) {
	repositoryRoot := filepath.Clean(filepath.Join("..", ".."))
	for _, subtree := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(filepath.Join(repositoryRoot, subtree), func(
			path string, entry fs.DirEntry, walkErr error,
		) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			files := token.NewFileSet()
			parsed, err := parser.ParseFile(files, path, nil, 0)
			if err != nil {
				return err
			}
			ast.Inspect(parsed, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || selector.Sel.Name != "Start" || !looksLikeApplicationReceiver(selector.X) {
					return true
				}
				t.Errorf("legacy Application.Start production caller: %s:%d",
					path, files.Position(call.Pos()).Line)
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func looksLikeApplicationReceiver(expression ast.Expr) bool {
	switch value := expression.(type) {
	case *ast.Ident:
		return value.Name == "app" || value.Name == "application"
	case *ast.SelectorExpr:
		return value.Sel.Name == "app" || value.Sel.Name == "application"
	case *ast.ParenExpr:
		return looksLikeApplicationReceiver(value.X)
	default:
		return false
	}
}
