package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/runtimegate"
)

func TestM7ApplicationRejectsCanonicalWorkUntilSharedRuntimeGateOpens(t *testing.T) {
	generator := &scriptedGenerator{steps: []generatorStep{{text: "recovered after activation"}}}
	fixture := newApplicationFixture(t, generator, 2)
	ctx := context.Background()
	if _, err := fixture.application.Ingress(ctx, "interrupted before restart"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.Prepare(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.Ingress(ctx, "must remain gated"); !errors.Is(err, ErrApplicationNotActivated) {
		t.Fatalf("Ingress while prepared = %v, want ErrApplicationNotActivated", err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); !errors.Is(err, ErrApplicationNotActivated) {
		t.Fatalf("ProcessResident while prepared = %v, want ErrApplicationNotActivated", err)
	}

	gate := runtimegate.New()
	if err := fixture.application.StartWorkers(gate); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if calls := generator.CallCount(); calls != 0 {
		t.Fatalf("provider calls before RuntimeStartGate = %d", calls)
	}
	if _, err := fixture.application.Ingress(ctx, "still gated"); !errors.Is(err, ErrApplicationNotActivated) {
		t.Fatalf("Ingress before gate open = %v, want ErrApplicationNotActivated", err)
	}
	if err := gate.Open(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for generator.CallCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if calls := generator.CallCount(); calls != 1 {
		t.Fatalf("provider calls after RuntimeStartGate = %d, want 1", calls)
	}
	landed := false
	for time.Now().Before(deadline) {
		history, err := fixture.repository.History(ctx, fixture.residentID, 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(history) == 2 && history[1].Content == "recovered after activation" {
			landed = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !landed {
		t.Fatal("recovered work did not land after RuntimeStartGate opened")
	}
	if _, err := fixture.application.Ingress(ctx, "accepted after activation"); err != nil {
		t.Fatalf("Ingress after gate open: %v", err)
	}
	fixture.application.Stop()
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := fixture.application.Wait(waitCtx); err != nil {
		t.Fatal(err)
	}
}
