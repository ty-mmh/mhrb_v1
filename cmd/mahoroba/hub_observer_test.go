package main

import (
	"context"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/httpui"
)

func TestHubObserverGenerationStatusIdentifiesRun(t *testing.T) {
	residentID, err := canonical.ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	if err != nil {
		t.Fatal(err)
	}
	runID, err := canonical.ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAX")
	if err != nil {
		t.Fatal(err)
	}
	hub := httpui.NewHub(2)
	subscription := hub.Subscribe(context.Background())
	defer subscription.Close()
	observer := hubObserver{hub: hub}
	observer.GenerationStarted(residentID, runID, 1)
	observer.GenerationFailed(residentID, runID, 1, "test_failure", false)
	for _, status := range []string{"generating", "retry_pending"} {
		select {
		case event := <-subscription.Events:
			if event.Type != httpui.EventStatus || event.Status != status ||
				event.ResidentID != residentID.String() || event.GenerationRunID != runID.String() {
				t.Fatalf("%s event = %#v", status, event)
			}
		default:
			t.Fatalf("missing %s status", status)
		}
	}
}
