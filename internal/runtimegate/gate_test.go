package runtimegate

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRuntimeStartGateOpensExactlyOnce(t *testing.T) {
	gate := New()
	waited := make(chan error, 1)
	go func() { waited <- gate.Wait(context.Background()) }()
	select {
	case err := <-waited:
		t.Fatalf("closed gate released early: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if err := gate.Open(); err != nil {
		t.Fatal(err)
	}
	if err := <-waited; err != nil {
		t.Fatal(err)
	}
	if !gate.IsOpen() {
		t.Fatal("gate did not report open")
	}
	if err := gate.Open(); !errors.Is(err, ErrAlreadyResolved) {
		t.Fatalf("second open = %v", err)
	}
}

func TestRuntimeStartGateFailureIsPermanent(t *testing.T) {
	gate := New()
	want := errors.New("projection init failed")
	if err := gate.Fail(want); err != nil {
		t.Fatal(err)
	}
	if err := gate.Wait(context.Background()); !errors.Is(err, want) {
		t.Fatalf("failed gate wait = %v", err)
	}
	if err := gate.Open(); !errors.Is(err, ErrAlreadyResolved) {
		t.Fatalf("failed gate reopened: %v", err)
	}
}
