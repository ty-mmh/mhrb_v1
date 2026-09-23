package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"mahoroba.local/mahoroba/internal/projection"
)

func TestM7ProjectionApplyAndDropVerifyWritableDatabaseBoundary(t *testing.T) {
	for _, operation := range []string{"apply", "drop"} {
		for _, testCase := range []struct {
			name   string
			failAt int
			want   int
		}{
			{name: "before_transaction", failAt: 1, want: 0},
			{name: "after_transaction_open", failAt: 2, want: 0},
			{name: "pre_commit", failAt: 3, want: 0},
			{name: "post_commit", failAt: 4, want: 1},
		} {
			t.Run(operation+"/"+testCase.name, func(t *testing.T) {
				database, err := Open(context.Background(), filepath.Join(t.TempDir(), "mahoroba.db"))
				if err != nil {
					t.Fatal(err)
				}
				defer database.Close()
				residentID := projectionTestID(t, "00000000000000000000000001")
				request := statusProjectionApply(t, residentID, nil, 1, "active")
				if operation == "drop" {
					if err := database.Projection().Apply(context.Background(), request); err != nil {
						t.Fatal(err)
					}
				}
				probe := &canonicalBoundaryProbe{failAt: testCase.failAt}
				database.canonicalBoundary = probe
				if operation == "apply" {
					err = database.Projection().Apply(context.Background(), request)
				} else {
					err = database.Projection().Drop(context.Background(), projection.DropRequest{
						Definition: request.Definition, ResidentID: residentID, Observed: &request.Watermark,
					})
				}
				if !errors.Is(err, errCanonicalBoundaryChanged) {
					t.Fatalf("boundary failure = %v", err)
				}
				if probe.calls != testCase.failAt {
					t.Fatalf("boundary calls = %d, want %d", probe.calls, testCase.failAt)
				}
				var rows int
				if err := database.reader.QueryRow(`SELECT COUNT(*) FROM resident_current_status WHERE resident_id=?`, residentID.String()).Scan(&rows); err != nil {
					t.Fatal(err)
				}
				if operation == "drop" {
					rows = 1 - rows
				}
				if rows != testCase.want {
					t.Fatalf("committed operation = %d, want %d", rows, testCase.want)
				}
			})
		}
	}
}

func TestM7ProjectionPostCommitBoundaryVerificationRetainsWriteGate(t *testing.T) {
	database, err := Open(context.Background(), filepath.Join(t.TempDir(), "mahoroba.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	postCommitObserved := false
	probe := &canonicalBoundaryProbe{hook: func(call int) error {
		if call != 4 {
			return nil
		}
		database.writes.mu.Lock()
		defer database.writes.mu.Unlock()
		postCommitObserved = true
		if !database.writes.active {
			return errors.New("Projection write gate released before post-commit boundary verification")
		}
		return nil
	}}
	database.canonicalBoundary = probe
	residentID := projectionTestID(t, "00000000000000000000000001")
	if err := database.Projection().Apply(context.Background(), statusProjectionApply(t, residentID, nil, 1, "active")); err != nil {
		t.Fatal(err)
	}
	if !postCommitObserved || probe.calls != 4 {
		t.Fatalf("post-commit boundary observation=%v calls=%d", postCommitObserved, probe.calls)
	}
	database.writes.mu.Lock()
	active := database.writes.active
	database.writes.mu.Unlock()
	if active {
		t.Fatal("Projection write gate remained active after post-commit verification")
	}
}

func TestM7BoundProjectionHeadReadUsesCanonicalBoundary(t *testing.T) {
	database, err := Open(context.Background(), filepath.Join(t.TempDir(), "mahoroba.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	probe := &canonicalBoundaryProbe{failAt: 1}
	database.canonicalBoundary = probe
	_, err = database.Projection().Head(context.Background())
	if !errors.Is(err, errCanonicalBoundaryChanged) {
		t.Fatalf("Projection head boundary failure = %v", err)
	}
}
