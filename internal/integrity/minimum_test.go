package integrity

import (
	"context"
	"errors"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
)

type staticMinimumSource struct {
	snapshot ClaimStatementAliasSnapshot
	copies   []PresentBlobCopy
}

func (source staticMinimumSource) LoadClaimStatementAliases(context.Context) (ClaimStatementAliasSnapshot, error) {
	return source.snapshot, nil
}

func (source staticMinimumSource) LoadPresentBlobCopies(context.Context) ([]PresentBlobCopy, error) {
	return append([]PresentBlobCopy(nil), source.copies...), nil
}

type staticBlobObjects map[canonical.Digest][]byte

func (objects staticBlobObjects) Read(_ context.Context, _ canonical.ID, digest canonical.Digest) ([]byte, error) {
	value, ok := objects[digest]
	if !ok {
		return nil, errors.New("missing")
	}
	return append([]byte(nil), value...), nil
}

func TestMinimumCheckerClaimStatementAliasMatrix(t *testing.T) {
	tests := []struct {
		name      string
		snapshot  ClaimStatementAliasSnapshot
		wantFatal bool
	}{
		{name: "valid_present_alias_group", snapshot: presentAliasSnapshot()},
		{name: "valid_fully_erased_alias_group", snapshot: erasedAliasSnapshot()},
		{name: "half_erased_alias", snapshot: mutateErasedAlias(func(snapshot *ClaimStatementAliasSnapshot) {
			snapshot.Contents[0].Claims[1].StatementHashIsSet = true
		}), wantFatal: true},
		{name: "present_null_hash", snapshot: mutatePresentAlias(func(snapshot *ClaimStatementAliasSnapshot) {
			snapshot.Contents[0].Claims[0].StatementHashIsSet = false
		}), wantFatal: true},
		{name: "present_pair", snapshot: mutatePresentAlias(func(snapshot *ClaimStatementAliasSnapshot) {
			snapshot.ErasureEvents = append(snapshot.ErasureEvents, validPair("pair-a", "claim-a"))
		}), wantFatal: true},
		{name: "erased_missing_content_event", snapshot: mutateErasedAlias(func(snapshot *ClaimStatementAliasSnapshot) {
			snapshot.Contents[0].ErasureEvents = nil
		}), wantFatal: true},
		{name: "erased_missing_pair", snapshot: mutateErasedAlias(func(snapshot *ClaimStatementAliasSnapshot) {
			snapshot.ErasureEvents = snapshot.ErasureEvents[:1]
		}), wantFatal: true},
		{name: "erased_multiple_pairs", snapshot: mutateErasedAlias(func(snapshot *ClaimStatementAliasSnapshot) {
			snapshot.ErasureEvents = append(snapshot.ErasureEvents, ClaimStatementErasureEvent{
				EventID: "pair-a-second", ClaimID: "claim-a", ResidentID: "resident-a",
				CanonicalCommitID: "commit-a", ContentErasureEventID: "content-event-a",
				RecordedAt: 20, RecordedTZ: "UTC",
			})
		}), wantFatal: true},
		{name: "wrong_content_event", snapshot: mutateErasedAlias(func(snapshot *ClaimStatementAliasSnapshot) {
			snapshot.ErasureEvents[0].ContentErasureEventID = "missing-event"
		}), wantFatal: true},
		{name: "pair_commit_mismatch", snapshot: mutateErasedAlias(func(snapshot *ClaimStatementAliasSnapshot) {
			snapshot.ErasureEvents[0].CanonicalCommitID = "other-commit"
		}), wantFatal: true},
		{name: "pair_resident_mismatch", snapshot: mutateErasedAlias(func(snapshot *ClaimStatementAliasSnapshot) {
			snapshot.ErasureEvents[0].ResidentID = "resident-b"
		}), wantFatal: true},
		{name: "orphan_pair", snapshot: mutatePresentAlias(func(snapshot *ClaimStatementAliasSnapshot) {
			snapshot.ErasureEvents = append(snapshot.ErasureEvents, ClaimStatementErasureEvent{
				EventID: "orphan-pair", ClaimID: "missing-claim", ResidentID: "resident-a",
				CanonicalCommitID: "commit-a", ContentErasureEventID: "content-event-a",
				RecordedAt: 20, RecordedTZ: "UTC",
			})
		}), wantFatal: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			checker := NewMinimumChecker(staticMinimumSource{snapshot: test.snapshot})
			err := checker.Check(context.Background())
			if !test.wantFatal {
				if err != nil {
					t.Fatalf("MinimumCheck() error = %v", err)
				}
				return
			}
			if !errors.Is(err, ErrFatal) {
				t.Fatalf("MinimumCheck() error = %v, want ErrFatal", err)
			}
			var fatal *FatalError
			if !errors.As(err, &fatal) || fatal.Code != ClaimStatementAliasGroupInvalid {
				t.Fatalf("MinimumCheck() fatal = %#v", fatal)
			}
		})
	}
}

func TestM7MinimumCheckRequiresMatchingDualBlobCopies(t *testing.T) {
	resident, err := canonical.ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	if err != nil {
		t.Fatal(err)
	}
	want := []byte("canonical bytes")
	digest := canonical.HashBlob(want)
	base := PresentBlobCopy{
		ContentID: "01ARZ3NDEKTSV4RRFFQ69G5FAW", ResidentID: resident,
		ExpectedHash: digest, DatabaseBlob: want, DeclaredSize: int64(len(want)), BlobFound: true,
	}
	tests := []struct {
		name      string
		copy      PresentBlobCopy
		objects   staticBlobObjects
		wantFatal bool
	}{
		{name: "matching", copy: base, objects: staticBlobObjects{digest: want}},
		{name: "missing sqlite blob", copy: func() PresentBlobCopy { value := base; value.BlobFound = false; value.DatabaseBlob = nil; return value }(), objects: staticBlobObjects{digest: want}, wantFatal: true},
		{name: "sqlite digest mismatch", copy: func() PresentBlobCopy { value := base; value.DatabaseBlob = []byte("changed"); return value }(), objects: staticBlobObjects{digest: want}, wantFatal: true},
		{name: "sqlite byte size mismatch", copy: func() PresentBlobCopy { value := base; value.DeclaredSize++; return value }(), objects: staticBlobObjects{digest: want}, wantFatal: true},
		{name: "missing filesystem blob", copy: base, objects: staticBlobObjects{}, wantFatal: true},
		{name: "filesystem bytes mismatch", copy: base, objects: staticBlobObjects{digest: []byte("changed")}, wantFatal: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			checker := NewMinimumChecker(staticMinimumSource{copies: []PresentBlobCopy{test.copy}}, WithBlobObjects(test.objects))
			err := checker.Check(context.Background())
			if !test.wantFatal {
				if err != nil {
					t.Fatalf("MinimumCheck() error = %v", err)
				}
				return
			}
			var fatal *FatalError
			if !errors.Is(err, ErrFatal) || !errors.As(err, &fatal) || fatal.Code != PresentBlobCopyInvalid {
				t.Fatalf("MinimumCheck() fatal = %#v, error = %v", fatal, err)
			}
		})
	}
}

func presentAliasSnapshot() ClaimStatementAliasSnapshot {
	return ClaimStatementAliasSnapshot{Contents: []ClaimStatementContent{{
		ContentID: "content-a", OwnerResidentID: "resident-a", ErasureState: "present",
		Claims: []ClaimStatementAlias{
			{ClaimID: "claim-a", OwnerResidentID: "resident-a", StatementHashIsSet: true},
			{ClaimID: "claim-b", OwnerResidentID: "resident-a", StatementHashIsSet: true},
		},
	}}}
}

func erasedAliasSnapshot() ClaimStatementAliasSnapshot {
	return ClaimStatementAliasSnapshot{
		Contents: []ClaimStatementContent{{
			ContentID: "content-a", OwnerResidentID: "resident-a", ErasureState: "erased",
			Claims: []ClaimStatementAlias{
				{ClaimID: "claim-a", OwnerResidentID: "resident-a"},
				{ClaimID: "claim-b", OwnerResidentID: "resident-a"},
			},
			ErasureEvents: []ContentErasureEvent{{
				EventID: "content-event-a", ContentID: "content-a",
				CanonicalCommitID: "commit-a", CommitResidentID: "resident-a", ErasureScope: "content",
				RecordedAt: 20, RecordedTZ: "UTC", CommitRecordedAt: 20, CommitRecordedTZ: "UTC",
			}},
		}},
		ErasureEvents: []ClaimStatementErasureEvent{
			validPair("pair-a", "claim-a"),
			validPair("pair-b", "claim-b"),
		},
	}
}

func validPair(eventID, claimID string) ClaimStatementErasureEvent {
	return ClaimStatementErasureEvent{
		EventID: eventID, ClaimID: claimID, ResidentID: "resident-a",
		CanonicalCommitID: "commit-a", ContentErasureEventID: "content-event-a",
		RecordedAt: 20, RecordedTZ: "UTC",
	}
}

func mutatePresentAlias(mutate func(*ClaimStatementAliasSnapshot)) ClaimStatementAliasSnapshot {
	snapshot := cloneAliasSnapshot(presentAliasSnapshot())
	mutate(&snapshot)
	return snapshot
}

func mutateErasedAlias(mutate func(*ClaimStatementAliasSnapshot)) ClaimStatementAliasSnapshot {
	snapshot := cloneAliasSnapshot(erasedAliasSnapshot())
	mutate(&snapshot)
	return snapshot
}

func cloneAliasSnapshot(snapshot ClaimStatementAliasSnapshot) ClaimStatementAliasSnapshot {
	clone := ClaimStatementAliasSnapshot{
		Contents:      append([]ClaimStatementContent(nil), snapshot.Contents...),
		ErasureEvents: append([]ClaimStatementErasureEvent(nil), snapshot.ErasureEvents...),
	}
	for index := range clone.Contents {
		clone.Contents[index].Claims = append([]ClaimStatementAlias(nil), snapshot.Contents[index].Claims...)
		clone.Contents[index].ErasureEvents = append([]ContentErasureEvent(nil), snapshot.Contents[index].ErasureEvents...)
	}
	return clone
}
