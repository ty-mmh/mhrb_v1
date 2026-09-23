package canonical_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/testsupport"
)

type command struct {
	name    string
	scope   canonical.Scope
	execute func(context.Context, canonical.CanonicalUoW) (any, error)
}

type changingScopeCommand struct {
	calls  int
	first  canonical.Scope
	second canonical.Scope
}

type fencedCommand struct{ command }

func (fencedCommand) RequiresResidentAdmissionFence() bool { return true }

func (command *changingScopeCommand) Name() string { return "changing-scope" }
func (command *changingScopeCommand) Scope() canonical.Scope {
	command.calls++
	if command.calls <= 2 {
		return command.first
	}
	return command.second
}
func (*changingScopeCommand) Validate() error { return nil }
func (*changingScopeCommand) Execute(context.Context, canonical.CanonicalUoW) (any, error) {
	return nil, nil
}

func (command command) Name() string           { return command.name }
func (command command) Scope() canonical.Scope { return command.scope }
func (command command) Validate() error        { return nil }
func (command command) Execute(ctx context.Context, uow canonical.CanonicalUoW) (any, error) {
	if command.execute == nil {
		return nil, nil
	}
	return command.execute(ctx, uow)
}

func openTestWriter(t *testing.T, capacity int) (*canonical.Writer, *testsupport.MemoryCanonicalBackend, *testsupport.ManualClock) {
	t.Helper()
	clock := testsupport.NewManualClock(time.Unix(1_700_000_000, 0))
	ids, err := canonical.NewIDGenerator(clock, bytes.NewReader(bytes.Repeat([]byte{0x33}, 10)))
	if err != nil {
		t.Fatal(err)
	}
	backend := testsupport.NewMemoryCanonicalBackend()
	writer, err := canonical.OpenWriter(context.Background(), canonical.WriterOptions{
		Backend: backend, IDs: ids, Clock: clock,
		Timezone: canonical.MustTimezone("UTC"), QueueCapacity: capacity,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close(context.Background()) })
	return writer, backend, clock
}

func TestWriterRollbackDoesNotAdvanceCursor(t *testing.T) {
	writer, backend, _ := openTestWriter(t, 4)
	wantError := errors.New("reject command")
	_, err := writer.Submit(context.Background(), command{
		name: "reject", scope: canonical.GlobalScope(),
		execute: func(context.Context, canonical.CanonicalUoW) (any, error) { return nil, wantError },
	})
	if !errors.Is(err, wantError) {
		t.Fatalf("rejected command error = %v", err)
	}
	if len(backend.Commits()) != 0 {
		t.Fatal("rollback left a Canonical Commit")
	}
	result, err := writer.Submit(context.Background(), command{name: "accept", scope: canonical.GlobalScope()})
	if err != nil {
		t.Fatal(err)
	}
	if result.Commit.CommitSeq.Int64() != 1 {
		t.Fatalf("commit seq after rollback = %s", result.Commit.CommitSeq)
	}
}

func TestM7WriterAdmissionFenceDrainsAndRejectsProjectionRace(t *testing.T) {
	writer, _, _ := openTestWriter(t, 4)
	started := make(chan struct{})
	release := make(chan struct{})
	commandDone := make(chan error, 1)
	go func() {
		_, err := writer.Submit(context.Background(), command{
			name: "startup-mutation", scope: canonical.GlobalScope(),
			execute: func(context.Context, canonical.CanonicalUoW) (any, error) {
				close(started)
				<-release
				return nil, nil
			},
		})
		commandDone <- err
	}()
	<-started

	pauseDone := make(chan error, 1)
	go func() { pauseDone <- writer.PauseAdmissions(context.Background()) }()
	deadline := time.Now().Add(time.Second)
	for !writer.AdmissionsPaused() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !writer.AdmissionsPaused() {
		t.Fatal("admission fence did not close")
	}
	if _, err := writer.Submit(context.Background(), command{name: "projection-race"}); !errors.Is(err, canonical.ErrWriterAdmissionPaused) {
		t.Fatalf("submission during Projection preflight error = %v, want ErrWriterAdmissionPaused", err)
	}
	select {
	case err := <-pauseDone:
		t.Fatalf("PauseAdmissions returned before admitted mutation drained: %v", err)
	default:
	}

	close(release)
	if err := <-commandDone; err != nil {
		t.Fatal(err)
	}
	if err := <-pauseDone; err != nil {
		t.Fatal(err)
	}
	if err := writer.ResumeAdmissions(); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Submit(context.Background(), command{name: "activated"}); err != nil {
		t.Fatalf("submission after activation: %v", err)
	}
}

func TestM7ResidentAdmissionFenceTracksActivityAndExcludesOnlyCurrentToken(t *testing.T) {
	writer, _, _ := openTestWriter(t, 4)
	residentID, err := canonical.ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	if err != nil {
		t.Fatal(err)
	}
	otherResidentID, err := canonical.ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAW")
	if err != nil {
		t.Fatal(err)
	}
	residentScope, err := canonical.ResidentScope(residentID)
	if err != nil {
		t.Fatal(err)
	}
	otherScope, err := canonical.ResidentScope(otherResidentID)
	if err != nil {
		t.Fatal(err)
	}

	fenceStarted := make(chan struct{})
	releaseFence := make(chan struct{})
	fenceDone := make(chan error, 1)
	go func() {
		_, submitErr := writer.Submit(context.Background(), fencedCommand{command: command{
			name: "resident-erase", scope: residentScope,
			execute: func(ctx context.Context, _ canonical.CanonicalUoW) (any, error) {
				if err := canonical.RequireCurrentResidentFence(ctx, residentID); err != nil {
					return nil, err
				}
				close(fenceStarted)
				<-releaseFence
				return nil, nil
			},
		}})
		fenceDone <- submitErr
	}()
	<-fenceStarted

	activity, err := writer.ResidentActivity(residentID)
	if err != nil {
		t.Fatal(err)
	}
	if activity.Accepted != 0 || activity.Queued != 0 || activity.InFlight != 1 {
		t.Fatalf("fence activity = %+v", activity)
	}
	if _, err := writer.Submit(context.Background(), command{name: "late-same-resident", scope: residentScope}); !errors.Is(err, canonical.ErrResidentAdmissionClosed) {
		t.Fatalf("late same-resident admission error = %v", err)
	}
	otherDone := make(chan error, 1)
	go func() {
		_, submitErr := writer.Submit(context.Background(), command{name: "other-resident", scope: otherScope})
		otherDone <- submitErr
	}()
	deadline := time.Now().Add(time.Second)
	for {
		otherActivity, snapshotErr := writer.ResidentActivity(otherResidentID)
		if snapshotErr != nil {
			t.Fatal(snapshotErr)
		}
		if otherActivity.Total() == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("other resident was not admitted while fence was held")
		}
		time.Sleep(time.Millisecond)
	}
	close(releaseFence)
	if err := <-fenceDone; err != nil {
		t.Fatal(err)
	}
	if err := <-otherDone; err != nil {
		t.Fatalf("other resident was fenced: %v", err)
	}
	activity, err = writer.ResidentActivity(residentID)
	if err != nil || activity.Total() != 0 {
		t.Fatalf("released fence activity = %+v, %v", activity, err)
	}
	if _, err := writer.Submit(context.Background(), command{name: "after-fence", scope: residentScope}); err != nil {
		t.Fatalf("resident admission did not reopen: %v", err)
	}
}

func TestM7ResidentAdmissionFenceRejectsPreexistingAcceptedQueuedOrInFlightActivity(t *testing.T) {
	writer, _, _ := openTestWriter(t, 1)
	residentID, err := canonical.ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	if err != nil {
		t.Fatal(err)
	}
	residentScope, err := canonical.ResidentScope(residentID)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		_, submitErr := writer.Submit(context.Background(), command{
			name: "preexisting", scope: residentScope,
			execute: func(context.Context, canonical.CanonicalUoW) (any, error) {
				close(started)
				<-release
				return nil, nil
			},
		})
		firstDone <- submitErr
	}()
	<-started
	fenceDone := make(chan error, 1)
	go func() {
		_, submitErr := writer.Submit(context.Background(), fencedCommand{command: command{
			name: "contended-resident-erase", scope: residentScope,
			execute: func(ctx context.Context, _ canonical.CanonicalUoW) (any, error) {
				return nil, canonical.RequireCurrentResidentFence(ctx, residentID)
			},
		}})
		fenceDone <- submitErr
	}()
	deadline := time.Now().Add(time.Second)
	for {
		activity, err := writer.ResidentActivity(residentID)
		if err != nil {
			t.Fatal(err)
		}
		if activity.InFlight == 1 && activity.Queued == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("contended activity did not become observable: %+v", activity)
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-fenceDone; !errors.Is(err, canonical.ErrResidentActivityBusy) {
		t.Fatalf("contended fence error = %v, want ErrResidentActivityBusy", err)
	}
	if activity, err := writer.ResidentActivity(residentID); err != nil || activity.Total() != 0 {
		t.Fatalf("failed fence was not released: %+v, %v", activity, err)
	}
}

func TestM7ResidentAdmissionFenceReleasesAfterRollbackAndPanic(t *testing.T) {
	residentID, _ := canonical.ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	residentScope, _ := canonical.ResidentScope(residentID)
	for _, testCase := range []struct {
		name    string
		execute func(context.Context, canonical.CanonicalUoW) (any, error)
	}{
		{name: "rollback", execute: func(context.Context, canonical.CanonicalUoW) (any, error) { return nil, errors.New("rollback") }},
		{name: "panic", execute: func(context.Context, canonical.CanonicalUoW) (any, error) { panic("injected") }},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			writer, _, _ := openTestWriter(t, 2)
			_, err := writer.Submit(context.Background(), fencedCommand{command: command{name: "resident-erase-" + testCase.name, scope: residentScope, execute: testCase.execute}})
			if err == nil {
				t.Fatal("failing fenced command succeeded")
			}
			activity, snapshotErr := writer.ResidentActivity(residentID)
			if snapshotErr != nil {
				t.Fatal(snapshotErr)
			}
			if activity.Total() != 0 {
				t.Fatalf("fence activity leaked after %s: %+v", testCase.name, activity)
			}
		})
	}
}

func TestWriterAlreadyAppliedRollsBackNoOpAndReturnsSuccess(t *testing.T) {
	writer, backend, _ := openTestWriter(t, 4)
	result, err := writer.Submit(context.Background(), command{
		name: "already-applied", scope: canonical.GlobalScope(),
		execute: func(context.Context, canonical.CanonicalUoW) (any, error) {
			return "durable-result", canonical.ErrNoMutation
		},
	})
	if err != nil {
		t.Fatalf("already-applied command error = %v", err)
	}
	if result.Value != "durable-result" {
		t.Fatalf("already-applied result = %#v", result.Value)
	}
	if !result.Commit.CommitID.IsZero() || result.Commit.CommitSeq != 0 {
		t.Fatalf("no-op returned a commit = %+v", result.Commit)
	}
	if len(backend.Commits()) != 0 {
		t.Fatal("already-applied command left a Canonical Commit")
	}

	committed, err := writer.Submit(context.Background(), command{name: "accept", scope: canonical.GlobalScope()})
	if err != nil {
		t.Fatal(err)
	}
	if committed.Commit.CommitSeq.Int64() != 1 {
		t.Fatalf("commit seq after already-applied no-op = %s", committed.Commit.CommitSeq)
	}
}

func TestWriterDoesNotHideErrorJoinedWithAlreadyAppliedSentinel(t *testing.T) {
	writer, backend, _ := openTestWriter(t, 4)
	conflict := errors.New("payload conflict")
	_, err := writer.Submit(context.Background(), command{
		name: "conflicting-no-op", scope: canonical.GlobalScope(),
		execute: func(context.Context, canonical.CanonicalUoW) (any, error) {
			return nil, errors.Join(canonical.ErrNoMutation, conflict)
		},
	})
	if !errors.Is(err, conflict) {
		t.Fatalf("joined conflict error = %v", err)
	}
	if len(backend.Commits()) != 0 {
		t.Fatal("joined conflict left a Canonical Commit")
	}
}

func TestWriterSerializesConcurrentCommands(t *testing.T) {
	writer, backend, _ := openTestWriter(t, 32)
	const count = 24
	var wait sync.WaitGroup
	errorsSeen := make(chan error, count)
	for i := 0; i < count; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := writer.Submit(context.Background(), command{name: "concurrent", scope: canonical.GlobalScope()})
			errorsSeen <- err
		}()
	}
	wait.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Fatal(err)
		}
	}
	commits := backend.Commits()
	if len(commits) != count {
		t.Fatalf("commits = %d, want %d", len(commits), count)
	}
	for i, commit := range commits {
		if commit.CommitSeq.Int64() != int64(i+1) {
			t.Fatalf("commit[%d] seq = %s", i, commit.CommitSeq)
		}
	}
}

func TestWriterBoundedQueueHonorsCancellation(t *testing.T) {
	writer, _, _ := openTestWriter(t, 1)
	started := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		_, err := writer.Submit(context.Background(), command{
			name: "blocking", scope: canonical.GlobalScope(),
			execute: func(context.Context, canonical.CanonicalUoW) (any, error) {
				close(started)
				<-release
				return nil, nil
			},
		})
		firstDone <- err
	}()
	<-started
	secondDone := make(chan error, 1)
	go func() {
		_, err := writer.Submit(context.Background(), command{name: "queued", scope: canonical.GlobalScope()})
		secondDone <- err
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := writer.Submit(ctx, command{name: "overflow", scope: canonical.GlobalScope()}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("overflow submit error = %v", err)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
}

func TestWriterRejectsCommandScopeMutationInsideUoW(t *testing.T) {
	writer, backend, _ := openTestWriter(t, 4)
	resident, err := canonical.ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	if err != nil {
		t.Fatal(err)
	}
	residentScope, err := canonical.ResidentScope(resident)
	if err != nil {
		t.Fatal(err)
	}
	_, err = writer.Submit(context.Background(), &changingScopeCommand{
		first: canonical.GlobalScope(), second: residentScope,
	})
	if err == nil {
		t.Fatal("scope-changing command was accepted")
	}
	if len(backend.Commits()) != 0 {
		t.Fatal("scope-changing command left a commit")
	}
}

func TestWriterRestoresCursorAndLedgerClockFromHead(t *testing.T) {
	backend := testsupport.NewMemoryCanonicalBackend()
	clock := testsupport.NewManualClock(time.Unix(1_700_000_000, 0))
	open := func(entropyByte byte) *canonical.Writer {
		t.Helper()
		ids, err := canonical.NewIDGenerator(clock, bytes.NewReader(bytes.Repeat([]byte{entropyByte}, 10)))
		if err != nil {
			t.Fatal(err)
		}
		writer, err := canonical.OpenWriter(context.Background(), canonical.WriterOptions{
			Backend: backend, IDs: ids, Clock: clock,
			Timezone: canonical.MustTimezone("UTC"), QueueCapacity: 4,
		})
		if err != nil {
			t.Fatal(err)
		}
		return writer
	}

	firstWriter := open(0x41)
	first, err := firstWriter.Submit(context.Background(), command{name: "first", scope: canonical.GlobalScope()})
	if err != nil {
		t.Fatal(err)
	}
	if err := firstWriter.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	clock.Set(clock.Now().Add(-time.Hour))
	secondWriter := open(0x42)
	defer secondWriter.Close(context.Background())
	second, err := secondWriter.Submit(context.Background(), command{name: "second", scope: canonical.GlobalScope()})
	if err != nil {
		t.Fatal(err)
	}
	if second.Commit.CommitSeq.Int64() != 2 {
		t.Fatalf("restored commit seq = %s", second.Commit.CommitSeq)
	}
	if second.Commit.CommittedAt != first.Commit.CommittedAt+1 {
		t.Fatalf("restored ledger time = %s, want %s", second.Commit.CommittedAt, first.Commit.CommittedAt+1)
	}
}
