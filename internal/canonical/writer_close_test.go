package canonical

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type closeTestClock struct{ now time.Time }

func (clock closeTestClock) Now() time.Time { return clock.now }

type closeTestBackend struct {
	mu        sync.Mutex
	head      Head
	commits   []CommitMetadata
	commitErr error
}

func (backend *closeTestBackend) LoadHead(context.Context) (Head, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	return backend.head, nil
}

func (backend *closeTestBackend) Begin(_ context.Context, metadata CommitMetadata) (CanonicalUoW, error) {
	return &closeTestUoW{backend: backend, metadata: metadata}, nil
}

func (backend *closeTestBackend) committed() []CommitMetadata {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	return append([]CommitMetadata(nil), backend.commits...)
}

type closeTestUoW struct {
	backend  *closeTestBackend
	metadata CommitMetadata
	finished bool
}

func (uow *closeTestUoW) Metadata() CommitMetadata { return uow.metadata }

func (uow *closeTestUoW) Commit(context.Context) error {
	if uow.finished {
		return errors.New("close test UoW already finished")
	}
	uow.finished = true
	uow.backend.mu.Lock()
	defer uow.backend.mu.Unlock()
	if uow.backend.commitErr != nil {
		return uow.backend.commitErr
	}
	uow.backend.commits = append(uow.backend.commits, uow.metadata)
	uow.backend.head = Head{
		Exists:      true,
		CommitSeq:   uow.metadata.CommitSeq,
		CommittedAt: uow.metadata.CommittedAt,
	}
	return nil
}

func (uow *closeTestUoW) Rollback(context.Context) error {
	if uow.finished {
		return errors.New("close test UoW already finished")
	}
	uow.finished = true
	return nil
}

type closeTestCommand struct {
	name    string
	execute func(context.Context, CanonicalUoW) (any, error)
}

func (command closeTestCommand) Name() string { return command.name }
func (closeTestCommand) Scope() Scope         { return GlobalScope() }
func (closeTestCommand) Validate() error      { return nil }
func (command closeTestCommand) Execute(ctx context.Context, uow CanonicalUoW) (any, error) {
	if command.execute == nil {
		return nil, nil
	}
	return command.execute(ctx, uow)
}

func openCloseTestWriter(t *testing.T, capacity int) (*Writer, *closeTestBackend) {
	t.Helper()
	clock := closeTestClock{now: time.Unix(1_700_000_000, 0)}
	ids, err := NewIDGenerator(clock, bytes.NewReader(bytes.Repeat([]byte{0x71}, 32)))
	if err != nil {
		t.Fatal(err)
	}
	backend := &closeTestBackend{}
	writer, err := OpenWriter(context.Background(), WriterOptions{
		Backend:       backend,
		IDs:           ids,
		Clock:         clock,
		Timezone:      MustTimezone("UTC"),
		QueueCapacity: capacity,
	})
	if err != nil {
		t.Fatal(err)
	}
	return writer, backend
}

func waitForCloseTest(t *testing.T, condition func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", description)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestWriterCloseDeadlineIsBoundedAndDrainsAcceptedCommands(t *testing.T) {
	writer, backend := openCloseTestWriter(t, 1)
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})

	type submission struct {
		name string
		err  error
	}
	results := make(chan submission, 3)
	var executions atomic.Int32
	submit := func(name string, execute func(context.Context, CanonicalUoW) (any, error)) {
		go func() {
			_, err := writer.Submit(context.Background(), closeTestCommand{name: name, execute: execute})
			results <- submission{name: name, err: err}
		}()
	}

	submit("first", func(context.Context, CanonicalUoW) (any, error) {
		executions.Add(1)
		close(firstStarted)
		<-releaseFirst
		return nil, nil
	})
	<-firstStarted

	submit("second", func(context.Context, CanonicalUoW) (any, error) {
		executions.Add(1)
		return nil, nil
	})
	waitForCloseTest(t, func() bool { return len(writer.queue) == 1 }, "second command to fill the queue")

	submit("third", func(context.Context, CanonicalUoW) (any, error) {
		executions.Add(1)
		return nil, nil
	})
	waitForCloseTest(t, func() bool {
		writer.acceptMu.Lock()
		defer writer.acceptMu.Unlock()
		return writer.pendingEnqueue == 1 && len(writer.queue) == 1
	}, "third command to reserve acceptance while the queue is full")

	closeCtx, cancelClose := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancelClose()
	closeReturned := make(chan error, 1)
	go func() { closeReturned <- writer.Close(closeCtx) }()

	select {
	case err := <-closeReturned:
		if !errors.Is(err, context.DeadlineExceeded) {
			close(releaseFirst)
			t.Fatalf("Close error = %v, want context deadline exceeded", err)
		}
	case <-time.After(500 * time.Millisecond):
		// Release the command before failing so a broken implementation cannot
		// strand its writer goroutine for the remainder of the test process.
		close(releaseFirst)
		<-closeReturned
		t.Fatal("Close remained blocked after its context deadline")
	}

	if _, err := writer.Submit(context.Background(), closeTestCommand{name: "late"}); !errors.Is(err, ErrWriterClosed) {
		close(releaseFirst)
		t.Fatalf("submission after Close error = %v, want ErrWriterClosed", err)
	}

	select {
	case <-writer.Done():
		close(releaseFirst)
		t.Fatal("writer completed before its accepted commands could drain")
	default:
	}
	close(releaseFirst)

	seen := make(map[string]error, 3)
	for range 3 {
		result := <-results
		seen[result.name] = result.err
	}
	for _, name := range []string{"first", "second", "third"} {
		if err := seen[name]; err != nil {
			t.Fatalf("%s submission error = %v", name, err)
		}
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	commits := backend.committed()
	if len(commits) != 3 {
		t.Fatalf("commits = %d, want 3", len(commits))
	}
	if got := executions.Load(); got != 3 {
		t.Fatalf("command executions = %d, want exactly 3", got)
	}
	for index, commit := range commits {
		if got, want := commit.CommitSeq.Int64(), int64(index+1); got != want {
			t.Fatalf("commit[%d] sequence = %d, want %d", index, got, want)
		}
	}
}

func TestWriterConcurrentCloseCompletesOrRejectsEachSubmissionExactlyOnce(t *testing.T) {
	writer, backend := openCloseTestWriter(t, 2)
	const submissionCount = 128

	start := make(chan struct{})
	results := make(chan error, submissionCount)
	var executions atomic.Int32
	var submissions sync.WaitGroup
	for index := range submissionCount {
		submissions.Add(1)
		go func() {
			defer submissions.Done()
			<-start
			_, err := writer.Submit(context.Background(), closeTestCommand{
				name: "concurrent-close",
				execute: func(context.Context, CanonicalUoW) (any, error) {
					executions.Add(1)
					return index, nil
				},
			})
			results <- err
		}()
	}
	close(start)
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	submissions.Wait()
	close(results)

	succeeded := 0
	for err := range results {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrWriterClosed):
		default:
			t.Fatalf("submission error = %v, want success or ErrWriterClosed", err)
		}
	}
	if commits := backend.committed(); len(commits) != succeeded {
		t.Fatalf("commits = %d, successful submissions = %d", len(commits), succeeded)
	}
	if got := int(executions.Load()); got != succeeded {
		t.Fatalf("command executions = %d, successful submissions = %d", got, succeeded)
	}
}

func TestWriterCloseDrainsAcceptedCommandsAfterPoisonExactlyOnce(t *testing.T) {
	writer, backend := openCloseTestWriter(t, 1)
	commitFailure := errors.New("ambiguous commit")
	backend.commitErr = commitFailure

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	results := make(chan error, 3)
	var executions atomic.Int32
	submit := func(name string, execute func(context.Context, CanonicalUoW) (any, error)) {
		go func() {
			_, err := writer.Submit(context.Background(), closeTestCommand{name: name, execute: execute})
			results <- err
		}()
	}
	submit("poison-source", func(context.Context, CanonicalUoW) (any, error) {
		executions.Add(1)
		close(firstStarted)
		<-releaseFirst
		return nil, nil
	})
	<-firstStarted
	submit("queued-after-poison", func(context.Context, CanonicalUoW) (any, error) {
		executions.Add(1)
		return nil, nil
	})
	waitForCloseTest(t, func() bool { return len(writer.queue) == 1 }, "second poison command to fill the queue")
	submit("accepted-after-poison", func(context.Context, CanonicalUoW) (any, error) {
		executions.Add(1)
		return nil, nil
	})
	waitForCloseTest(t, func() bool {
		writer.acceptMu.Lock()
		defer writer.acceptMu.Unlock()
		return writer.pendingEnqueue == 1
	}, "third poison command to reserve acceptance")

	closed := make(chan error, 1)
	go func() { closed <- writer.Close(context.Background()) }()
	close(releaseFirst)

	for range 3 {
		err := <-results
		if !errors.Is(err, ErrWriterPoisoned) || !errors.Is(err, commitFailure) {
			t.Fatalf("accepted command error = %v, want poisoned commit failure", err)
		}
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if got := executions.Load(); got != 1 {
		t.Fatalf("executions after poison = %d, want only poison source", got)
	}
	if commits := backend.committed(); len(commits) != 0 {
		t.Fatalf("commits after ambiguous failure = %d, want 0 recorded", len(commits))
	}
}
