// Package testsupport provides deterministic, Writer-backed fixtures. It does
// not bypass Canonical commands with direct SQL inserts.
package testsupport

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"sync"

	"mahoroba.local/mahoroba/internal/canonical"
)

// MemoryCanonicalBackend is an adapter-neutral test backend for Canonical
// Writer and ledger verification tests. Production behavior still enters via
// canonical.Command and a CanonicalUoW capability.
type MemoryCanonicalBackend struct {
	mu      sync.RWMutex
	commits []canonical.CommitMetadata
	events  map[canonical.ID][]canonical.LedgerRecord
}

func NewMemoryCanonicalBackend() *MemoryCanonicalBackend {
	return &MemoryCanonicalBackend{events: make(map[canonical.ID][]canonical.LedgerRecord)}
}

func (backend *MemoryCanonicalBackend) LoadHead(context.Context) (canonical.Head, error) {
	backend.mu.RLock()
	defer backend.mu.RUnlock()
	if len(backend.commits) == 0 {
		return canonical.Head{}, nil
	}
	last := backend.commits[len(backend.commits)-1]
	return canonical.Head{Exists: true, CommitSeq: last.CommitSeq, CommittedAt: last.CommittedAt}, nil
}

func (backend *MemoryCanonicalBackend) Begin(_ context.Context, metadata canonical.CommitMetadata) (canonical.CanonicalUoW, error) {
	if err := metadata.Validate(); err != nil {
		return nil, err
	}
	return &memoryCanonicalUoW{backend: backend, metadata: metadata}, nil
}

func (backend *MemoryCanonicalBackend) Commits() []canonical.CommitMetadata {
	backend.mu.RLock()
	defer backend.mu.RUnlock()
	return append([]canonical.CommitMetadata(nil), backend.commits...)
}

func (backend *MemoryCanonicalBackend) Events(residentID canonical.ID) []canonical.LedgerRecord {
	backend.mu.RLock()
	defer backend.mu.RUnlock()
	events := backend.events[residentID]
	return cloneLedgerRecords(events)
}

func (backend *MemoryCanonicalBackend) WalkResidentContents(ctx context.Context, residentID canonical.ID, visit func(canonical.LedgerContent) error) error {
	if visit == nil {
		return fmt.Errorf("testsupport: nil content visitor")
	}
	byID := make(map[canonical.ID]canonical.LedgerContent)
	for _, event := range backend.Events(residentID) {
		byID[event.Content.ID] = event.Content
	}
	ids := make([]canonical.ID, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(byID[id]); err != nil {
			return err
		}
	}
	return nil
}

func (backend *MemoryCanonicalBackend) WalkResidentEvents(ctx context.Context, residentID canonical.ID, visit func(canonical.LedgerRecord) error) error {
	if visit == nil {
		return fmt.Errorf("testsupport: nil ledger visitor")
	}
	events := backend.Events(residentID)
	sort.Slice(events, func(i, j int) bool { return events[i].Seq < events[j].Seq })
	for _, event := range events {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(event); err != nil {
			return err
		}
	}
	return nil
}

type memoryCanonicalUoW struct {
	backend    *MemoryCanonicalBackend
	metadata   canonical.CommitMetadata
	pending    []canonical.LedgerRecord
	committed  bool
	rolledBack bool
}

type fixtureEventEnvelope struct {
	EventID                 canonical.ID            `json:"event_id"`
	ResidentID              canonical.ID            `json:"resident_id"`
	Seq                     canonical.Seq           `json:"seq"`
	ContentID               canonical.ID            `json:"content_id"`
	PayloadCommitment       canonical.Digest        `json:"payload_commitment"`
	PrevEventHash           *canonical.Digest       `json:"prev_event_hash"`
	EventHashAlgorithm      string                  `json:"event_hash_algorithm"`
	EventHashDomain         string                  `json:"event_hash_domain"`
	CanonicalizationVersion string                  `json:"canonicalization_version"`
	Payload                 canonical.CanonicalJSON `json:"payload"`
}

func (uow *memoryCanonicalUoW) Metadata() canonical.CommitMetadata { return uow.metadata }

// AppendFixtureEvent is a narrow testsupport capability asserted by the
// fixture command below; it is not generic table mutation.
func (uow *memoryCanonicalUoW) AppendFixtureEvent(eventID, residentID canonical.ID, payload canonical.CanonicalJSON) (canonical.LedgerRecord, error) {
	if uow.committed || uow.rolledBack {
		return canonical.LedgerRecord{}, fmt.Errorf("testsupport: UoW is closed")
	}
	scopeResident, scoped := uow.metadata.Scope.ResidentID()
	if !scoped || scopeResident != residentID {
		return canonical.LedgerRecord{}, fmt.Errorf("testsupport: event resident does not match commit scope")
	}

	uow.backend.mu.RLock()
	for _, records := range uow.backend.events {
		for _, record := range records {
			if record.EventID == eventID {
				uow.backend.mu.RUnlock()
				return canonical.LedgerRecord{}, fmt.Errorf("testsupport: duplicate event ID")
			}
		}
	}
	existing := uow.backend.events[residentID]
	var previous *canonical.LedgerRecord
	if len(existing) > 0 {
		copyRecord := existing[len(existing)-1]
		previous = &copyRecord
	}
	uow.backend.mu.RUnlock()
	for i := range uow.pending {
		if uow.pending[i].EventID == eventID {
			return canonical.LedgerRecord{}, fmt.Errorf("testsupport: duplicate pending event ID")
		}
		if uow.pending[i].ResidentID == residentID {
			copyRecord := uow.pending[i]
			previous = &copyRecord
		}
	}

	nextValue := int64(1)
	var previousHash *canonical.Digest
	if previous != nil {
		nextValue = previous.Seq.Int64() + 1
		hash := previous.EventHash
		previousHash = &hash
	}
	seq, err := canonical.NewSeq(nextValue)
	if err != nil {
		return canonical.LedgerRecord{}, err
	}
	logicalBytes := payload.Bytes()
	blobHash := canonical.HashBlob(logicalBytes)
	saltSeed := canonical.HashBlob([]byte("testsupport:fixture-content-salt:v1\x00" + eventID.String()))
	commitmentSalt, err := canonical.ContentSaltFromBytes(saltSeed.Bytes())
	if err != nil {
		return canonical.LedgerRecord{}, err
	}
	commitment, err := canonical.CommitContent("event_payload", commitmentSalt, logicalBytes)
	if err != nil {
		return canonical.LedgerRecord{}, err
	}
	envelope, err := canonical.MarshalCanonical(fixtureEventEnvelope{
		EventID:                 eventID,
		ResidentID:              residentID,
		Seq:                     seq,
		ContentID:               eventID,
		PayloadCommitment:       commitment,
		PrevEventHash:           previousHash,
		EventHashAlgorithm:      canonical.HashAlgorithm,
		EventHashDomain:         canonical.EventHashDomain,
		CanonicalizationVersion: canonical.CanonicalizationVersion,
		Payload:                 payload,
	})
	if err != nil {
		return canonical.LedgerRecord{}, err
	}
	eventHash, err := canonical.HashEvent(envelope)
	if err != nil {
		return canonical.LedgerRecord{}, err
	}
	record := canonical.LedgerRecord{
		EventID:           eventID,
		ResidentID:        residentID,
		Seq:               seq,
		ContentID:         eventID,
		PayloadCommitment: commitment,
		Content: canonical.LedgerContent{
			Found: true, ID: eventID, ResidentID: residentID, Class: "event_payload",
			ErasureState: canonical.ContentErasurePresent, BlobFound: true,
			LogicalBytes: logicalBytes, BlobHash: &blobHash, Commitment: commitment,
			CommitmentSalt: &commitmentSalt,
		},
		PrevEventHash: previousHash,
		EventHash:     eventHash,
		Envelope:      envelope,
	}
	uow.pending = append(uow.pending, record)
	return cloneLedgerRecord(record), nil
}

func (backend *MemoryCanonicalBackend) ValidateLedgerEnvelope(record canonical.LedgerRecord) error {
	var envelope fixtureEventEnvelope
	if err := json.Unmarshal(record.Envelope.Bytes(), &envelope); err != nil {
		return fmt.Errorf("testsupport: decode fixture envelope: %w", err)
	}
	if envelope.EventID != record.EventID || envelope.ResidentID != record.ResidentID || envelope.Seq != record.Seq {
		return fmt.Errorf("testsupport: fixture envelope identity mismatch")
	}
	if !equalDigestPointers(envelope.PrevEventHash, record.PrevEventHash) {
		return fmt.Errorf("testsupport: fixture envelope previous hash mismatch")
	}
	if envelope.ContentID != record.ContentID {
		return fmt.Errorf("testsupport: fixture envelope content reference mismatch")
	}
	if envelope.PayloadCommitment != record.PayloadCommitment {
		return fmt.Errorf("testsupport: fixture envelope payload commitment mismatch")
	}
	if envelope.EventHashAlgorithm != canonical.HashAlgorithm ||
		envelope.EventHashDomain != canonical.EventHashDomain ||
		envelope.CanonicalizationVersion != canonical.CanonicalizationVersion {
		return fmt.Errorf("testsupport: fixture envelope hash metadata mismatch")
	}
	if envelope.Payload.IsZero() {
		return fmt.Errorf("testsupport: fixture envelope payload is empty")
	}
	return nil
}

func (uow *memoryCanonicalUoW) Commit(context.Context) error {
	if uow.committed || uow.rolledBack {
		return fmt.Errorf("testsupport: UoW is closed")
	}
	uow.backend.mu.Lock()
	defer uow.backend.mu.Unlock()
	if len(uow.backend.commits) == 0 {
		if uow.metadata.CommitSeq.Int64() != 1 {
			return fmt.Errorf("testsupport: first commit sequence is not 1")
		}
	} else {
		last := uow.backend.commits[len(uow.backend.commits)-1]
		if uow.metadata.CommitSeq.Int64() != last.CommitSeq.Int64()+1 {
			return fmt.Errorf("testsupport: non-consecutive commit sequence")
		}
		if uow.metadata.CommittedAt <= last.CommittedAt {
			return fmt.Errorf("testsupport: ledger time did not advance")
		}
	}
	uow.backend.commits = append(uow.backend.commits, uow.metadata)
	for _, record := range uow.pending {
		uow.backend.events[record.ResidentID] = append(uow.backend.events[record.ResidentID], cloneLedgerRecord(record))
	}
	uow.committed = true
	return nil
}

func (uow *memoryCanonicalUoW) Rollback(context.Context) error {
	if uow.committed {
		return fmt.Errorf("testsupport: cannot rollback committed UoW")
	}
	uow.pending = nil
	uow.rolledBack = true
	return nil
}

type fixtureEventAppender interface {
	AppendFixtureEvent(eventID, residentID canonical.ID, envelope canonical.CanonicalJSON) (canonical.LedgerRecord, error)
}

type appendFixtureEventCommand struct {
	eventID    canonical.ID
	residentID canonical.ID
	payload    canonical.CanonicalJSON
	scope      canonical.Scope
}

func (command appendFixtureEventCommand) Name() string           { return "testsupport.AppendFixtureEvent" }
func (command appendFixtureEventCommand) Scope() canonical.Scope { return command.scope }
func (command appendFixtureEventCommand) Validate() error {
	if err := command.eventID.Validate(); err != nil {
		return err
	}
	if err := command.residentID.Validate(); err != nil {
		return err
	}
	if command.payload.IsZero() {
		return fmt.Errorf("testsupport: empty event payload")
	}
	return nil
}
func (command appendFixtureEventCommand) Execute(_ context.Context, uow canonical.CanonicalUoW) (any, error) {
	appender, ok := uow.(fixtureEventAppender)
	if !ok {
		return nil, fmt.Errorf("testsupport: UoW lacks fixture event capability")
	}
	return appender.AppendFixtureEvent(command.eventID, command.residentID, command.payload)
}

type CanonicalFixture struct {
	Backend *MemoryCanonicalBackend
	Writer  *canonical.Writer
	ids     *canonical.IDGenerator
}

func OpenCanonicalFixture(ctx context.Context, clock canonical.Clock, entropy io.Reader) (*CanonicalFixture, error) {
	ids, err := canonical.NewIDGenerator(clock, entropy)
	if err != nil {
		return nil, err
	}
	backend := NewMemoryCanonicalBackend()
	writer, err := canonical.OpenWriter(ctx, canonical.WriterOptions{
		Backend:       backend,
		IDs:           ids,
		Clock:         clock,
		Timezone:      canonical.MustTimezone("UTC"),
		QueueCapacity: 16,
	})
	if err != nil {
		return nil, err
	}
	return &CanonicalFixture{Backend: backend, Writer: writer, ids: ids}, nil
}

func (fixture *CanonicalFixture) NewID() (canonical.ID, error) { return fixture.ids.New() }

func (fixture *CanonicalFixture) AppendEvent(ctx context.Context, residentID canonical.ID, payload canonical.CanonicalJSON) (canonical.LedgerRecord, canonical.CommitMetadata, error) {
	eventID, err := fixture.ids.New()
	if err != nil {
		return canonical.LedgerRecord{}, canonical.CommitMetadata{}, err
	}
	scope, err := canonical.ResidentScope(residentID)
	if err != nil {
		return canonical.LedgerRecord{}, canonical.CommitMetadata{}, err
	}
	result, err := fixture.Writer.Submit(ctx, appendFixtureEventCommand{
		eventID: eventID, residentID: residentID, payload: payload, scope: scope,
	})
	if err != nil {
		return canonical.LedgerRecord{}, canonical.CommitMetadata{}, err
	}
	record, ok := result.Value.(canonical.LedgerRecord)
	if !ok {
		return canonical.LedgerRecord{}, canonical.CommitMetadata{}, fmt.Errorf("testsupport: unexpected fixture result %T", result.Value)
	}
	return record, result.Commit, nil
}

func equalDigestPointers(left, right *canonical.Digest) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func cloneLedgerRecords(records []canonical.LedgerRecord) []canonical.LedgerRecord {
	cloned := make([]canonical.LedgerRecord, len(records))
	for i := range records {
		cloned[i] = cloneLedgerRecord(records[i])
	}
	return cloned
}

func cloneLedgerRecord(record canonical.LedgerRecord) canonical.LedgerRecord {
	copyRecord := record
	if record.PrevEventHash != nil {
		hash := *record.PrevEventHash
		copyRecord.PrevEventHash = &hash
	}
	copyRecord.Content.LogicalBytes = append([]byte(nil), record.Content.LogicalBytes...)
	if record.Content.BlobHash != nil {
		hash := *record.Content.BlobHash
		copyRecord.Content.BlobHash = &hash
	}
	if record.Content.CommitmentSalt != nil {
		salt := *record.Content.CommitmentSalt
		copyRecord.Content.CommitmentSalt = &salt
	}
	return copyRecord
}
