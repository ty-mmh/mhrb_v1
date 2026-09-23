package canonical

import "errors"

var (
	ErrInvalidID               = errors.New("canonical: invalid ID")
	ErrZeroID                  = errors.New("canonical: zero ID is not allowed")
	ErrULIDTimeOutOfRange      = errors.New("canonical: ULID timestamp is out of range")
	ErrULIDEntropyOverflow     = errors.New("canonical: monotonic ULID entropy overflow")
	ErrInvalidLogicalValue     = errors.New("canonical: invalid logical value")
	ErrInvalidTimezone         = errors.New("canonical: invalid IANA timezone")
	ErrInvalidCanonicalJSON    = errors.New("canonical: invalid canonical JSON")
	ErrDuplicateJSONKey        = errors.New("canonical: duplicate JSON object key")
	ErrJSONNumberForbidden     = errors.New("canonical: untyped JSON number is forbidden; use a Mahoroba logical integer wrapper")
	ErrInvalidDigest           = errors.New("canonical: invalid SHA-256 digest")
	ErrInvalidScope            = errors.New("canonical: invalid command scope")
	ErrWriterClosed            = errors.New("canonical: writer is closed")
	ErrWriterAdmissionPaused   = errors.New("canonical: writer admission is paused")
	ErrResidentAdmissionClosed = errors.New("canonical: resident admission is closed")
	ErrResidentActivityBusy    = errors.New("canonical: resident has competing Canonical activity")
	ErrInvalidActivityToken    = errors.New("canonical: current activity token is invalid")
	ErrWriterPoisoned          = errors.New("canonical: writer state is uncertain and requires restart")
	// ErrNoMutation is returned from inside a Canonical UoW when the exact
	// logical command was already applied durably. Writer rolls the provisional
	// transaction back (including its Canonical Commit row) and reports success.
	// Backends must perform authorization and payload equality checks before
	// returning this sentinel.
	ErrNoMutation        = errors.New("canonical: logical command already applied")
	ErrCommitSeqOverflow = errors.New("canonical: commit sequence overflow")
	ErrLedgerViolation   = errors.New("canonical: ledger invariant violation")
)
