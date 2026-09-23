package app

import (
	"context"
	"errors"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/operationalmetrics"
)

// mandatoryWorkerError carries only closed routing metadata alongside the
// original error. The cause is returned to synchronous callers unchanged;
// only the closed fields cross the asynchronous OperationalObserver boundary.
type mandatoryWorkerError struct {
	residentID    canonical.ID
	sourceEventID canonical.ID
	phase         operationalmetrics.MandatoryWorkerPhase
	class         operationalmetrics.MandatoryWorkerErrorClass
	cause         error
}

func (workerErr *mandatoryWorkerError) Error() string { return workerErr.cause.Error() }
func (workerErr *mandatoryWorkerError) Unwrap() error { return workerErr.cause }

func markMandatoryWorkerError(
	err error,
	residentID, sourceEventID canonical.ID,
	phase operationalmetrics.MandatoryWorkerPhase,
	class operationalmetrics.MandatoryWorkerErrorClass,
) error {
	if err == nil {
		return nil
	}
	var existing *mandatoryWorkerError
	if errors.As(err, &existing) {
		return err
	}
	if class == "" {
		class = classifyMandatoryWorkerError(err)
	}
	return &mandatoryWorkerError{
		residentID: residentID, sourceEventID: sourceEventID,
		phase: phase, class: class, cause: err,
	}
}

func classifyMandatoryWorkerError(err error) operationalmetrics.MandatoryWorkerErrorClass {
	switch {
	case errors.Is(err, context.Canceled):
		return operationalmetrics.MandatoryWorkerErrorContextCancelled
	case errors.Is(err, context.DeadlineExceeded):
		return operationalmetrics.MandatoryWorkerErrorDeadlineExceeded
	case errors.Is(err, domain.ErrDialogueAssemblyTargetChanged):
		return operationalmetrics.MandatoryWorkerErrorAssemblyTargetChanged
	case errors.Is(err, canonical.ErrLedgerViolation), errors.Is(err, ErrGenerationEnvelopeUnsupported):
		return operationalmetrics.MandatoryWorkerErrorIntegrityRejected
	case errors.Is(err, canonical.ErrWriterClosed),
		errors.Is(err, canonical.ErrWriterAdmissionPaused),
		errors.Is(err, canonical.ErrWriterPoisoned):
		return operationalmetrics.MandatoryWorkerErrorWriterUnavailable
	default:
		return operationalmetrics.MandatoryWorkerErrorInternalFailure
	}
}

func (a *Application) observeMandatoryWorkerFailure(
	residentID, sourceEventID canonical.ID,
	phase operationalmetrics.MandatoryWorkerPhase,
	err error,
	rescanExpected bool,
) {
	if a == nil || a.operationalObserver == nil || err == nil {
		return
	}
	class := classifyMandatoryWorkerError(err)
	var workerErr *mandatoryWorkerError
	if errors.As(err, &workerErr) {
		if !workerErr.residentID.IsZero() {
			residentID = workerErr.residentID
		}
		if !workerErr.sourceEventID.IsZero() {
			sourceEventID = workerErr.sourceEventID
		}
		phase = workerErr.phase
		class = workerErr.class
	}
	if phase == "" {
		return
	}
	redactIdentifiers := phase == operationalmetrics.MandatoryWorkerPhaseProjectionReconcile ||
		phase == operationalmetrics.MandatoryWorkerPhaseDialogueAssembly ||
		phase == operationalmetrics.MandatoryWorkerPhaseDialoguePrepare
	if redactIdentifiers {
		residentID = canonical.ID{}
		sourceEventID = canonical.ID{}
	} else if residentID.IsZero() {
		return
	}
	failure := operationalmetrics.MandatoryWorkerFailure{
		Phase: phase, Class: class, RescanExpected: rescanExpected,
	}
	if !residentID.IsZero() {
		failure.ResidentID = residentID.String()
	}
	if !sourceEventID.IsZero() {
		failure.SourceEventID = sourceEventID.String()
	}
	a.operationalObserver.MandatoryWorkerFailed(failure)
}
