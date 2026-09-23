package app

import (
	"errors"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/operationalmetrics"
)

type mandatoryWorkerObservationRecorder struct {
	failures []operationalmetrics.MandatoryWorkerFailure
}

func (*mandatoryWorkerObservationRecorder) ProviderFinished(
	operationalmetrics.ProviderTerminal,
	time.Duration,
) {
}

func (*mandatoryWorkerObservationRecorder) SetMandatoryWorkCounts(operationalmetrics.MandatoryWorkCounts) {
}

func (recorder *mandatoryWorkerObservationRecorder) MandatoryWorkerFailed(
	failure operationalmetrics.MandatoryWorkerFailure,
) {
	recorder.failures = append(recorder.failures, failure)
}

func TestCOV56PrePrepareMandatoryWorkerObservationsRedactIdentifiers(t *testing.T) {
	residentID, err := canonical.ParseID("01J00000000000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	sourceEventID, err := canonical.ParseID("01J00000000000000000000002")
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []operationalmetrics.MandatoryWorkerPhase{
		operationalmetrics.MandatoryWorkerPhaseProjectionReconcile,
		operationalmetrics.MandatoryWorkerPhaseDialogueAssembly,
		operationalmetrics.MandatoryWorkerPhaseDialoguePrepare,
	} {
		t.Run(string(phase), func(t *testing.T) {
			recorder := &mandatoryWorkerObservationRecorder{}
			application := &Application{operationalObserver: recorder}
			application.observeMandatoryWorkerFailure(
				residentID, sourceEventID, phase, errors.New("secret database message"), true,
			)
			if len(recorder.failures) != 1 {
				t.Fatalf("failures = %d, want 1", len(recorder.failures))
			}
			failure := recorder.failures[0]
			if failure.ResidentID != "" || failure.SourceEventID != "" ||
				failure.Phase != phase || failure.Class != operationalmetrics.MandatoryWorkerErrorInternalFailure ||
				!failure.RescanExpected {
				t.Fatalf("failure = %+v", failure)
			}
			if err := failure.Validate(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
