package durablepublish

import "fmt"

// RecoveryAction is the only mutation a producer may perform after it has
// independently authenticated marker/input/namespace identities.
type RecoveryAction string

const (
	RecoveryStartNew          RecoveryAction = "start_new"
	RecoveryResumeSingleFile  RecoveryAction = "resume_single_file"
	RecoveryCompletePublished RecoveryAction = "complete_published"
	RecoveryRemoveStaleMarker RecoveryAction = "remove_stale_marker"
	RecoveryRepairRequired    RecoveryAction = "repair_static_design"
)

// RecoveryObservation is intentionally evidence-only. Filesystem consumers
// gather each boolean through retained handles; DecideRecovery never treats a
// pathname or an unauthenticated marker as authority.
type RecoveryObservation struct {
	Variant                Variant
	MarkerPresent          bool
	MarkerMatchesInput     bool
	NamespaceIdentityValid bool
	TargetPresent          bool
	StagingPresent         bool
	TargetPayloadMatches   bool
	StagingPayloadMatches  bool
}

// DecideRecovery implements the M7 section 8 closed crash-state machine. Directory
// staging is never resumed or deleted automatically.
func DecideRecovery(observed RecoveryObservation) (RecoveryAction, error) {
	if observed.Variant != VariantDirectory && observed.Variant != VariantSingleFile {
		return RecoveryRepairRequired, fmt.Errorf("%w: recovery variant", ErrInvalidMarker)
	}
	if !observed.NamespaceIdentityValid {
		return RecoveryRepairRequired, ErrDurabilityUnknown
	}
	if !observed.MarkerPresent {
		if !observed.TargetPresent && !observed.StagingPresent {
			return RecoveryStartNew, nil
		}
		return RecoveryRepairRequired, ErrDurabilityUnknown
	}
	if !observed.MarkerMatchesInput {
		return RecoveryRepairRequired, ErrDurabilityUnknown
	}
	if observed.TargetPresent {
		if observed.StagingPresent {
			return RecoveryRepairRequired, ErrDurabilityUnknown
		}
		if observed.TargetPayloadMatches {
			return RecoveryCompletePublished, nil
		}
		return RecoveryRepairRequired, ErrDurabilityUnknown
	}
	if !observed.StagingPresent {
		return RecoveryRemoveStaleMarker, nil
	}
	if !observed.StagingPayloadMatches {
		return RecoveryRepairRequired, ErrDurabilityUnknown
	}
	if observed.Variant == VariantSingleFile {
		return RecoveryResumeSingleFile, nil
	}
	return RecoveryRepairRequired, ErrDurabilityUnknown
}
