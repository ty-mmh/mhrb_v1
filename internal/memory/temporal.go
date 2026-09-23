package memory

import (
	"fmt"
	"math/big"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
)

type TemporalInput struct {
	Kind      TemporalKind
	AnchorAt  canonical.Instant
	ValidFrom *canonical.Instant
	ValidTo   *canonical.Instant
	AsOf      canonical.Instant
}

type TemporalState struct {
	Relation    TemporalRelation
	Currentness canonical.Ratio
}

// EvaluateTemporal uses inclusive valid_from and exclusive valid_to. Absent
// explicit bounds, volatile claims become stale_unknown after 30 completed
// days and episodic claims become past after 24 completed hours.
func EvaluateTemporal(policy Policy, input TemporalInput) (TemporalState, error) {
	if err := policy.RequireEnabled(); err != nil {
		return TemporalState{}, err
	}
	if err := input.Kind.Validate(); err != nil {
		return TemporalState{}, fmt.Errorf("%w: %v", ErrInvalidTemporal, err)
	}
	if input.ValidFrom != nil && input.ValidTo != nil && *input.ValidFrom > *input.ValidTo {
		return TemporalState{}, fmt.Errorf("%w: valid_from is after valid_to", ErrInvalidTemporal)
	}

	relation := RelationCurrent
	switch {
	case input.ValidFrom != nil && input.AsOf < *input.ValidFrom:
		relation = RelationFuture
	case input.AsOf < input.AnchorAt:
		relation = RelationFuture
	case input.ValidTo != nil && input.AsOf >= *input.ValidTo:
		relation = RelationPast
	default:
		elapsed, err := instantDifference(input.AsOf, input.AnchorAt)
		if err != nil {
			return TemporalState{}, fmt.Errorf("%w: %v", ErrInvalidTemporal, err)
		}
		switch input.Kind {
		case TemporalStable:
			relation = RelationCurrent
		case TemporalVolatile:
			threshold, err := durationMicroseconds(policy.Temporal.VolatileStaleDays, 24*time.Hour)
			if err != nil || threshold <= 0 {
				return TemporalState{}, fmt.Errorf("%w: invalid volatile threshold", ErrInvalidTemporal)
			}
			if elapsed >= threshold {
				relation = RelationStaleUnknown
			}
		case TemporalEpisodic:
			threshold, err := durationMicroseconds(policy.Temporal.EpisodicCurrentHours, time.Hour)
			if err != nil || threshold <= 0 {
				return TemporalState{}, fmt.Errorf("%w: invalid episodic threshold", ErrInvalidTemporal)
			}
			if elapsed >= threshold {
				relation = RelationPast
			}
		}
	}
	currentness, err := CurrentnessForRelation(policy, relation)
	if err != nil {
		return TemporalState{}, err
	}
	return TemporalState{Relation: relation, Currentness: currentness}, nil
}

func CurrentnessForRelation(policy Policy, relation TemporalRelation) (canonical.Ratio, error) {
	if err := policy.RequireEnabled(); err != nil {
		return 0, err
	}
	if err := relation.Validate(); err != nil {
		return 0, fmt.Errorf("%w: %v", ErrInvalidTemporal, err)
	}
	var value int64
	switch relation {
	case RelationFuture:
		value = policy.Temporal.FutureCurrentness
	case RelationCurrent:
		value = policy.Temporal.CurrentCurrentness
	case RelationPast:
		value = policy.Temporal.PastCurrentness
	case RelationStaleUnknown:
		value = policy.Temporal.StaleCurrentness
	}
	ratio, err := canonical.NewRatio(value)
	if err != nil {
		return 0, fmt.Errorf("%w: policy currentness: %v", ErrInvalidTemporal, err)
	}
	return ratio, nil
}

func instantDifference(newer, older canonical.Instant) (int64, error) {
	if newer < older {
		return 0, fmt.Errorf("newer instant precedes older instant")
	}
	difference := new(big.Int).Sub(big.NewInt(int64(newer)), big.NewInt(int64(older)))
	if !difference.IsInt64() {
		return 0, fmt.Errorf("instant difference overflows int64")
	}
	return difference.Int64(), nil
}
