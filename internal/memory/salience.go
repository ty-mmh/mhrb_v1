package memory

import (
	"fmt"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
)

type SaliencePoint struct {
	Contribution canonical.Weight
	RecordedAt   canonical.Instant
}

type UsageRecord struct {
	Type       UsageType
	RecordedAt canonical.Instant
}

// SalienceContribution is the pointwise half of claim-state replay. Candidate
// deliberately contributes zero and therefore cannot self-reinforce memory.
func SalienceContribution(policy Policy, usage UsageType) (canonical.Weight, error) {
	if err := policy.RequireEnabled(); err != nil {
		return 0, err
	}
	if err := usage.Validate(); err != nil {
		return 0, fmt.Errorf("%w: %v", ErrInvalidSalience, err)
	}
	var value int64
	switch usage {
	case UsageCandidate:
		value = policy.Salience.CandidateContribution
	case UsageSelected:
		value = policy.Salience.SelectedContribution
	case UsagePromptIncluded:
		value = policy.Salience.PromptIncludedContribution
	case UsageExplicitlyReferenced:
		value = policy.Salience.ExplicitlyReferencedContribution
	}
	weight, err := canonical.NewWeight(value)
	if err != nil {
		return 0, fmt.Errorf("%w: contribution: %v", ErrInvalidSalience, err)
	}
	return weight, nil
}

// AggregateSalience applies one integer halving for every completed policy
// half-life interval and caps the sum. No floating-point value participates in
// a Canonical decision.
func AggregateSalience(policy Policy, points []SaliencePoint, asOf canonical.Instant) (canonical.Ratio, error) {
	if err := policy.RequireEnabled(); err != nil {
		return 0, err
	}
	halfLife, err := durationMicroseconds(policy.Salience.HalfLifeDays, 24*time.Hour)
	if err != nil || halfLife <= 0 {
		return 0, fmt.Errorf("%w: invalid half life", ErrInvalidSalience)
	}
	capValue := policy.Salience.Cap
	if capValue < 0 || capValue > fixedPointScale {
		return 0, fmt.Errorf("%w: invalid cap %d", ErrInvalidSalience, capValue)
	}
	total := int64(0)
	for _, point := range points {
		if err := point.Contribution.Validate(); err != nil {
			return 0, fmt.Errorf("%w: contribution: %v", ErrInvalidSalience, err)
		}
		if point.RecordedAt > asOf {
			return 0, fmt.Errorf("%w: usage occurs after as_of", ErrInvalidSalience)
		}
		elapsed := int64(asOf) - int64(point.RecordedAt)
		intervals := elapsed / halfLife
		value := point.Contribution.Millionths()
		if intervals >= 63 {
			value = 0
		} else {
			value >>= uint(intervals)
		}
		total, err = checkedAdd(total, value)
		if err != nil {
			return 0, fmt.Errorf("%w: contribution sum: %v", ErrInvalidSalience, err)
		}
		if total >= capValue {
			total = capValue
			break
		}
	}
	return canonical.NewRatio(total)
}

func CalculateSalience(policy Policy, usages []UsageRecord, asOf canonical.Instant) (canonical.Ratio, error) {
	points := make([]SaliencePoint, 0, len(usages))
	for _, usage := range usages {
		contribution, err := SalienceContribution(policy, usage.Type)
		if err != nil {
			return 0, err
		}
		points = append(points, SaliencePoint{Contribution: contribution, RecordedAt: usage.RecordedAt})
	}
	return AggregateSalience(policy, points, asOf)
}

func durationMicroseconds(multiplier int64, unit time.Duration) (int64, error) {
	if multiplier < 0 || unit < 0 {
		return 0, fmt.Errorf("memory: negative duration")
	}
	return multiplyDivideFloor(multiplier, unit.Microseconds(), 1)
}
