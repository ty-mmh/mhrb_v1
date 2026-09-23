package integrity

import (
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
)

// HeadMetadata is the complete public identity of one durable Canonical head.
// canonical.Head remains the Writer cursor; this richer form is used only at
// offline/readiness boundaries that must report the exact commit and ledger
// timezone as well as the cursor.
type HeadMetadata struct {
	Head        canonical.Head
	CommitID    canonical.ID
	CommittedTZ canonical.Timezone
}

func (metadata HeadMetadata) Validate() error {
	if err := metadata.Head.Validate(); err != nil {
		return err
	}
	if !metadata.Head.Exists {
		if !metadata.CommitID.IsZero() || metadata.CommittedTZ != "" {
			return fmt.Errorf("integrity: empty head metadata contains commit identity")
		}
		return nil
	}
	if err := metadata.CommitID.Validate(); err != nil {
		return fmt.Errorf("integrity: invalid head commit ID: %w", err)
	}
	if err := metadata.CommittedTZ.Validate(); err != nil {
		return fmt.Errorf("integrity: invalid head timezone: %w", err)
	}
	return nil
}
