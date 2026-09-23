package domain

import (
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
)

func replacementIntentCommandFixture(t *testing.T) CreateMemoryReplacementIntent {
	t.Helper()
	return CreateMemoryReplacementIntent{
		ResidentID: claimCommandID(t, 101), NewClaimID: claimCommandID(t, 102),
		OldClaimID: claimCommandID(t, 103), RelationID: claimCommandID(t, 104),
		OwnerPrincipalID: claimCommandID(t, 105),
	}
}

func TestCreateMemoryReplacementIntentCommandRequiresDistinctTypedIdentities(t *testing.T) {
	valid := replacementIntentCommandFixture(t)
	if err := CreateMemoryReplacementIntentCommand(valid).Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*CreateMemoryReplacementIntent){
		"missing owner": func(value *CreateMemoryReplacementIntent) { value.OwnerPrincipalID = canonical.ID{} },
		"same claim":    func(value *CreateMemoryReplacementIntent) { value.OldClaimID = value.NewClaimID },
		"relation is new claim": func(value *CreateMemoryReplacementIntent) {
			value.RelationID = value.NewClaimID
		},
	} {
		t.Run(name, func(t *testing.T) {
			value := valid
			mutate(&value)
			if err := CreateMemoryReplacementIntentCommand(value).Validate(); err == nil {
				t.Fatal("invalid replacement intent command was accepted")
			}
		})
	}
}
