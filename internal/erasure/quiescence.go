package erasure

import "mahoroba.local/mahoroba/internal/canonical"

var mandatoryPurposes = map[string]struct{}{
	"dialogue": {}, "memory_extraction": {}, "memory_alignment": {},
	"memory_abstraction": {}, "memory_differentiation": {},
}

type QuiescenceSnapshot struct {
	Activity                      canonical.ActivitySnapshot
	MandatoryDialogueWork         int
	MandatoryMemoryExtractionWork int
	RunningPurposes               []string
}

func (snapshot QuiescenceSnapshot) BranchQuiescentV1() bool {
	if snapshot.Activity.Total() != 0 || snapshot.MandatoryDialogueWork != 0 || snapshot.MandatoryMemoryExtractionWork != 0 {
		return false
	}
	for _, purpose := range snapshot.RunningPurposes {
		if _, mandatory := mandatoryPurposes[purpose]; mandatory {
			return false
		}
	}
	return true
}

func (snapshot QuiescenceSnapshot) ResidentEraseSafe() bool {
	return snapshot.BranchQuiescentV1() && len(snapshot.RunningPurposes) == 0
}
