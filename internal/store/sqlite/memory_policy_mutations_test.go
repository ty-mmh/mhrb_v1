package sqlite

import (
	"strings"
	"testing"

	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/memory"
)

func TestCOV6MemoryPolicyV4TransitionContract(t *testing.T) {
	tests := []struct {
		name        string
		current     memory.Policy
		target      memory.Policy
		expected    memory.PolicyVersion
		ackRecall   bool
		ackSelfTalk bool
		wantError   string
	}{
		{
			name: "v1 requires both acknowledgements", current: memory.DefaultPolicyV1(),
			target: memory.DefaultPolicyV4(), expected: memory.PolicyVersionV1,
			wantError: "requires recall-enable and self-talk-extraction acknowledgements",
		},
		{
			name: "v1 to v4", current: memory.DefaultPolicyV1(), target: memory.DefaultPolicyV4(),
			expected: memory.PolicyVersionV1, ackRecall: true, ackSelfTalk: true,
		},
		{
			name: "v2 requires self-talk acknowledgement", current: memory.DefaultPolicyV2(),
			target: memory.DefaultPolicyV4(), expected: memory.PolicyVersionV2,
			wantError: "requires self-talk-extraction acknowledgement",
		},
		{
			name: "v2 to v4", current: memory.DefaultPolicyV2(), target: memory.DefaultPolicyV4(),
			expected: memory.PolicyVersionV2, ackSelfTalk: true,
		},
		{
			name: "v3 to v4", current: memory.DefaultPolicyV3(), target: memory.DefaultPolicyV4(),
			expected: memory.PolicyVersionV3,
		},
		{
			name: "v4 exact retry", current: memory.DefaultPolicyV4(), target: memory.DefaultPolicyV4(),
			expected: memory.PolicyVersionV4,
		},
		{
			name: "stale expected-from", current: memory.DefaultPolicyV3(), target: memory.DefaultPolicyV4(),
			expected: memory.PolicyVersionV2, wantError: "expected-from mismatch",
		},
		{
			name: "legacy v1 to v2 bypass", current: memory.DefaultPolicyV1(), target: memory.DefaultPolicyV2(),
			expected: memory.PolicyVersionV1, wantError: "use memory-policy-v4",
		},
		{
			name: "legacy v2 to v3 bypass", current: memory.DefaultPolicyV2(), target: memory.DefaultPolicyV3(),
			expected: memory.PolicyVersionV2, wantError: "use memory-policy-v4",
		},
		{
			name: "v4 downgrade", current: memory.DefaultPolicyV4(), target: memory.DefaultPolicyV3(),
			expected: memory.PolicyVersionV4, wantError: "use memory-policy-v4",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateMemoryPolicyActivationTransition(test.current, test.target, domain.ActivateMemoryPolicy{
				ExpectedFrom:                  test.expected,
				AcknowledgeRecallEnable:       test.ackRecall,
				AcknowledgeSelfTalkExtraction: test.ackSelfTalk,
			})
			if test.wantError == "" {
				if err != nil {
					t.Fatalf("transition rejected: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("transition error = %v, want containing %q", err, test.wantError)
			}
		})
	}
}
