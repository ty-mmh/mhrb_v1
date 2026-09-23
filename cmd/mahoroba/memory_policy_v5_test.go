package main

import (
	"bytes"
	"context"
	"reflect"
	"strings"
	"testing"
)

func TestAdminMemoryPolicyV5RouteAndAcknowledgements(t *testing.T) {
	service := newAdminCLIService("fixed.toml", "fixed-data")
	argv, err := service.arguments("admin.memory.policy.activate-v5", map[string][]string{"resident": {"resident"}, "from": {"memory-policy-v1"}, "ack-enable-recall": {"true"}, "ack-self-talk-extraction": {"true"}})
	want := []string{"admin", "memory", "policy", "activate-v5", "--resident=resident", "--from=memory-policy-v1", "--ack-enable-recall=true", "--ack-self-talk-extraction=true", "--config=fixed.toml", "--data-dir=fixed-data"}
	if err != nil || !reflect.DeepEqual(argv, want) {
		t.Fatalf("argv=%v err=%v", argv, err)
	}
	var out, stderr bytes.Buffer
	err = runAdmin(context.Background(), []string{"memory", "policy", "activate-v5", "--resident=01ARZ3NDEKTSV4RRFFQ69G5FAV"}, &out, &stderr)
	if err == nil || !strings.Contains(err.Error(), "--from is required") {
		t.Fatalf("V5 route/preflight=%v", err)
	}
}
