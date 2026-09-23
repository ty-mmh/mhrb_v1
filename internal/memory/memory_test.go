package memory

import (
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
)

func mustID(t *testing.T, raw string) canonical.ID {
	t.Helper()
	value, err := canonical.ParseID(raw)
	if err != nil {
		t.Fatalf("ParseID(%q): %v", raw, err)
	}
	return value
}

func testID(t *testing.T, index int) canonical.ID {
	t.Helper()
	values := []string{
		"01ARZ3NDEKTSV4RRFFQ69G5FAV",
		"01ARZ3NDEKTSV4RRFFQ69G5FAW",
		"01ARZ3NDEKTSV4RRFFQ69G5FAX",
		"01ARZ3NDEKTSV4RRFFQ69G5FAY",
		"01ARZ3NDEKTSV4RRFFQ69G5FAZ",
		"01ARZ3NDEKTSV4RRFFQ69G5FB0",
		"01ARZ3NDEKTSV4RRFFQ69G5FB1",
		"01ARZ3NDEKTSV4RRFFQ69G5FB2",
		"01ARZ3NDEKTSV4RRFFQ69G5FB3",
		"01ARZ3NDEKTSV4RRFFQ69G5FB4",
		"01ARZ3NDEKTSV4RRFFQ69G5FB5",
		"01ARZ3NDEKTSV4RRFFQ69G5FB6",
	}
	if index < 0 || index >= len(values) {
		t.Fatalf("test ID index %d is out of range", index)
	}
	return mustID(t, values[index])
}

func mustRatio(t *testing.T, value int64) canonical.Ratio {
	t.Helper()
	result, err := canonical.NewRatio(value)
	if err != nil {
		t.Fatalf("NewRatio(%d): %v", value, err)
	}
	return result
}

func mustWeight(t *testing.T, value int64) canonical.Weight {
	t.Helper()
	result, err := canonical.NewWeight(value)
	if err != nil {
		t.Fatalf("NewWeight(%d): %v", value, err)
	}
	return result
}

func mustCount(t *testing.T, value int64) canonical.Count {
	t.Helper()
	result, err := canonical.NewCount(value)
	if err != nil {
		t.Fatalf("NewCount(%d): %v", value, err)
	}
	return result
}

func mustByteSize(t *testing.T, value int64) canonical.ByteSize {
	t.Helper()
	result, err := canonical.NewByteSize(value)
	if err != nil {
		t.Fatalf("NewByteSize(%d): %v", value, err)
	}
	return result
}
