package healthwire

import (
	"bytes"
	"testing"

	"mahoroba.local/mahoroba/internal/readiness"
)

func TestM7HealthWireExactGoldenAndStrictDecoder(t *testing.T) {
	ready := Response{
		CheckedAtUnixMicros: "1787220000000000",
		FormatVersion:       FormatVersion,
		Ready:               true,
		ReasonCode:          readiness.ReasonReady,
	}
	want := []byte(`{"checked_at_unix_micros":"1787220000000000","format_version":"mahoroba-health-v1","ready":true,"reason_code":"ready"}` + "\n")
	got, err := MarshalExact(ready)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("golden mismatch\n got: %s\nwant: %s", got, want)
	}
	if parsed, err := ParseExact(got); err != nil || parsed != ready {
		t.Fatalf("round trip = %+v, %v", parsed, err)
	}

	invalid := [][]byte{
		[]byte(`{"checked_at_unix_micros":"1787220000000000","format_version":"mahoroba-health-v1","ready":true,"reason_code":"ready","unknown":null}` + "\n"),
		[]byte(`{"checked_at_unix_micros":"1787220000000000","checked_at_unix_micros":"1787220000000000","format_version":"mahoroba-health-v1","ready":true,"reason_code":"ready"}` + "\n"),
		[]byte(`{"format_version":"mahoroba-health-v1","checked_at_unix_micros":"1787220000000000","ready":true,"reason_code":"ready"}` + "\n"),
		[]byte(`{"checked_at_unix_micros":"1787220000000000","format_version":"mahoroba-health-v1","ready":false,"reason_code":"ready"}` + "\n"),
		[]byte(`{"checked_at_unix_micros":"01787220000000000","format_version":"mahoroba-health-v1","ready":true,"reason_code":"ready"}` + "\n"),
		append(append([]byte(nil), want...), '\n'),
	}
	for index, candidate := range invalid {
		if _, err := ParseExact(candidate); err == nil {
			t.Fatalf("invalid[%d] accepted: %q", index, candidate)
		}
	}
}

func TestM7HealthWireUsesFirstClosedReadinessReason(t *testing.T) {
	response, err := New(readiness.Result{
		Ready: false,
		ReasonCodes: []readiness.ReasonCode{
			readiness.ReasonIntegrityBlocked,
			readiness.ReasonShutdown,
		},
	}, "1")
	if err != nil {
		t.Fatal(err)
	}
	if response.ReasonCode != readiness.ReasonIntegrityBlocked {
		t.Fatalf("reason = %q", response.ReasonCode)
	}
}
