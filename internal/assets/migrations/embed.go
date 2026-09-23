// Package migrations owns the immutable SQLite v0.1.3 migration baseline
// embedded in the Mahoroba binary.
package migrations

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"sort"
	"strings"
)

const (
	// BaselineVersion is the highest migration version in the v0.1.3 bundle.
	BaselineVersion int64 = 13
	// GooseTableName is deliberately fixed by the physical database design.
	GooseTableName = "schema_migrations"
	// ForwardOnlyMarker is embedded in a migration source so validators and
	// the runtime loader share the same machine-readable rollback policy.
	ForwardOnlyMarker = "-- mahoroba: migration-policy=forward-only"
)

// Files contains only the byte-for-byte copies of the reviewed migrations.
//
//go:embed *.sql
var Files embed.FS

// Descriptor fixes the identity and order of an embedded migration.
type Descriptor struct {
	Name        string
	Bytes       int
	SHA256      string
	ForwardOnly bool
}

var baseline = [...]Descriptor{
	{Name: "00001_canonical_core.sql", Bytes: 4310, SHA256: "49cc48766aed55a8cbb68065b6da4863fe9439dbf9b84af116c3229f2a163af2"},
	{Name: "00002_resident_revisions_and_lifecycle.sql", Bytes: 10074, SHA256: "03e0569b159b10bc50d203c1ab90a648b18e1a7513a43302d2d472c290f70a77"},
	{Name: "00003_content.sql", Bytes: 5993, SHA256: "22e2611559df447bcf082e89caa2adb0cf9acb963fd039e2a1f371c3f899a57b"},
	{Name: "00004_versions_generation_recall.sql", Bytes: 14031, SHA256: "41331dae69937eddb4c6a8941f44b17c718144415729d9861a3d85bacc54f90b"},
	{Name: "00005_events.sql", Bytes: 4451, SHA256: "cff06f32c5fa69953ca1a0e5b3570d85bb78cdd7e89eaa4333b0ac71f4bcc6b9"},
	{Name: "00006_memory.sql", Bytes: 33544, SHA256: "fd65615bcb8dcd43eb4749f446d87ccd7513de69f0dc295f8a8373fdd367a28c"},
	{Name: "00007_projections.sql", Bytes: 7065, SHA256: "d80d8bb4ac6dd8e884889243b9e2f08d5e5b074bf0571c8668ed0b00a7565a1e"},
	{Name: "00008_operational.sql", Bytes: 2752, SHA256: "10e61129aede704dbdfb33389575b14b8c2410ca6eafe27afa997c6c8557a829"},
	{Name: "00009_indexes.sql", Bytes: 8055, SHA256: "3b48569e9f41b604afd535015970f5c4e23a4469ee962dec114845f394377514"},
	{Name: "00010_immutability_triggers.sql", Bytes: 32103, SHA256: "32bcf99beb4d1a7d36ee7434bf08d38fc0559ebd9f3fc1bd80fd937dd3e9527b"},
	{Name: "00011_m7_slice0.sql", Bytes: 25959, SHA256: "ae7ee15a32de43097e366ad2160e47534abeb80a125c0e75b23faa2f7906cd3b", ForwardOnly: true},
	{Name: "00012_m7_integrity_foundation.sql", Bytes: 5381, SHA256: "5b9fc625b57ab0f6813e5e29b308d1b2f157f96fa146a64140cf7dac8361b239", ForwardOnly: true},
	{Name: "00013_covr02_progress_indexes.sql", Bytes: 761, SHA256: "500f906fadfe8c5a7bad43ab90896a886eede33dbdbdddf366cbf6ee929a3f9e", ForwardOnly: true},
}

// Descriptors returns a defensive copy of the ordered baseline manifest.
func Descriptors() []Descriptor {
	result := make([]Descriptor, len(baseline))
	copy(result, baseline[:])
	return result
}

// IsForwardOnlyVersion reports whether a migration version has an explicit
// no-rollback policy. Unknown versions are treated as reversible here; the
// caller still rejects versions newer than the supported baseline.
func IsForwardOnlyVersion(version int64) bool {
	for index, descriptor := range baseline {
		if int64(index+1) == version {
			return descriptor.ForwardOnly
		}
	}
	return false
}

// Validate checks the embedded file set, byte counts, hashes, and Goose markers.
// It is cheap enough to run before every migration attempt.
func Validate() error {
	names, err := fs.Glob(Files, "*.sql")
	if err != nil {
		return fmt.Errorf("glob embedded migrations: %w", err)
	}
	sort.Strings(names)
	if len(names) != len(baseline) {
		return fmt.Errorf("embedded migration count = %d, want %d", len(names), len(baseline))
	}
	for i, want := range baseline {
		if names[i] != want.Name {
			return fmt.Errorf("embedded migration %d = %q, want %q", i+1, names[i], want.Name)
		}
		body, readErr := Files.ReadFile(want.Name)
		if readErr != nil {
			return fmt.Errorf("read embedded migration %q: %w", want.Name, readErr)
		}
		if len(body) != want.Bytes {
			return fmt.Errorf("embedded migration %q has %d bytes, want %d", want.Name, len(body), want.Bytes)
		}
		digest := sha256.Sum256(body)
		if got := hex.EncodeToString(digest[:]); got != want.SHA256 {
			return fmt.Errorf("embedded migration %q sha256 = %s, want %s", want.Name, got, want.SHA256)
		}
		text := string(body)
		if strings.Count(text, "-- +goose Up") != 1 || strings.Count(text, "-- +goose Down") != 1 {
			return fmt.Errorf("embedded migration %q must contain exactly one Goose Up and Down marker", want.Name)
		}
		markedForwardOnly := strings.Contains(text, ForwardOnlyMarker)
		if markedForwardOnly != want.ForwardOnly {
			return fmt.Errorf("embedded migration %q forward-only marker = %v, want %v", want.Name, markedForwardOnly, want.ForwardOnly)
		}
	}
	return nil
}
