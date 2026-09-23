// Package durablepublish implements the M7 crash-visible publication barrier
// shared by directory and single-file artifact producers.
package durablepublish

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"mahoroba.local/mahoroba/internal/canonical"
)

const (
	MarkerFormat = "mahoroba-publish-marker-v1"
	MarkerName   = "PUBLISH_PENDING"

	CommandBackupCreate        = "backup.create"
	CommandBackupRestore       = "backup.restore"
	CommandExportJSONL         = "export.jsonl"
	CommandErasurePlanContent  = "admin.erasure.plan.content"
	CommandErasurePlanResident = "admin.erasure.plan.resident"
	CommandErasureDecide       = "admin.erasure.decide"

	producerInputDomainV1 = "mahoroba:publish-input:v1"
	directoryDigestDomain = "mahoroba:directory-payload:v1"
)

var producerCommands = map[string]Variant{
	CommandBackupCreate:        VariantDirectory,
	CommandBackupRestore:       VariantDirectory,
	CommandExportJSONL:         VariantSingleFile,
	CommandErasurePlanContent:  VariantSingleFile,
	CommandErasurePlanResident: VariantSingleFile,
	CommandErasureDecide:       VariantSingleFile,
}

type Variant string

const (
	VariantDirectory  Variant = "directory"
	VariantSingleFile Variant = "single_file"
)

var (
	ErrInvalidMarker        = errors.New("durable publish: invalid marker")
	ErrInvalidProducerInput = errors.New("durable publish: invalid producer input")
	ErrPayloadMismatch      = errors.New("durable publish: payload digest differs")
	ErrTargetExists         = errors.New("durable publish: target exists")
	ErrRecoveryMarkerAbsent = errors.New("durable publish: recovery marker is absent")
	ErrDurabilityUnknown    = errors.New("durable publish: durability is unknown")
	ErrUnsupportedBarrier   = errors.New("durable publish: required durability barrier is unsupported")
)

// Marker is the exact JCS envelope persisted both beside the target and, for
// directory artifacts, inside the staging/target root.
type Marker struct {
	FormatVersion       string       `json:"format_version"`
	PublishID           canonical.ID `json:"publish_id"`
	Variant             Variant      `json:"variant"`
	ProducerCommand     string       `json:"producer_command"`
	ProducerInputDigest string       `json:"producer_input_digest"`
	TargetBasename      string       `json:"target_basename"`
	StagingBasename     string       `json:"staging_basename"`
	PayloadSHA256       string       `json:"payload_sha256"`
}

func NewMarker(
	publishID canonical.ID,
	variant Variant,
	producerCommand string,
	producerInputDigest canonical.Digest,
	targetBasename, stagingBasename string,
	payloadSHA256 canonical.Digest,
) (Marker, error) {
	marker := Marker{
		FormatVersion:       MarkerFormat,
		PublishID:           publishID,
		Variant:             variant,
		ProducerCommand:     producerCommand,
		ProducerInputDigest: "sha256:" + producerInputDigest.Hex(),
		TargetBasename:      targetBasename,
		StagingBasename:     stagingBasename,
		PayloadSHA256:       "sha256:" + payloadSHA256.Hex(),
	}
	return marker, marker.Validate()
}

func (marker Marker) Validate() error {
	if marker.FormatVersion != MarkerFormat {
		return fmt.Errorf("%w: format version", ErrInvalidMarker)
	}
	if err := marker.PublishID.Validate(); err != nil {
		return fmt.Errorf("%w: publish ID: %v", ErrInvalidMarker, err)
	}
	if marker.Variant != VariantDirectory && marker.Variant != VariantSingleFile {
		return fmt.Errorf("%w: variant", ErrInvalidMarker)
	}
	wantVariant, ok := producerCommands[marker.ProducerCommand]
	if !ok || wantVariant != marker.Variant {
		return fmt.Errorf("%w: producer command", ErrInvalidMarker)
	}
	if !validDigest(marker.ProducerInputDigest) || !validDigest(marker.PayloadSHA256) {
		return fmt.Errorf("%w: digest", ErrInvalidMarker)
	}
	if !validBasename(marker.TargetBasename) || !validBasename(marker.StagingBasename) ||
		marker.TargetBasename == marker.StagingBasename {
		return fmt.Errorf("%w: artifact basename", ErrInvalidMarker)
	}
	return nil
}

func (marker Marker) Bytes() ([]byte, error) {
	if err := marker.Validate(); err != nil {
		return nil, err
	}
	encoded, err := canonical.MarshalCanonical(marker)
	if err != nil {
		return nil, err
	}
	return append(encoded.Bytes(), '\n'), nil
}

func ParseMarker(input []byte) (Marker, error) {
	var marker Marker
	if len(input) < 2 || input[len(input)-1] != '\n' || input[len(input)-2] == '\n' ||
		bytes.Contains(input[:len(input)-1], []byte{'\r'}) {
		return marker, fmt.Errorf("%w: marker must be one JCS object and LF", ErrInvalidMarker)
	}
	body := input[:len(input)-1]
	if _, err := canonical.ParseCanonicalJSON(body); err != nil {
		return marker, fmt.Errorf("%w: marker is not exact JCS: %v", ErrInvalidMarker, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&marker); err != nil {
		return Marker{}, fmt.Errorf("%w: marker schema: %v", ErrInvalidMarker, err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return Marker{}, fmt.Errorf("%w: trailing marker data", ErrInvalidMarker)
	}
	if err := marker.Validate(); err != nil {
		return Marker{}, err
	}
	return marker, nil
}

func producerInputDigest(input any) (canonical.Digest, error) {
	encoded, err := canonical.MarshalCanonical(input)
	if err != nil {
		return canonical.Digest{}, err
	}
	material := make([]byte, 0, len(producerInputDomainV1)+1+len(encoded.Bytes()))
	material = append(material, producerInputDomainV1...)
	material = append(material, 0)
	material = append(material, encoded.Bytes()...)
	return canonical.HashBlob(material), nil
}

func validDigest(value string) bool {
	if !strings.HasPrefix(value, "sha256:") {
		return false
	}
	_, err := canonical.ParseDigestHex(strings.TrimPrefix(value, "sha256:"))
	return err == nil
}

func validBasename(value string) bool {
	return value != "" && value != "." && value != ".." &&
		!strings.ContainsAny(value, "/\\\x00") && filepath.Base(value) == value
}
