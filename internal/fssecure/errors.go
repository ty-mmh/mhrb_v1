package fssecure

import "errors"

var ErrUnsupportedSecureFilesystem = errors.New("fssecure: unsupported secure filesystem")

// ReasonCode is intentionally stable and machine-readable. It is kept
// internal to the filesystem boundary so callers do not need to parse paths
// or operating-system error strings.
type ReasonCode string

const (
	ReasonUnsupportedFilesystem      ReasonCode = "unsupported_filesystem"
	ReasonOwnerMismatch              ReasonCode = "owner_mismatch"
	ReasonUnsafeMode                 ReasonCode = "unsafe_mode"
	ReasonUnsafeACL                  ReasonCode = "unsafe_acl"
	ReasonDirectoryIdentityChanged   ReasonCode = "directory_identity_changed"
	ReasonFileIdentityChanged        ReasonCode = "file_identity_changed"
	ReasonReparseOrSymlink           ReasonCode = "reparse_or_symlink"
	ReasonPublishDestinationMismatch ReasonCode = "publish_destination_mismatch"
	ReasonQuarantineIdentityChanged  ReasonCode = "quarantine_identity_changed"
)

type reasonError struct {
	Code  ReasonCode
	Cause error
}

func (err *reasonError) Error() string { return string(err.Code) }
func (err *reasonError) Unwrap() error { return err.Cause }

func WithReason(code ReasonCode, cause error) error {
	if cause == nil {
		cause = ErrUnsafeFilesystem
	}
	return &reasonError{Code: code, Cause: cause}
}

func identityChanged(code ReasonCode, cause error) error {
	return WithReason(code, errors.Join(ErrUnsafeFilesystem, ErrIdentityChanged, cause))
}

func Reason(err error) (ReasonCode, bool) {
	var target *reasonError
	if errors.As(err, &target) {
		return target.Code, true
	}
	return "", false
}
