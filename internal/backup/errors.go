// Package backup implements the offline M7 directory-bundle format.
package backup

import "errors"

var (
	ErrInvalidBundle       = errors.New("backup: invalid bundle")
	ErrTargetExists        = errors.New("backup: target exists")
	ErrSourceUnavailable   = errors.New("backup: source unavailable")
	ErrUnsafeOverlap       = errors.New("backup: source and output overlap")
	ErrArtifactIO          = errors.New("backup: artifact I/O failed")
	ErrDurabilityUnknown   = errors.New("backup: publication durability unknown")
	ErrUnsupportedSecurity = errors.New("backup: secure filesystem is unsupported")
)
