//go:build !windows

package namespacelock

// PrepareHostedCITestTemp is available only on Windows, where Hosted tests
// require a current-token Known Folder authority.
func PrepareHostedCITestTemp(string) (string, error) {
	return "", ErrUnsupported
}

// PrepareCITestTemp is available only on Windows, where Hosted tests require
// a handle-created protected test namespace.
func PrepareCITestTemp(string, string) (string, error) {
	return "", ErrUnsupported
}

// ValidateCITestTemp is available only on Windows, where the existing root
// can be reopened and its exact protected DACL verified by handle.
func ValidateCITestTemp(string) error {
	return ErrUnsupported
}

// PrepareProtectedCITestChild is Windows-only.
func PrepareProtectedCITestChild(string, string) (string, error) {
	return "", ErrUnsupported
}

// ValidateProtectedCITestChild is Windows-only.
func ValidateProtectedCITestChild(string, string) error {
	return ErrUnsupported
}
