//go:build !linux

package secret

func loadFilePlatform(name Name, _ string) ([]byte, error) {
	return nil, namedError(ErrFileUnsupported, name)
}
