package sqlite

import (
	"io/fs"
	"testing"
	"testing/fstest"

	"mahoroba.local/mahoroba/internal/assets/migrations"
)

// migrationFilesThrough creates a test-only forward migration view. It is
// intentionally incapable of using a Down migration to manufacture an older
// schema fixture.
func migrationFilesThrough(t *testing.T, version int64) fs.FS {
	t.Helper()
	files := make(fstest.MapFS)
	for index, descriptor := range migrations.Descriptors() {
		if int64(index+1) > version {
			break
		}
		body, err := migrations.Files.ReadFile(descriptor.Name)
		if err != nil {
			t.Fatal(err)
		}
		files[descriptor.Name] = &fstest.MapFile{Data: body}
	}
	return files
}
