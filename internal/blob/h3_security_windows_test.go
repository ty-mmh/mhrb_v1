//go:build windows

package blob

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"golang.org/x/sys/windows"
)

func makeManagedRootUnsafe(t *testing.T, path string) {
	t.Helper()
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := windows.CreateFile(
		name,
		windows.READ_CONTROL|windows.WRITE_DAC,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		t.Fatalf("open managed root with WRITE_DAC: %v", err)
	}
	defer windows.CloseHandle(handle)
	descriptor, err := windows.GetSecurityInfo(handle, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		t.Fatal(err)
	}
	dacl, _, err := descriptor.DACL()
	if err != nil || dacl == nil {
		t.Fatalf("read managed DACL: %v", err)
	}
	everyone, err := windows.StringToSid("S-1-1-0")
	if err != nil {
		t.Fatal(err)
	}
	var pinner runtime.Pinner
	pinner.Pin(everyone)
	defer pinner.Unpin()
	unsafeACL, err := windows.ACLFromEntries([]windows.EXPLICIT_ACCESS{{
		AccessPermissions: windows.FILE_GENERIC_READ,
		AccessMode:        windows.GRANT_ACCESS,
		Inheritance:       windows.NO_INHERITANCE,
		Trustee: windows.TRUSTEE{
			TrusteeForm:  windows.TRUSTEE_IS_SID,
			TrusteeType:  windows.TRUSTEE_IS_WELL_KNOWN_GROUP,
			TrusteeValue: windows.TrusteeValueFromSID(everyone),
		},
	}}, dacl)
	if err != nil {
		t.Fatal(err)
	}
	if err := windows.SetSecurityInfo(handle, windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION,
		nil, nil, unsafeACL, nil); err != nil {
		t.Fatalf("mutate live managed DACL: %v", err)
	}
}

func TestM7FileStoreHandleBlocksWindowsWriteDeleteSharing(t *testing.T) {
	store := newTestStore(t)
	layout, err := store.openLayout()
	if err != nil {
		t.Fatal(err)
	}
	displacedDirectory := filepath.Join(store.root, "staging-displaced")
	if err := os.Rename(store.stagingDir, displacedDirectory); err == nil {
		_ = layout.Close()
		t.Fatal("managed directory handle allowed rename/delete sharing")
	}
	if err := layout.Close(); err != nil {
		t.Fatal(err)
	}

	residentID := testResident(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	content := []byte("sharing contract")
	staged, err := store.StageBytes(t.Context(), residentID, content)
	if err != nil {
		t.Fatal(err)
	}
	layout, err = store.openLayout()
	if err != nil {
		t.Fatal(err)
	}
	defer layout.Close()
	handle, err := layout.staging.OpenRegular(staged.name)
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	path := filepath.Join(store.stagingDir, staged.name)
	if err := os.WriteFile(path, []byte("hostile write"), 0o600); err == nil {
		t.Fatal("managed regular handle allowed external write sharing")
	}
	if err := os.Remove(path); err == nil {
		t.Fatal("managed regular handle allowed external delete sharing")
	}
	if err := os.Rename(path, path+"-renamed"); err == nil {
		t.Fatal("managed regular handle allowed external rename sharing")
	}
	if err := handle.VerifyBound(); err != nil {
		t.Fatalf("blocked sharing changed handle binding: %v", err)
	}
	if _, err := handle.File().Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(content))
	if _, err := handle.File().Read(got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("blocked sharing changed bytes: %q", got)
	}
}
