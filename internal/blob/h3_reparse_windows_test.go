//go:build windows

package blob

import (
	"os"
	"os/exec"
	"testing"

	"golang.org/x/sys/windows"
)

func createDirectoryReparse(t *testing.T, target, link string) {
	t.Helper()
	output, err := exec.Command(`C:\Windows\System32\cmd.exe`, "/d", "/c", "mklink", "/J", link, target).CombinedOutput()
	if err != nil {
		t.Fatalf("create junction: %v: %s", err, output)
	}
	info, err := os.Lstat(link)
	if err != nil {
		t.Fatalf("lstat created junction: %v", err)
	}
	attributes, err := windows.GetFileAttributes(windows.StringToUTF16Ptr(link))
	if err != nil || attributes&windows.FILE_ATTRIBUTE_REPARSE_POINT == 0 || info.Mode().IsRegular() {
		t.Fatalf("mklink did not create a reparse point: mode=%v attributes=%#x err=%v", info.Mode(), attributes, err)
	}
}
