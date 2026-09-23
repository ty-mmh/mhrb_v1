package fssecure

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestM7FSsecureStaticAuthorityBoundary(t *testing.T) {
	directory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	forbiddenOS := map[string]bool{
		"Lstat": true, "Open": true, "OpenFile": true, "CreateTemp": true,
		"Mkdir": true, "MkdirAll": true, "Rename": true, "Remove": true,
		"RemoveAll": true, "ReadDir": true,
	}
	bootstrapCalls := map[string]int{"unix.Open": 0, "windows.CreateFile": 0}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join(directory, name)
		source, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, legacy := range []string{"identityFromPath", "validateSecurity(", "mkdirSecure", "createTempSecure", "openDirectoryNoFollow", "removeVerified", "OpenNoFollow", "legacyFile", "legacyParent", "AT_FDCWD"} {
			if strings.Contains(string(source), legacy) {
				t.Errorf("%s retains legacy authority helper %q", name, legacy)
			}
		}
		fileset := token.NewFileSet()
		parsed, err := parser.ParseFile(fileset, path, source, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			pkg, ok := selector.X.(*ast.Ident)
			if !ok {
				return true
			}
			function := enclosingFunction(parsed, call.Pos())
			if pkg.Name == "os" && forbiddenOS[selector.Sel.Name] {
				t.Errorf("%s:%d uses path authority os.%s in %s", name, fileset.Position(call.Pos()).Line, selector.Sel.Name, function)
			}
			key := pkg.Name + "." + selector.Sel.Name
			switch key {
			case "unix.Open":
				bootstrapCalls[key]++
				if name != "relative_linux.go" || function != "openRootRelative" {
					t.Errorf("%s:%d has non-allowlisted unix.Open in %s", name, fileset.Position(call.Pos()).Line, function)
				}
			case "windows.CreateFile":
				bootstrapCalls[key]++
				if name != "relative_windows.go" || function != "openAbsoluteDirectoryRaw" {
					t.Errorf("%s:%d has non-allowlisted windows.CreateFile in %s", name, fileset.Position(call.Pos()).Line, function)
				}
			}
			return true
		})
	}
	for primitive, count := range bootstrapCalls {
		if count != 1 {
			t.Errorf("bootstrap primitive %s count = %d, want exactly 1", primitive, count)
		}
	}
}

func enclosingFunction(file *ast.File, position token.Pos) string {
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if ok && function.Pos() <= position && position <= function.End() {
			return function.Name.Name
		}
	}
	return "<package>"
}
