package canonical_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// TestCanonicalCommandImplementationsCannotImportExternalIO is the CI/static
// half of RT-I-2. Runtime commands may only delegate to the narrow UoW
// capability they receive; a package that implements canonical.Command must
// not also have production access to network, process-exec, plugin, or unsafe
// escape hatches.
func TestCanonicalCommandImplementationsCannotImportExternalIO(t *testing.T) {
	t.Parallel()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot resolve architecture test path")
	}
	internalRoot := filepath.Clean(filepath.Join(filepath.Dir(currentFile), ".."))

	type receiver struct {
		packageDir string
		name       string
	}
	methods := make(map[receiver]map[string]struct{})
	packageImports := make(map[string]map[string]struct{})
	fset := token.NewFileSet()
	err := filepath.WalkDir(internalRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			return nil
		}
		parsed, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		packageDir := filepath.Dir(path)
		if packageImports[packageDir] == nil {
			packageImports[packageDir] = make(map[string]struct{})
		}
		for _, imported := range parsed.Imports {
			value, err := strconv.Unquote(imported.Path.Value)
			if err != nil {
				return err
			}
			packageImports[packageDir][value] = struct{}{}
		}
		for _, declaration := range parsed.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Recv == nil || len(function.Recv.List) != 1 {
				continue
			}
			name := receiverName(function.Recv.List[0].Type)
			if name == "" {
				continue
			}
			key := receiver{packageDir: packageDir, name: name}
			if methods[key] == nil {
				methods[key] = make(map[string]struct{})
			}
			methods[key][function.Name.Name] = struct{}{}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	commandMethods := []string{"Name", "Scope", "Validate", "Execute"}
	for candidate, present := range methods {
		implementsCommand := true
		for _, method := range commandMethods {
			if _, ok := present[method]; !ok {
				implementsCommand = false
				break
			}
		}
		if !implementsCommand {
			continue
		}
		for imported := range packageImports[candidate.packageDir] {
			if forbiddenCommandImport(imported) {
				t.Errorf("production canonical.Command %s in %s has forbidden external-I/O import %q",
					candidate.name, candidate.packageDir, imported)
			}
		}
	}
}

func receiverName(expression ast.Expr) string {
	switch value := expression.(type) {
	case *ast.Ident:
		return value.Name
	case *ast.StarExpr:
		return receiverName(value.X)
	default:
		return ""
	}
}

func forbiddenCommandImport(path string) bool {
	return path == "net" || strings.HasPrefix(path, "net/") ||
		path == "os/exec" || path == "plugin" || path == "syscall" || path == "unsafe" ||
		strings.HasSuffix(path, "/internal/generation/chatcompletions")
}
