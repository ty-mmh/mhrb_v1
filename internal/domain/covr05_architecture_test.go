package domain

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestCOVR05ProductionSurface keeps the current dialogue path from silently
// regaining the removed generic aliases, combined ingress command, or simple
// Application assembler. Struct fields carrying persisted version names are
// intentionally allowed; only declarations and the removed method surface are
// architectural violations.
func TestCOVR05ProductionSurface(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	forbidden := map[string]bool{
		"DialoguePipelineVersion": true, "ContextPolicyVersion": true,
		"MemoryRenderingVersion": true, "IngressDialogue": true,
		"IngressDialogueCommand": true, "IngressAndPrepareDialogue": true,
	}
	set := token.NewFileSet()
	err := filepath.Walk(root, func(path string, info fs.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() {
			// The repository may contain process-local scratch directories from
			// other tests or tools. They are not production source and can be
			// inaccessible under the managed workspace policy, so keep the
			// architectural scan on visible repository directories only.
			if strings.HasPrefix(info.Name(), ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" {
			return nil
		}
		if strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fileAST, err := parser.ParseFile(set, path, nil, 0)
		if err != nil {
			return err
		}
		for _, declaration := range fileAST.Decls {
			switch declaration := declaration.(type) {
			case *ast.GenDecl:
				for _, spec := range declaration.Specs {
					for _, name := range declaredNames(spec) {
						if forbidden[name] {
							t.Errorf("removed COVR-05 declaration %q remains in %s", name, path)
						}
					}
				}
			case *ast.FuncDecl:
				if declaration.Name.Name == "assemble" && declaration.Recv != nil && strings.Contains(exprString(declaration.Recv.List[0].Type), "Application") {
					t.Errorf("removed Application.assemble remains in %s", path)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func declaredNames(spec ast.Spec) []string {
	switch spec := spec.(type) {
	case *ast.TypeSpec:
		return []string{spec.Name.Name}
	case *ast.ValueSpec:
		result := make([]string, 0, len(spec.Names))
		for _, name := range spec.Names {
			result = append(result, name.Name)
		}
		return result
	case *ast.ImportSpec:
		return nil
	default:
		return nil
	}
}

func exprString(expr ast.Expr) string {
	switch expr := expr.(type) {
	case *ast.Ident:
		return expr.Name
	case *ast.StarExpr:
		return "*" + exprString(expr.X)
	case *ast.SelectorExpr:
		return exprString(expr.X) + "." + expr.Sel.Name
	default:
		return ""
	}
}
