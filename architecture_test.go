package framework

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestArchitecture_NoRawCommandPublishOutsideSendCommand(t *testing.T) {
	checks := []struct {
		path         string
		allowedFuncs map[string]bool
	}{
		{path: "device.go", allowedFuncs: map[string]bool{"SendCommand": true}},
		{path: "entity.go", allowedFuncs: map[string]bool{"SendCommand": true}},
		{path: filepath.Join("pkg", "script", "worker.go"), allowedFuncs: map[string]bool{}},
	}

	for _, tc := range checks {
		t.Run(tc.path, func(t *testing.T) {
			assertNoRawCommandPublishOutsideAllowedFuncs(t, tc.path, tc.allowedFuncs)
		})
	}
}

func assertNoRawCommandPublishOutsideAllowedFuncs(t *testing.T, path string, allowedFuncs map[string]bool) {
	t.Helper()

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, parser.AllErrors)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}

	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		currentFunc := fn.Name.Name
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			x, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := x.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel == nil || sel.Sel.Name != "Publish" {
				return true
			}
			if !containsCommandSubjectLiteral(x) {
				return true
			}
			if allowedFuncs[currentFunc] {
				return true
			}
			pos := fset.Position(x.Pos())
			t.Errorf("%s:%d raw Publish to *.command is not allowed outside SendCommand (func=%s)", pos.Filename, pos.Line, currentFunc)
			return true
		})
	}
}

func containsCommandSubjectLiteral(n ast.Node) bool {
	found := false
	ast.Inspect(n, func(n ast.Node) bool {
		if found {
			return false
		}
		lit, ok := n.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}
		s, err := parseStringLiteral(lit.Value)
		if err != nil {
			return true
		}
		if strings.Contains(s, ".command") {
			found = true
			return false
		}
		return true
	})
	return found
}

func parseStringLiteral(v string) (string, error) {
	if s, err := strconv.Unquote(v); err == nil {
		return s, nil
	}
	return v, nil
}
