package ferricstore

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"testing"
)

func TestExamplesDoNotImportLogPackage(t *testing.T) {
	t.Helper()
	err := filepath.WalkDir("examples", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" {
			return nil
		}
		file, parseErr := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if parseErr != nil {
			t.Errorf("parse %s: %v", path, parseErr)
			return nil
		}
		for _, spec := range file.Imports {
			if spec.Path.Value == `"log"` {
				t.Errorf("%s imports log; examples must not print potentially sensitive runtime errors", path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
