package ferricstore

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const maxProductionGoFileLines = 525

func TestProductionGoFilesStayFocused(t *testing.T) {
	directory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	err = filepath.WalkDir(directory, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if entry.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		lines := bytes.Count(contents, []byte{'\n'})
		if len(contents) > 0 && contents[len(contents)-1] != '\n' {
			lines++
		}
		if lines > maxProductionGoFileLines {
			relative, relErr := filepath.Rel(directory, path)
			if relErr != nil {
				return relErr
			}
			t.Errorf("%s has %d lines; split production files above %d lines by responsibility", relative, lines, maxProductionGoFileLines)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
