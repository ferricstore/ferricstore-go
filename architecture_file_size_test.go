package ferricstore

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const maxProductionGoFileLines = 650

func TestProductionGoFilesStayFocused(t *testing.T) {
	directory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		contents, err := os.ReadFile(filepath.Join(directory, name))
		if err != nil {
			t.Errorf("read %s: %v", name, err)
			continue
		}
		lines := bytes.Count(contents, []byte{'\n'})
		if len(contents) > 0 && contents[len(contents)-1] != '\n' {
			lines++
		}
		if lines > maxProductionGoFileLines {
			t.Errorf("%s has %d lines; split production files above %d lines by responsibility", name, lines, maxProductionGoFileLines)
		}
	}
}
