package ferricstore

import (
	"bufio"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGoFilesStayWithinLineLimits(t *testing.T) {
	const productionLimit = 675
	const testLimit = 800
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") {
			return nil
		}
		limit := productionLimit
		if strings.HasSuffix(name, "_test.go") {
			limit = testLimit
		} else if name == "response.go" {
			limit = 500
		} else if name == "topology_routing.go" {
			limit = 400
		} else if name == "native_codec_wire.go" {
			limit = 600
		}
		if lines := goFileLineCount(t, path); lines > limit {
			t.Errorf("%s has %d lines; limit is %d", filepath.Clean(path), lines, limit)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func goFileLineCount(t *testing.T, name string) int {
	t.Helper()
	file, err := os.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	scanner := bufio.NewScanner(file)
	lines := 0
	for scanner.Scan() {
		lines++
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return lines
}
