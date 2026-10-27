package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestIndexCannotHidePrivateOrOversizedStagedContent(t *testing.T) {
	for _, test := range []struct {
		name string
		data string
	}{
		{"docs/private.md", "private"},
		{"review/upstream.json", "{}"},
		{"capture.JSON", "{}"},
		{"nested/.env.local", "TOKEN=fixture"},
		{"large.txt", strings.Repeat("x", maxSourceBytes+1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			if _, err := git(root, "init", "-q"); err != nil {
				t.Fatal(err)
			}
			name := filepath.Join(root, test.name)
			if err := os.MkdirAll(filepath.Dir(name), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(name, []byte(test.data), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := git(root, "add", "--", test.name); err != nil {
				t.Fatal(err)
			}
			// A harmless worktree replacement must not hide a disallowed staged blob.
			if err := os.WriteFile(name, []byte("small"), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := checkIndex(root); err == nil {
				t.Fatal("accepted disallowed staged content")
			}
		})
	}
}

func TestIndexAcceptsSourceWithoutPrivateBook(t *testing.T) {
	root := t.TempDir()
	if _, err := git(root, "init", "-q"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "main.go"), []byte("package main\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := git(root, "add", "main.go"); err != nil {
		t.Fatal(err)
	}
	if n, err := checkIndex(root); err != nil || n != 1 {
		t.Fatalf("count %d: %v", n, err)
	}
}
