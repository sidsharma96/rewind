package main

import (
	"os"
	"path/filepath"
	"testing"
)

func writeFixture(t *testing.T, root, name, text string) {
	t.Helper()
	file := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(file), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestSiteLinksCheckFragmentsAndEscapes(t *testing.T) {
	for _, test := range []struct {
		link  string
		valid bool
	}{
		{"chapter.html#saved", true}, {"chapter.html#missing", false},
		{"missing.html", false}, {"../outside.html", false},
		{"https://example.invalid/unchecked", true}, {"/chapter.html#saved", true},
	} {
		t.Run(test.link, func(t *testing.T) {
			root := t.TempDir()
			writeFixture(t, root, "index.html", `<a href="`+test.link+`">Read</a>`)
			writeFixture(t, root, "chapter.html", `<h1 id="saved">Saved</h1>`)
			if err := checkSite(root); (err == nil) != test.valid {
				t.Fatalf("valid=%v: %v", test.valid, err)
			}
		})
	}
}

func TestInventoryRequiresEveryPage(t *testing.T) {
	root := t.TempDir()
	writeFixture(t, root, "SUMMARY.md", "# Summary\n- [Start](index.md)\n")
	writeFixture(t, root, "index.md", "# Start\n")
	if err := checkInventory(root); err != nil {
		t.Fatal(err)
	}
	writeFixture(t, root, "lost.md", "# Lost\n")
	if err := checkInventory(root); err == nil {
		t.Fatal("accepted unlisted page")
	}
}
