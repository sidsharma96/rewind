// Command checkdocs checks an mdBook source inventory or local links in built HTML.
// It does not fetch external resources or evaluate scripts.
package main

import (
	"flag"
	"fmt"
	"html"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var (
	markdownLink = regexp.MustCompile(`\[[^\]]*\]\(([^\s)]+)\)`)
	htmlLink     = regexp.MustCompile(`(?i)\b(?:href|src)\s*=\s*["']([^"']+)["']`)
	htmlID       = regexp.MustCompile(`(?i)\b(?:id|name)\s*=\s*["']([^"']+)["']`)
)

func main() {
	src := flag.String("src", "docs/src", "book source directory")
	site := flag.String("site", "", "built site directory; checks local links and fragments")
	flag.Parse()
	var err error
	if flag.NArg() != 0 {
		err = fmt.Errorf("unexpected positional arguments")
	} else if *site != "" {
		err = checkSite(*site)
	} else {
		err = checkInventory(*src)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("Book check passed (local inventory/links only).")
}

func checkInventory(root string) error {
	data, err := os.ReadFile(filepath.Join(root, "SUMMARY.md"))
	if err != nil {
		return err
	}
	listed := map[string]bool{"SUMMARY.md": true}
	for _, match := range markdownLink.FindAllSubmatch(data, -1) {
		u, err := url.Parse(string(match[1]))
		if err != nil {
			return err
		}
		if u.IsAbs() || u.Host != "" {
			continue
		}
		name := filepath.Clean(filepath.FromSlash(u.Path))
		if !filepath.IsLocal(name) || filepath.Ext(name) != ".md" {
			return fmt.Errorf("invalid SUMMARY page: %q", name)
		}
		if listed[name] {
			return fmt.Errorf("duplicate SUMMARY page: %q", name)
		}
		if _, err := os.ReadFile(filepath.Join(root, name)); err != nil {
			return err
		}
		listed[name] = true
	}
	return filepath.WalkDir(root, func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("book source must not contain symlinks: %s", name)
		}
		if entry.IsDir() || filepath.Ext(name) != ".md" {
			return nil
		}
		rel, err := filepath.Rel(root, name)
		if err != nil {
			return err
		}
		if !listed[rel] {
			return fmt.Errorf("page missing from SUMMARY: %s", rel)
		}
		return nil
	})
}

func checkSite(root string) error {
	pages := map[string][]byte{}
	ids := map[string]map[string]bool{}
	err := filepath.WalkDir(root, func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("built site must not contain symlinks: %s", name)
		}
		if entry.IsDir() || filepath.Ext(name) != ".html" {
			return nil
		}
		rel, err := filepath.Rel(root, name)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(name)
		if err != nil {
			return err
		}
		pages[rel] = data
		ids[rel] = map[string]bool{}
		for _, match := range htmlID.FindAllSubmatch(data, -1) {
			ids[rel][html.UnescapeString(string(match[1]))] = true
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(pages) == 0 {
		return fmt.Errorf("no HTML pages in %s", root)
	}
	for page, data := range pages {
		for _, match := range htmlLink.FindAllSubmatch(data, -1) {
			if err := checkTarget(root, page, html.UnescapeString(string(match[1])), ids); err != nil {
				return fmt.Errorf("%s: %w", page, err)
			}
		}
	}
	return nil
}

func checkTarget(root, page, link string, ids map[string]map[string]bool) error {
	u, err := url.Parse(link)
	if err != nil {
		return err
	}
	if u.IsAbs() || u.Host != "" {
		return nil
	}
	target := page
	if u.Path != "" {
		if strings.HasPrefix(u.Path, "/") {
			target = filepath.Clean(filepath.FromSlash(strings.TrimPrefix(u.Path, "/")))
		} else {
			target = filepath.Join(filepath.Dir(page), filepath.FromSlash(u.Path))
		}
	}
	if !filepath.IsLocal(target) {
		return fmt.Errorf("link escapes site: %q", link)
	}
	info, err := os.Stat(filepath.Join(root, target))
	if err != nil {
		return fmt.Errorf("broken local link %q: %w", link, err)
	}
	if info.IsDir() {
		target = filepath.Join(target, "index.html")
		if _, err := os.Stat(filepath.Join(root, target)); err != nil {
			return fmt.Errorf("missing directory index for %q", link)
		}
	}
	if u.Fragment != "" && filepath.Ext(target) == ".html" && !ids[target][u.Fragment] {
		return fmt.Errorf("missing fragment in %q", link)
	}
	return nil
}
