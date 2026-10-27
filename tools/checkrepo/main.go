// Command checkrepo checks the actual Git index for private and generated artifacts.
package main

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
)

const maxSourceBytes = 1 << 20

func main() {
	count, err := checkIndex(".")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("Checked %d indexed source files.\n", count)
}

func forbidden(name string) bool {
	for _, prefix := range []string{"docs", ".claude", ".tools", "review", ".wrangler", "scripts/utils", "bin", "__pycache__"} {
		if name == prefix || strings.HasPrefix(name, prefix+"/") {
			return true
		}
	}
	base := path.Base(name)
	if base == ".DS_Store" || base == "coverage.out" || base == ".env" || (strings.HasPrefix(base, ".env.") && base != ".env.example") {
		return true
	}
	switch strings.ToLower(path.Ext(name)) {
	case ".json", ".jsonl", ".ndjson", ".py", ".pyw", ".pyc", ".pyo", ".pprof", ".trace", ".pcap", ".pcapng", ".test":
		return true
	}
	return false
}

func git(root string, args ...string) ([]byte, error) {
	out, err := exec.Command("git", append([]string{"-C", root}, args...)...).Output()
	if err != nil {
		return nil, fmt.Errorf("git %s: %w", args[0], err)
	}
	return out, nil
}

func checkIndex(root string) (int, error) {
	data, err := git(root, "ls-files", "--stage", "-z")
	if err != nil {
		return 0, err
	}
	count := 0
	for _, record := range strings.Split(string(data), "\x00") {
		if record == "" {
			continue
		}
		metadata, name, ok := strings.Cut(record, "\t")
		fields := strings.Fields(metadata)
		if !ok || len(fields) != 3 || fields[2] != "0" {
			return count, fmt.Errorf("invalid or unmerged index entry: %q", name)
		}
		if forbidden(name) {
			return count, fmt.Errorf("private/generated artifact in index: %q", name)
		}
		if fields[0] != "100644" && fields[0] != "100755" {
			return count, fmt.Errorf("source must be a regular file: %q", name)
		}
		size, err := git(root, "cat-file", "-s", fields[1])
		if err != nil {
			return count, err
		}
		n, err := strconv.ParseInt(strings.TrimSpace(string(size)), 10, 64)
		if err != nil || n < 0 || n > maxSourceBytes {
			return count, fmt.Errorf("invalid size or source exceeds 1 MiB: %q", name)
		}
		count++
	}
	return count, nil
}
