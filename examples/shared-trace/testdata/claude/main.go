// A bounded fixture for the documented CLI boundary, never a model substitute
// in claims about actual agent decisions. HOME belongs to each isolated test.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	var system string
	for i, arg := range os.Args {
		if arg == "--system-prompt" && i+1 < len(os.Args) {
			system = os.Args[i+1]
		}
	}
	if os.Getenv("R2_SECRET_ACCESS_KEY") != "" || os.Getenv("REWIND_SERVICE_SECRET") != "" || os.Getenv("PGSERVICE") != "" {
		os.Exit(93)
	}
	b, err := io.ReadAll(io.LimitReader(os.Stdin, (128<<10)+1))
	if err != nil || len(b) > 128<<10 {
		os.Exit(94)
	}
	var input map[string]any
	if json.Unmarshal(b, &input) != nil {
		os.Exit(95)
	}
	mode, _ := os.ReadFile(filepath.Join(os.Getenv("HOME"), "model-fixture-mode"))
	label := ""
	result := ""
	switch {
	case strings.Contains(system, "Delegate the supplied"):
		label = "delegate"
		result = "Research the supplied documentation question and preserve unknowns."
	case strings.Contains(system, "Choose exactly one"):
		label = "choose"
		result = `{"query":"failure","reason":"Exercise a complete failed search"}`
		if string(mode) == "evidence" {
			result = `{"query":"ContainerRestartRules","reason":"Inspect a saved source passage"}`
		}
	case strings.Contains(system, "Bob reporting"):
		label = "report"
		if input["stderr_utf8"] == "selected edition unavailable\n" {
			result = "Search failed: selected edition unavailable. No supporting documentation was returned."
		} else {
			result = "Tool observation received; distinguish available evidence from unknowns."
		}
	case strings.Contains(system, "Alice answering"):
		label = "answer"
		result = "The search failed, so the requested fact remains unknown. Bob reported: " + fmt.Sprint(input["bob_report"])
	default:
		os.Exit(96)
	}
	file, err := os.OpenFile(filepath.Join(os.Getenv("HOME"), "model-invocations"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		os.Exit(97)
	}
	if _, err = file.WriteString(label + "\n"); err != nil {
		os.Exit(98)
	}
	if file.Close() != nil {
		os.Exit(98)
	}
	failed := string(mode) == "failed" && label == "choose"
	if string(mode) == "invalid" && label == "choose" {
		result = "not query JSON"
	}
	if string(mode) == "outer" && label == "choose" {
		fmt.Print("not CLI JSON")
		return
	}
	if err = json.NewEncoder(os.Stdout).Encode(map[string]any{"is_error": failed, "result": result, "num_turns": 1}); err != nil {
		os.Exit(99)
	}
}
