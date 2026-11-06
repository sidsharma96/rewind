//go:build unix

package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sidsharma96/rewind/internal/record"
)

func searchFixture(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "concordance")
	cmd := exec.CommandContext(t.Context(), "go", "build", "-o", path, "./testdata/concordance")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build controlled tool: %s", output)
	}
	return path
}

func TestConcordanceCommandObservation(t *testing.T) {
	t.Parallel()
	binary := searchFixture(t)
	for _, query := range []string{"success", "failure", "empty", "overflow", "wait", "drain", "drain-failure"} {
		t.Run(query, func(t *testing.T) {
			t.Parallel()
			in := searchInput{Binary: binary, Target: "fixture", Edition: "1.36", Query: query}
			args, err := in.args()
			if err != nil {
				t.Fatal(err)
			}
			timeout := 5 * time.Second
			if query == "wait" {
				timeout = 200 * time.Millisecond
			}
			result, launched, err := runSearchCommand(t.Context(), in, args, timeout)
			if err != nil || !launched {
				t.Fatalf("command did not launch: %v", err)
			}
			switch query {
			case "success":
				if result.Outcome != "exited" || result.ExitCode == nil || *result.ExitCode != 0 || !bytes.Equal(result.Stdout, []byte{0, 255, '4', '2', '\n'}) || !bytes.Equal(result.Stderr, []byte{128, 'd'}) {
					t.Fatal("binary command evidence changed")
				}
			case "failure":
				if result.Outcome != "exited" || result.ExitCode == nil || *result.ExitCode != 17 || len(result.Stdout) != 0 || string(result.Stderr) != "selected edition unavailable\n" {
					t.Fatal("nonzero exit lost its complete evidence")
				}
			case "empty":
				if result.Outcome != "exited" || result.ExitCode == nil || *result.ExitCode != 0 || len(result.Stdout)+len(result.Stderr) != 0 {
					t.Fatal("empty command output lost successful termination")
				}
			case "overflow":
				if result.Outcome != "output_limit" || len(result.Stdout) != searchOutputLimit {
					t.Fatal("overflow was not bounded and explicit")
				}
			case "wait":
				if result.Outcome != "interrupted" || result.ExitCode != nil || result.Signal == "" {
					t.Fatal("deadline fabricated an ordinary exit")
				}
			case "drain", "drain-failure":
				wantExit := 0
				if query == "drain-failure" {
					wantExit = 17
				}
				if result.Outcome != "incomplete_streams" || result.ExitCode == nil || *result.ExitCode != wantExit {
					t.Fatal("child-held pipe was reported complete")
				}
				// The child wrote its PID before holding the inherited stream.
				// A zombie has exited; init owns reaping it, not this process.
				pid := strings.TrimSpace(string(result.Stdout))
				if pid == "" {
					t.Fatal("child never reached the inherited stream")
				}
				deadline := time.After(2 * time.Second)
				ticker := time.NewTicker(5 * time.Millisecond)
				defer ticker.Stop()
				for {
					state, _ := exec.CommandContext(t.Context(), "ps", "-p", pid, "-o", "stat=").Output()
					if s := strings.TrimSpace(string(state)); s == "" || strings.HasPrefix(s, "Z") {
						break
					}
					select {
					case <-deadline:
						t.Fatal("owned pipe holder still running")
					case <-ticker.C:
					}
				}
			}
		})
	}
}

func TestSearchCaptureStates(t *testing.T) {
	t.Parallel()
	binary := searchFixture(t)
	for _, query := range []string{"success", "failure", "empty", "overflow", "drain-failure"} {
		plans, err := prepareSearch(t.Context(), searchInput{binary, "fixture", "1.36", query}, "demo", "run", "01000000000000000000000000000000", "0200000000000000")
		if err != nil || len(plans) != 1 {
			t.Fatalf("prepare command observation: %v", err)
		}
		p := plans[0]
		want := record.BodyPresent
		if query == "overflow" || query == "drain-failure" {
			want = record.BodyTruncated
		}
		if *p.Capture.Response.State != want || p.Owner.Instance != "626f62" || p.Execution.Trace != "01000000000000000000000000000000" {
			t.Fatal("command state or Bob's execution changed")
		}
		body, err := base64.StdEncoding.DecodeString(*p.Capture.Response.Data)
		var result searchResult
		if err != nil || json.Unmarshal(body, &result) != nil || result.Version != 1 {
			t.Fatal("invalid command observation body")
		}
	}
	invalid := filepath.Join(t.TempDir(), "cannot-execute")
	if err := os.WriteFile(invalid, []byte("invalid executable format"), 0700); err != nil {
		t.Fatal(err)
	}
	plans, err := prepareSearch(t.Context(), searchInput{invalid, "fixture", "1.36", "success"}, "demo", "run", "01000000000000000000000000000000", "0200000000000000")
	if err != nil || *plans[0].Capture.Response.State != record.BodyMissing || *plans[0].Capture.Response.Data != "" {
		t.Fatal("start failure fabricated a tool response")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	plans, err = prepareSearch(ctx, searchInput{binary, "fixture", "1.36", "success"}, "demo", "run", "01000000000000000000000000000000", "0200000000000000")
	if err != nil || *plans[0].Capture.Response.State != record.BodyMissing {
		t.Fatal("cancellation before launch fabricated a response")
	}
}

func TestSearchEnvironmentExcludesAgentCredentials(t *testing.T) {
	t.Setenv("REWIND_SERVICE_SECRET", "synthetic-must-not-reach-tool")
	t.Setenv("SSH_AUTH_SOCK", "synthetic-must-not-reach-tool")
	t.Setenv("CONCORDANCE_COLLECTION", "fixture")
	binary := searchFixture(t)
	in := searchInput{binary, "fixture", "1.36", "environment"}
	args, err := in.args()
	if err != nil {
		t.Fatal(err)
	}
	result, launched, err := runSearchCommand(t.Context(), in, args, 5*time.Second)
	if err != nil || !launched || result.ExitCode == nil || *result.ExitCode != 0 || string(result.Stdout) != "filtered" {
		t.Fatal("tool environment was not explicit")
	}
}
