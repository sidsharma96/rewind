//go:build unix

package main

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func taskFixture(t *testing.T) (taskOptions, executableInfo, executableInfo) {
	t.Helper()
	home := t.TempDir()
	model := filepath.Join(home, "claude")
	cmd := exec.CommandContext(t.Context(), "go", "build", "-o", model, "./testdata/claude")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build model fixture: %s", b)
	}
	tool := searchFixture(t)
	t.Setenv("HOME", home)
	modelInfo, err := fingerprintExecutable(model)
	if err != nil {
		t.Fatal(err)
	}
	toolInfo, err := fingerprintExecutable(tool)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(home, "task")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return taskOptions{Directory: dir, Project: "demo", Run: "demo-run", Model: "fixture", ModelBinary: model, Search: searchInput{Binary: tool, Target: "fixture", Edition: "1.36"}}, modelInfo, toolInfo
}

func TestTaskFailedSearchIsReportedAndSaved(t *testing.T) {
	o, model, tool := taskFixture(t)
	t.Setenv("R2_SECRET_ACCESS_KEY", "never-forward-to-model")
	t.Setenv("PGSERVICE", "fixture-source")
	t.Setenv("REWIND_SERVICE_SECRET", "never-forward")
	if err := prepareTask(t.Context(), o, "Find documented facts", model, tool); err != nil {
		t.Fatal(err)
	}
	plans, err := readTaskPlans(o)
	if err != nil || len(plans) != 5 {
		t.Fatal("task did not preserve five observations", err)
	}
	raw, err := base64.StdEncoding.DecodeString(*plans[2].Capture.Response.Data)
	if err != nil {
		t.Fatal(err)
	}
	var command searchResult
	if json.Unmarshal(raw, &command) != nil || command.ExitCode == nil || *command.ExitCode != 17 || *plans[2].Capture.Response.State != "present" {
		t.Fatal("failed search lost complete evidence")
	}
	raw, _ = base64.StdEncoding.DecodeString(*plans[3].Capture.Request.Data)
	var request modelRequest
	if json.Unmarshal(raw, &request) != nil || !strings.Contains(string(request.Input), "selected edition unavailable") || strings.Contains(string(raw), "never-forward") {
		t.Fatal("report missed actual failure or forwarded secret")
	}
	raw, _ = base64.StdEncoding.DecodeString(*plans[4].Capture.Response.Data)
	if !strings.Contains(string(raw), "stdout") {
		t.Fatal("answer observation absent")
	}
	if err := os.Remove(o.ModelBinary); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(o.Search.Binary); err != nil {
		t.Fatal(err)
	}
	if _, err := readTaskPlans(o); err != nil {
		t.Fatal("delivery plans depend on removed tools", err)
	}
	calls, _ := os.ReadFile(filepath.Join(os.Getenv("HOME"), "model-invocations"))
	if string(calls) != "delegate\nchoose\nreport\nanswer\n" {
		t.Fatal("task reran or skipped model calls")
	}
}

func TestTaskModelFailureAndInvalidQueryKeepPrefix(t *testing.T) {
	for _, mode := range []string{"failed", "invalid", "outer"} {
		t.Run(mode, func(t *testing.T) {
			o, m, s := taskFixture(t)
			if err := os.WriteFile(filepath.Join(os.Getenv("HOME"), "model-fixture-mode"), []byte(mode), 0600); err != nil {
				t.Fatal(err)
			}
			if err := prepareTask(t.Context(), o, "Question", m, s); err == nil {
				t.Fatal("invalid model step continued")
			}
			plans, err := readTaskPlans(o)
			if err != nil || len(plans) != 2 {
				t.Fatal("completed prefix lost", err)
			}
			calls, _ := os.ReadFile(filepath.Join(os.Getenv("HOME"), "model-invocations"))
			if string(calls) != "delegate\nchoose\n" {
				t.Fatal("dependent work ran after failure")
			}
		})
	}
}

func TestTaskRefusesTornOrWrongScopePlans(t *testing.T) {
	o, m, s := taskFixture(t)
	if err := prepareTask(t.Context(), o, "Question", m, s); err != nil {
		t.Fatal(err)
	}
	wrong := o
	wrong.Run = "other-run"
	if _, err := readTaskPlans(wrong); err == nil {
		t.Fatal("task scope changed")
	}
	file := filepath.Join(o.Directory, taskPlanName(4))
	if err := os.WriteFile(file, []byte("["), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readTaskPlans(o); err == nil {
		t.Fatal("torn observation accepted")
	}
	if err := os.WriteFile(filepath.Join(o.Directory, "06.plan"), []byte("[]"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readTaskPlans(o); err == nil {
		t.Fatal("unbounded plan count accepted")
	}
}
