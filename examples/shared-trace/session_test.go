package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func sessionFixture() []byte {
	return []byte(`{"type":"system","subtype":"init","session_id":"s","claude_code_version":"2.1.318"}
{"type":"assistant","session_id":"s","message":{"content":[{"type":"tool_use","id":"delegate","name":"Agent","input":{"run_in_background":false}}]}}
{"type":"system","subtype":"task_started","session_id":"s","task_id":"worker-1","tool_use_id":"delegate","is_backgrounded":false,"spawn_depth":1}
{"type":"assistant","session_id":"s","agent_id":"worker-1","parent_tool_use_id":"delegate","message":{"content":[{"type":"tool_use","id":"read-bad","name":"Read","input":{"file_path":"missing"}},{"type":"tool_use","id":"read-ok","name":"Read","input":{"file_path":"fixture"}}]}}
{"type":"user","session_id":"s","agent_id":"worker-1","parent_tool_use_id":"delegate","message":{"content":[{"type":"tool_result","tool_use_id":"read-ok","content":"1\tretained source\n"},{"type":"tool_result","tool_use_id":"read-bad","content":"file missing","is_error":true}]}}
{"type":"user","session_id":"s","message":{"content":[{"type":"tool_result","tool_use_id":"delegate","content":[{"type":"text","text":"worker report"}]}]}}
{"type":"result","session_id":"s","result":"Final answer <script>unsafe</script>","is_error":false}
`)
}

func TestSessionCapturePairsByIDAndPreservesSource(t *testing.T) {
	raw := sessionFixture()
	calls, final, failed, err := parseSession(raw)
	if err != nil || len(calls) != 3 || failed || !calls[1].failed || calls[2].failed {
		t.Fatal("lost actual outcomes", err)
	}
	if !bytes.Equal(calls[1].response, calls[2].response) || !bytes.Contains(calls[2].response, []byte("retained source")) {
		t.Fatal("batch event not preserved")
	}
	plans, err := prepareSession(t.Context(), raw, []byte("operator prompt"), "demo", "run")
	if err != nil || len(plans) != 4 {
		t.Fatal(err)
	}
	root, delegate := plans[0].Execution, plans[1].Execution
	for i, p := range plans {
		if p.Execution.Trace != root.Trace || p.Owner.Instance != "616c696365" && p.Owner.Instance != "626f62" {
			t.Fatal("scope drift")
		}
		if i >= 2 && !strings.Contains(string(p.Span.Metadata), delegate.Span) {
			t.Fatal("worker call lost delegation parent")
		}
		if i == 0 && bytes.Contains(p.Span.Metadata, []byte("startTimeUnixNano")) {
			t.Fatal("invented run time")
		}
		data, _ := base64.StdEncoding.DecodeString(*p.Capture.Response.Data)
		var captured sessionCapture
		if json.Unmarshal(data, &captured) != nil {
			t.Fatal("invalid capture")
		}
		if i == 0 && !bytes.Equal(captured.Payload, final) {
			t.Fatal("changed exact final CLI event")
		}
		if i > 0 && (!bytes.Equal(captured.Payload, calls[i-1].response) || captured.CallID != calls[i-1].id) {
			t.Fatal("changed selected CLI event")
		}
	}
	file := filepath.Join(t.TempDir(), "saved.plan")
	if err := save(file, plans); err != nil {
		t.Fatal(err)
	}
	saved, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	var retry []plan
	if json.Unmarshal(saved, &retry) != nil || retry[0].Execution != plans[0].Execution {
		t.Fatal("saved retry identity changed")
	}
	// Input mutation after preparation cannot change the private plans.
	for i := range raw {
		raw[i] = 'x'
	}
	if err := validateSessionPlans(plans, "demo", "run"); err != nil {
		t.Fatal(err)
	}
}

func TestSessionRefusesAmbiguousOrIncompleteStreams(t *testing.T) {
	raw := string(sessionFixture())
	cases := map[string]string{
		"version":           strings.Replace(raw, "2.1.318", "2.1.319", 1),
		"session":           strings.Replace(raw, `"session_id":"s"`, `"session_id":"other"`, 1),
		"torn":              strings.TrimSuffix(raw, "\n"),
		"unfinished":        raw[:strings.LastIndex(raw, `{"type":"result"`)],
		"orphan":            strings.Replace(raw, `"tool_use_id":"read-ok"`, `"tool_use_id":"unknown"`, 1),
		"duplicate call":    strings.Replace(raw, `"id":"read-ok"`, `"id":"read-bad"`, 1),
		"wrong agent":       strings.Replace(raw, `"agent_id":"worker-1"`, `"agent_id":"worker-2"`, 1),
		"wrong parent":      strings.Replace(raw, `"parent_tool_use_id":"delegate"`, `"parent_tool_use_id":"unknown"`, 1),
		"background":        strings.Replace(raw, `"run_in_background":false`, `"run_in_background":true`, 1),
		"null background":   strings.Replace(raw, `"run_in_background":false`, `"run_in_background":null`, 1),
		"null error":        strings.Replace(raw, `"is_error":true`, `"is_error":null`, 1),
		"nesting":           strings.Replace(raw, `"id":"read-ok","name":"Read"`, `"id":"read-ok","name":"Agent"`, 1),
		"duplicate result":  strings.Replace(raw, `"tool_use_id":"read-ok"`, `"tool_use_id":"read-bad"`, 1),
		"duplicate key":     strings.Replace(raw, `"id":"read-ok"`, `"id":"read-ok","id":"read-bad"`, 1),
		"unsupported block": strings.Replace(raw, `"type":"tool_use"`, `"type":"server_tool_use"`, 1),
		"after result":      raw + `{"type":"system","subtype":"task_updated","session_id":"s"}` + "\n",
		"line cap":          strings.Repeat(" ", sessionLineLimit) + raw,
		"stream cap":        strings.Repeat(" ", sessionLimit) + raw,
	}
	for name, input := range cases {
		t.Run(name, func(t *testing.T) {
			calls, result, _, err := parseSession([]byte(input))
			if err == nil || calls != nil || result != nil {
				t.Fatal("accepted ambiguous input")
			}
		})
	}
}

func FuzzSessionStream(f *testing.F) {
	f.Add(sessionFixture())
	f.Fuzz(func(t *testing.T, raw []byte) {
		calls, result, _, err := parseSession(raw)
		if err != nil && (calls != nil || result != nil) {
			t.Fatal("partial result on refusal")
		}
	})
}

func TestSessionRetryRefusesScopeAndBodyChanges(t *testing.T) {
	plans, err := prepareSession(t.Context(), sessionFixture(), []byte("question"), "demo", "run")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(plans)
	for _, name := range []string{"project", "run", "owner", "duplicate", "missing", "no worker"} {
		t.Run(name, func(t *testing.T) {
			var changed []plan
			if json.Unmarshal(raw, &changed) != nil {
				t.Fatal("bad fixture")
			}
			switch name {
			case "project":
				changed[1].Execution.Project = "6f74686572"
			case "run":
				changed[1].Owner.Run = "6f74686572"
			case "owner":
				changed[1].Owner.Instance = "6f74686572"
			case "duplicate":
				changed[1].Execution.Span = changed[0].Execution.Span
			case "missing":
				changed[1].Capture.Response.Data = nil
			case "no worker":
				for i := range changed {
					changed[i].Owner.Instance = changed[0].Owner.Instance
				}
			}
			if validateSessionPlans(changed, "demo", "run") == nil {
				t.Fatal("unsafe retry plan accepted")
			}
		})
	}
}

func TestSessionRequiresObservedForeground(t *testing.T) {
	raw := string(sessionFixture())
	for name, input := range map[string]string{
		"runtime background despite request": strings.Replace(raw, `"is_backgrounded":false`, `"is_backgrounded":true`, 1),
		"missing requested mode":             strings.Replace(raw, `"run_in_background":false`, `"other":false`, 1),
		"missing observed mode":              strings.Replace(raw, `"is_backgrounded":false`, `"other":false`, 1),
		"reported nested spawn":              strings.Replace(raw, `"spawn_depth":1`, `"spawn_depth":2`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, _, err := parseSession([]byte(input)); err == nil {
				t.Fatal("ambiguous/background runtime accepted")
			}
		})
	}
}

func TestSessionInvalidPlanLeavesNoFile(t *testing.T) {
	var lines []string
	for _, line := range strings.Split(string(sessionFixture()), "\n") {
		if strings.Contains(line, `"agent_id":"worker-1"`) {
			continue
		}
		if strings.HasPrefix(line, `{"type":"user","session_id":"s","message"`) {
			lines = append(lines, `{"type":"assistant","session_id":"s","agent_id":"worker-1","parent_tool_use_id":"delegate","message":{"content":[{"type":"text","text":"No tools needed"}]}}`)
		}
		lines = append(lines, line)
	}
	dir := t.TempDir()
	stream, prompt, planFile := filepath.Join(dir, "stream"), filepath.Join(dir, "prompt"), filepath.Join(dir, "plan")
	if err := os.WriteFile(stream, []byte(strings.Join(lines, "\n")), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(prompt, []byte("question"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := sessionCommand(t.Context(), taskOptions{Project: "demo", Run: "run", InputFile: prompt}, stream, planFile, false); err == nil {
		t.Fatal("unsupported session accepted")
	}
	if _, err := os.Lstat(planFile); !os.IsNotExist(err) {
		t.Fatal("refused session left an unusable plan")
	}
}
