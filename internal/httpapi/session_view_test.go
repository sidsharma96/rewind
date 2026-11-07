package httpapi

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/sidsharma96/rewind/internal/record"
)

func sessionDisplayBody(t *testing.T, source, id, payload string) Body {
	t.Helper()
	raw, err := json.Marshal(sessionObservation{1, source, id, []byte(payload)})
	if err != nil {
		t.Fatal(err)
	}
	return displayBody(record.BodyPresent, raw)
}

func TestSessionViewSelectsBatchCallAndPreservesBytes(t *testing.T) {
	c := Capture{
		Request:  sessionDisplayBody(t, "claude-cli-event", "b", `{"type":"assistant","message":{"content":[{"type":"tool_use","id":"a","name":"Other","input":{}},{"type":"tool_use","id":"b","name":"Read","input":{"file_path":"missing"}}]}}`),
		Response: sessionDisplayBody(t, "claude-cli-event", "b", `{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"a","content":"wrong sibling"},{"type":"tool_result","tool_use_id":"b","content":"failed <script>attack</script>","is_error":true}]}}`),
	}
	before, _ := json.Marshal(c)
	v := sessionView(c)
	var out bytes.Buffer
	if err := pageTemplate.Execute(&out, viewData{Format: "claude-stream-v1", Capture: &c, Command: &v}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"failed &lt;script&gt;attack&lt;/script&gt;", "Tool is_error: true", "missing", "CLI-visible"} {
		if !strings.Contains(out.String(), want) {
			t.Fatal("missing", want)
		}
	}
	if strings.Contains(out.String(), "wrong sibling") || strings.Contains(out.String(), "<script>attack") {
		t.Fatal("wrong selection or unsafe rendering")
	}
	after, _ := json.Marshal(c)
	if !bytes.Equal(before, after) {
		t.Fatal("reader changed saved capture")
	}
	c.Request = sessionDisplayBody(t, "operator-prompt", "", "Question")
	c.Response = sessionDisplayBody(t, "claude-cli-event", "", `{"type":"result","result":"Answer","is_error":false}`)
	v = sessionView(c)
	if len(v.Fields) != 2 || v.Fields[1].Value.Text != "Answer" || !strings.Contains(v.Fields[0].Note, "does not prove") {
		t.Fatal("lost answer or provenance limit")
	}
}

func TestSessionViewRefusesUnknownAndMismatchedEvidence(t *testing.T) {
	for _, payload := range []string{`{"type":"result","result":"x","is_error":null}`, `{"type":"result","result":"x","is_error":false,"is_error":true}`, `{"type":"future","result":"x","is_error":false}`} {
		c := Capture{Request: sessionDisplayBody(t, "operator-prompt", "", "q"), Response: sessionDisplayBody(t, "claude-cli-event", "", payload)}
		if v := sessionView(c); len(v.Fields) != 0 {
			t.Fatal("interpreted unsupported result")
		}
	}
	c := Capture{Request: sessionDisplayBody(t, "claude-cli-event", "a", "{}"), Response: sessionDisplayBody(t, "claude-cli-event", "b", "{}")}
	if v := sessionView(c); len(v.Fields) != 0 || len(v.Notices) < 2 {
		t.Fatal("mismatched calls interpreted")
	}
	if !supportedViewFormat("claude-stream-v1") {
		t.Fatal("display choice rejected")
	}
}
