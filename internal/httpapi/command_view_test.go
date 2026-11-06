package httpapi

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/sidsharma96/rewind/internal/record"
)

func displayBody(state record.BodyState, data []byte) Body {
	text := base64.StdEncoding.EncodeToString(data)
	return Body{&state, &text}
}
func commandFixture(t *testing.T, tool string, output []byte) Capture {
	t.Helper()
	req, err := json.Marshal(commandRequest{Version: 1, Tool: tool, Args: []string{"search", "two words"}, Input: []byte("Which container policy?")})
	if err != nil {
		t.Fatal(err)
	}
	code := 17
	res, err := json.Marshal(commandResponse{Version: 1, Outcome: "exited", ExitCode: &code, Stdout: output, Stderr: []byte("failed query\n")})
	if err != nil {
		t.Fatal(err)
	}
	return Capture{Request: displayBody(record.BodyPresent, req), Response: displayBody(record.BodyPresent, res)}
}
func renderedCommand(t *testing.T, c Capture) string {
	t.Helper()
	v := commandView(c)
	var out bytes.Buffer
	if err := pageTemplate.Execute(&out, viewData{Title: "Capture", Capture: &c, Command: &v, Request: bodyPreview(c.Request), Response: bodyPreview(c.Response)}); err != nil {
		t.Fatal(err)
	}
	return out.String()
}
func TestCommandViewReadableEvidenceAndExactBytes(t *testing.T) {
	c := commandFixture(t, "claude print", []byte(`{"result":"Saved answer <script>bad()</script>","is_error":false,"usage":{"extra":3}}`))
	before, _ := json.Marshal(c)
	html := renderedCommand(t, c)
	for _, want := range []string{"Saved answer &lt;script&gt;bad()&lt;/script&gt;", "Which container policy?", "failed query", "exit code: 17", "CLI is_error: false", "Command exit does not establish answer correctness"} {
		if !strings.Contains(html, want) {
			t.Fatalf("missing %q", want)
		}
	}
	if strings.Contains(html, "<script>bad()") {
		t.Fatal("untrusted output became HTML")
	}
	after, _ := json.Marshal(c)
	if !bytes.Equal(before, after) {
		t.Fatal("display changed capture")
	}
	c = commandFixture(t, "concordance search", []byte("source/path.md revision abc blocks [1,2)\nreturned evidence"))
	if !strings.Contains(renderedCommand(t, c), "returned evidence") {
		t.Fatal("search stdout remains encoded")
	}
}
func TestCommandViewRefusesAmbiguousInterpretation(t *testing.T) {
	tests := map[string]string{
		"future":        `{"version":2,"outcome":"exited","exit_code":0,"signal":"","stdout":"","stderr":""}`,
		"duplicate":     `{"version":1,"version":2,"outcome":"exited","exit_code":0,"signal":"","stdout":"","stderr":""}`,
		"unknown":       `{"version":1,"outcome":"exited","exit_code":0,"signal":"","stdout":"","stderr":"","new":1}`,
		"invalid bytes": `{"version":1,"outcome":"exited","exit_code":0,"signal":"","stdout":"???","stderr":""}`,
		"malformed":     `{"version":1`,
		"outcome":       `{"version":1,"outcome":"future","exit_code":0,"signal":"","stdout":"","stderr":""}`,
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			c := commandFixture(t, "claude print", nil)
			c.Response = displayBody(record.BodyPresent, []byte(raw))
			v := commandView(c)
			for _, field := range v.Fields {
				if field.Title == "Standard output" || field.Title == "CLI result (reported)" {
					t.Fatal("ambiguous evidence interpreted")
				}
			}
			if len(v.Notices) < 2 {
				t.Fatal("no interpretation refusal")
			}
		})
	}
	c := commandFixture(t, "claude print", []byte(`{"result":"one","result":"two","is_error":false}`))
	if strings.Contains(renderedCommand(t, c), "<h3>CLI result") {
		t.Fatal("duplicate CLI result interpreted")
	}
}
func TestCommandViewAvailabilityBinaryAndBounds(t *testing.T) {
	for _, state := range []record.BodyState{record.BodyMissing, record.BodyRedacted, record.BodyTruncated} {
		t.Run(string(state), func(t *testing.T) {
			c := commandFixture(t, "tool", []byte{0xff, 0x01})
			c.Response.State = &state
			html := renderedCommand(t, c)
			if !strings.Contains(html, string(state)) {
				t.Fatal("saved state hidden")
			}
			if state == record.BodyTruncated {
				if !strings.Contains(html, "ff01") || !strings.Contains(html, "partial observations") {
					t.Fatal("partial binary evidence hidden")
				}
			} else if strings.Contains(html, "<h3>Standard output") {
				t.Fatal("unavailable bytes invented")
			}
		})
	}
	c := commandFixture(t, "tool", []byte(strings.Repeat("x", commandTextLimit+1)))
	c.Request = displayBody(record.BodyMissing, nil)
	if html := renderedCommand(t, c); !strings.Contains(html, "first 16384") || !strings.Contains(html, "Standard output") {
		t.Fatal("response depends on available request or preview unbounded")
	}
	c.Response = displayBody(record.BodyPresent, bytes.Repeat([]byte{'x'}, commandViewLimit+1))
	if !strings.Contains(renderedCommand(t, c), "256 KiB interpretation limit") {
		t.Fatal("oversized input decoded")
	}
	c.Response = displayBody(record.BodyPresent, []byte(`{"version":1,"outcome":"exited","exit_code":0,"signal":"","stdout":"","stderr":""}`))
	if !strings.Contains(renderedCommand(t, c), "(zero bytes)") {
		t.Fatal("Present empty stream lost")
	}
}
func TestTaskTraceLocatorAndLoginDestination(t *testing.T) {
	client, err := NewClient("https://example.test", strings.Repeat("a", 64), &http.Client{Timeout: time.Second}, 1024, 1024)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := client.TraceViewURL("64656d6f", "01010101010101010101010101010101")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(raw, "https://example.test/view?") || strings.Contains(raw, client.token) || !strings.Contains(raw, "format=command-v1") {
		t.Fatal("unsafe or unusable locator")
	}
	path := strings.TrimPrefix(raw, client.base)
	if _, err = loginDestination(path); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"//evil.test/view", "https://evil.test", "/login", "/view/response", path + "&project=00", path + "#fragment", path + "&unknown=x", strings.Replace(path, "/view?", "/%76iew?", 1), "/view?project=00&trace=bad"} {
		if _, err := loginDestination(bad); err == nil {
			t.Errorf("accepted unsafe return %q", bad)
		}
	}
	if _, err := client.TraceViewURL("zz", "bad"); err == nil {
		t.Fatal("invalid locator accepted")
	}
}
