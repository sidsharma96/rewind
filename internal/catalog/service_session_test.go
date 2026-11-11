//go:build unix

package catalog

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"html"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sidsharma96/rewind/internal/access"
	"github.com/sidsharma96/rewind/internal/httpapi"
	"github.com/sidsharma96/rewind/internal/ingest"
)

func TestNativeClaudeSessionInspectionAndRetry(t *testing.T) {
	f := database(t)
	f.seed(t, f.r)
	f.policy(t, "DELETE FROM "+f.c.table("creator_grants"))
	for _, instance := range []string{"alice", "bob"} {
		f.policy(t, "INSERT INTO "+f.c.table("host_creator_grants")+" VALUES($1,$2,$3,$4)", []byte(f.r.Execution.Tenant), []byte(f.r.Owner.Run), []byte(instance), []byte("host"))
		f.policy(t, "INSERT INTO "+f.c.table("writer_grants")+" VALUES($1,$2,$3,$4)", []byte(f.r.Execution.Tenant), []byte(f.r.Owner.Run), []byte(instance), []byte(instance))
	}
	f.policy(t, "INSERT INTO "+f.c.table("reader_grants")+" VALUES($1,$2)", []byte(f.r.Execution.Tenant), []byte("reader"))
	// A browser session stays read-only even for a principal with both grants.
	f.policy(t, "INSERT INTO "+f.c.table("writer_grants")+" VALUES($1,$2,$3,$4)", []byte(f.r.Execution.Tenant), []byte(f.r.Owner.Run), []byte("alice"), []byte("reader"))
	tokens := map[string]string{}
	var entries []access.Credential
	for _, principal := range []string{"host", "alice", "bob", "reader"} {
		token, digest, err := access.NewToken()
		if err != nil {
			t.Fatal(err)
		}
		tokens[principal] = token
		entries = append(entries, access.Credential{Digest: digest, Principal: principal, ExpiresAt: time.Now().Add(time.Hour)})
	}
	credentials, err := access.NewCredentials(4, entries, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := access.NewSessions(credentials, 8, 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	auth, err := access.NewAuthorizer(f.c)
	if err != nil {
		t.Fatal(err)
	}
	registrar, err := ingest.NewRegistrar(auth, f.c, 4)
	if err != nil {
		t.Fatal(err)
	}
	peer := &recoveryObjects{peer: &publicationObjects{data: make(map[string][]byte)}}
	objects := httptest.NewServer(peer)
	t.Cleanup(objects.Close)
	spans := nativeSpans(t, f.c, objects, auth, 32768)
	captures := nativeCaptures(t, spans, 32768)
	reader, err := ingest.NewTraceReader(spans, ingest.TraceLimits{MaxCandidates: 2, MaxStoredBytes: 64 << 10, OperationTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(nil)
	origin := "https://" + server.Listener.Addr().String()
	handler, err := httpapi.New(origin, httpapi.Limits{MaxActive: 4, MaxLoginActive: 1, MaxRequestBytes: 128 << 10, MaxResponseBytes: 64 << 10, Timeout: 5 * time.Second},
		credentials, sessions, registrar, spans, captures, reader, nativeMetadataCodec(t))
	if err != nil {
		t.Fatal(err)
	}

	// A completed execution's span and capture must reach the TLS receiver
	// together. Registration still goes through normally before either write.
	var deliveryMu sync.Mutex
	pending := make(map[string]chan struct{})
	server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && (strings.HasSuffix(r.URL.Path, "/span") || strings.HasSuffix(r.URL.Path, "/capture")) {
			key := r.URL.Path[:strings.LastIndex(r.URL.Path, "/")]
			deliveryMu.Lock()
			peer, found := pending[key]
			if found {
				delete(pending, key)
				close(peer)
			} else {
				peer = make(chan struct{})
				pending[key] = peer
			}
			deliveryMu.Unlock()
			ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
			defer cancel()
			select {
			case <-peer:
			case <-ctx.Done():
				t.Error("span and capture did not overlap at the TLS receiver")
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
		}
		handler.ServeHTTP(w, r)
	})
	server.StartTLS()
	t.Cleanup(server.Close)
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	for who, token := range tokens {
		if err := os.WriteFile(filepath.Join(dir, who), []byte(token), 0600); err != nil {
			t.Fatal(err)
		}
	}
	sample := filepath.Join(dir, "shared-trace")
	if b, err := exec.CommandContext(t.Context(), "go", "build", "-o", sample, "../../examples/shared-trace").CombinedOutput(); err != nil {
		t.Fatalf("build: %s", b)
	}
	stream, prompt, plan := filepath.Join(dir, "stream"), filepath.Join(dir, "prompt"), filepath.Join(dir, "saved.plan")
	input := `{"type":"system","subtype":"init","session_id":"test","claude_code_version":"2.1.318"}
{"type":"assistant","session_id":"test","message":{"content":[{"type":"tool_use","id":"delegate","name":"Agent","input":{"run_in_background":false}}]}}
{"type":"system","subtype":"task_started","session_id":"test","task_id":"child","tool_use_id":"delegate","is_backgrounded":false,"spawn_depth":1}
{"type":"assistant","session_id":"test","agent_id":"child","parent_tool_use_id":"delegate","message":{"content":[{"type":"tool_use","id":"read","name":"Read","input":{"file_path":"missing"}}]}}
{"type":"user","session_id":"test","agent_id":"child","parent_tool_use_id":"delegate","message":{"content":[{"type":"tool_result","tool_use_id":"read","content":"File missing <script>bad()</script>","is_error":true}]}}
{"type":"user","session_id":"test","message":{"content":[{"type":"tool_result","tool_use_id":"delegate","content":"worker saw a failed read"}]}}
{"type":"result","session_id":"test","result":"The file was unavailable.","is_error":false}
`
	if err := os.WriteFile(stream, []byte(input), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(prompt, []byte("Read the requested file"), 0600); err != nil {
		t.Fatal(err)
	}
	producer := func(role string, more ...string) []byte {
		t.Helper()
		args := []string{"--role", role, "--endpoint", origin, "--ca-file", ca, "--project", f.r.Execution.Tenant, "--run", f.r.Owner.Run, "--host-token-file", filepath.Join(dir, "host"), "--alice-token-file", filepath.Join(dir, "alice"), "--bob-token-file", filepath.Join(dir, "bob"), "--plan-file", plan}
		cmd := exec.CommandContext(t.Context(), sample, append(args, more...)...)
		b, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("session process failed: %s", b)
		}
		return b
	}
	var receiptLines []struct {
		TraceURL  string            `json:"trace_url"`
		Execution httpapi.Execution `json:"execution"`
		Span      httpapi.Receipt   `json:"span_receipt"`
		Capture   httpapi.Receipt   `json:"capture_receipt"`
	}
	decode := func(raw []byte) {
		t.Helper()
		receiptLines = nil
		dec := json.NewDecoder(bytes.NewReader(raw))
		for dec.More() {
			var item struct {
				TraceURL  string            `json:"trace_url"`
				Execution httpapi.Execution `json:"execution"`
				Span      httpapi.Receipt   `json:"span_receipt"`
				Capture   httpapi.Receipt   `json:"capture_receipt"`
			}
			if err := dec.Decode(&item); err != nil {
				t.Fatal(err)
			}
			receiptLines = append(receiptLines, item)
		}
	}
	decode(producer("session", "--session-file", stream, "--task-file", prompt))
	if len(receiptLines) != 3 {
		t.Fatal("missing session records")
	}
	first := append([]struct {
		TraceURL  string            `json:"trace_url"`
		Execution httpapi.Execution `json:"execution"`
		Span      httpapi.Receipt   `json:"span_receipt"`
		Capture   httpapi.Receipt   `json:"capture_receipt"`
	}{}, receiptLines...)
	for _, path := range []string{stream, prompt} {
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
	readerHTTP := *server.Client()
	readerHTTP.Timeout = 5 * time.Second
	fresh, err := httpapi.NewClient(origin, tokens["reader"], &readerHTTP, 128<<10, 128<<10)
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	cursor := ""
	for {
		page, err := fresh.Trace(t.Context(), first[0].Execution.Project, first[0].Execution.Trace, cursor)
		if err != nil {
			t.Fatal(err)
		}
		total += len(page.Items)
		if page.Next == "" {
			break
		}
		cursor = page.Next
	}
	if total != 3 {
		t.Fatal("fresh reader missed records")
	}
	for i, item := range first {
		cap, err := fresh.Capture(t.Context(), item.Execution)
		if err != nil {
			t.Fatal(err)
		}
		playback, err := fresh.Playback(t.Context(), item.Execution)
		if err != nil || playback.Data != *cap.Response.Data {
			t.Fatal("saved playback differs", err)
		}
		raw, _ := base64.StdEncoding.DecodeString(playback.Data)
		var observed struct {
			CallID  string `json:"call_id"`
			Payload []byte `json:"payload"`
		}
		if json.Unmarshal(raw, &observed) != nil {
			t.Fatal("bad observation")
		}
		if i == 2 && (!bytes.Contains(observed.Payload, []byte(`"is_error":true`)) || item.Capture.Instance != httpapi.Opaque("bob")) {
			t.Fatal("failure or verified owner lost")
		}
	}
	// A current reader session reaches the new interpretation through ordinary login.
	browser := *server.Client()
	browser.Jar, _ = cookiejar.New(nil)
	browser.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := browser.Get(first[0].TraceURL)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 303 {
		t.Fatal("signed-out read accepted")
	}
	resp, err = browser.Get(origin + resp.Header.Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	fields := url.Values{"credential": {tokens["reader"]}}
	for _, name := range []string{"csrf", "return"} {
		m := regexp.MustCompile(`name="` + name + `" value="([^"]*)"`).FindSubmatch(b)
		if len(m) != 2 {
			t.Fatal("missing login field")
		}
		fields.Set(name, html.UnescapeString(string(m[1])))
	}
	req, _ := http.NewRequest(http.MethodPost, origin+"/login", strings.NewReader(fields.Encode()))
	req.Header.Set("Origin", origin)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err = browser.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 303 || !strings.Contains(resp.Header.Get("Location"), "claude-stream-v1") {
		t.Fatal("session display lost at login")
	}
	for i, item := range first {
		u := origin + "/view/capture?" + url.Values{"project": {item.Execution.Project}, "trace": {item.Execution.Trace}, "span": {item.Execution.Span}, "format": {"claude-stream-v1"}}.Encode()
		resp, err = browser.Get(u)
		if err != nil {
			t.Fatal(err)
		}
		b, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 || !bytes.Contains(b, []byte("Recorded Claude CLI exchange")) {
			t.Fatal("session viewer unavailable")
		}
		if i == 0 && !bytes.Contains(b, []byte("The file was unavailable.")) {
			t.Fatal("final answer unreadable")
		}
		if i == 2 && (!bytes.Contains(b, []byte("Tool is_error: true")) || bytes.Contains(b, []byte("<script>bad()"))) {
			t.Fatal("failure hidden or unsafe display")
		}
	}
	decode(producer("session-retry"))
	for i, item := range receiptLines {
		old := first[i]
		if item.Execution != old.Execution || !item.Span.Duplicate || !item.Capture.Duplicate || item.Capture.Accepted != old.Capture.Accepted || item.Capture.Digest != old.Capture.Digest {
			t.Fatal("retry replaced acceptance")
		}
	}
	f.policy(t, "DELETE FROM "+f.c.table("reader_grants")+" WHERE project=$1 AND principal_id=$2", []byte(f.r.Execution.Tenant), []byte("reader"))
	if _, err := fresh.Playback(t.Context(), first[0].Execution); err == nil {
		t.Fatal("revoked reader accepted")
	}
	t.Log("Claude session fixture: overlapping span/capture delivery, independent owners, reported failed tool state, current trace pages, login-preserved readable answer, exact saved-byte playback, original retry receipts after stream/prompt removal, reader revocation")
}
