package catalog

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
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
	"sync/atomic"
	"testing"
	"time"

	"github.com/sidsharma96/rewind/internal/access"
	"github.com/sidsharma96/rewind/internal/httpapi"
	"github.com/sidsharma96/rewind/internal/ingest"
	"github.com/sidsharma96/rewind/internal/record"
	"github.com/sidsharma96/rewind/internal/traceid"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

func TestNativeHTTPSharedTraceRetryAndBrowser(t *testing.T) {
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
	reader, err := ingest.NewTraceReader(spans, ingest.TraceLimits{MaxCandidates: 2, MaxStoredBytes: 4096, OperationTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(nil)
	origin := "https://" + server.Listener.Addr().String()
	handler, err := httpapi.New(origin, httpapi.Limits{MaxActive: 4, MaxLoginActive: 1, MaxRequestBytes: 8192, MaxResponseBytes: 64 << 10, Timeout: 5 * time.Second},
		credentials, sessions, registrar, spans, captures, reader, nativeMetadataCodec(t))
	if err != nil {
		t.Fatal(err)
	}
	var drop atomic.Bool
	previewDir := os.Getenv("REWIND_SERVICE_BROWSER_DIR")
	previewControl, _, _ := access.NewToken()
	previewDone := make(chan struct{})
	var finishPreview sync.Once
	server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if previewDir != "" && r.Method == http.MethodPost && r.URL.Path == "/__preview_finished" && r.Header.Get("X-Preview-Control") == previewControl {
			finishPreview.Do(func() { close(previewDone) })
			w.WriteHeader(204)
			return
		}
		if drop.CompareAndSwap(true, false) {
			// Discard only the application reply, after real SQL/object work.
			handler.ServeHTTP(httptest.NewRecorder(), r)
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = conn.Close()
			return
		}
		handler.ServeHTTP(w, r)
	})
	server.StartTLS()
	t.Cleanup(server.Close)
	newClient := func(principal string) *httpapi.Client {
		t.Helper()
		transport := *server.Client()
		transport.Timeout = 10 * time.Second
		client, err := httpapi.NewClient(origin, tokens[principal], &transport, 8192, 64<<10)
		if err != nil {
			t.Fatal(err)
		}
		return client
	}
	host, alice, bob, readClient := newClient("host"), newClient("alice"), newClient("bob"), newClient("reader")

	emptyBefore := peer.snapshot()
	empty, err := readClient.Trace(t.Context(), httpapi.Opaque(f.r.Execution.Tenant), f.r.Execution.Trace.String(), "")
	if err != nil || len(empty.Items) != 0 || empty.Next != "" || peer.snapshot() != emptyBefore {
		t.Fatal("empty trace miss used objects or lost permission check", err)
	}
	if _, err := readClient.Trace(t.Context(), httpapi.Opaque("other-project"), f.r.Execution.Trace.String(), ""); err == nil || peer.snapshot() != emptyBefore {
		t.Fatal("reader escaped project")
	}
	provider := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	tracer := provider.Tracer("rewind-public-path")
	parent, a := tracer.Start(t.Context(), "handoff")
	_, b := tracer.Start(parent, "failed-attempt")
	_, c := tracer.Start(parent, "successful-attempt")
	a.End()
	b.End()
	c.End()
	contexts := []struct {
		trace traceid.Trace
		span  traceid.Span
	}{
		{traceid.Trace(a.SpanContext().TraceID()), traceid.Span(a.SpanContext().SpanID())},
		{traceid.Trace(b.SpanContext().TraceID()), traceid.Span(b.SpanContext().SpanID())},
		{traceid.Trace(c.SpanContext().TraceID()), traceid.Span(c.SpanContext().SpanID())},
	}
	var executions []httpapi.Execution
	var accepted []httpapi.Receipt
	var capturesIn []httpapi.CaptureInput
	for i, ids := range contexts {
		instance, client, name := "bob", bob, "tool-attempt <script>bad()</script>"
		if i == 0 {
			instance, client, name = "alice", alice, ""
		}
		ex := httpapi.Execution{Project: httpapi.Opaque(f.r.Execution.Tenant), Trace: ids.trace.String(), Span: ids.span.String()}
		executions = append(executions, ex)
		reg, err := host.Register(t.Context(), ex, httpapi.RegisterInput{Run: httpapi.Opaque(f.r.Owner.Run), Instance: httpapi.Opaque(instance)})
		if err != nil || reg.Duplicate {
			t.Fatal("HTTP registration", err)
		}
		r := f.r
		r.Execution.Trace = ids.trace
		r.Execution.Span = ids.span
		r.Owner.Instance = instance
		ended := nativeEnded(t, r, name, false)
		view, _ := ended.View()
		if i == 0 {
			view.Metadata.Status = &tracepb.Status{Code: 999}
		} else if i == 1 {
			view.Metadata.Status = &tracepb.Status{Code: tracepb.Status_STATUS_CODE_ERROR}
		} else {
			view.Metadata.Status = &tracepb.Status{Code: tracepb.Status_STATUS_CODE_OK}
		}
		if i > 0 {
			view.Metadata.ParentSpanId = contexts[0].span[:]
		}
		packet, _ := view.OTLP()
		metadata, err := nativeMetadataCodec(t).Encode(packet)
		if err != nil {
			t.Fatal(err)
		}
		yes := true
		spanIn := httpapi.SpanInput{Ended: &yes, Metadata: metadata}
		missing, present := record.BodyMissing, record.BodyPresent
		empty, result := "", base64.StdEncoding.EncodeToString([]byte{0, 255, byte('0' + i)})
		in := httpapi.CaptureInput{Request: &httpapi.Body{State: &missing, Data: &empty}, Response: &httpapi.Body{State: &present, Data: &result}}
		capturesIn = append(capturesIn, in)
		if i == 1 {
			if _, err := client.SaveSpan(t.Context(), ex, spanIn); err != nil {
				t.Fatal(err)
			}
		}
		if i == 2 {
			drop.Store(true)
		}
		capReceipt, err := client.SaveCapture(t.Context(), ex, in)
		if i == 2 {
			var fail *httpapi.Failure
			if !errors.As(err, &fail) || fail.Outcome != "unknown" {
				t.Fatal("lost reply was not uncertainty", err)
			}
			// Fresh reader discovers acceptance before the producer retries.
			fresh := newClient("reader")
			saved, err := fresh.Playback(t.Context(), ex)
			if err != nil {
				t.Fatal(err)
			}
			capReceipt = saved.Receipt
		} else if err != nil {
			t.Fatal(err)
		}
		accepted = append(accepted, capReceipt)
		if i != 1 {
			if _, err := client.SaveSpan(t.Context(), ex, spanIn); err != nil {
				t.Fatal(err)
			}
		}
	}
	before := peer.snapshot()
	retry, err := bob.SaveCapture(t.Context(), executions[2], capturesIn[2])
	want := accepted[2]
	want.Duplicate = true
	if err != nil || retry != want || peer.snapshot() != before {
		t.Fatal("retry changed receipt or touched objects", err)
	}
	if _, err := alice.SaveCapture(t.Context(), executions[2], capturesIn[2]); err == nil || peer.snapshot() != before {
		t.Fatal("Alice acquired Bob's execution", err)
	}
	if _, err := bob.Trace(t.Context(), executions[0].Project, executions[0].Trace, ""); err == nil {
		t.Fatal("writer gained project-reader access")
	}
	page, err := readClient.Trace(t.Context(), executions[0].Project, executions[0].Trace, "")
	if err != nil || len(page.Items) != 2 || page.Next == "" {
		t.Fatal("bounded discovery", err)
	}
	last, err := newClient("reader").Trace(t.Context(), executions[0].Project, executions[0].Trace, page.Next)
	if err != nil || len(last.Items) != 1 || last.Next != "" {
		t.Fatal("fresh-client continuation", err)
	}
	t.Log("HTTP: two owners, independent capture/span arrival, lost reply, original retry receipt, bounded fresh-reader discovery")

	// Actual CLI subprocesses have no database/provider credentials or inherited
	// HOME configuration. They use only the service origin, CA and reader token.
	commandDir := t.TempDir()
	binary := filepath.Join(commandDir, "rewind")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "../../cmd/rewind")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %s", output)
	}
	certFile := filepath.Join(commandDir, "ca.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	tokenFile := filepath.Join(commandDir, "reader")
	if err := os.WriteFile(tokenFile, []byte(tokens["reader"]), 0600); err != nil {
		t.Fatal(err)
	}
	cli := func(name string, args ...string) []byte {
		t.Helper()
		all := []string{name, "--endpoint", origin, "--token-file", tokenFile, "--ca-file", certFile, "--project", f.r.Execution.Tenant, "--trace", executions[0].Trace}
		all = append(all, args...)
		command := exec.CommandContext(t.Context(), binary, all...)
		command.Env = []string{"HOME=" + commandDir, "TMPDIR=" + commandDir, "PATH=" + os.Getenv("PATH")}
		data, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("CLI %s: %s", name, data)
		}
		return data
	}
	var cliPage httpapi.Page
	if json.Unmarshal(cli("trace"), &cliPage) != nil || len(cliPage.Items) != 2 {
		t.Fatal("CLI lost bounded discovery")
	}
	outputFile := filepath.Join(commandDir, "response.bin")
	var cliReceipt struct {
		RequestState string          `json:"request_state"`
		SavedBytes   int             `json:"saved_bytes"`
		Receipt      httpapi.Receipt `json:"receipt"`
	}
	if json.Unmarshal(cli("playback", "--span", executions[2].Span, "--out", outputFile), &cliReceipt) != nil || cliReceipt.RequestState != "missing" || cliReceipt.SavedBytes != 3 || cliReceipt.Receipt != accepted[2] {
		t.Fatal("CLI discarded playback evidence")
	}
	if data, err := os.ReadFile(outputFile); err != nil || string(data) != string([]byte{0, 255, '2'}) {
		t.Fatal("CLI changed recorded bytes")
	}
	t.Log("CLI: separate process, no storage credentials, bounded trace page, exact saved response and original receipt")
	// Run both ordinary producer programs as independent processes. Their host
	// wrapper is trusted; the agent labels in metadata are never authorization.
	exampleBinary := filepath.Join(commandDir, "shared-trace")
	build = exec.CommandContext(t.Context(), "go", "build", "-o", exampleBinary, "../../examples/shared-trace")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build producer: %s", output)
	}
	for _, principal := range []string{"host", "alice", "bob"} {
		if err := os.WriteFile(filepath.Join(commandDir, principal), []byte(tokens[principal]), 0600); err != nil {
			t.Fatal(err)
		}
	}
	example := func(role, writer, planFile string, args ...string) []byte {
		t.Helper()
		all := []string{"--role", role, "--endpoint", origin, "--ca-file", certFile, "--host-token-file", filepath.Join(commandDir, "host"), "--token-file", filepath.Join(commandDir, writer),
			"--project", f.r.Execution.Tenant, "--run", f.r.Owner.Run, "--plan-file", planFile}
		all = append(all, args...)
		command := exec.CommandContext(t.Context(), exampleBinary, all...)
		command.Env = []string{"HOME=" + commandDir, "TMPDIR=" + commandDir, "PATH=" + os.Getenv("PATH")}
		data, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("producer %s: %s", role, data)
		}
		return data
	}
	var aResult struct {
		Execution httpapi.Execution `json:"execution"`
	}
	if json.Unmarshal(example("alice", "alice", filepath.Join(commandDir, "alice-plan")), &aResult) != nil || aResult.Execution.Trace == "" {
		t.Fatal("Alice did not report SDK IDs")
	}
	var toolCalls atomic.Int32
	tool := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if toolCalls.Add(1) == 1 {
			w.WriteHeader(503)
			_, _ = io.WriteString(w, "busy")
			return
		}
		_, _ = io.WriteString(w, "42")
	}))
	bobPlan := filepath.Join(commandDir, "bob-plan")
	produced := example("bob", "bob", bobPlan, "--trace", aResult.Execution.Trace, "--parent", aResult.Execution.Span, "--tool", strings.TrimPrefix(tool.URL, "http://"))
	tool.Close()
	if toolCalls.Load() != 2 {
		t.Fatal("did not execute failed/successful tool attempts")
	}
	var bobResults []struct {
		Execution httpapi.Execution `json:"execution"`
		Capture   httpapi.Receipt   `json:"capture_receipt"`
	}
	for _, line := range bytes.Split(bytes.TrimSpace(produced), []byte("\n")) {
		var result struct {
			Execution httpapi.Execution `json:"execution"`
			Capture   httpapi.Receipt   `json:"capture_receipt"`
		}
		if json.Unmarshal(line, &result) != nil {
			t.Fatal("producer output malformed")
		}
		bobResults = append(bobResults, result)
	}
	if len(bobResults) != 2 {
		t.Fatal("attempts lost")
	}
	saved, err := newClient("reader").Playback(t.Context(), bobResults[1].Execution)
	if err != nil {
		t.Fatal("read after stopping tool and producer processes", err)
	}
	savedBytes, err := saved.Bytes()
	if err != nil || string(savedBytes) != "42" || saved.Receipt != bobResults[1].Capture {
		t.Fatal("producer result changed")
	}
	before = peer.snapshot()
	_ = example("retry", "bob", bobPlan)
	if peer.snapshot() != before {
		t.Fatal("saved plan retry used object I/O")
	}
	t.Log("Producers: separate SDK-ID processes, failed/successful tool calls, stopped tool/processes, fresh saved-byte playback, same-plan retry without object I/O")
	for i, state := range []record.BodyState{record.BodyMissing, record.BodyRedacted, record.BodyTruncated, record.BodyPresent} {
		ex := httpapi.Execution{Project: executions[0].Project, Trace: (traceid.Trace{77}).String(), Span: (traceid.Span{byte(i + 1)}).String()}
		if _, err := host.Register(t.Context(), ex, httpapi.RegisterInput{Run: httpapi.Opaque(f.r.Owner.Run), Instance: httpapi.Opaque("alice")}); err != nil {
			t.Fatal(err)
		}
		present, empty := record.BodyPresent, ""
		in := httpapi.CaptureInput{Request: &httpapi.Body{State: &present, Data: &empty}, Response: &httpapi.Body{State: &state, Data: &empty}}
		if _, err := alice.SaveCapture(t.Context(), ex, in); err != nil {
			t.Fatal(err)
		}
		observed, err := readClient.Capture(t.Context(), ex)
		if err != nil || observed.Response.State == nil || *observed.Response.State != state {
			t.Fatal("availability state changed")
		}
		saved, err := readClient.Playback(t.Context(), ex)
		if state != record.BodyPresent {
			var failure *httpapi.Failure
			if !errors.As(err, &failure) || failure.Code != "result_unavailable" {
				t.Fatal("unavailable result played back", err)
			}
		} else {
			bytes, decodeErr := saved.Bytes()
			if err != nil || decodeErr != nil || len(bytes) != 0 || saved.RequestState != record.BodyPresent {
				t.Fatal("zero-byte result became missing")
			}
		}
	}

	jar, _ := cookiejar.New(nil)
	browser := *server.Client()
	browser.Jar = jar
	browser.Timeout = 10 * time.Second
	browser.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	get := func(path string) (string, *http.Response) {
		t.Helper()
		resp, err := browser.Get(origin + path)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		return string(data), resp
	}
	csrfPattern := regexp.MustCompile(`name="csrf" value="([a-f0-9]{64})"`)
	csrf := func(html string) string {
		t.Helper()
		m := csrfPattern.FindStringSubmatch(html)
		if len(m) != 2 {
			t.Fatal("missing form challenge")
		}
		return m[1]
	}
	html, resp := get("/login")
	loginPreview := html
	if resp.StatusCode != 200 {
		t.Fatal(resp.StatusCode)
	}
	post := func(path string, form url.Values, from string) *http.Response {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, origin+path, strings.NewReader(form.Encode()))
		req.Header.Set("Origin", from)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		resp, err := browser.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		return resp
	}
	form := url.Values{"csrf": {csrf(html)}, "credential": {tokens["reader"]}}
	if post("/login", form, "https://other.invalid").StatusCode != 400 {
		t.Fatal("cross-origin login accepted")
	}
	resp = post("/login", form, origin)
	if resp.StatusCode != 303 {
		t.Fatal("login failed", resp.StatusCode)
	}
	var session *http.Cookie
	for _, v := range resp.Cookies() {
		if v.Name == "__Host-rewind_session" {
			session = v
		}
	}
	if session == nil || !session.Secure || !session.HttpOnly || session.SameSite != http.SameSiteStrictMode {
		t.Fatal("unsafe session cookie")
	}
	location := "/view?project=" + executions[0].Project + "&trace=" + executions[0].Trace
	html, resp = get(location)
	if resp.StatusCode != 200 || !strings.Contains(html, "unknown span kind -1") || !strings.Contains(html, "Unnamed operation") && !strings.Contains(html, "tool-attempt") {
		t.Fatal("browser lost evidence", resp.StatusCode)
	}
	logoutCSRF := csrf(html)
	if strings.Contains(html, "<script>bad()") {
		t.Fatal("untrusted operation became HTML")
	}
	if dir := os.Getenv("REWIND_SERVICE_PREVIEW_DIR"); dir != "" {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "login.html"), []byte(loginPreview), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "trace.html"), []byte(html), 0600); err != nil {
			t.Fatal(err)
		}
		capturePage, captureResp := get("/view/capture?project=" + executions[2].Project + "&trace=" + executions[2].Trace + "&span=" + executions[2].Span)
		if captureResp.StatusCode != 200 {
			t.Fatal("capture preview unavailable")
		}
		if err := os.WriteFile(filepath.Join(dir, "capture.html"), []byte(capturePage), 0600); err != nil {
			t.Fatal(err)
		}
		css, _ := get("/style.css")
		if err := os.WriteFile(filepath.Join(dir, "style.css"), []byte(css), 0600); err != nil {
			t.Fatal(err)
		}
	}

	if strings.Contains(html, tokens["reader"]) || strings.Contains(html, session.Value) {
		t.Fatal("browser exposed credential")
	}
	writePath := "/api/v1/projects/" + executions[0].Project + "/executions/" + executions[0].Trace + "/" + executions[0].Span + "/capture"
	req, _ := http.NewRequest(http.MethodPut, origin+writePath, strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/json")
	resp, err = browser.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatal("cookie authenticated write")
	}
	resultPath := "/view/response?project=" + executions[2].Project + "&trace=" + executions[2].Trace + "&span=" + executions[2].Span
	result, resp := get(resultPath)
	if resp.StatusCode != 200 || result != string([]byte{0, 255, '2'}) || !strings.HasPrefix(resp.Header.Get("Content-Disposition"), "attachment;") || resp.Header.Get("Rewind-Request-State") != "missing" || resp.Header.Get("Rewind-Receipt") == "" {
		t.Fatal("browser download changed bytes")
	}

	if previewDir != "" {
		if err := os.MkdirAll(previewDir, 0700); err != nil {
			t.Fatal(err)
		}
		for name, data := range map[string]string{"origin.txt": origin, "trace-path.txt": location, "reader-token.txt": tokens["reader"], "control.txt": previewControl} {
			if err := os.WriteFile(filepath.Join(previewDir, name), []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
		}
		t.Log("Actual TLS browser fixture ready; private control files written")
		select {
		case <-previewDone:
		case <-time.After(5 * time.Minute):
			t.Fatal("browser fixture was not released")
		case <-t.Context().Done():
			t.Fatal("browser fixture canceled")
		}
	}
	// A real grant change is checked before an object read, even with a session.
	before = peer.snapshot()
	f.policy(t, "DELETE FROM "+f.c.table("reader_grants"))
	_, resp = get(location)
	if resp.StatusCode != 403 || peer.snapshot() != before {
		t.Fatal("browser cached revoked project permission")
	}
	resp = post("/logout", url.Values{"csrf": {logoutCSRF}}, origin)

	if resp.StatusCode != 303 {
		t.Fatal("logout failed", resp.StatusCode)
	}
	_, resp = get(location)
	if resp.StatusCode != 303 {
		t.Fatal("logged-out browser retained access")
	}
	t.Log("Browser: scoped login, secure cookie, unknown evidence, exact byte download, write refusal, current permission revocation, logout")
}
