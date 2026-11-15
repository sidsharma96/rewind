//go:build unix

package catalog

import (
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sidsharma96/rewind/internal/access"
	"github.com/sidsharma96/rewind/internal/httpapi"
	"github.com/sidsharma96/rewind/internal/ingest"
	"github.com/sidsharma96/rewind/internal/record"
)

// The actual CLI reads several trace pages and every capture through the service,
// then states coverage, gaps, failures and identical requests explicitly.
func TestNativeDiagnoseCommandShowsCoverageAndGaps(t *testing.T) {
	f := database(t)
	f.seed(t, f.r)
	f.policy(t, "DELETE FROM "+f.c.table("creator_grants"))
	for _, instance := range []string{"alice", "bob"} {
		f.policy(t, "INSERT INTO "+f.c.table("host_creator_grants")+" VALUES($1,$2,$3,$4)", []byte(f.r.Execution.Tenant), []byte(f.r.Owner.Run), []byte(instance), []byte("host"))
		f.policy(t, "INSERT INTO "+f.c.table("writer_grants")+" VALUES($1,$2,$3,$4)", []byte(f.r.Execution.Tenant), []byte(f.r.Owner.Run), []byte(instance), []byte(instance))
	}
	f.policy(t, "INSERT INTO "+f.c.table("reader_grants")+" VALUES($1,$2)", []byte(f.r.Execution.Tenant), []byte("reader"))
	dir := t.TempDir()
	tokens := map[string]string{}
	var entries []access.Credential
	for _, principal := range []string{"host", "alice", "bob", "reader"} {
		token, digest, err := access.NewToken()
		if err != nil {
			t.Fatal(err)
		}
		tokens[principal] = token
		entries = append(entries, access.Credential{Digest: digest, Principal: principal, ExpiresAt: time.Now().Add(time.Hour)})
		if err := os.WriteFile(filepath.Join(dir, principal), []byte(token), 0600); err != nil {
			t.Fatal(err)
		}
	}
	credentials, err := access.NewCredentials(4, entries, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := access.NewSessions(credentials, 8, time.Minute)
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
	objects := httptest.NewServer(&publicationObjects{data: make(map[string][]byte)})
	t.Cleanup(objects.Close)
	spans := nativeSpans(t, f.c, objects, auth, 32768)
	envelope, err := record.NewEnvelopeCodec(record.EnvelopeLimits{MaxStoredBytes: 4096, MaxExpandedBytes: 4096}, record.DefaultCompressionPolicy())
	if err != nil {
		t.Fatal(err)
	}
	codec, err := record.NewCaptureCodec(record.CaptureLimits{MaxMetadataBytes: 512, MaxBodyBytes: 2048}, envelope)
	if err != nil {
		t.Fatal(err)
	}
	captures, err := ingest.NewCaptures(spans, codec)
	if err != nil {
		t.Fatal(err)
	}
	// Two candidates per page forces the command to follow continuations.
	reader, err := ingest.NewTraceReader(spans, ingest.TraceLimits{MaxCandidates: 2, MaxStoredBytes: 16384, OperationTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(nil)
	origin := "https://" + server.Listener.Addr().String()
	handler, err := httpapi.New(origin, httpapi.Limits{MaxActive: 4, MaxLoginActive: 1, MaxRequestBytes: 1 << 20, MaxResponseBytes: 1 << 20, Timeout: 5 * time.Second}, credentials, sessions, registrar, spans, captures, reader, nativeMetadataCodec(t))
	if err != nil {
		t.Fatal(err)
	}
	server.Config.Handler = handler
	server.StartTLS()
	t.Cleanup(server.Close)
	ca := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	client := func(principal string) *httpapi.Client {
		hc := *server.Client()
		hc.Timeout = 5 * time.Second
		c, err := httpapi.NewClient(origin, tokens[principal], &hc, 1<<20, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}

	trace := "4bf92f3577b34da6a3ce929d0e0e4736"
	base := time.Now().Add(-time.Minute)
	type step struct {
		span, owner, name, parent, link string
		offset                          time.Duration
		status                          int
		request, response               string
		noCapture, noSpan               bool
	}
	steps := []step{
		{span: "00000000000000a1", owner: "alice", name: "alice.plan", offset: 0, status: 1, request: "plan the audit", response: "bob: query cart-sync"},
		{span: "00000000000000b2", owner: "bob", name: "metricsctl.query", parent: "00000000000000a1", offset: time.Second, status: 2, request: "metricsctl query --service cart-sync", response: "busy\x1b[2J"},
		{span: "00000000000000b3", owner: "bob", name: "metricsctl.query", parent: "00000000000000a1", offset: 2 * time.Second, status: 2, request: "metricsctl query --service cart-sync", response: "busy again"},
		{span: "00000000000000a4", owner: "alice", name: "alice.answer", link: "00000000000000ff", offset: 3 * time.Second, status: 1, noCapture: true},
		{span: "00000000000000b5", owner: "bob", name: "bob.unfinished", noSpan: true, request: "started", response: "partial"},
	}
	project := httpapi.Opaque(f.r.Execution.Tenant)
	ctx := t.Context()
	for _, s := range steps {
		e := httpapi.Execution{Project: project, Trace: trace, Span: s.span}
		if _, err := client("host").Register(ctx, e, httpapi.RegisterInput{Run: httpapi.Opaque(f.r.Owner.Run), Instance: httpapi.Opaque(s.owner)}); err != nil {
			t.Fatal(err)
		}
		if !s.noSpan {
			start := base.Add(s.offset)
			m := map[string]any{"traceId": trace, "spanId": s.span, "name": s.name, "kind": 3,
				"startTimeUnixNano": strconv.FormatInt(start.UnixNano(), 10), "endTimeUnixNano": strconv.FormatInt(start.Add(500*time.Millisecond).UnixNano(), 10),
				"status": map[string]any{"code": s.status}}
			if s.parent != "" {
				m["parentSpanId"] = s.parent
			}
			if s.link != "" {
				m["links"] = []any{map[string]any{"traceId": trace, "spanId": s.link}}
			}
			packet, _ := json.Marshal(map[string]any{"resourceSpans": []any{map[string]any{"scopeSpans": []any{map[string]any{"spans": []any{m}}}}}})
			yes := true
			if _, err := client(s.owner).SaveSpan(ctx, e, httpapi.SpanInput{Ended: &yes, Metadata: packet}); err != nil {
				t.Fatal(err)
			}
		}
		if !s.noCapture {
			present := record.BodyPresent
			req, res := base64.StdEncoding.EncodeToString([]byte(s.request)), base64.StdEncoding.EncodeToString([]byte(s.response))
			if _, err := client(s.owner).SaveCapture(ctx, e, httpapi.CaptureInput{Request: &httpapi.Body{State: &present, Data: &req}, Response: &httpapi.Body{State: &present, Data: &res}}); err != nil {
				t.Fatal(err)
			}
		}
	}

	cli := filepath.Join(dir, "rewind")
	if out, err := exec.CommandContext(ctx, "go", "build", "-o", cli, "../../cmd/rewind").CombinedOutput(); err != nil {
		t.Fatalf("build test executable: %s", out)
	}
	diagnose := func(token string, extra ...string) (string, error) {
		args := append([]string{"diagnose", "--endpoint", origin, "--token-file", filepath.Join(dir, token), "--ca-file", ca,
			"--project", f.r.Execution.Tenant, "--trace", trace}, extra...)
		out, err := exec.CommandContext(ctx, cli, args...).CombinedOutput()
		return string(out), err
	}
	out, err := diagnose("reader")
	if err != nil {
		t.Fatalf("diagnose failed: %v\n%s", err, out)
	}
	for _, want := range []string{
		"Read status: the last page reported no continuation.",
		"items: 5 (available 4, no final span 1, expired 0)",
		"captures: saved 4, no capture saved 1, capture expired 0, capture read failed 0, not read 0",
		"spans with no capture saved: #004",
		"link 00000000000000ff (not found in this read) of #004",
		"error (code 2): metricsctl.query x2 -> #002, #003",
		"same owner bob, metricsctl.query x2: #002 ERROR, #003 ERROR",
		"#005 item state no_span span=00000000000000b5",
		"    | partial",
		`busy\u{1b}[2J`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in\n%s", want, out)
		}
	}
	if strings.Contains(out, "\x1b") {
		t.Fatal("terminal control byte reached output")
	}
	if strings.Index(out, "alice.plan") > strings.Index(out, "alice.answer") {
		t.Fatal("not in reported start order")
	}

	limited, err := diagnose("reader", "--max-spans", "3")
	if err != nil || !strings.Contains(limited, "STOPPED (span limit 3 reached; more items exist)") || !strings.Contains(limited, "items: 3 ") {
		t.Fatalf("span limit not explicit: %v\n%s", err, limited)
	}
	if denied, err := diagnose("alice"); err == nil || strings.Contains(denied, "REWIND DIAGNOSIS VIEW") {
		t.Fatalf("writer credential read the trace:\n%s", denied)
	}
}
