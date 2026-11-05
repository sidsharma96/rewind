//go:build unix

package catalog

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sidsharma96/rewind/internal/access"
	"github.com/sidsharma96/rewind/internal/httpapi"
	"github.com/sidsharma96/rewind/internal/ingest"
	"github.com/sidsharma96/rewind/internal/record"
)

func TestNativeConcordanceCommandCapture(t *testing.T) {
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
	peer := &recoveryObjects{peer: &publicationObjects{data: make(map[string][]byte)}}
	objects := httptest.NewServer(peer)
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
	var drop atomic.Bool
	var dropped atomic.Bool
	server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/capture") && drop.CompareAndSwap(true, false) {
			reply := httptest.NewRecorder()
			handler.ServeHTTP(reply, r)
			if reply.Code != http.StatusOK {
				t.Error("capture reply fault did not reach confirmed publication")
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(reply.Code)
				_, _ = w.Write(reply.Body.Bytes())
				return
			}
			dropped.Store(true)
			connection, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = connection.Close()
			return
		}
		handler.ServeHTTP(w, r)
	})
	server.StartTLS()
	t.Cleanup(server.Close)
	ca := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	build := func(output, source string) {
		t.Helper()
		cmd := exec.CommandContext(t.Context(), "go", "build", "-o", output, source)
		if data, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build test executable: %s", data)
		}
	}
	sample, cli, tool := filepath.Join(dir, "shared-trace"), filepath.Join(dir, "rewind"), filepath.Join(dir, "concordance")
	build(sample, "../../examples/shared-trace")
	build(cli, "../../cmd/rewind")
	actual := os.Getenv("REWIND_TEST_CONCORDANCE_BIN")
	if actual == "" {
		build(tool, "../../examples/shared-trace/testdata/concordance")
	} else {
		// Copy the explicitly provided tool; never rename/remove the operator's binary.
		in, err := os.Open(actual)
		if err != nil {
			t.Fatal("explicit Concordance executable unavailable")
		}
		out, err := os.OpenFile(tool, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0700)
		if err != nil {
			_ = in.Close()
			t.Fatal(err)
		}
		n, copyErr := io.Copy(out, io.LimitReader(in, (128<<20)+1))
		readErr, closeErr := in.Close(), out.Close()
		if copyErr != nil || readErr != nil || closeErr != nil || n > 128<<20 {
			t.Fatal("copy explicit Concordance executable failed")
		}
	}
	example := func(role, writer, plan string, args ...string) ([]byte, error) {
		all := []string{"--role", role, "--endpoint", origin, "--ca-file", ca, "--host-token-file", filepath.Join(dir, "host"), "--token-file", filepath.Join(dir, writer), "--project", f.r.Execution.Tenant, "--run", f.r.Owner.Run, "--plan-file", plan}
		cmd := exec.CommandContext(t.Context(), sample, append(all, args...)...)
		cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + dir, "TMPDIR=" + dir}
		if actual != "" {
			for _, key := range []string{"PGSERVICE", "PGSERVICEFILE", "PGPASSFILE", "CONCORDANCE_COLLECTION", "CONCORDANCE_R2_ENDPOINT", "CONCORDANCE_R2_BUCKET", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY"} {
				if v, ok := os.LookupEnv(key); ok {
					cmd.Env = append(cmd.Env, key+"="+v)
				}
			}
		}
		return cmd.CombinedOutput()
	}
	var a struct {
		Execution httpapi.Execution `json:"execution"`
	}
	data, err := example("alice", "alice", filepath.Join(dir, "alice-plan"))
	if err != nil || json.Unmarshal(data, &a) != nil {
		t.Fatal("Alice handoff failed")
	}
	query := "ContainerRestartRules"
	plan := filepath.Join(dir, "bob-plan")
	drop.Store(true)
	_, err = example("bob-search", "bob", plan, "--trace", a.Execution.Trace, "--parent", a.Execution.Span, "--concordance-bin", tool, "--search-target", "kubernetes", "--search-edition", "1.36", "--search-query", query)
	if err == nil || drop.Load() || !dropped.Load() {
		t.Fatal("lost capture reply did not leave producer uncertain")
	}
	if err := os.Remove(tool); err != nil {
		t.Fatal(err)
	}
	// A fresh CLI gets only project/trace and reader service credentials. It
	// discovers the operation before any writer plan or retry is used.
	runCLI := func(name string, args ...string) []byte {
		t.Helper()
		all := []string{name, "--endpoint", origin, "--ca-file", ca, "--token-file", filepath.Join(dir, "reader"), "--project", f.r.Execution.Tenant, "--trace", a.Execution.Trace}
		cmd := exec.CommandContext(t.Context(), cli, append(all, args...)...)
		cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + dir, "TMPDIR=" + dir}
		data, err := cmd.Output()
		if err != nil {
			var failure *exec.ExitError
			if errors.As(err, &failure) {
				t.Fatalf("fresh reader CLI %s failed: %s", name, failure.Stderr)
			}
			t.Fatal("fresh reader CLI could not start")
		}
		return data
	}
	var bob httpapi.Execution
	owners := map[string]bool{}
	cursor := ""
	for pages := 0; pages < 8; pages++ {
		var page httpapi.Page
		if json.Unmarshal(runCLI("trace", "--cursor", cursor), &page) != nil {
			t.Fatal("invalid trace page")
		}
		for _, item := range page.Items {
			if item.Receipt == nil {
				t.Fatal("accepted span missing")
			}
			owners[item.Receipt.Instance] = true
			if item.Receipt.Instance == httpapi.Opaque("bob") {
				bob = item.Execution
			}
		}
		cursor = page.Next
		if cursor == "" {
			break
		}
	}
	if bob.Span == "" || !owners[httpapi.Opaque("alice")] || !owners[httpapi.Opaque("bob")] || cursor != "" {
		t.Fatal("trace-only discovery lost independent owners")
	}
	responsePath := filepath.Join(dir, "command-response")
	var receipt struct {
		Receipt httpapi.Receipt `json:"receipt"`
	}
	if json.Unmarshal(runCLI("playback", "--span", bob.Span, "--out", responsePath), &receipt) != nil || receipt.Receipt.Instance != httpapi.Opaque("bob") {
		t.Fatal("playback lost Bob's original acceptance")
	}
	body, err := os.ReadFile(responsePath)
	var observed struct {
		Outcome  string `json:"outcome"`
		ExitCode *int   `json:"exit_code"`
		Stdout   []byte `json:"stdout"`
		Stderr   []byte `json:"stderr"`
	}
	if err != nil || json.Unmarshal(body, &observed) != nil || observed.Outcome != "exited" || observed.ExitCode == nil || *observed.ExitCode != 0 || !bytes.Contains(observed.Stdout, []byte(query)) {
		t.Fatal("saved command result is not the completed search")
	}
	// The reader did not receive the producer's plan. The parent may compare it
	// only now, after discovery and playback completed with independent inputs.
	input, err := os.ReadFile(plan)
	var original []struct {
		Capture httpapi.CaptureInput `json:"capture"`
	}
	if err != nil || json.Unmarshal(input, &original) != nil || len(original) != 1 || original[0].Capture.Response.Data == nil {
		t.Fatal("saved producer plan unavailable for comparison")
	}
	exact, err := base64.StdEncoding.DecodeString(*original[0].Capture.Response.Data)
	if err != nil || !bytes.Equal(body, exact) {
		t.Fatal("fresh playback changed opaque command evidence")
	}
	// Only after fresh discovery/read does the producer get its plan again.
	before := peer.snapshot()
	data, err = example("retry", "bob", plan)
	var retried struct {
		Capture httpapi.Receipt `json:"capture_receipt"`
	}
	if err != nil || json.Unmarshal(data, &retried) != nil || peer.snapshot() != before {
		t.Fatal("plan retry reran tool or used object I/O")
	}
	retried.Capture.Duplicate = false
	if retried.Capture != receipt.Receipt {
		t.Fatal("retry changed original acceptance")
	}
	f.policy(t, "DELETE FROM "+f.c.table("reader_grants"))
	transport := server.Client()
	transport.Timeout = 10 * time.Second
	client, err := httpapi.NewClient(origin, tokens["reader"], transport, 1<<20, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	before = peer.snapshot()
	_, err = client.Playback(t.Context(), bob)
	var denied *httpapi.Failure
	if !errors.As(err, &denied) || denied.Code != "denied" || peer.snapshot() != before {
		t.Fatal("revoked reader accessed command evidence")
	}
	if actual == "" {
		t.Log("controlled command fixture: Alice/Bob HTTP producers; actual lost application reply; tool removed before fresh trace-only CLI discovery/playback; same-plan retry preserved original receipt without tool/object I/O; denied reader used no objects")
	} else {
		t.Log("explicit real Concordance CLI: selected-edition search recorded through Alice/Bob HTTP producers; tool copy removed before fresh trace-only read/playback; original receipt preserved after lost application reply; no source-store writes requested")
	}
}
