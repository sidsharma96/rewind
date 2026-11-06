//go:build unix

package catalog

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sidsharma96/rewind/internal/objectstore"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sidsharma96/rewind/internal/access"
	"github.com/sidsharma96/rewind/internal/httpapi"
	"github.com/sidsharma96/rewind/internal/ingest"
	"github.com/sidsharma96/rewind/internal/record"
)

func TestNativeAgentTaskRetryDoesNotExecute(t *testing.T) {
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
	peer := &recoveryObjects{peer: &publicationObjects{data: make(map[string][]byte), maxBytes: 512 << 10}}
	objects := httptest.NewServer(peer)
	t.Cleanup(objects.Close)
	sdk := s3.New(s3.Options{Region: "auto", BaseEndpoint: aws.String(objects.URL), UsePathStyle: true, HTTPClient: objects.Client(), Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: "synthetic", SecretAccessKey: "synthetic"}, nil
	})})
	obj, err := objectstore.New(sdk, "test-bucket", objectstore.Limits{MaxBytes: 512 << 10, MaxActive: 4, OperationTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	env, err := record.NewEnvelopeCodec(record.EnvelopeLimits{MaxStoredBytes: 512 << 10, MaxExpandedBytes: 512 << 10}, record.DefaultCompressionPolicy())
	if err != nil {
		t.Fatal(err)
	}
	spanCodec, err := record.NewSpanCodec(nativeMetadataCodec(t), env)
	if err != nil {
		t.Fatal(err)
	}
	spans, err := ingest.NewSpans(auth, obj, f.c, ingest.Limits{MaxRecordBytes: 512 << 10, MaxActive: 4}, spanCodec)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := record.NewEnvelopeCodec(record.EnvelopeLimits{MaxStoredBytes: 512 << 10, MaxExpandedBytes: 512 << 10}, record.DefaultCompressionPolicy())
	if err != nil {
		t.Fatal(err)
	}
	codec, err := record.NewCaptureCodec(record.CaptureLimits{MaxMetadataBytes: 512, MaxBodyBytes: 256 << 10}, envelope)
	if err != nil {
		t.Fatal(err)
	}
	captures, err := ingest.NewCaptures(spans, codec)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := ingest.NewTraceReader(spans, ingest.TraceLimits{MaxCandidates: 2, MaxStoredBytes: 1 << 20, OperationTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(nil)
	origin := "https://" + server.Listener.Addr().String()
	handler, err := httpapi.New(origin, httpapi.Limits{MaxActive: 4, MaxLoginActive: 1, MaxRequestBytes: 1 << 20, MaxResponseBytes: 1 << 20, Timeout: 5 * time.Second}, credentials, sessions, registrar, spans, captures, reader, nativeMetadataCodec(t))
	if err != nil {
		t.Fatal(err)
	}
	var captureWrites atomic.Int32
	var drop atomic.Bool
	var dropped atomic.Bool
	server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/capture") && captureWrites.Add(1) == 5 && drop.CompareAndSwap(true, false) {
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
		if b, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build example: %s", b)
		}
	}
	sample, cli, model, tool := filepath.Join(dir, "shared-trace"), filepath.Join(dir, "rewind"), filepath.Join(dir, "claude"), filepath.Join(dir, "concordance")
	build(sample, "../../examples/shared-trace")
	build(cli, "../../cmd/rewind")
	actualModel, actualTool := os.Getenv("REWIND_TEST_CLAUDE_BIN"), os.Getenv("REWIND_TEST_CONCORDANCE_BIN")
	if (actualModel == "") != (actualTool == "") {
		t.Fatal("real task check requires both explicitly selected binaries")
	}
	copyTool := func(from, to string) {
		t.Helper()
		in, err := os.Open(from)
		if err != nil {
			t.Fatal(err)
		}
		out, err := os.OpenFile(to, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0700)
		if err != nil {
			_ = in.Close()
			t.Fatal(err)
		}
		n, e := io.Copy(out, io.LimitReader(in, (512<<20)+1))
		a, b := in.Close(), out.Close()
		if e != nil || a != nil || b != nil || n > 512<<20 {
			t.Fatal("copy selected executable failed")
		}
	}
	if actualModel == "" {
		build(model, "../../examples/shared-trace/testdata/claude")
		build(tool, "../../examples/shared-trace/testdata/concordance")
	} else {
		copyTool(actualModel, model)
		copyTool(actualTool, tool)
	}
	home := dir
	if actualModel != "" {
		home = os.Getenv("HOME")
	}
	childEnv := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + home, "TMPDIR=" + dir}
	for _, key := range []string{"USER", "LOGNAME", "SHELL", "XDG_CONFIG_HOME"} {
		if v, ok := os.LookupEnv(key); ok {
			childEnv = append(childEnv, key+"="+v)
		}
	}
	if actualModel != "" {
		for _, key := range []string{"ANTHROPIC_API_KEY", "PGSERVICE", "PGSERVICEFILE", "PGPASSFILE", "CONCORDANCE_COLLECTION", "CONCORDANCE_R2_ENDPOINT", "CONCORDANCE_R2_BUCKET", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY"} {
			if v, ok := os.LookupEnv(key); ok {
				childEnv = append(childEnv, key+"="+v)
			}
		}
	}
	taskDir, question := filepath.Join(dir, "task"), filepath.Join(dir, "question")
	if err := os.WriteFile(question, []byte("In Kubernetes 1.36, when can container restart rules override the Pod policy and which containers are covered? Use only retrieved evidence; explain unknowns."), 0600); err != nil {
		t.Fatal(err)
	}
	producer := func(role string, args ...string) ([]byte, error) {
		all := []string{"--role", role, "--endpoint", origin, "--ca-file", ca, "--host-token-file", filepath.Join(dir, "host"), "--alice-token-file", filepath.Join(dir, "alice"), "--bob-token-file", filepath.Join(dir, "bob"), "--project", f.r.Execution.Tenant, "--run", f.r.Owner.Run, "--task-dir", taskDir}
		cmd := exec.CommandContext(t.Context(), sample, append(all, args...)...)
		cmd.Env = childEnv
		return cmd.CombinedOutput()
	}
	drop.Store(true)
	data, err := producer("task", "--task-file", question, "--claude-bin", model, "--model", "claude-opus-5-5", "--concordance-bin", tool, "--search-target", "kubernetes", "--search-edition", "1.36")
	if err == nil || !dropped.Load() {
		t.Fatalf("task did not reach confirmed fifth publication reply loss: %s", data)
	}
	var first struct {
		Execution httpapi.Execution `json:"execution"`
	}
	line := bytes.SplitN(data, []byte("\n"), 2)[0]
	if json.Unmarshal(line, &first) != nil {
		t.Fatalf("task emitted no confirmed locator: %s", data)
	}
	for _, path := range []string{model, tool, question} {
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
	runCLI := func(name string, args ...string) []byte {
		t.Helper()
		all := []string{name, "--endpoint", origin, "--ca-file", ca, "--token-file", filepath.Join(dir, "reader"), "--project", f.r.Execution.Tenant, "--trace", first.Execution.Trace}
		cmd := exec.CommandContext(t.Context(), cli, append(all, args...)...)
		cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + dir}
		b, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("fresh reader %s failed: %s", name, b)
		}
		return b
	}
	// Fresh reader receives only its service credential/project/trace. No task files.
	items := map[string]httpapi.Item{}
	owners := map[string]bool{}
	cursor := ""
	for n := 0; n < 8; n++ {
		var page httpapi.Page
		if json.Unmarshal(runCLI("trace", "--cursor", cursor), &page) != nil {
			t.Fatal("invalid page")
		}
		for _, item := range page.Items {
			if item.Receipt == nil {
				t.Fatal("unavailable saved span")
			}
			items[item.Span] = item
			owners[item.Receipt.Instance] = true
		}
		cursor = page.Next
		if cursor == "" {
			break
		}
	}
	if len(items) != 5 || len(owners) != 2 || cursor != "" {
		t.Fatal("task records not discoverable")
	}
	observedCaptures := map[string]httpapi.Capture{}
	for id := range items {
		var c httpapi.Capture
		if json.Unmarshal(runCLI("capture", "--span", id), &c) != nil {
			t.Fatal("invalid capture")
		}
		observedCaptures[id] = c
		output := filepath.Join(dir, "read-"+id)
		runCLI("playback", "--span", id, "--out", output)
		b, err := os.ReadFile(output)
		want, _ := base64.StdEncoding.DecodeString(*c.Response.Data)
		if err != nil || !bytes.Equal(b, want) {
			t.Fatal("playback changed task observation")
		}
	}
	// Only after independent discovery compare each complete saved observation.
	names := []string{"alice.delegate", "bob.choose-search", "concordance.search", "bob.report", "alice.answer"}
	for i, name := range names {
		raw, err := os.ReadFile(filepath.Join(taskDir, fmt.Sprintf("%02d.plan", i+1)))
		if err != nil {
			t.Fatal(err)
		}
		var plans []struct {
			Execution httpapi.Execution     `json:"execution"`
			Capture   httpapi.CaptureInput  `json:"capture"`
			Owner     httpapi.RegisterInput `json:"owner"`
			Span      httpapi.SpanInput     `json:"span"`
		}
		if json.Unmarshal(raw, &plans) != nil || len(plans) != 1 {
			t.Fatal("invalid saved plan")
		}
		p := plans[0]
		c := observedCaptures[p.Execution.Span]
		if !reflect.DeepEqual(c.Request, *p.Capture.Request) || !reflect.DeepEqual(c.Response, *p.Capture.Response) {
			t.Fatal("saved observation differs from independent read")
		}

		if c.Receipt.Instance != p.Owner.Instance || !bytes.Contains(items[p.Execution.Span].Metadata, []byte(name)) {
			t.Fatal("task ownership/name changed")
		}
		if i == 2 && actualModel == "" {
			b, _ := base64.StdEncoding.DecodeString(*c.Response.Data)
			var command struct {
				Exit   *int   `json:"exit_code"`
				Stderr []byte `json:"stderr"`
			}
			if json.Unmarshal(b, &command) != nil || command.Exit == nil || *command.Exit != 17 || string(command.Stderr) != "selected edition unavailable\n" || *c.Response.State != record.BodyPresent {
				t.Fatal("complete failed search lost")
			}
		}
	}
	before := peer.snapshot()
	out, err := producer("task-retry")
	if err != nil {
		t.Fatalf("delivery-only retry needs removed tool/input: %s", out)
	}
	if peer.snapshot() != before {
		t.Fatal("identical task retry used objects")
	}
	dec := json.NewDecoder(bytes.NewReader(out))
	count := 0
	for {
		var got struct {
			Execution httpapi.Execution `json:"execution"`
			Capture   httpapi.Receipt   `json:"capture_receipt"`
		}
		if err := dec.Decode(&got); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal("invalid retry receipt")
		}
		want := observedCaptures[got.Execution.Span].Receipt
		want.Duplicate = true
		if got.Capture != want {
			t.Fatal("retry changed original capture receipt")
		}
		count++
	}
	if count != 5 {
		t.Fatal("retry lost observations")
	}
	if actualModel == "" {
		calls, _ := os.ReadFile(filepath.Join(dir, "model-invocations"))
		if string(calls) != "delegate\nchoose\nreport\nanswer\n" {
			t.Fatal("model calls repeated or failure skipped answer")
		}
	}
	if output := os.Getenv("REWIND_TEST_TASK_EXPORT"); output != "" {
		// Optional private operator evidence, populated only from fresh reader results.
		if err := os.Mkdir(output, 0700); err != nil {
			t.Fatal("evidence directory must be new")
		}
		b, err := json.Marshal(struct {
			Items    map[string]httpapi.Item
			Captures map[string]httpapi.Capture
		}{items, observedCaptures})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(output, "recovered.json"), b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if actualModel != "" {
		t.Log("explicit real task: four model CLI exchanges and Concordance search saved; lost successful final-capture reply; fresh project/trace read before retry; both executables/input removed; original receipts retained with zero object I/O")
	} else {
		t.Log("controlled task: failed search preserved through report/answer; lost final-capture reply; fresh trace-only read; delivery retry with no executables/input retains original receipts and zero object I/O")
	}
}
