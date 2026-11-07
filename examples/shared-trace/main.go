// shared-trace is a small trusted application-host example, not an agent runtime,
// automatic SDK exporter or credential broker. Each producer has separate host
// creation and writer credentials. Retry reuses the private saved plan.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/sidsharma96/rewind/internal/httpapi"
	"github.com/sidsharma96/rewind/internal/record"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type plan struct {
	Execution httpapi.Execution     `json:"execution"`
	Owner     httpapi.RegisterInput `json:"owner"`
	Span      httpapi.SpanInput     `json:"span"`
	Capture   httpapi.CaptureInput  `json:"capture"`
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "shared-trace:", err)
		os.Exit(1)
	}
}
func private(path string, max int) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > int64(max) {
		return nil, errors.New("private input file required")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.New("private input unavailable")
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, int64(max)+1))
	if err != nil || len(b) > max {
		return nil, errors.New("private input exceeds limit")
	}
	return b, nil
}
func save(path string, plans []plan) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return errors.New("plan file must be new")
	}
	writeErr := json.NewEncoder(f).Encode(plans)
	if writeErr == nil {
		writeErr = f.Sync()
	}
	closeErr := f.Close()
	if writeErr != nil || closeErr != nil {
		return errors.New("plan save failed; do not generate another identity to retry")
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return errors.New("plan directory unavailable")
	}
	syncErr := dir.Sync()
	closeErr = dir.Close()
	if syncErr != nil || closeErr != nil {
		return errors.New("plan directory save failed")
	}
	return nil
}
func run(ctx context.Context) error {
	role := flag.String("role", "", "alice, bob, bob-search, retry, task, task-retry, session, session-retry or tool")
	sessionFile := flag.String("session-file", "", "private completed Claude Code stream-json file for session")
	endpoint := flag.String("endpoint", "", "Rewind HTTPS origin")
	writerFile := flag.String("token-file", "", "private producer credential")
	hostFile := flag.String("host-token-file", "", "private trusted host creator credential")
	caFile := flag.String("ca-file", "", "optional trusted service CA")
	project := flag.String("project", "demo", "registered project")
	run := flag.String("run", "demo-run", "registered run")
	traceText := flag.String("trace", "", "Alice's trace ID for Bob")
	parentText := flag.String("parent", "", "Alice's span ID for Bob")
	planFile := flag.String("plan-file", "", "new saved plan; existing plan for retry")
	toolAddress := flag.String("tool", "127.0.0.1:8088", "controlled loopback tool address")
	searchBinary := flag.String("concordance-bin", "", "absolute trusted Concordance executable for bob-search")
	searchTarget := flag.String("search-target", "", "Concordance search target")
	searchEdition := flag.String("search-edition", "", "exact Concordance edition")
	searchQuery := flag.String("search-query", "", "Concordance query")
	task := taskOptions{}
	flag.StringVar(&task.Directory, "task-dir", "", "new private directory; saved directory for task-retry")
	flag.StringVar(&task.InputFile, "task-file", "", "private UTF-8 question file")
	flag.StringVar(&task.ModelBinary, "claude-bin", "", "absolute trusted Claude Code executable")
	flag.StringVar(&task.Model, "model", "", "explicit model name; no fallback")
	flag.StringVar(&task.AliceToken, "alice-token-file", "", "private Alice writer credential")
	flag.StringVar(&task.BobToken, "bob-token-file", "", "private Bob writer credential")
	flag.Parse()
	if flag.NArg() != 0 {
		return errors.New("unexpected arguments")
	}
	if *role == "session" || *role == "session-retry" {
		if *writerFile != "" || *traceText != "" || *parentText != "" || task.Directory != "" || task.ModelBinary != "" || task.Model != "" || *searchBinary != "" || *searchTarget != "" || *searchEdition != "" || *searchQuery != "" {
			return errors.New("session roles take saved stream/prompt and separate delivery credentials")
		}
		task.Project, task.Run, task.Endpoint, task.CAFile, task.HostToken = *project, *run, *endpoint, *caFile, *hostFile
		return sessionCommand(ctx, task, *sessionFile, *planFile, *role == "session-retry")
	}
	if *sessionFile != "" {
		return errors.New("session-file requires role session")
	}
	if *role == "task" || *role == "task-retry" {
		task.Project, task.Run = *project, *run
		task.Search = searchInput{Binary: *searchBinary, Target: *searchTarget, Edition: *searchEdition}
		task.Endpoint, task.CAFile, task.HostToken = *endpoint, *caFile, *hostFile
		if *writerFile != "" || *planFile != "" || *traceText != "" || *parentText != "" || *searchQuery != "" {
			return errors.New("task roles choose their own identities and query; use task-specific flags")
		}
		return taskCommand(ctx, task, *role == "task-retry")
	}
	if task != (taskOptions{}) {
		return errors.New("task flags require role task or task-retry")
	}
	if *role != "bob-search" && *role != "retry" && (*searchBinary != "" || *searchTarget != "" || *searchEdition != "" || *searchQuery != "") {
		return errors.New("Concordance flags require role bob-search")
	}
	if *role == "tool" {
		return tool(ctx, *toolAddress)
	}
	if *role != "alice" && *role != "bob" && *role != "bob-search" && *role != "retry" {
		return errors.New("choose alice, bob, bob-search, retry or tool")
	}
	client, cleanup, err := exampleClients(*endpoint, *caFile)
	if err != nil {
		return err
	}
	defer cleanup()
	host, err := client(*hostFile)
	if err != nil {
		return err
	}
	writer, err := client(*writerFile)
	if err != nil {
		return err
	}
	var plans []plan
	if *role == "retry" {
		b, err := private(*planFile, 1<<20)
		if err != nil {
			return err
		}
		if httpapi.DecodeDocument(b, &plans) != nil || len(plans) == 0 || len(plans) > 2 {
			return errors.New("invalid retry plan")
		}
	} else {
		if _, err := os.Lstat(*planFile); err == nil {
			return errors.New("plan exists; use role retry before invoking the tool again")
		} else if !errors.Is(err, os.ErrNotExist) {
			return errors.New("plan path unavailable")
		}
		var err error
		if *role == "bob-search" {
			plans, err = prepareSearch(ctx, searchInput{Binary: *searchBinary, Target: *searchTarget, Edition: *searchEdition, Query: *searchQuery}, *project, *run, *traceText, *parentText)
		} else {
			plans, err = prepare(ctx, *role, *project, *run, *traceText, *parentText, *toolAddress)
		}
		if err != nil {
			return err
		}
		if err := save(*planFile, plans); err != nil {
			return err
		}
	}
	for i, p := range plans {
		if _, err := host.Register(ctx, p.Execution, p.Owner); err != nil {
			return fmt.Errorf("registration: %w; use role retry with the same plan", err)
		}

		var span, capture httpapi.Receipt
		if i%2 == 0 && *role != "bob-search" {
			capture, err = writer.SaveCapture(ctx, p.Execution, p.Capture)
			if err == nil {
				span, err = writer.SaveSpan(ctx, p.Execution, p.Span)
			}
		} else {
			span, err = writer.SaveSpan(ctx, p.Execution, p.Span)
			if err == nil {
				capture, err = writer.SaveCapture(ctx, p.Execution, p.Capture)
			}
		}
		if err != nil {
			return fmt.Errorf("record delivery: %w; use role retry with the same plan", err)
		}

		if err := json.NewEncoder(os.Stdout).Encode(struct {
			Execution httpapi.Execution `json:"execution"`
			Span      httpapi.Receipt   `json:"span_receipt"`
			Capture   httpapi.Receipt   `json:"capture_receipt"`
		}{p.Execution, span, capture}); err != nil {
			return err
		}
	}
	return nil
}
func prepare(ctx context.Context, role, project, run, traceText, parentText, address string) ([]plan, error) {
	provider := sdktrace.NewTracerProvider()
	defer provider.Shutdown(context.Background())
	tracer := provider.Tracer("rewind-shared-example")
	parent := ctx
	if role == "bob" {
		traceID, err := trace.TraceIDFromHex(traceText)
		if err != nil {
			return nil, err
		}
		spanID, err := trace.SpanIDFromHex(parentText)
		if err != nil {
			return nil, err
		}
		parent = trace.ContextWithRemoteSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled, Remote: true}))
	}
	count := 1
	if role == "bob" {
		count = 2
	}
	var plans []plan
	for i := 0; i < count; i++ {
		start := time.Now()
		name := "handoff"
		if role == "bob" {
			name = "tool-attempt-" + strconv.Itoa(i+1)
		}
		_, span := tracer.Start(parent, name)
		request, response := []byte("calculate 6 * 7"), []byte("delegated to bob")
		state, status := record.BodyPresent, int32(1)
		if role == "bob" {
			var err error
			response, state, err = callTool(ctx, address, request)
			if err != nil {
				status = 2
				span.SetStatus(codes.Error, "tool failed")
			} else {
				span.SetStatus(codes.Ok, "")
			}
		}
		if role == "alice" {
			span.SetStatus(codes.Ok, "")
		}
		span.End()
		spanContext := span.SpanContext()
		parentContext := trace.SpanContextFromContext(parent)
		metadata := map[string]any{"traceId": spanContext.TraceID().String(), "spanId": spanContext.SpanID().String(), "name": name, "kind": 1,
			"flags": uint32(spanContext.TraceFlags()), "startTimeUnixNano": strconv.FormatInt(start.UnixNano(), 10), "endTimeUnixNano": strconv.FormatInt(time.Now().UnixNano(), 10), "status": map[string]any{"code": status}}
		if role == "bob" {
			metadata["kind"] = 3
		}
		if parentContext.IsValid() {
			metadata["parentSpanId"] = parentContext.SpanID().String()
		}
		packet := map[string]any{"resourceSpans": []any{map[string]any{"resource": map[string]any{"attributes": []any{map[string]any{"key": "agent.name", "value": map[string]any{"stringValue": role}}}},
			"scopeSpans": []any{map[string]any{"scope": map[string]any{"name": "rewind-shared-example"}, "spans": []any{metadata}}}}}}
		raw, err := json.Marshal(packet)
		if err != nil {
			return nil, err
		}
		yes := true
		present := record.BodyPresent
		requestData, responseData := base64.StdEncoding.EncodeToString(request), base64.StdEncoding.EncodeToString(response)
		plans = append(plans, plan{Execution: httpapi.Execution{Project: httpapi.Opaque(project), Trace: spanContext.TraceID().String(), Span: spanContext.SpanID().String()},
			Owner:   httpapi.RegisterInput{Run: httpapi.Opaque(run), Instance: httpapi.Opaque(role)},
			Span:    httpapi.SpanInput{Ended: &yes, Metadata: raw},
			Capture: httpapi.CaptureInput{Request: &httpapi.Body{State: &present, Data: &requestData}, Response: &httpapi.Body{State: &state, Data: &responseData}}})
	}
	return plans, nil
}
func loopback(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
func callTool(ctx context.Context, address string, body []byte) ([]byte, record.BodyState, error) {
	if !loopback(address) {
		return nil, record.BodyMissing, errors.New("controlled tool must be loopback")
	}
	transport := &http.Transport{DisableCompression: true, DialContext: (&net.Dialer{Timeout: time.Second}).DialContext, MaxResponseHeaderBytes: 8192, MaxConnsPerHost: 1, ResponseHeaderTimeout: 3 * time.Second, IdleConnTimeout: 3 * time.Second}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+address, bytes.NewReader(body))
	if err != nil {
		return nil, record.BodyMissing, err
	}
	response, err := client.Do(req)
	if err != nil {
		return nil, record.BodyMissing, errors.New("tool transport failed")
	}
	data, readErr := io.ReadAll(io.LimitReader(response.Body, 513))
	_ = response.Body.Close()
	if readErr != nil || len(data) > 512 {
		return data[:min(len(data), 512)], record.BodyTruncated, errors.New("incomplete tool response")
	}
	if response.StatusCode != 200 {
		return data, record.BodyPresent, errors.New("tool refused")
	}
	return data, record.BodyPresent, nil
}
func tool(ctx context.Context, address string) error {
	if !loopback(address) {
		return errors.New("controlled tool must be loopback")
	}
	var calls atomic.Int32
	server := &http.Server{Addr: address, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second, IdleTimeout: 3 * time.Second, MaxHeaderBytes: 8192,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				w.WriteHeader(405)
				return
			}
			r.Body = http.MaxBytesReader(w, r.Body, 1024)
			if _, err := io.Copy(io.Discard, r.Body); err != nil {
				w.WriteHeader(413)
				return
			}
			if calls.Add(1) == 1 {
				w.WriteHeader(503)
				_, _ = io.WriteString(w, "busy")
				return
			}
			_, _ = io.WriteString(w, "42")
		})}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	bounded, err := httpapi.LimitConnections(listener, 8)
	if err != nil {
		_ = listener.Close()
		return err
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(bounded) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		stop, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := server.Shutdown(stop)
		if err != nil {
			_ = server.Close()
		}
		<-done
		return err
	}
}

func exampleClients(endpoint, caFile string) (func(string) (*httpapi.Client, error), func(), error) {
	tc := &tls.Config{MinVersion: tls.VersionTLS12}
	if caFile != "" {
		info, err := os.Stat(caFile)
		if err != nil || info.Size() > 1<<20 {
			return nil, nil, errors.New("invalid CA file")
		}
		b, err := os.ReadFile(caFile)
		if err != nil {
			return nil, nil, errors.New("CA unavailable")
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(b) {
			return nil, nil, errors.New("invalid CA")
		}
		tc.RootCAs = roots
	}
	transport := &http.Transport{DisableCompression: true, TLSClientConfig: tc, TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 10 * time.Second,
		MaxResponseHeaderBytes: 16 << 10, MaxConnsPerHost: 2, IdleConnTimeout: 20 * time.Second}
	client := func(path string) (*httpapi.Client, error) {
		token, err := private(path, 65)
		if err != nil {
			return nil, err
		}
		if len(token) == 65 && token[64] == '\n' {
			token = token[:64]
		}
		return httpapi.NewClient(endpoint, string(token), &http.Client{Transport: transport, Timeout: 20 * time.Second}, 1<<20, 1<<20)
	}
	return client, transport.CloseIdleConnections, nil
}
