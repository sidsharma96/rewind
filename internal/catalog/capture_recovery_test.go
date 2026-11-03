package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sidsharma96/rewind/internal/ingest"
	"github.com/sidsharma96/rewind/internal/record"
	"github.com/sidsharma96/rewind/internal/traceid"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// These controlled producers are a workload, not a model runtime. Alice records
// a handoff. Bob makes two real HTTP calls; the wrapper exposes a byte result only
// for status 200. Its failed call and later attempt have independent identities.
func captureRecoveryWrite(t *testing.T, ctx context.Context, spans *ingest.Spans, config recoveryConfig, in *json.Decoder, out *json.Encoder) {
	captures := nativeCaptures(t, spans, 1)
	client := &http.Client{Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer client.CloseIdleConnections()
	for _, r := range config.Registrations {
		if r.Owner != config.Registration.Owner || r.Execution.Tenant != config.Registration.Execution.Tenant {
			t.Fatal("producer crossed its scoped owner/project")
		}
		name, code := "handoff", tracepb.Status_STATUS_CODE_OK
		v := record.Capture{Trace: r.Execution.Trace, Span: r.Execution.Span,
			Request:  record.Body{State: record.BodyPresent, Data: []byte("calculate")},
			Response: record.Body{State: record.BodyPresent, Data: []byte("delegated")}}
		if r.Owner.Instance == "bob" {
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, config.ToolURL, bytes.NewReader(v.Request.Data))
			if err != nil {
				t.Fatal("construct tool request failed")
			}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal("controlled tool call failed")
			}
			body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1025))
			closeErr := resp.Body.Close()
			if readErr != nil || closeErr != nil || len(body) > 1024 {
				t.Fatal("tool response did not fit controlled workload")
			}
			if resp.StatusCode != http.StatusOK {
				name, code = "attempt-failed", tracepb.Status_STATUS_CODE_ERROR
				v.Response = record.Body{State: record.BodyMissing}
			} else {
				name = "attempt-succeeded"
				v.Request = record.Body{State: record.BodyRedacted}
				v.Response = record.Body{State: record.BodyPresent, Data: body}
			}
		}
		ended := nativeEnded(t, r, name, false)
		view, _ := ended.View()
		view.Metadata.Status = &tracepb.Status{Code: code}
		if r.Owner.Instance == "bob" {
			view.Metadata.ParentSpanId = []byte{42, 0, 0, 0, 0, 0, 0, 0}
		}
		if _, err := spans.WriteSpan(ctx, r.Execution, ended); err != nil {
			t.Fatal("capture workload span publication failed")
		}
		receipt, err := captures.WriteCapture(ctx, r.Execution, v)
		if config.Fault == "capture-commit-reply" && name == "attempt-succeeded" {
			if !errors.Is(err, ingest.ErrUncertain) || receipt != (ingest.Receipt{}) {
				t.Fatal("lost capture COMMIT reply did not return uncertainty without receipt")
			}
			recoverySend(t, out, recoveryEvent{Phase: "capture-uncertain"})
		} else if err != nil || receipt.Duplicate {
			t.Fatal("capture workload publication failed")
		}
	}
	if config.Fault != "capture-commit-reply" {
		recoverySend(t, out, recoveryEvent{Phase: "capture-acknowledged"})
	}
	var stop recoveryEvent
	_ = in.Decode(&stop)
	t.Fatal("capture writer was not killed at barrier")
}

func captureRecoveryRead(t *testing.T, ctx context.Context, spans *ingest.Spans, config recoveryConfig, out *json.Encoder) {
	if config.Registration != (ingest.Registration{}) || len(config.Registrations) != 0 || config.ToolURL != "" {
		t.Fatal("replacement received producer identity or tool access")
	}
	captures := nativeCaptures(t, spans, 1)
	reader, err := ingest.NewTraceReader(spans, ingest.TraceLimits{MaxCandidates: 1, MaxStoredBytes: 4096, OperationTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	var cursor ingest.TraceCursor
	event := recoveryEvent{Phase: "capture-recovered"}
	for i, name := range []string{"handoff", "attempt-failed", "attempt-succeeded"} {
		page, err := reader.ReadTrace(ctx, config.TraceProject, config.TraceID, cursor)
		if config.Role == "capture-denied" {
			if !errors.Is(err, ingest.ErrDenied) || len(page.Items) != 0 || !page.Next.IsZero() {
				t.Fatal("revoked capture reader discovered trace")
			}
			// Known IDs grant no read authority either.
			if got, err := captures.Playback(ctx, ingest.Execution{Tenant: config.TraceProject, Trace: config.TraceID, Span: traceid.Span{44}}); !errors.Is(err, ingest.ErrDenied) || got.Receipt != (ingest.Receipt{}) || got.Bytes != nil {
				t.Fatal("revoked reader played saved result")
			}
			recoverySend(t, out, recoveryEvent{Phase: "capture-denied"})
			return
		}
		if err != nil || len(page.Items) != 1 || page.Items[0].State != ingest.TraceAvailable || page.Next.IsZero() != (i == 2) {
			t.Fatal("capture replacement discovery failed")
		}
		item := page.Items[0]
		if item.Span.Metadata.Name != name {
			t.Fatal("capture replacement changed reported operation")
		}
		event.Spans = append(event.Spans, item.Receipt)
		if name == "attempt-failed" {
			read, err := captures.ReadCapture(ctx, item.Execution)
			if err != nil || read.Capture.Request.State != record.BodyPresent || read.Capture.Response.State != record.BodyMissing || item.Span.Metadata.GetStatus().GetCode() != tracepb.Status_STATUS_CODE_ERROR {
				t.Fatal("failed attempt lost unavailable result evidence")
			}
			if got, err := captures.Playback(ctx, item.Execution); !errors.Is(err, ingest.ErrResultUnavailable) || got.Bytes != nil || got.Receipt != (ingest.Receipt{}) {
				t.Fatal("failed attempt fabricated a complete result")
			}
			event.Captures = append(event.Captures, read.Receipt)
		} else {
			got, err := captures.Playback(ctx, item.Execution)
			want, state := "delegated", record.BodyPresent
			if name == "attempt-succeeded" {
				want, state = `{"answer":42}`, record.BodyRedacted
			}
			if err != nil || string(got.Bytes) != want || got.RequestState != state || got.Execution != item.Execution || item.Span.Metadata.GetStatus().GetCode() != tracepb.Status_STATUS_CODE_OK {
				t.Fatal("saved result bytes or input availability changed")
			}
			event.Captures = append(event.Captures, got.Receipt)
		}
		cursor = page.Next
	}
	recoverySend(t, out, event)
}

func TestCapturePlaybackTwoAgentProcessRecovery(t *testing.T) {
	for _, fault := range []string{"acknowledged", "capture-commit-reply"} {
		t.Run(fault, func(t *testing.T) {
			f := database(t)
			f.writer(t)
			registrations := []ingest.Registration{f.r}
			for _, key := range []byte{43, 44} {
				bob := f
				bob.r.Owner.Instance = "bob"
				bob.r.Execution.Span = traceid.Span{key}
				bob.writer(t)
				registrations = append(registrations, bob.r)
			}
			peer := &recoveryObjects{peer: &publicationObjects{data: make(map[string][]byte)}}
			server := httptest.NewServer(peer)
			t.Cleanup(server.Close)
			var calls atomic.Int32
			tool := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(io.LimitReader(r.Body, 1025))
				if err != nil || r.Method != http.MethodPost || string(body) != "calculate" {
					http.Error(w, "bad controlled request", http.StatusBadRequest)
					return
				}
				if calls.Add(1) == 1 {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				_, _ = io.WriteString(w, `{"answer":42}`)
			}))
			t.Cleanup(tool.Close)
			for _, group := range [][]ingest.Registration{registrations[:1], registrations[1:]} {
				config := recoveryConfig{DSN: f.pool.Config().ConnString(), Schema: f.c.schema, ObjectURL: server.URL,
					Role: "capture-writer", Registration: group[0], Registrations: group, ToolURL: tool.URL}
				var proxy *recoveryProxy
				if group[0].Owner.Instance == "bob" && fault == "capture-commit-reply" {
					config.Fault = fault
					proxy = newRecoveryProxy(t, f.pool.Config())
					config.ProxyAddress = proxy.address
				}
				child := startRecoveryChild(t, config)
				if proxy != nil {
					child.event(t, "capture-publishing")
					proxy.armed.Store(true)
					if err := child.encode.Encode(recoveryEvent{Resume: true}); err != nil {
						t.Fatal("resume capture publication failed")
					}
					child.event(t, "capture-uncertain")
					if proxy.dropped.Load() != 1 {
						t.Fatal("capture COMMIT reply was not withheld")
					}
				} else {
					child.event(t, "capture-acknowledged")
				}
				child.finish(t, true)
			}
			tool.Close() // No tool is available when the fresh playback process starts.
			if calls.Load() != 2 {
				t.Fatal("producer workload did not make two actual tool calls")
			}
			before := peer.snapshot()
			config := recoveryConfig{DSN: f.pool.Config().ConnString(), Schema: f.c.schema, ObjectURL: server.URL,
				Role: "capture-reader", TraceProject: f.r.Execution.Tenant, TraceID: f.r.Execution.Trace}
			reader := startRecoveryChild(t, config)
			event := reader.event(t, "capture-recovered")
			reader.finish(t, false)
			if len(event.Spans) != 3 || len(event.Captures) != 3 {
				t.Fatal("replacement did not discover all records")
			}
			var transferred int64
			for i, r := range registrations {
				for j, kind := range []ingest.Kind{ingest.SpanRecord, ingest.CaptureRecord} {
					id := spanID(r.Execution)
					id.Kind = kind
					want, err := f.c.Lookup(t.Context(), id, "reader")
					got := event.Spans[i]
					if j == 1 {
						got = event.Captures[i]
					}
					if err != nil || !sameRecoveryReceipt(got, want) || got.Attribution.Owner != r.Owner {
						t.Fatal("replacement changed original acceptance/owner", err)
					}
					transferred += int64(want.Reference.Size)
					if i == 1 && j == 1 {
						transferred += int64(want.Reference.Size) // Inspection plus refusal of failed result.
					}
				}
			}
			if delta := peer.snapshot().minus(before); delta != (recoveryTraffic{GET: 7, Response: transferred}) || calls.Load() != 2 {
				t.Fatal("playback used unexpected objects or executed tool", delta)
			}
			f.policy(t, "DELETE FROM "+f.c.table("reader_grants"))
			before = peer.snapshot()
			config.Role = "capture-denied"
			denied := startRecoveryChild(t, config)
			denied.event(t, "capture-denied")
			denied.finish(t, false)
			if peer.snapshot() != before {
				t.Fatal("revoked reader touched objects")
			}
			t.Log("two independently owned producers; two actual synthetic tool calls; failed and successful attempts have separate IDs; writers killed/reaped and cwd removed; tool stopped; trace-only replacement refused missing result and returned exact saved response with redacted request; GET=7 PUT=0; surviving local services are not host-loss proof")
		})
	}
}
