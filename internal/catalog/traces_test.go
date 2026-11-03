package catalog

import (
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sidsharma96/rewind/internal/ingest"
	"github.com/sidsharma96/rewind/internal/traceid"
)

func TestNativeTracePages(t *testing.T) {
	f := database(t)
	alice := f.writer(t)
	peer := &recoveryObjects{peer: &publicationObjects{data: make(map[string][]byte)}}
	server := httptest.NewServer(peer)
	t.Cleanup(server.Close)
	s := nativeSpans(t, f.c, server, publicationAuthorizer{f.r.Execution.Tenant, alice}, 32768)
	if _, err := s.WriteSpan(t.Context(), f.r.Execution, nativeEnded(t, f.r, "alice", false)); err != nil {
		t.Fatal(err)
	}
	// Earlier keys contain a capture only and a confirmed expired final span.
	for _, key := range []byte{10, 20} {
		other := f
		other.r.Execution.Span = traceid.Span{key}
		writer := other.writer(t)
		id := spanID(other.r.Execution)
		if key == 10 {
			id.Kind = ingest.CaptureRecord
		}
		if _, err := f.c.Publish(t.Context(), other.reference(id.Kind, "synthetic"), writer); err != nil {
			t.Fatal(err)
		}
		if key == 20 {
			f.policy(t, "UPDATE "+f.c.table("records")+" SET expired=true WHERE project=$1 AND trace_id=$2 AND span_id=$3", []byte(id.Tenant), id.Trace[:], id.Span[:])
		}
	}
	r, err := ingest.NewTraceReader(s, ingest.TraceLimits{MaxCandidates: 2, MaxStoredBytes: 8192, OperationTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	before := peer.snapshot()
	first, err := r.ReadTrace(t.Context(), f.r.Execution.Tenant, f.r.Execution.Trace, ingest.TraceCursor{})
	if err != nil || len(first.Items) != 2 || first.Items[0].State != ingest.TraceNoSpan || first.Items[1].State != ingest.TraceExpired || first.Next.IsZero() || peer.snapshot() != before {
		t.Fatal("unavailable prefix", err)
	}
	last, err := r.ReadTrace(t.Context(), f.r.Execution.Tenant, f.r.Execution.Trace, first.Next)
	if err != nil || len(last.Items) != 1 || last.Items[0].Span.Metadata.Name != "alice" || !last.Next.IsZero() {
		t.Fatal("available tail", err)
	}
	// Current grant is checked even for an empty trace and after a cursor exists.
	f.policy(t, "DELETE FROM "+f.c.table("reader_grants")+" WHERE project=$1", []byte(f.r.Execution.Tenant))
	for _, trace := range []traceid.Trace{f.r.Execution.Trace, {99}} {
		if keys, err := f.c.ScanTrace(t.Context(), f.r.Execution.Tenant, trace, traceid.Span{}, 2, "reader"); !errors.Is(err, ingest.ErrDenied) || keys != nil {
			t.Fatal("scan leaked state without reader grant", err)
		}
	}
	if page, err := r.ReadTrace(t.Context(), f.r.Execution.Tenant, f.r.Execution.Trace, first.Next); !errors.Is(err, ingest.ErrDenied) || len(page.Items) != 0 || !page.Next.IsZero() {
		t.Fatal("cursor carried authority", err)
	}
}

func TestNativeTraceScanOrderedPlan(t *testing.T) {
	f := database(t)
	writer := f.writer(t)
	// Populate a capture-only prefix through the same constraints, without
	// object requests. This is a plan/candidate experiment, not a timing run.
	for i := 1; i <= 256; i++ {
		r := f.r
		r.Execution.Span = traceid.Span{byte(i >> 8), byte(i)}
		f.seed(t, r)
		if _, _, err := f.c.RegisterOwnership(t.Context(), r); err != nil {
			t.Fatal(err)
		}
		id := spanID(r.Execution)
		id.Kind = ingest.CaptureRecord
		other := f
		other.r = r
		if _, err := f.c.Publish(t.Context(), other.reference(id.Kind, "capture"), writer); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.pool.Exec(t.Context(), "ANALYZE "+f.c.table("records")); err != nil {
		t.Fatal(err)
	}
	args := []any{[]byte(f.r.Execution.Tenant), f.r.Execution.Trace[:], make([]byte, 8), 3}
	rows, err := f.pool.Query(t.Context(), "EXPLAIN (COSTS OFF) "+f.c.traceQuery(), args...)
	if err != nil {
		t.Fatal(err)
	}
	var plan []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, line)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	joined := strings.Join(plan, "\n")
	t.Log("observed plan (not a physical-work guarantee):\n" + joined)
	// A planner may choose differently on another installation. Correctness and
	// the candidate cap must hold for that plan too; preserve the observed plan
	// as evidence rather than making one optimizer choice an API guarantee.
	keys, err := f.c.ScanTrace(t.Context(), f.r.Execution.Tenant, f.r.Execution.Trace, traceid.Span{}, 3, "reader")
	if err != nil || len(keys) != 3 || keys[0] != (traceid.Span{0, 1}) || keys[2] != (traceid.Span{0, 3}) {
		t.Fatal("capture-only prefix not bounded in candidates", err)
	}
}

func TestTraceIDOnlyProcessRecovery(t *testing.T) {
	f := database(t)
	f.writer(t)
	bob := f
	bob.r.Execution.Span = traceid.Span{80}
	bob.r.Owner.Instance = "bob"
	bob.writer(t)
	peer := &recoveryObjects{peer: &publicationObjects{data: make(map[string][]byte)}}
	server := httptest.NewServer(peer)
	t.Cleanup(server.Close)
	for _, r := range []ingest.Registration{f.r, bob.r} {
		writer := startRecoveryChild(t, recoveryConfig{DSN: f.pool.Config().ConnString(), Schema: f.c.schema, ObjectURL: server.URL, Role: "writer", Fault: "acknowledged", Registration: r, CaptureAccepted: true})
		writer.event(t, "acknowledged")
		writer.finish(t, true) // Kill, reap and remove each writer's private cwd.
	}
	before := peer.snapshot()
	config := recoveryConfig{DSN: f.pool.Config().ConnString(), Schema: f.c.schema, ObjectURL: server.URL, Role: "trace-reader", TraceProject: f.r.Execution.Tenant, TraceID: f.r.Execution.Trace}
	reader := startRecoveryChild(t, config)
	event := reader.event(t, "trace-recovered")
	reader.finish(t, false)
	var stored int64
	for i, r := range []ingest.Registration{f.r, bob.r} {
		want, err := f.c.Lookup(t.Context(), spanID(r.Execution), "reader")
		if err != nil || !sameRecoveryReceipt(event.Receipts[i], want) || want.Attribution.Owner != r.Owner {
			t.Fatal("trace-only replacement lost original attribution", err)
		}
		stored += int64(want.Reference.Size)
	}
	if delta := peer.snapshot().minus(before); delta != (recoveryTraffic{GET: 2, Response: stored}) {
		t.Fatal("trace read fetched more than final spans", delta)
	}
	f.policy(t, "DELETE FROM "+f.c.table("reader_grants")+" WHERE project=$1", []byte(f.r.Execution.Tenant))
	before = peer.snapshot()
	config.Role = "trace-denied"
	denied := startRecoveryChild(t, config)
	denied.event(t, "trace-denied")
	denied.finish(t, false)
	if peer.snapshot() != before {
		t.Fatal("revoked replacement touched objects")
	}
	t.Log("trace-ID-only replacement discovered Alice and Bob after both writers were killed/reaped and their working directories removed; current reader denial made no object request; services survived, not machine-loss proof")
}
