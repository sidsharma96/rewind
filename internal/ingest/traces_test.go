package ingest

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/sidsharma96/rewind/internal/record"
	"github.com/sidsharma96/rewind/internal/traceid"
)

type traceModel struct {
	*spanModel
	afterScan       func()
	changeKeys      func([]traceid.Span) []traceid.Span
	waitForDeadline bool
}

func (m *traceModel) ScanTrace(ctx context.Context, tenant string, trace traceid.Trace, after traceid.Span, limit int, principal string) ([]traceid.Span, error) {
	if m.waitForDeadline {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	if !m.readers[readerGrant{tenant, principal}] {
		m.mu.Unlock()
		return nil, ErrDenied
	}
	set := make(map[traceid.Span]bool)
	for id := range m.rows {
		if id.Tenant == tenant && id.Trace == trace && bytes.Compare(id.Span[:], after[:]) > 0 {
			set[id.Span] = true
		}
	}
	m.mu.Unlock()
	keys := make([]traceid.Span, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i][:], keys[j][:]) < 0 })
	keys = keys[:min(limit, len(keys))]
	if m.afterScan != nil {
		m.afterScan()
	}
	if m.changeKeys != nil {
		keys = m.changeKeys(keys)
	}
	return keys, nil
}

func traceFixture(t *testing.T, count int) (*TraceReader, *modelObjects, *traceModel) {
	t.Helper()
	s, o, c := typedFixture(t, 32768)
	m := &traceModel{spanModel: c}
	s.catalog, s.store.catalog = m, m
	r, err := NewTraceReader(s, TraceLimits{count, 8192, time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return r, o, m
}

func addTraceSpan(t *testing.T, r *TraceReader, m *traceModel, key byte) Receipt {
	t.Helper()
	id := sampleID
	id.Span = traceid.Span{key}
	provision(t, m.modelCatalog, id, Attribution{sampleOwner, "principal-a"})
	view, _ := endedFixture(t, false, "saved").View()
	view.Metadata.SpanId = id.Span[:]
	ended, err := view.DeclareEnded()
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := r.spans.WriteSpan(t.Context(), executionOf(id), ended)
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}

func tracePage(t *testing.T, r *TraceReader, cursor TraceCursor) TracePage {
	t.Helper()
	p, err := r.ReadTrace(t.Context(), sampleID.Tenant, sampleID.Trace, cursor)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestTracePagesLiveMembershipAndOwnedResults(t *testing.T) {
	r, _, m := traceFixture(t, 1)
	a := addTraceSpan(t, r, m, 20)
	b := addTraceSpan(t, r, m, 80)
	first := tracePage(t, r, TraceCursor{})
	if len(first.Items) != 1 || first.Items[0].Receipt != a || first.Next.IsZero() {
		t.Fatal("first page", first)
	}
	addTraceSpan(t, r, m, 10) // Behind the cursor: only a restart can discover it.
	addTraceSpan(t, r, m, 50) // Ahead of the cursor: the next live page can see it.
	next := tracePage(t, r, first.Next)
	if len(next.Items) != 1 || next.Items[0].Execution.Span != (traceid.Span{50}) || next.Next.IsZero() {
		t.Fatal("late key ahead missing")
	}
	last := tracePage(t, r, next.Next)
	if len(last.Items) != 1 || last.Items[0].Receipt != b || !last.Next.IsZero() {
		t.Fatal("last observed key")
	}
	refresh := tracePage(t, r, TraceCursor{})
	if refresh.Items[0].Execution.Span != (traceid.Span{10}) {
		t.Fatal("restart lost late key behind")
	}
	first.Items[0].Span.Metadata.Name = "caller changed owned graph"
	read, err := r.spans.ReadSpan(t.Context(), first.Items[0].Execution)
	if err != nil || read.Metadata.Name != "saved" {
		t.Fatal("returned graph aliases storage", err)
	}
	// Exhaustion was only an observation, not a permanent completion marker.
	addTraceSpan(t, r, m, 90)
	if p := tracePage(t, r, last.Next); p.Items[0].Execution.Span != (traceid.Span{10}) {
		t.Fatal("zero cursor did not refresh")
	}
}

func TestTraceBudgetsAndUnavailablePrefix(t *testing.T) {
	r, o, m := traceFixture(t, 2)
	a := addTraceSpan(t, r, m, 20)
	b := addTraceSpan(t, r, m, 30)
	// A retained capture-only identity is a candidate, not a made-up final span.
	m.mu.Lock()
	capture := a
	capture.Reference.Identity.Span = traceid.Span{10}
	capture.Reference.Identity.Kind = CaptureRecord
	m.rows[capture.Reference.Identity] = capture
	m.mu.Unlock()
	m.expiry = func(id Identity) error {
		if id.Span == (traceid.Span{20}) {
			return ErrExpired
		}
		return nil
	}
	p := tracePage(t, r, TraceCursor{})
	if len(p.Items) != 2 || p.Items[0].State != TraceNoSpan || p.Items[1].State != TraceExpired || p.Next.IsZero() || o.reads.Load() != 0 {
		t.Fatal("unavailable prefix did not consume bounded candidates", p)
	}
	for _, item := range p.Items {
		if item.Receipt != (Receipt{}) || !reflect.DeepEqual(item.Span, record.Span{}) {
			t.Fatal("unavailable item carries data")
		}
	}
	p = tracePage(t, r, p.Next)
	if len(p.Items) != 1 || p.Items[0].Receipt != b || !p.Next.IsZero() {
		t.Fatal("prefix prevented progress")
	}
	m.expiry = nil
	r.limits.MaxCandidates = 4
	r.limits.MaxStoredBytes = a.Reference.Size
	p = tracePage(t, r, TraceCursor{})
	if len(p.Items) != 2 || p.Items[1].Receipt != a || p.Next.IsZero() {
		t.Fatal("byte cap skipped or consumed next span")
	}
	p = tracePage(t, r, p.Next)
	if len(p.Items) != 1 || p.Items[0].Receipt != b {
		t.Fatal("byte continuation lost span")
	}
	r.limits.MaxStoredBytes = b.Reference.Size - 1
	p, err := r.ReadTrace(t.Context(), sampleID.Tenant, sampleID.Trace, TraceCursor{})
	if !errors.Is(err, ErrTooLarge) || !reflect.DeepEqual(p, TracePage{}) {
		t.Fatal("oversize record silently consumed", err)
	}
}

func TestTraceFailuresNeverReturnPartialPages(t *testing.T) {
	broken := errors.New("controlled backend failure")
	for _, fault := range []string{"deny-empty", "revoke-after-scan", "expiry-commit", "object", "digest", "decode", "ids", "cancel", "deadline"} {
		t.Run(fault, func(t *testing.T) {
			r, o, m := traceFixture(t, 3)
			a := addTraceSpan(t, r, m, 20)
			b := addTraceSpan(t, r, m, 30)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			want := broken
			switch fault {
			case "deny-empty":
				m.rows = make(map[Identity]Receipt)
				m.readers = make(map[readerGrant]bool)
				want = ErrDenied
			case "revoke-after-scan":
				m.afterScan = func() { m.readers = make(map[readerGrant]bool) }
				want = ErrDenied
			case "expiry-commit":
				m.expiry = func(id Identity) error {
					if id == b.Reference.Identity {
						return broken
					}
					return nil
				}
			case "object":
				o.readErr = broken
			case "digest":
				o.changeRead = func(b []byte) []byte { b[0] ^= 1; return b }
				want = ErrIntegrity
			case "decode":
				// Valid reference/digest to invalid saved format must remain an error.
				m.mu.Lock()
				bad := b
				bad.Reference.Digest = a.Reference.Digest
				bad.Reference.Size = a.Reference.Size
				m.rows[b.Reference.Identity] = bad
				m.mu.Unlock()
				want = ErrIntegrity
			case "ids":
				m.changeKeys = func([]traceid.Span) []traceid.Span { return []traceid.Span{{30}, {20}} }
				want = ErrIntegrity
			case "cancel":
				m.afterScan = cancel
				want = context.Canceled
			case "deadline":
				m.waitForDeadline = true
				r.limits.OperationTimeout = time.Millisecond
				want = context.DeadlineExceeded
			}
			p, err := r.ReadTrace(ctx, sampleID.Tenant, sampleID.Trace, TraceCursor{})
			if !errors.Is(err, want) || !reflect.DeepEqual(p, TracePage{}) {
				t.Fatalf("partial or wrong failure: %v %+v", err, p)
			}
			if (fault == "revoke-after-scan" || fault == "deny-empty" || fault == "ids") && o.reads.Load() != 0 {
				t.Fatal("failure performed object I/O")
			}
			// Admission must be released on every failed path.
			if len(r.spans.store.active) != 0 {
				t.Fatal("leaked admission slot")
			}
		})
	}
}

func TestTraceRejectsMalformedCursorAndBackend(t *testing.T) {
	r, o, m := traceFixture(t, 2)
	addTraceSpan(t, r, m, 20)
	for _, cursor := range []TraceCursor{{tenant: "other", trace: sampleID.Trace, after: traceid.Span{1}}, {tenant: sampleID.Tenant, trace: traceid.Trace{2}, after: traceid.Span{1}}, {tenant: sampleID.Tenant, trace: sampleID.Trace}} {
		if p, err := r.ReadTrace(t.Context(), sampleID.Tenant, sampleID.Trace, cursor); !errors.Is(err, ErrInvalid) || !reflect.DeepEqual(p, TracePage{}) {
			t.Fatal("malformed scope accepted")
		}
	}
	for _, keys := range [][]traceid.Span{{{}}, {{20}, {20}}, {{1}, {2}, {3}, {4}}} {
		m.changeKeys = func([]traceid.Span) []traceid.Span { return keys }
		if _, err := r.ReadTrace(t.Context(), sampleID.Tenant, sampleID.Trace, TraceCursor{}); !errors.Is(err, ErrIntegrity) {
			t.Fatal("malformed peer keys accepted", err)
		}
	}
	if o.reads.Load() != 0 {
		t.Fatal("invalid keys caused object reads")
	}
	m.changeKeys = nil
	r.spans.store.active <- struct{}{}
	if _, err := r.ReadTrace(t.Context(), sampleID.Tenant, sampleID.Trace, TraceCursor{}); !errors.Is(err, ErrBusy) {
		t.Fatal("trace page bypassed shared gate")
	}
	if _, err := r.spans.ReadSpan(t.Context(), executionOf(sampleID)); !errors.Is(err, ErrBusy) {
		t.Fatal("known read has separate gate")
	}
	r.spans.store.end()
	if _, err := NewTraceReader(r.spans, TraceLimits{1, 1, 0}); !errors.Is(err, ErrInvalid) {
		t.Fatal("unbounded timeout")
	}
	if _, err := NewTraceReader(nil, r.limits); !errors.Is(err, ErrInvalid) {
		t.Fatal("nil coordinator")
	}
}
