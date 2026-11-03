package catalog

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/sidsharma96/rewind/internal/ingest"
	"github.com/sidsharma96/rewind/internal/record"
)

func nativeCaptureCodec(t *testing.T, threshold int) record.CaptureCodec {
	t.Helper()
	p := record.DefaultCompressionPolicy()
	p.Threshold = threshold
	e, err := record.NewEnvelopeCodec(record.EnvelopeLimits{MaxStoredBytes: 4096, MaxExpandedBytes: 4096}, p)
	if err != nil {
		t.Fatal(err)
	}
	c, err := record.NewCaptureCodec(record.CaptureLimits{MaxMetadataBytes: 512, MaxBodyBytes: 1024}, e)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func nativeCapture(r ingest.Registration) record.Capture {
	return record.Capture{Trace: r.Execution.Trace, Span: r.Execution.Span,
		Request:  record.Body{State: record.BodyRedacted},
		Response: record.Body{State: record.BodyPresent, Data: []byte{0, 255, 'r'}}}
}

func nativeCaptures(t *testing.T, spans *ingest.Spans, threshold int) *ingest.Captures {
	t.Helper()
	c, err := ingest.NewCaptures(spans, nativeCaptureCodec(t, threshold))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func captureID(e ingest.Execution) ingest.Identity {
	id := spanID(e)
	id.Kind = ingest.CaptureRecord
	return id
}

func TestNativeCaptureOriginalReceiptAndIndependentArrival(t *testing.T) {
	f := database(t)
	writer := f.writer(t)
	peer := &recoveryObjects{peer: &publicationObjects{data: make(map[string][]byte)}}
	server := httptest.NewServer(peer)
	t.Cleanup(server.Close)
	auth := publicationAuthorizer{f.r.Execution.Tenant, writer}
	s := nativeSpans(t, f.c, server, spanWriterOnly{auth}, 32768)
	c := nativeCaptures(t, s, 32768)
	v := nativeCapture(f.r)
	original, err := c.WriteCapture(t.Context(), f.r.Execution, v)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.c.Lookup(t.Context(), spanID(f.r.Execution), "reader"); !errors.Is(err, ingest.ErrNotFound) {
		t.Fatal("capture invented a final span")
	}
	before := peer.snapshot()
	want := original
	want.Duplicate = true
	if got, err := c.WriteCapture(t.Context(), f.r.Execution, v); err != nil || got != want || peer.snapshot() != before {
		t.Fatal("exact retry changed acceptance or used objects", err)
	}
	compressed := nativeCaptures(t, nativeSpans(t, freshCatalog(t, f), server, spanWriterOnly{auth}, 1), 1)
	if got, err := compressed.WriteCapture(t.Context(), f.r.Execution, v); err != nil || got != want {
		t.Fatal("logical retry changed original receipt", err)
	}
	if delta := peer.snapshot().minus(before); delta != (recoveryTraffic{GET: 1, Response: int64(original.Reference.Size)}) {
		t.Fatal("logical retry must read exactly the original", delta)
	}
	before = peer.snapshot()
	if _, err := c.Playback(t.Context(), f.r.Execution); !errors.Is(err, ingest.ErrDenied) || peer.snapshot() != before {
		t.Fatal("writer acquired reader authority", err)
	}
	for _, state := range []record.BodyState{record.BodyMissing, record.BodyTruncated} {
		changed := v
		changed.Request.State = state
		if _, err := compressed.WriteCapture(t.Context(), f.r.Execution, changed); !errors.Is(err, ingest.ErrConflict) {
			t.Fatal("changed state accepted", err)
		}
	}
	if peer.peer.count() != 1 {
		t.Fatal("known conflict uploaded a candidate")
	}
	if _, err := s.WriteSpan(t.Context(), f.r.Execution, nativeEnded(t, f.r, "tool", false)); err != nil {
		t.Fatal(err)
	}
	fresh := nativeCaptures(t, nativeSpans(t, freshCatalog(t, f), server, auth, 1), 1)
	got, err := fresh.Playback(t.Context(), f.r.Execution)
	if err != nil || got.Receipt != original || got.RequestState != record.BodyRedacted || string(got.Bytes) != string(v.Response.Data) {
		t.Fatal("saved result lost withheld input or original acceptance", err)
	}
}

type captureCatalogHooks struct {
	*Catalog
	beforeResolve func()
}

func (c *captureCatalogHooks) ResolveCapture(ctx context.Context, ref ingest.Reference, a ingest.Attribution, equal bool) (ingest.Receipt, error) {
	c.beforeResolve()
	return c.Catalog.ResolveCapture(ctx, ref, a, equal)
}

func TestNativeCaptureResolutionRechecksAuthorityAndExpiry(t *testing.T) {
	for _, fault := range []string{"grant", "run", "expiry", "reference"} {
		t.Run(fault, func(t *testing.T) {
			f := database(t)
			writer := f.writer(t)
			peer := &publicationObjects{data: make(map[string][]byte)}
			server := httptest.NewServer(peer)
			t.Cleanup(server.Close)
			auth := publicationAuthorizer{f.r.Execution.Tenant, writer}
			original, err := nativeCaptures(t, nativeSpans(t, f.c, server, auth, 32768), 32768).WriteCapture(t.Context(), f.r.Execution, nativeCapture(f.r))
			if err != nil {
				t.Fatal(err)
			}
			hook := &captureCatalogHooks{Catalog: f.c, beforeResolve: func() {
				switch fault {
				case "grant":
					f.policy(t, "DELETE FROM "+f.c.table("writer_grants"))
				case "run":
					f.policy(t, "UPDATE "+f.c.table("runs")+" SET phase='revoked'")
				case "expiry":
					f.policy(t, "UPDATE "+f.c.table("records")+" SET expired=true")
				case "reference":
					f.policy(t, "UPDATE "+f.c.table("records")+" SET size=size+1")
				}
			}}
			want := ingest.ErrDenied
			if fault == "expiry" {
				want = ingest.ErrExpired
			}
			if fault == "reference" {
				want = ingest.ErrIntegrity
			}
			fresh := nativeCaptures(t, nativeSpans(t, hook, server, auth, 1), 1)
			if r, err := fresh.WriteCapture(t.Context(), f.r.Execution, nativeCapture(f.r)); !errors.Is(err, want) || errors.Is(err, ingest.ErrConflict) || r != (ingest.Receipt{}) {
				t.Fatal("stale comparison confirmed retry", err)
			}
			if peer.count() != 1 {
				t.Fatal("failed retry uploaded a candidate")
			}
			if fault == "reference" {
				f.policy(t, "UPDATE "+f.c.table("records")+" SET size=$1", original.Reference.Size)
			}
		})
	}
}

func TestNativeCaptureKindGuards(t *testing.T) {
	// Nil catalogs prove the kind guards run before any transaction or I/O.
	c := (*Catalog)(nil)
	span := ingest.Identity{Kind: ingest.SpanRecord}
	capture := ingest.Identity{Kind: ingest.CaptureRecord}
	if _, _, err := c.InspectCapture(t.Context(), span, ingest.Attribution{}); !errors.Is(err, ingest.ErrInvalid) {
		t.Fatal(err)
	}
	if _, _, err := c.PublishCapture(t.Context(), ingest.Reference{Identity: span}, ingest.Attribution{}); !errors.Is(err, ingest.ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := c.ResolveCapture(t.Context(), ingest.Reference{Identity: span}, ingest.Attribution{}, true); !errors.Is(err, ingest.ErrInvalid) {
		t.Fatal(err)
	}
	if _, _, err := c.InspectSpan(t.Context(), capture, ingest.Attribution{}); !errors.Is(err, ingest.ErrInvalid) {
		t.Fatal(err)
	}
	if _, _, err := c.PublishSpan(t.Context(), ingest.Reference{Identity: capture}, ingest.Attribution{}); !errors.Is(err, ingest.ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := c.ResolveSpan(t.Context(), ingest.Reference{Identity: capture}, ingest.Attribution{}, true); !errors.Is(err, ingest.ErrInvalid) {
		t.Fatal(err)
	}
}
