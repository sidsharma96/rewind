package catalog

import (
	"context"
	"errors"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sidsharma96/rewind/internal/ingest"
	"github.com/sidsharma96/rewind/internal/objectstore"
	"github.com/sidsharma96/rewind/internal/record"
	"github.com/sidsharma96/rewind/internal/traceid"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

func nativeSpanCodec(t *testing.T, threshold int) record.SpanCodec {
	t.Helper()
	p := record.DefaultCompressionPolicy()
	p.Threshold = threshold
	e, err := record.NewEnvelopeCodec(record.EnvelopeLimits{MaxStoredBytes: 4096, MaxExpandedBytes: 4096}, p)
	if err != nil {
		t.Fatal(err)
	}
	c, err := record.NewSpanCodec(nativeMetadataCodec(t), e)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func nativeEnded(t *testing.T, r ingest.Registration, name string, present bool) record.EndedSpan {
	t.Helper()
	v := record.Span{Context: record.Context{ResourceSchemaURL: "https://resource.example/1", ScopeSchemaURL: "https://scope.example/1"},
		Metadata: &tracepb.Span{TraceId: r.Execution.Trace[:], SpanId: r.Execution.Span[:], Name: name, Kind: -1, Flags: 0x80000000,
			StartTimeUnixNano: 20, EndTimeUnixNano: 10, Links: []*tracepb.Span_Link{{}}}}
	if present {
		v.Context.Resource = &resourcepb.Resource{}
		v.Context.Scope = &commonpb.InstrumentationScope{}
		v.Metadata.Status = &tracepb.Status{}
	}
	ended, err := v.DeclareEnded()
	if err != nil {
		t.Fatal(err)
	}
	return ended
}
func nativeSpans(t *testing.T, c ingest.SpanCatalog, server *httptest.Server, auth ingest.Authorizer, threshold int) *ingest.Spans {
	t.Helper()
	client := s3.New(s3.Options{Region: "auto", BaseEndpoint: aws.String(server.URL), UsePathStyle: true, HTTPClient: server.Client(),
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "synthetic", SecretAccessKey: "synthetic"}, nil
		})})
	objects, err := objectstore.New(client, "test-bucket", objectstore.Limits{MaxBytes: 4096, MaxActive: 4, OperationTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	s, err := ingest.NewSpans(auth, objects, c, ingest.Limits{MaxRecordBytes: 4096, MaxActive: 4}, nativeSpanCodec(t, threshold))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

type spanWriterOnly struct{ publicationAuthorizer }

func (spanWriterOnly) AuthorizeRead(context.Context, string) (string, error) {
	return "", ingest.ErrDenied
}

func TestNativeTypedSpansTwoAgents(t *testing.T) {
	f := database(t)
	alice := f.writer(t)
	bobFixture := f
	bobFixture.r.Owner.Instance = "bob"
	bobFixture.r.Execution.Span = traceid.Span{43}
	bob := bobFixture.writer(t)
	peer := &recoveryObjects{peer: &publicationObjects{data: make(map[string][]byte)}}
	server := httptest.NewServer(peer)
	t.Cleanup(server.Close)
	auth := publicationAuthorizer{f.r.Execution.Tenant, alice}
	s := nativeSpans(t, f.c, server, spanWriterOnly{auth}, 32768)
	a := nativeEnded(t, f.r, "", false)
	first, err := s.WriteSpan(t.Context(), f.r.Execution, a)
	if err != nil {
		t.Fatal(err)
	}
	before := peer.snapshot()
	retry, err := s.WriteSpan(t.Context(), f.r.Execution, a)
	want := first
	want.Duplicate = true
	if err != nil || retry != want || peer.snapshot() != before {
		t.Fatal("identical stored retry used objects or changed receipt", err)
	}
	compressed := nativeSpans(t, freshCatalog(t, f), server, spanWriterOnly{auth}, 1)
	retry, err = compressed.WriteSpan(t.Context(), f.r.Execution, nativeEnded(t, f.r, "", true))
	delta := peer.snapshot().minus(before)
	if err != nil || retry != want || delta != (recoveryTraffic{GET: 1, Response: int64(first.Reference.Size)}) {
		t.Fatal("equivalent compression/presence retry did not read exactly accepted object", delta, err)
	}
	before = peer.snapshot()
	if _, err := compressed.ReadSpan(t.Context(), f.r.Execution); !errors.Is(err, ingest.ErrDenied) {
		t.Fatal("writer gained reads")
	}
	if _, err := compressed.WriteSpan(t.Context(), bobFixture.r.Execution, nativeEnded(t, bobFixture.r, "bob", false)); !errors.Is(err, ingest.ErrDenied) {
		t.Fatal("Alice claimed Bob's execution")
	}
	if _, err := compressed.WriteSpan(t.Context(), bobFixture.r.Execution, a); !errors.Is(err, ingest.ErrInvalid) {
		t.Fatal("catalog key did not bind metadata IDs")
	}
	if peer.snapshot() != before {
		t.Fatal("denial or identity mismatch reached objects")
	}
	b := nativeSpans(t, f.c, server, spanWriterOnly{publicationAuthorizer{f.r.Execution.Tenant, bob}}, 1)
	bEnded := nativeEnded(t, bobFixture.r, "bob", false)
	bView, _ := bEnded.View()
	bView.Metadata.ParentSpanId = f.r.Execution.Span[:]
	if _, err := b.WriteSpan(t.Context(), bobFixture.r.Execution, bEnded); err != nil {
		t.Fatal(err)
	}
	fresh := nativeSpans(t, freshCatalog(t, f), server, auth, 32768)
	for _, r := range []ingest.Registration{f.r, bobFixture.r} {
		view, err := fresh.ReadSpan(t.Context(), r.Execution)
		if err != nil {
			t.Fatal(err)
		}
		trace, id, err := view.IDs()
		if err != nil || trace != r.Execution.Trace || id != r.Execution.Span || view.Metadata.Kind != -1 || view.Metadata.Flags != 0x80000000 || view.Metadata.EndTimeUnixNano != 10 {
			t.Fatal("fresh reader changed execution evidence")
		}
		if r.Owner.Instance == "alice" && view.Metadata.Name != "" {
			t.Fatal("blank name replaced")
		}
		if r.Owner.Instance == "bob" && string(view.Metadata.ParentSpanId) != string(f.r.Execution.Span[:]) {
			t.Fatal("reported shared-trace relationship lost")
		}
	}
	if _, err := compressed.WriteSpan(t.Context(), f.r.Execution, nativeEnded(t, f.r, "new name", false)); !errors.Is(err, ingest.ErrConflict) {
		t.Fatal("changed content replaced original")
	}
	if peer.peer.count() != 2 {
		t.Fatal("known conflict uploaded an orphan candidate")
	}
	if _, err := f.c.Lookup(t.Context(), ingest.Identity{Tenant: f.r.Execution.Tenant, Trace: f.r.Execution.Trace, Span: f.r.Execution.Span, Kind: ingest.CaptureRecord}, "reader"); !errors.Is(err, ingest.ErrNotFound) {
		t.Fatal("span acceptance claimed capture")
	}
	t.Logf("two independently owned spans in one trace; same-byte retry zero object requests; equivalent retry GET=1 body=%d; no implicit reader/capture", first.Reference.Size)
}

type spanCatalogHooks struct {
	*Catalog
	afterInspect  func()
	beforeResolve func()
}

func (c *spanCatalogHooks) InspectSpan(ctx context.Context, id ingest.Identity, a ingest.Attribution) (ingest.Receipt, bool, error) {
	r, found, err := c.Catalog.InspectSpan(ctx, id, a)
	if err == nil && c.afterInspect != nil {
		c.afterInspect()
	}
	return r, found, err
}
func (c *spanCatalogHooks) ResolveSpan(ctx context.Context, ref ingest.Reference, a ingest.Attribution, equal bool) (ingest.Receipt, error) {
	if c.beforeResolve != nil {
		c.beforeResolve()
	}
	return c.Catalog.ResolveSpan(ctx, ref, a, equal)
}

func TestNativeSpanResolutionRechecksAuthorityAndExpiry(t *testing.T) {
	for _, fault := range []string{"grant", "run", "expiry", "reference"} {
		t.Run(fault, func(t *testing.T) {
			f := database(t)
			writer := f.writer(t)
			peer := &publicationObjects{data: make(map[string][]byte)}
			server := httptest.NewServer(peer)
			t.Cleanup(server.Close)
			auth := publicationAuthorizer{f.r.Execution.Tenant, writer}
			s := nativeSpans(t, f.c, server, auth, 32768)
			original, err := s.WriteSpan(t.Context(), f.r.Execution, nativeEnded(t, f.r, "", false))
			if err != nil {
				t.Fatal(err)
			}
			hook := &spanCatalogHooks{Catalog: f.c}
			want := ingest.ErrDenied
			hook.beforeResolve = func() {
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
			}
			if fault == "expiry" {
				want = ingest.ErrExpired
			}
			if fault == "reference" {
				want = ingest.ErrIntegrity
			}
			fresh := nativeSpans(t, hook, server, auth, 1)
			r, err := fresh.WriteSpan(t.Context(), f.r.Execution, nativeEnded(t, f.r, "", true))
			if !errors.Is(err, want) || errors.Is(err, ingest.ErrConflict) || r != (ingest.Receipt{}) {
				t.Fatal("stale comparison confirmed a retry", err)
			}
			if peer.count() != 1 {
				t.Fatal("failed retry wrote candidate")
			}
			if fault == "reference" {
				f.policy(t, "UPDATE "+f.c.table("records")+" SET size=$1", original.Reference.Size)
			}
		})
	}
}

func TestNativeConcurrentSpanEncodingsResolveOneAcceptance(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(map[bool]string{false: "equivalent", true: "different"}[changed], func(t *testing.T) {
			f := database(t)
			writer := f.writer(t)
			second := writer
			second.SubmittedBy = "collector-two"
			f.grantWriter(t, second)
			peer := &publicationObjects{data: make(map[string][]byte)}
			server := httptest.NewServer(peer)
			t.Cleanup(server.Close)
			arrived := make(chan struct{}, 2)
			release := make(chan struct{})
			hook := &spanCatalogHooks{Catalog: f.c, afterInspect: func() { arrived <- struct{}{}; <-release }}
			a := nativeSpans(t, hook, server, spanWriterOnly{publicationAuthorizer{f.r.Execution.Tenant, writer}}, 32768)
			b := nativeSpans(t, hook, server, spanWriterOnly{publicationAuthorizer{f.r.Execution.Tenant, second}}, 1)
			one := nativeEnded(t, f.r, "", false)
			name := ""
			if changed {
				name = "different"
			}
			two := nativeEnded(t, f.r, name, true)
			type reply struct {
				r   ingest.Receipt
				err error
			}
			results := make(chan reply, 2)
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				r, err := a.WriteSpan(t.Context(), f.r.Execution, one)
				results <- reply{r, err}
			}()
			go func() {
				defer wg.Done()
				r, err := b.WriteSpan(t.Context(), f.r.Execution, two)
				results <- reply{r, err}
			}()
			for range 2 {
				select {
				case <-arrived:
				case <-t.Context().Done():
					t.Fatal("preflight barrier failed")
				}
			}
			close(release)
			wg.Wait()
			close(results)
			original, err := f.c.Lookup(t.Context(), spanID(f.r.Execution), "reader")
			if err != nil {
				t.Fatal(err)
			}
			accepted, duplicates, conflicts := 0, 0, 0
			for result := range results {
				if result.err != nil {
					if !errors.Is(result.err, ingest.ErrConflict) {
						t.Fatal(result.err)
					}
					conflicts++
					continue
				}
				if result.r.Duplicate {
					duplicates++
				} else {
					accepted++
				}
				want := original
				want.Duplicate = result.r.Duplicate
				if result.r != want {
					t.Fatal("race changed original receipt")
				}
			}
			if accepted != 1 || (!changed && (duplicates != 1 || conflicts != 0)) || (changed && (duplicates != 0 || conflicts != 1)) {
				t.Fatal("wrong concurrent retry outcomes", accepted, duplicates, conflicts)
			}
		})
	}
}

func spanID(e ingest.Execution) ingest.Identity {
	return ingest.Identity{Tenant: e.Tenant, Trace: e.Trace, Span: e.Span, Kind: ingest.SpanRecord}
}

func TestNativeTypedReadBindsVerifiedBytesToCatalogExecution(t *testing.T) {
	f := database(t)
	writer := f.writer(t)
	peer := &publicationObjects{data: make(map[string][]byte)}
	server := httptest.NewServer(peer)
	t.Cleanup(server.Close)
	auth := publicationAuthorizer{f.r.Execution.Tenant, writer}
	// The legacy opaque primitive can store arbitrary controlled-test bytes.
	// Their valid digest cannot substitute for the typed reader's identity check.
	other := f.r
	other.Execution.Span = traceid.Span{88}
	_, stored, err := nativeSpanCodec(t, 1).Prepare(nativeEnded(t, other, "other", false))
	if err != nil {
		t.Fatal(err)
	}
	opaque := composedStore(t, f.c, server, auth)
	if _, err := opaque.Write(t.Context(), spanID(f.r.Execution), stored); err != nil {
		t.Fatal(err)
	}
	typed := nativeSpans(t, f.c, server, auth, 1)
	if got, err := typed.ReadSpan(t.Context(), f.r.Execution); !errors.Is(err, ingest.ErrIntegrity) || got != (record.Span{}) {
		t.Fatal("valid object digest hid mismatched saved execution", err)
	}
	if r, err := typed.WriteSpan(t.Context(), f.r.Execution, nativeEnded(t, f.r, "", false)); !errors.Is(err, ingest.ErrIntegrity) || errors.Is(err, ingest.ErrConflict) || r != (ingest.Receipt{}) {
		t.Fatal("mismatched accepted execution became a retry comparison", err)
	}
}
