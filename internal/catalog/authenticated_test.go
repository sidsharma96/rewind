package catalog

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sidsharma96/rewind/internal/access"
	"github.com/sidsharma96/rewind/internal/ingest"
	"github.com/sidsharma96/rewind/internal/producer"
	"github.com/sidsharma96/rewind/internal/record"
	"github.com/sidsharma96/rewind/internal/traceid"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

func TestAuthenticatedSDKRegistrationWriteDiscoverPlayback(t *testing.T) {
	f := database(t)
	f.seed(t, f.r) // Trusted operator setup, not an authenticated grant endpoint.
	f.policy(t, "DELETE FROM "+f.c.table("creator_grants"))
	for _, instance := range []string{"alice", "bob"} {
		f.policy(t, "INSERT INTO "+f.c.table("host_creator_grants")+" VALUES($1,$2,$3,$4)", []byte(f.r.Execution.Tenant), []byte(f.r.Owner.Run), []byte(instance), []byte("host"))
		f.policy(t, "INSERT INTO "+f.c.table("writer_grants")+" VALUES($1,$2,$3,$4)", []byte(f.r.Execution.Tenant), []byte(f.r.Owner.Run), []byte(instance), []byte(instance))
	}
	f.policy(t, "INSERT INTO "+f.c.table("writer_grants")+" VALUES($1,$2,$3,$4)", []byte(f.r.Execution.Tenant), []byte(f.r.Owner.Run), []byte("alice"), []byte("collector"))
	f.policy(t, "INSERT INTO "+f.c.table("reader_grants")+" VALUES($1,$2)", []byte(f.r.Execution.Tenant), []byte("reader"))

	tokens := make(map[string]string)
	var entries []access.Credential
	for _, principal := range []string{"host", "alice", "bob", "collector", "reader"} {
		token, digest, err := access.NewToken()
		if err != nil {
			t.Fatal(err)
		}
		tokens[principal] = token
		entries = append(entries, access.Credential{Digest: digest, Principal: principal, ExpiresAt: time.Now().Add(time.Hour)})
	}
	credentials, err := access.NewCredentials(8, entries, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	request := func(principal string) context.Context {
		t.Helper()
		ctx, err := credentials.Authenticate(t.Context(), tokens[principal])
		if err != nil {
			t.Fatal("authenticate controlled principal failed")
		}
		return ctx
	}
	authorizer, err := access.NewAuthorizer(f.c)
	if err != nil {
		t.Fatal(err)
	}
	registrar, err := ingest.NewRegistrar(authorizer, f.c, 4)
	if err != nil {
		t.Fatal(err)
	}
	provider := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	tracer := provider.Tracer("rewind-authenticated-fixture")
	parent, alice := tracer.Start(t.Context(), "handoff")
	_, failed := tracer.Start(parent, "attempt-failed")
	_, succeeded := tracer.Start(parent, "attempt-succeeded")
	spans := []trace.Span{alice, failed, succeeded}
	defer func() {
		for _, span := range spans {
			span.End()
		}
	}()
	var registrations []ingest.Registration
	for i, span := range spans {
		instance := "bob"
		if i == 0 {
			instance = "alice"
		}
		host, err := producer.NewHost(registrar, f.r.Execution.Tenant, ingest.Owner{Run: f.r.Owner.Run, Instance: instance})
		if err != nil {
			t.Fatal(err)
		}
		for _, principal := range []string{"alice", "bob", "collector", "reader"} {
			if _, err := host.Register(request(principal), span.SpanContext()); !errors.Is(err, ingest.ErrDenied) {
				t.Fatal("ordinary principal obtained creator permission", err)
			}
		}
		receipt, err := host.Register(request("host"), span.SpanContext())
		if err != nil || receipt.Duplicate || receipt.Registration.RegisteredBy != "host" {
			t.Fatal("trusted host registration failed", err)
		}
		r := receipt.Registration
		if r.Execution.Trace != traceid.Trace(span.SpanContext().TraceID()) || r.Execution.Span != traceid.Span(span.SpanContext().SpanID()) || r.Execution.Trace != traceid.Trace(alice.SpanContext().TraceID()) {
			t.Fatal("helper replaced SDK-generated IDs")
		}
		registrations = append(registrations, r)
	}
	if registrations[0].Execution == registrations[1].Execution || registrations[1].Execution == registrations[2].Execution {
		t.Fatal("separate SDK calls share execution identity")
	}
	var exactGrants int
	if err := f.pool.QueryRow(t.Context(), "SELECT count(*) FROM "+f.c.table("creator_grants")).Scan(&exactGrants); err != nil || exactGrants != 0 {
		t.Fatal("host helper synthesized per-call permission rows")
	}

	peer := &recoveryObjects{peer: &publicationObjects{data: make(map[string][]byte)}}
	server := httptest.NewServer(peer)
	t.Cleanup(server.Close)
	store := nativeSpans(t, f.c, server, authorizer, 32768)
	captures := nativeCaptures(t, store, 32768)
	var spanReceipts, captureReceipts []ingest.Receipt
	for i, r := range registrations {
		name, body, code := "handoff", "delegated", tracepb.Status_STATUS_CODE_OK
		if i == 1 {
			name, body, code = "attempt-failed", "", tracepb.Status_STATUS_CODE_ERROR
		} else if i == 2 {
			name, body = "attempt-succeeded", `{"answer":42}`
		}
		// The SDK supplies real identities/parentage. Payloads remain controlled
		// typed fixtures: this test is not a general SDK-to-OTLP exporter.
		ended := nativeEnded(t, r, name, false)
		view, _ := ended.View()
		view.Metadata.Status = &tracepb.Status{Code: code}
		if i > 0 {
			view.Metadata.ParentSpanId = append([]byte(nil), registrations[0].Execution.Span[:]...)
		}
		capture := record.Capture{Trace: r.Execution.Trace, Span: r.Execution.Span,
			Request: record.Body{State: record.BodyRedacted}, Response: record.Body{State: record.BodyPresent, Data: []byte(body)}}
		if i == 1 {
			capture.Response = record.Body{State: record.BodyMissing}
		}
		before := peer.snapshot()
		wrongPrincipal := "alice"
		if i == 0 {
			wrongPrincipal = "bob"
		}
		if _, err := store.WriteSpan(request(wrongPrincipal), r.Execution, ended); !errors.Is(err, ingest.ErrDenied) {
			t.Fatal("producer claimed another instance's span", err)
		}
		if _, err := captures.WriteCapture(request(wrongPrincipal), r.Execution, capture); !errors.Is(err, ingest.ErrDenied) || peer.snapshot() != before {
			t.Fatal("cross-instance denial reached objects", err)
		}
		for _, principal := range []string{"host", "reader"} {
			if _, err := store.WriteSpan(request(principal), r.Execution, ended); !errors.Is(err, ingest.ErrDenied) {
				t.Fatal("creator/reader permission implied writing", err)
			}
		}
		if i > 0 {
			if _, err := captures.WriteCapture(request("collector"), r.Execution, capture); !errors.Is(err, ingest.ErrDenied) {
				t.Fatal("collector escaped its explicit Alice scope", err)
			}
		}
		if peer.snapshot() != before {
			t.Fatal("non-writer permission reached objects")
		}
		savedSpan, err := store.WriteSpan(request(r.Owner.Instance), r.Execution, ended)
		if err != nil || savedSpan.Attribution != (ingest.Attribution{Owner: r.Owner, SubmittedBy: r.Owner.Instance}) {
			t.Fatal("authenticated span attribution failed", err)
		}
		savedCapture, err := captures.WriteCapture(request(r.Owner.Instance), r.Execution, capture)
		if err != nil || savedCapture.Attribution != savedSpan.Attribution {
			t.Fatal("authenticated capture attribution failed", err)
		}
		spanReceipts = append(spanReceipts, savedSpan)
		captureReceipts = append(captureReceipts, savedCapture)
		if i == 0 {
			want := savedCapture
			want.Duplicate = true
			before := peer.snapshot()
			if got, err := captures.WriteCapture(request("collector"), r.Execution, capture); err != nil || got != want || peer.snapshot() != before {
				t.Fatal("delegated retry changed original attribution or used objects", err)
			}
		}
	}
	before := peer.snapshot()
	for _, principal := range []string{"host", "alice", "bob", "collector"} {
		if _, err := captures.Playback(request(principal), registrations[2].Execution); !errors.Is(err, ingest.ErrDenied) {
			t.Fatal("non-reader acquired playback", err)
		}
	}
	if _, err := captures.Playback(t.Context(), registrations[2].Execution); !errors.Is(err, access.ErrUnauthenticated) || peer.snapshot() != before {
		t.Fatal("unverified request reached objects", err)
	}

	// New credential verifier, catalog pool and coordinators share no request
	// identity cache. Storage still lives: this is not ingest-host destruction.
	credentials, err = access.NewCredentials(8, entries, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	fresh := freshCatalog(t, f)
	freshAuth, _ := access.NewAuthorizer(fresh)
	freshSpans := nativeSpans(t, fresh, server, freshAuth, 1)
	freshCaptures := nativeCaptures(t, freshSpans, 1)
	reader, err := ingest.NewTraceReader(freshSpans, ingest.TraceLimits{MaxCandidates: 4, MaxStoredBytes: 16384, OperationTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	page, err := reader.ReadTrace(request("reader"), f.r.Execution.Tenant, registrations[0].Execution.Trace, ingest.TraceCursor{})
	if err != nil || len(page.Items) != 3 || !page.Next.IsZero() {
		t.Fatal("fresh authenticated reader failed to discover shared trace", err)
	}
	for _, item := range page.Items {
		index := -1
		for i, r := range registrations {
			if r.Execution == item.Execution {
				index = i
			}
		}
		if index < 0 || item.State != ingest.TraceAvailable || item.Receipt != spanReceipts[index] {
			t.Fatal("trace discovery lost original acceptance")
		}
		if index == 1 {
			if got, err := freshCaptures.Playback(request("reader"), item.Execution); !errors.Is(err, ingest.ErrResultUnavailable) || got.Bytes != nil {
				t.Fatal("failed attempt fabricated a result", err)
			}
			continue
		}
		got, err := freshCaptures.Playback(request("reader"), item.Execution)
		want := "delegated"
		if index == 2 {
			want = `{"answer":42}`
		}
		if err != nil || string(got.Bytes) != want || got.RequestState != record.BodyRedacted || got.Receipt != captureReceipts[index] {
			t.Fatal("authenticated playback changed original bytes/state/receipt", err)
		}
	}
	f.policy(t, "DELETE FROM "+f.c.table("reader_grants"))
	before = peer.snapshot()
	if _, err := reader.ReadTrace(request("reader"), f.r.Execution.Tenant, registrations[0].Execution.Trace, ingest.TraceCursor{}); !errors.Is(err, ingest.ErrDenied) {
		t.Fatal("fresh coordinator cached a removed reader grant", err)
	}
	if _, err := freshCaptures.Playback(request("reader"), registrations[2].Execution); !errors.Is(err, ingest.ErrDenied) || peer.snapshot() != before {
		t.Fatal("removed reader permission reached objects", err)
	}
	t.Log("SDK IDs registered unchanged; Alice/Bob isolated; collector attribution preserved; fresh reader recovered three spans and exact saved result; revoked reader made no object request")
}

func TestAuthenticatedPolicyUnavailableIsNotDenial(t *testing.T) {
	f := database(t)
	f.writer(t)
	fresh := freshCatalog(t, f)
	fresh.pool.Close()
	if _, err := fresh.WriterFor(t.Context(), f.r.Execution, "collector"); err == nil || errors.Is(err, ingest.ErrDenied) || errors.Is(err, ingest.ErrNotFound) {
		t.Fatal("unavailable writer policy became a confirmed domain outcome", err)
	}
	if err := fresh.CheckReader(t.Context(), f.r.Execution.Tenant, "reader"); err == nil || errors.Is(err, ingest.ErrDenied) {
		t.Fatal("unavailable reader policy became denial", err)
	}
	if err := fresh.CheckCreator(t.Context(), f.r.Execution, f.r.Owner, f.r.RegisteredBy); err == nil || errors.Is(err, ingest.ErrDenied) {
		t.Fatal("unavailable creator policy became denial", err)
	}
}

func TestAuthenticatedPolicyValidationBeforeDatabase(t *testing.T) {
	c, err := New(&pgxpool.Pool{}, "owned", Limits{1, time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.WriterFor(t.Context(), ingest.Execution{}, "principal"); !errors.Is(err, ingest.ErrInvalid) {
		t.Fatal("invalid writer key reached database")
	}
	if err := c.CheckReader(t.Context(), "project", ""); !errors.Is(err, ingest.ErrInvalid) {
		t.Fatal("invalid reader principal reached database")
	}
	if err := c.CheckCreator(t.Context(), ingest.Execution{}, ingest.Owner{}, "principal"); !errors.Is(err, ingest.ErrInvalid) {
		t.Fatal("invalid creator key reached database")
	}
}
