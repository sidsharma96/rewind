package catalog

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sidsharma96/rewind/internal/ingest"
	"github.com/sidsharma96/rewind/internal/objectstore"
	"github.com/sidsharma96/rewind/internal/record"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

// This peer stores only synthetic bytes in memory. The actual object adapter and
// SDK execute, but neither its memory nor this HTTP server establishes R2 durability.
// Native registration/publication/lookup use the same disposable PostgreSQL schema.
// Real credential verification is absent: the fixture below is a controlled capability.
type publicationObjects struct {
	mu       sync.Mutex
	data     map[string][]byte
	puts     int
	afterPut func() error
}

func peerError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, "<Error><Code>"+code+"</Code></Error>")
}
func (p *publicationObjects) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 ") {
		peerError(w, 403, "AccessDenied")
		return
	}
	switch r.Method {
	case "GET":
		p.mu.Lock()
		data, ok := p.data[r.URL.Path]
		data = bytes.Clone(data)
		p.mu.Unlock()
		if !ok {
			peerError(w, 404, "NoSuchKey")
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		_, _ = w.Write(data)
	case "PUT":
		if r.Header.Get("If-None-Match") != "*" {
			peerError(w, 400, "InvalidRequest")
			return
		}
		data, err := io.ReadAll(io.LimitReader(r.Body, 4097))
		if err != nil || len(data) > 4096 {
			peerError(w, 400, "InvalidRequest")
			return
		}
		p.mu.Lock()
		_, exists := p.data[r.URL.Path]
		if !exists {
			p.data[r.URL.Path] = data
			p.puts++
		}
		p.mu.Unlock()
		if exists {
			peerError(w, 412, "PreconditionFailed")
			return
		}
		if p.afterPut != nil {
			if err := p.afterPut(); err != nil {
				peerError(w, 500, "InternalError")
				return
			}
		}
		w.WriteHeader(http.StatusOK)
	default:
		peerError(w, 405, "MethodNotAllowed")
	}
}
func (p *publicationObjects) count() int { p.mu.Lock(); defer p.mu.Unlock(); return p.puts }

// The test principal is already authenticated by construction. These methods do
// not implement a credential verifier or production grant-control endpoint.
type publicationAuthorizer struct {
	project string
	writer  ingest.Attribution
}

func (a publicationAuthorizer) AuthorizeWrite(_ context.Context, e ingest.Execution) (ingest.Attribution, error) {
	if e.Tenant != a.project {
		return ingest.Attribution{}, ingest.ErrDenied
	}
	return a.writer, nil
}
func (a publicationAuthorizer) AuthorizeRead(_ context.Context, project string) (string, error) {
	if project != a.project {
		return "", ingest.ErrDenied
	}
	return "reader", nil
}
func composedStore(t *testing.T, c ingest.Catalog, server *httptest.Server, authorizer publicationAuthorizer) *ingest.Store {
	t.Helper()
	client := s3.New(s3.Options{Region: "auto", BaseEndpoint: aws.String(server.URL), UsePathStyle: true, HTTPClient: server.Client(), Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: "synthetic", SecretAccessKey: "synthetic"}, nil
	})})
	objects, err := objectstore.New(client, "test-bucket", objectstore.Limits{MaxBytes: 4096, MaxActive: 4, OperationTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	store, err := ingest.New(authorizer, objects, c, ingest.Limits{MaxRecordBytes: 4096, MaxActive: 4})
	if err != nil {
		t.Fatal(err)
	}
	return store
}
func spanPayload(t *testing.T, f fixture) []byte {
	t.Helper()
	view := record.Span{Metadata: &tracepb.Span{TraceId: f.r.Execution.Trace[:], SpanId: f.r.Execution.Span[:], Name: "batch-worker", StartTimeUnixNano: 1, EndTimeUnixNano: 2, Status: &tracepb.Status{Code: tracepb.Status_STATUS_CODE_ERROR, Message: "synthetic failed call"}, Links: []*tracepb.Span_Link{{}}}}
	ended, err := view.DeclareEnded()
	if err != nil {
		t.Fatal(err)
	}
	declared, err := ended.View()
	if err != nil {
		t.Fatal(err)
	}
	packet, err := declared.OTLP()
	if err != nil {
		t.Fatal(err)
	}
	// Recognizable generated metadata is a TEST PAYLOAD, not a production saved
	// format. The existing Store still treats these bytes opaquely.
	body, err := protojson.Marshal(packet)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

type lostPublicationReply struct {
	*Catalog
	lose atomic.Bool
}

func (c *lostPublicationReply) Publish(ctx context.Context, ref ingest.Reference, a ingest.Attribution) (ingest.Receipt, error) {
	receipt, err := c.Catalog.Publish(ctx, ref, a)
	if err == nil && c.lose.CompareAndSwap(true, false) {
		return ingest.Receipt{}, errors.New("synthetic discarded successful catalog reply")
	}
	return receipt, err
}
func TestNativeStoragePathWriteReadRetry(t *testing.T) {
	f := database(t)
	writer := f.writer(t)
	peer := &publicationObjects{data: make(map[string][]byte)}
	server := httptest.NewServer(peer)
	t.Cleanup(server.Close)
	auth := publicationAuthorizer{f.r.Execution.Tenant, writer}
	lost := &lostPublicationReply{Catalog: f.c}
	lost.lose.Store(true)
	store := composedStore(t, lost, server, auth)
	id := f.reference(ingest.SpanRecord, "").Identity
	body := spanPayload(t, f)
	if receipt, err := store.Write(t.Context(), id, body); !errors.Is(err, ingest.ErrUncertain) || receipt != (ingest.Receipt{}) {
		t.Fatal("discarded confirmed-publication reply was not uncertain")
	}
	// No accepted rows are seeded. A fresh pool/store/SDK client finds what the
	// real publisher committed. The original process and HTTP peer remain alive;
	// this is fresh-client evidence, not a complete ingest-host-loss drill.
	c := freshCatalog(t, f)
	fresh := composedStore(t, c, server, auth)
	read, err := fresh.Read(t.Context(), id)
	if err != nil || !bytes.Equal(read, body) {
		t.Fatalf("fresh read failed: %v", err)
	}
	original, err := c.Lookup(t.Context(), id, "reader")
	if err != nil {
		t.Fatal(err)
	}
	retry, err := fresh.Write(t.Context(), id, body)
	want := original
	want.Duplicate = true
	if err != nil || retry != want || peer.count() != 1 {
		t.Fatal("retry changed original receipt or wrote another object")
	}
	if _, err := fresh.Write(t.Context(), id, append(bytes.Clone(body), ' ')); !errors.Is(err, ingest.ErrConflict) {
		t.Fatal("different opaque bytes overwrote original")
	}
	read, err = fresh.Read(t.Context(), id)
	if err != nil || !bytes.Equal(read, body) || peer.count() != 2 {
		t.Fatal("conflict changed accepted bytes or discarded unreferenced object")
	}
	capture := id
	capture.Kind = ingest.CaptureRecord
	if _, err := fresh.Read(t.Context(), capture); !errors.Is(err, ingest.ErrNotFound) {
		t.Fatal("span publication created capture")
	}
	if receipt, err := fresh.Write(t.Context(), capture, body); err != nil || receipt.Duplicate || peer.count() != 2 {
		t.Fatal("independent capture did not reuse immutable object safely")
	}
	t.Log("native catalog: discarded reply -> fresh read -> original retry -> conflict preserves original; independent capture")
}

func TestNativeStoragePathRechecksGrantAfterUpload(t *testing.T) {
	f := database(t)
	writer := f.writer(t)
	peer := &publicationObjects{data: make(map[string][]byte)}
	// The first PUT occurs after CheckWrite finished and while no catalog lock is
	// held. Change authority under the same exclusive gate as production controls
	// must use. This hook intentionally changes DATABASE policy, not authorizer state.
	var once sync.Once
	var hookErr error
	peer.afterPut = func() error {
		once.Do(func() {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			tx, err := f.pool.Begin(ctx)
			if err != nil {
				hookErr = err
				return
			}
			defer rollback(tx)
			if err = f.c.gate(ctx, tx, f.r.Execution.Tenant, "UPDATE"); err != nil {
				hookErr = err
				return
			}
			if _, err = tx.Exec(ctx, "DELETE FROM "+f.c.table("writer_grants")); err != nil {
				hookErr = err
				return
			}
			hookErr = tx.Commit(ctx)
		})
		return hookErr
	}
	server := httptest.NewServer(peer)
	t.Cleanup(server.Close)
	store := composedStore(t, f.c, server, publicationAuthorizer{f.r.Execution.Tenant, writer})
	id := f.reference(ingest.SpanRecord, "").Identity
	if _, err := store.Write(t.Context(), id, spanPayload(t, f)); !errors.Is(err, ingest.ErrDenied) || peer.count() != 1 {
		t.Fatalf("revoked grant admitted after upload: %v", err)
	}
	requireLookup(t, f.c, id, ingest.Receipt{}, ingest.ErrNotFound)
	// The denied attempt leaves its uploaded bytes intact and a later denied
	// attempt stops at CheckWrite, without uploading another object.
	if _, err := store.Write(t.Context(), id, []byte("another")); !errors.Is(err, ingest.ErrDenied) || peer.count() != 1 {
		t.Fatal("denied precheck uploaded bytes")
	}
}
