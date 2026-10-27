package objectstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
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
)

// A controlled HTTP peer exercises actual SDK headers, signing, XML errors and
// retry settings. Its in-memory conditional PUT models, rather than proves, R2.
type backend struct {
	mu                      sync.Mutex
	objects                 map[string][]byte
	requests, puts, created int
	before                  func(http.ResponseWriter, *http.Request) bool
	afterPut                func(http.ResponseWriter, *http.Request) bool
}

func apiFailure(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, "<Error><Code>%s</Code><Message>private-do-not-log</Message></Error>", code)
}
func (b *backend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b.mu.Lock()
	b.requests++
	if r.Method == "PUT" {
		b.puts++
	}
	b.mu.Unlock()
	if b.before != nil && b.before(w, r) {
		return
	}
	if !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 ") {
		apiFailure(w, 403, "AccessDenied")
		return
	}
	switch r.Method {
	case "GET":
		b.mu.Lock()
		v, ok := b.objects[r.URL.Path]
		v = bytes.Clone(v)
		b.mu.Unlock()
		if !ok {
			apiFailure(w, 404, "NoSuchKey")
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(v)))
		w.Header().Set("ETag", `"not-sha256"`)
		_, _ = w.Write(v)
	case "PUT":
		if r.Header.Get("If-None-Match") != "*" || r.Header.Get("Content-Type") != "application/octet-stream" {
			apiFailure(w, 400, "InvalidRequest")
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 4097))
		if err != nil || len(body) > 4096 {
			apiFailure(w, 400, "InvalidRequest")
			return
		}
		b.mu.Lock()
		_, exists := b.objects[r.URL.Path]
		if !exists {
			b.objects[r.URL.Path] = body
			b.created++
		}
		b.mu.Unlock()
		if exists {
			apiFailure(w, 412, "PreconditionFailed")
			return
		}
		if b.afterPut != nil && b.afterPut(w, r) {
			return
		}
		w.WriteHeader(200)
	default:
		apiFailure(w, 405, "MethodNotAllowed")
	}
}
func (b *backend) counts() (int, int, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.requests, b.puts, b.created
}
func object(tenant string, body []byte) ingest.ObjectID {
	return ingest.ObjectID{Tenant: tenant, Digest: sha256.Sum256(body)}
}
func limits() Limits { return Limits{MaxBytes: 1024, MaxActive: 4, OperationTimeout: 5 * time.Second} }
func sdkClient(endpoint string, client s3.HTTPClient) *s3.Client {
	return s3.New(s3.Options{Region: "auto", BaseEndpoint: aws.String(endpoint), UsePathStyle: true, HTTPClient: client,
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "synthetic-key", SecretAccessKey: "synthetic-secret"}, nil
		})})
}
func fixture(t *testing.T, b *backend) (*Store, *s3.Client) {
	t.Helper()
	b.objects = make(map[string][]byte)
	server := httptest.NewServer(b)
	t.Cleanup(server.Close)
	client := sdkClient(server.URL, server.Client())
	s, err := New(client, "fixture-bucket", limits())
	if err != nil {
		t.Fatal(err)
	}
	return s, client
}
func TestCreateRetryFreshClientAndTenantSeparation(t *testing.T) {
	b := &backend{}
	s, client := fixture(t, b)
	for _, body := range [][]byte{[]byte("Alice call 42"), nil} {
		id := object("project-a", body)
		if err := s.Create(t.Context(), id, body); err != nil {
			t.Fatal(err)
		}
		fresh, err := New(sdkClient(*client.Options().BaseEndpoint, client.Options().HTTPClient), "fixture-bucket", limits())
		if err != nil {
			t.Fatal(err)
		}
		if err = fresh.Create(t.Context(), id, body); err != nil {
			t.Fatal(err)
		}
		got, err := fresh.Read(t.Context(), id, len(body))
		if err != nil || !bytes.Equal(got, body) {
			t.Fatalf("read %q: %v", got, err)
		}
		if len(got) > 0 {
			got[0] = 'X'
			again, err := s.Read(t.Context(), id, 1024)
			if err != nil || !bytes.Equal(again, body) {
				t.Fatal("result aliases remote", err)
			}
		}
	}
	if _, p, c := b.counts(); p != 2 || c != 2 {
		t.Fatalf("puts=%d creates=%d", p, c)
	}
	body := []byte("Alice call 42")
	if err := s.Create(t.Context(), object("project-b", body), body); err != nil {
		t.Fatal(err)
	}
	if _, p, c := b.counts(); p != 3 || c != 3 {
		t.Fatalf("tenant collapse puts=%d creates=%d", p, c)
	}
}
func TestConcurrentFirstCreators(t *testing.T) {
	arrived := make(chan struct{}, 2)
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	var gets atomic.Int32
	b := &backend{before: func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == "GET" && gets.Add(1) <= 2 {
			arrived <- struct{}{}
			select {
			case <-release:
				apiFailure(w, 404, "NoSuchKey")
			case <-r.Context().Done():
			}
			return true
		}
		return false
	}}
	s, _ := fixture(t, b)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	results := make(chan error, 2)
	body := []byte("same bytes")
	id := object("project", body)
	for range 2 {
		go func() { results <- s.Create(ctx, id, body) }()
	}
	for range 2 {
		select {
		case <-arrived:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	once.Do(func() { close(release) })
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if r, p, c := b.counts(); r != 6 || p != 2 || c != 1 {
		t.Fatalf("requests=%d puts=%d creates=%d", r, p, c)
	}
}
func TestLostPutResponseReconciles(t *testing.T) {
	b := &backend{afterPut: func(w http.ResponseWriter, _ *http.Request) bool {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close()
		}
		return true
	}}
	s, _ := fixture(t, b)
	body := []byte("saved before reply loss")
	if err := s.Create(t.Context(), object("project", body), body); err != nil {
		t.Fatal(err)
	}
	if r, p, c := b.counts(); r != 3 || p != 1 || c != 1 {
		t.Fatalf("requests=%d puts=%d creates=%d", r, p, c)
	}
}
func TestUnresolvedPutNeverClaimsAbsence(t *testing.T) {
	b := &backend{before: func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == "PUT" {
			apiFailure(w, 503, "SlowDown")
			return true
		}
		return false
	}}
	s, _ := fixture(t, b)
	body := []byte("uncertain")
	err := s.Create(t.Context(), object("project", body), body)
	if !errors.Is(err, ErrUncertain) || errors.Is(err, ingest.ErrObjectMissing) || strings.Contains(err.Error(), "private") {
		t.Fatal(err)
	}
	if r, p, c := b.counts(); r != 3 || p != 1 || c != 0 {
		t.Fatalf("SDK retry or write: requests=%d puts=%d creates=%d", r, p, c)
	}
}
func TestReadFailureCannotStartPut(t *testing.T) {
	for _, tc := range []struct {
		code   string
		status int
	}{{"AccessDenied", 403}, {"NotFound", 404}, {"NoSuchKey", 500}, {"SlowDown", 503}} {
		t.Run(tc.code+strconv.Itoa(tc.status), func(t *testing.T) {
			b := &backend{before: func(w http.ResponseWriter, _ *http.Request) bool { apiFailure(w, tc.status, tc.code); return true }}
			s, _ := fixture(t, b)
			body := []byte("private")
			err := s.Create(t.Context(), object("project", body), body)
			if err == nil || errors.Is(err, ingest.ErrObjectMissing) || strings.Contains(err.Error(), "private") {
				t.Fatal(err)
			}
			if r, p, _ := b.counts(); r != 1 || p != 0 {
				t.Fatalf("requests=%d puts=%d", r, p)
			}
		})
	}
}
func TestCorruptExistingObjectIsNeverOverwritten(t *testing.T) {
	b := &backend{}
	s, _ := fixture(t, b)
	body := []byte("right")
	id := object("project", body)
	k, _ := key(id)
	for _, bad := range []string{"wrong", "longer wrong"} {
		b.mu.Lock()
		b.objects["/fixture-bucket/"+k] = []byte(bad)
		b.mu.Unlock()
		if err := s.Create(t.Context(), id, body); !errors.Is(err, ingest.ErrIntegrity) {
			t.Fatal(err)
		}
	}
	if _, p, _ := b.counts(); p != 0 {
		t.Fatal("overwrote corrupt existing object")
	}
}
func TestAdmissionCancellationAndTimeout(t *testing.T) {
	entered := make(chan struct{}, 1)
	b := &backend{before: func(_ http.ResponseWriter, r *http.Request) bool {
		entered <- struct{}{}
		<-r.Context().Done()
		return true
	}}
	s, client := fixture(t, b)
	s, err := New(client, "fixture-bucket", Limits{MaxBytes: 1024, MaxActive: 1, OperationTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	id := object("project", nil)
	go func() { _, err := s.Read(ctx, id, 10); result <- err }()
	<-entered
	if _, err := s.Read(t.Context(), id, 10); !errors.Is(err, ingest.ErrBusy) {
		t.Fatal(err)
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	// The first return releases admission. The next real operation times out.
	if _, err := s.Read(t.Context(), id, 10); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if len(s.active) != 0 {
		t.Fatal("admission leaked")
	}
}
func TestValidationAndOpaqueTenantKeys(t *testing.T) {
	b := &backend{}
	s, _ := fixture(t, b)
	seen := map[string]bool{}
	for _, tenant := range []string{"a/b", "a%2fb", "a/../b", "b", "\x00\xff", strings.Repeat("x", 256)} {
		k, err := key(object(tenant, nil))
		if err != nil || seen[k] {
			t.Fatalf("key %q: %v", k, err)
		}
		seen[k] = true
	}
	for _, tenant := range []string{"", strings.Repeat("x", 257)} {
		if err := s.Create(t.Context(), object(tenant, nil), nil); !errors.Is(err, ingest.ErrInvalid) {
			t.Fatal(err)
		}
	}
	if err := s.Create(t.Context(), object("p", nil), []byte("wrong digest")); !errors.Is(err, ingest.ErrIntegrity) {
		t.Fatal(err)
	}
	large := make([]byte, 1025)
	if err := s.Create(t.Context(), object("p", large), large); !errors.Is(err, ingest.ErrTooLarge) {
		t.Fatal(err)
	}
	if _, err := s.Read(t.Context(), object("p", nil), -1); !errors.Is(err, ingest.ErrInvalid) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.Read(ctx, object("p", nil), 10); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if r, _, _ := b.counts(); r != 0 {
		t.Fatal("invalid call accessed storage")
	}
}

func TestMissingReadAndInvalidConfiguration(t *testing.T) {
	b := &backend{}
	s, client := fixture(t, b)
	if data, err := s.Read(t.Context(), object("p", nil), 10); data != nil || !errors.Is(err, ingest.ErrObjectMissing) || errors.Is(err, ingest.ErrNotFound) {
		t.Fatalf("%q %v", data, err)
	}
	for _, l := range []Limits{{}, {MaxBytes: 1, MaxActive: 1}, {MaxBytes: 1, OperationTimeout: time.Second}, {MaxBytes: -1, MaxActive: 1, OperationTimeout: time.Second}, {MaxBytes: int(^uint(0) >> 1), MaxActive: 1, OperationTimeout: time.Second}} {
		if _, err := New(client, "fixture-bucket", l); !errors.Is(err, ingest.ErrInvalid) {
			t.Fatal("invalid limits", l, err)
		}
	}
	if _, err := New(nil, "fixture-bucket", limits()); !errors.Is(err, ingest.ErrInvalid) {
		t.Fatal(err)
	}
	for _, bucket := range []string{"", "../bucket", "bucket/other", "-bucket", "bucket-", strings.Repeat("x", 64)} {
		if _, err := New(client, bucket, limits()); !errors.Is(err, ingest.ErrInvalid) {
			t.Fatal("invalid bucket", bucket, err)
		}
	}
	if r, _, _ := b.counts(); r != 1 {
		t.Fatal("constructor contacted storage")
	}
}
