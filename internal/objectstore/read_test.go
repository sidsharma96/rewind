package objectstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/logging"
	"github.com/sidsharma96/rewind/internal/ingest"
)

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type probeBody struct {
	reader            io.Reader
	readErr, closeErr error
	read              int
	requests          int
	closed            bool
}

func (b *probeBody) Read(p []byte) (int, error) {
	if b.readErr != nil {
		return 0, b.readErr
	}
	n, err := b.reader.Read(p)
	b.read += n
	return n, err
}
func (b *probeBody) Close() error { b.closed = true; return b.closeErr }
func probeStore(t *testing.T, status int, body *probeBody, header http.Header) *Store {
	t.Helper()
	client := sdkClient("https://fixture.invalid", &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
		body.requests++
		return &http.Response{StatusCode: status, Header: header, Body: body, ContentLength: -1, Request: r}, nil
	})})
	// Explicit logging on the borrowed client must be disabled for every operation.
	o := client.Options()
	o.ClientLogMode = aws.LogRequestWithBody | aws.LogResponseWithBody
	o.Logger = logging.LoggerFunc(func(logging.Classification, string, ...interface{}) { t.Error("SDK logging leaked") })
	client = s3.New(o)
	s, err := New(client, "fixture-bucket", limits())
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func TestExactReadIntegrityAndBodyClosure(t *testing.T) {
	readErr, closeErr := errors.New("private read failure"), errors.New("private close failure")
	for _, tc := range []struct {
		name, body              string
		advertised              *int
		expectedBytes           int
		readErr, closeErr, want error
		maxRead                 int
	}{
		{name: "good", body: "right", expectedBytes: 5, maxRead: 5},
		{name: "empty", expectedBytes: 0},
		{name: "wrong hash", body: "wrong", expectedBytes: 5, want: ingest.ErrIntegrity, maxRead: 5},
		{name: "truncated", body: "righ", expectedBytes: 5, want: ingest.ErrIntegrity, maxRead: 4},
		{name: "excess", body: strings.Repeat("x", 1000), expectedBytes: 5, want: ingest.ErrIntegrity, maxRead: 6},
		{name: "oversized header", body: "right", advertised: newInt(6), expectedBytes: 5, want: ingest.ErrIntegrity},
		{name: "short header", body: "right", advertised: newInt(4), expectedBytes: 5, want: ingest.ErrIntegrity},
		{name: "matching hash short expectation", body: "right", expectedBytes: 4, want: ingest.ErrIntegrity, maxRead: 5},
		{name: "matching hash long expectation", body: "right", expectedBytes: 6, want: ingest.ErrIntegrity, maxRead: 5},
		{name: "empty expectation refuses bytes", body: "right", expectedBytes: 0, want: ingest.ErrIntegrity, maxRead: 1},
		{name: "read failure", expectedBytes: 5, readErr: readErr, want: readErr},
		{name: "close failure", body: "right", expectedBytes: 5, closeErr: closeErr, want: closeErr, maxRead: 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := &probeBody{reader: strings.NewReader(tc.body), readErr: tc.readErr, closeErr: tc.closeErr}
			h := make(http.Header)
			if tc.advertised != nil {
				h.Set("Content-Length", strconv.Itoa(*tc.advertised))
			}
			s := probeStore(t, 200, b, h)
			expected := []byte("right")
			if tc.name == "empty" {
				expected = nil
			}
			got, err := s.ReadExact(t.Context(), object("project", expected), tc.expectedBytes)
			if tc.want == nil {
				if err != nil || !bytes.Equal(got, expected) {
					t.Fatalf("%q %v", got, err)
				}
			} else if !errors.Is(err, tc.want) || got != nil || strings.Contains(err.Error(), "private") {
				t.Fatalf("%q %v", got, err)
			}
			if !b.closed || b.read > tc.maxRead {
				t.Fatalf("closed=%v bytes=%d max=%d", b.closed, b.read, tc.maxRead)
			}
		})
	}
}
func newInt(n int) *int { return &n }
func TestControlXMLIsBoundedBeforeSDKDecode(t *testing.T) {
	b := &probeBody{reader: strings.NewReader("<Error><Code>NoSuchKey</Code><Message>private</Message></Error>" + strings.Repeat("x", int(controlBodyLimit)))}
	s := probeStore(t, 404, b, http.Header{"Content-Type": {"application/xml"}})
	err := s.Create(t.Context(), object("project", nil), nil)
	if !errors.Is(err, ErrResponseLimit) || errors.Is(err, ingest.ErrObjectMissing) || strings.Contains(err.Error(), "private") {
		t.Fatal(err)
	}
	if !b.closed || b.read > int(controlBodyLimit)+1 {
		t.Fatalf("closed=%v bytes=%d", b.closed, b.read)
	}
}
func TestExpectedLengthAboveConfiguredCapStopsBeforeObjectIO(t *testing.T) {
	for _, expectedBytes := range []int{1025, 4096, int(^uint(0) >> 1)} {
		b := &probeBody{reader: strings.NewReader("right")}
		s := probeStore(t, 200, b, make(http.Header))
		if data, err := s.ReadExact(t.Context(), object("p", []byte("right")), expectedBytes); data != nil || !errors.Is(err, ingest.ErrTooLarge) {
			t.Fatalf("expected bytes %d: %q %v", expectedBytes, data, err)
		}
		if b.requests != 0 || b.read != 0 || b.closed {
			t.Fatalf("requests=%d bytes=%d closed=%v", b.requests, b.read, b.closed)
		}
	}
}
func TestCreateReconciliationIntegrityFailure(t *testing.T) {
	b := &backend{afterPut: func(w http.ResponseWriter, _ *http.Request) bool { apiFailure(w, 503, "SlowDown"); return true }}
	s, _ := fixture(t, b)
	// After PUT, replace the response bytes with corruption without modifying the
	// stored key; Create must not trust the PUT reply or accept an arbitrary GET.
	var gets int
	b.before = func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == "GET" {
			gets++
			if gets > 1 {
				_, _ = w.Write([]byte("wrong"))
				return true
			}
		}
		return false
	}
	err := s.Create(context.Background(), object("p", []byte("right")), []byte("right"))
	if !errors.Is(err, ingest.ErrIntegrity) {
		t.Fatal(err)
	}
	if _, p, c := b.counts(); p != 1 || c != 1 {
		t.Fatalf("puts=%d created=%d", p, c)
	}
}

func TestCanceledUploadReleasesCallerBuffer(t *testing.T) {
	var retained io.ReadCloser
	calls := 0
	client := sdkClient("https://fixture.invalid", &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.Method == "PUT" {
			// RoundTripper may close the request body asynchronously after returning.
			// Retain the wrapper until after Create has returned. The pinned SDK
			// must have closed it and detached the caller's byte reader.
			retained = r.Body
			return nil, context.DeadlineExceeded
		}
		return &http.Response{StatusCode: 404, Header: http.Header{"Content-Type": {"application/xml"}},
			Body: io.NopCloser(strings.NewReader("<Error><Code>NoSuchKey</Code></Error>")), ContentLength: -1, Request: r}, nil
	})})
	s, err := New(client, "fixture-bucket", limits())
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("original")
	id := object("p", body)
	err = s.Create(t.Context(), id, body)
	if !errors.Is(err, ErrUncertain) || retained == nil || calls != 3 {
		t.Fatalf("calls=%d retained=%v err=%v", calls, retained != nil, err)
	}
	t.Cleanup(func() { _ = retained.Close() })
	copy(body, []byte("modified"))
	got, err := io.ReadAll(retained)
	if err != nil || len(got) != 0 {
		t.Fatalf("request retained caller storage: %q %v", got, err)
	}
}
