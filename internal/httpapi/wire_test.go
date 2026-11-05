package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sidsharma96/rewind/internal/access"
	"github.com/sidsharma96/rewind/internal/ingest"
	"github.com/sidsharma96/rewind/internal/record"
	"github.com/sidsharma96/rewind/internal/traceid"
)

func TestStrictWireRefusesAmbiguousAndExcessiveInput(t *testing.T) {
	for _, data := range []string{
		`{"request":null,"response":null}{}`,
		`{"request":{"state":"missing","state":"present","data_base64":""},"response":{}}`,
		`{"request":{},"unexpected":1}`,
		strings.Repeat("[", 66) + strings.Repeat("]", 66),
		"{\"request\":\"" + string([]byte{255}) + "\"}",
	} {
		var in CaptureInput
		if strictJSON([]byte(data), &in) == nil {
			t.Fatal("accepted malformed input")
		}
	}
	for _, data := range []string{
		`{"request":null,"response":null}`,
		`{"request":{"state":"missing","data_base64":null},"response":{"state":"present","data_base64":""}}`,
		`{"request":{"state":"redacted","data_base64":"YQ=="},"response":{"state":"present","data_base64":""}}`,
		`{"request":{"state":"missing","data_base64":""},"response":{"state":"present","data_base64":"YQ==\n"}}`,
	} {
		var in CaptureInput
		err := strictJSON([]byte(data), &in)
		if err == nil {
			_, err = in.value(ingest.Execution{})
		}
		if err == nil {
			t.Fatal("accepted ambiguous capture")
		}
	}
	var in CaptureInput
	if err := strictJSON([]byte(`{"request":{"state":"missing","data_base64":""},"response":{"state":"present","data_base64":""}}`), &in); err != nil {
		t.Fatal(err)
	}
	v, err := in.value(ingest.Execution{})
	if err != nil || v.Response.State != record.BodyPresent || len(v.Response.Data) != 0 || v.Request.State != record.BodyMissing {
		t.Fatal("empty result collapsed into missing")
	}
}

func TestCursorScopeAndNoAuthority(t *testing.T) {
	e := ingest.Execution{Tenant: string([]byte{0, 255}), Trace: traceid.Trace{1}}
	position, err := ingest.ResumeTrace(e.Tenant, e.Trace, traceid.Span{2})
	if err != nil {
		t.Fatal(err)
	}
	wire := cursor(e, position)
	got, err := parseCursor(e, wire)
	if err != nil || got != position {
		t.Fatal("cursor round trip")
	}
	other := e
	other.Trace = traceid.Trace{3}
	if _, err := parseCursor(other, wire); err == nil {
		t.Fatal("cursor escaped trace")
	}
	other = e
	other.Tenant = "other"
	if _, err := parseCursor(other, wire); err == nil {
		t.Fatal("cursor escaped project")
	}
	if _, err := parseCursor(e, wire+".extra"); err == nil {
		t.Fatal("cursor accepted trailing input")
	}
}

type blockedBody struct {
	entered, release chan struct{}
	once             bool
}

func (b *blockedBody) Read(p []byte) (int, error) {
	if !b.once {
		b.once = true
		close(b.entered)
		<-b.release
	}
	return 0, io.EOF
}
func (*blockedBody) Close() error { return nil }

func TestLoginFloodBoundBeforeBodyAndLeavesOperationCapacity(t *testing.T) {
	token, digest, _ := access.NewToken()
	credentials, _ := access.NewCredentials(1, []access.Credential{{Digest: digest, Principal: "reader", ExpiresAt: time.Now().Add(time.Hour)}}, time.Now)
	sessions, _ := access.NewSessions(credentials, 1, time.Minute)
	metadata, _ := record.NewMetadataCodec(record.MetadataLimits{MaxBytes: 8192, MaxWork: 16384, MaxDepth: 32})
	server, err := New("https://test.invalid", Limits{MaxActive: 2, MaxLoginActive: 1, MaxRequestBytes: 8192, MaxResponseBytes: 8192, Timeout: time.Second},
		credentials, sessions, &ingest.Registrar{}, &ingest.Spans{}, &ingest.Captures{}, &ingest.TraceReader{}, metadata)
	if err != nil {
		t.Fatal(err)
	}
	_ = token
	body := &blockedBody{entered: make(chan struct{}), release: make(chan struct{})}
	request := httptest.NewRequest(http.MethodPost, "https://test.invalid/login", body)
	request.Header.Set("Origin", "https://test.invalid")
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	done := make(chan struct{})
	go func() { defer close(done); server.ServeHTTP(httptest.NewRecorder(), request) }()
	<-body.entered
	defer func() { close(body.release); <-done }()
	rec := httptest.NewRecorder()
	again := httptest.NewRequest(http.MethodPost, "https://test.invalid/login", strings.NewReader("ignored"))
	server.ServeHTTP(rec, again)
	if rec.Code != 503 {
		t.Fatal("login exceeded class cap", rec.Code)
	}
	rec = httptest.NewRecorder()
	api := httptest.NewRequest(http.MethodGet, "https://test.invalid/api/v1/projects/aa/traces/"+(traceid.Trace{1}).String(), nil)
	server.ServeHTTP(rec, api)
	if rec.Code != 401 {
		t.Fatal("login flood occupied every operation slot", rec.Code)
	}
}

func TestClientLostOrMalformedReplyNeverConfirmsWrite(t *testing.T) {
	for _, body := range []string{`{"duplicate":false}`, `{"code":"denied","outcome":"refused"} trailing`, strings.Repeat("x", 1025)} {
		s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, body)
		}))
		client := *s.Client()
		client.Timeout = time.Second
		token, _, _ := access.NewToken()
		c, err := NewClient(s.URL, token, &client, 1024, 1024)
		if err != nil {
			t.Fatal(err)
		}
		e := Execution{Project: "aa", Trace: (traceid.Trace{1}).String(), Span: (traceid.Span{2}).String()}
		_, err = c.SaveCapture(context.Background(), e, CaptureInput{})
		var failure *Failure
		if !errors.As(err, &failure) || failure.Outcome != "unknown" {
			t.Fatal("malformed reply was not uncertain", err)
		}
		s.Close()
	}
}

func FuzzStrictCaptureJSON(f *testing.F) {
	f.Add([]byte(`{"request":{"state":"missing","data_base64":""},"response":{"state":"present","data_base64":""}}`))
	f.Add([]byte(`{"request":{"state":"missing","state":"present"}}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 8192 {
			return
		}
		var value CaptureInput
		if strictJSON(data, &value) != nil {
			return
		}
		b, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		var round CaptureInput
		if strictJSON(b, &round) != nil {
			t.Fatal("accepted input failed typed roundtrip")
		}
	})
}

// A TLS downgrade must be refused even when a credential is otherwise valid.
func TestTLSRequiredBeforeStaticOrProtectedContent(t *testing.T) {
	s := &Server{host: "test.invalid"}
	r := httptest.NewRequest("GET", "http://test.invalid/style.css", nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, r)
	if rec.Code != 400 || bytes.Contains(rec.Body.Bytes(), []byte(styles)) {
		t.Fatal("plaintext served application")
	}
}

func TestAcceptedCodecFailureIsNotAnInputRefusal(t *testing.T) {
	server := &Server{}
	request := httptest.NewRequest("GET", "https://test.invalid/api/v1/records", nil)
	for _, cause := range []error{record.ErrSavedSpan, record.ErrSavedCapture, record.ErrEnvelopeKindMismatch, record.ErrEnvelopeUnsupported} {
		rec := httptest.NewRecorder()
		server.fail(rec, request, cause, false)
		var failure Failure
		if rec.Code != 503 || json.Unmarshal(rec.Body.Bytes(), &failure) != nil || failure.Code == "invalid_request" || failure.Outcome != "failed" || failureStatus(failure.Code) != rec.Code {
			t.Fatal("accepted evidence blamed on reader", rec.Code)
		}
	}
	rec := httptest.NewRecorder()
	server.fail(rec, httptest.NewRequest("PUT", "https://test.invalid/api/v1/records", nil), errors.Join(ingest.ErrInvalid, record.ErrMetadataInvalid), true)
	var failure Failure
	if rec.Code != 400 || json.Unmarshal(rec.Body.Bytes(), &failure) != nil || failure.Outcome != "refused" {
		t.Fatal("invalid preparation was not a refusal")
	}
}

func TestWireUnicodeIsNotSilentlyReplaced(t *testing.T) {
	for _, input := range []string{`{"name":"\ud800"}`, `{"name":"\udc00"}`, `{"name":"\ud800A"}`} {
		var out map[string]string
		if strictJSON([]byte(input), &out) == nil {
			t.Fatal("unpaired Unicode escape became replacement text")
		}
	}
	for _, input := range []string{`{"name":"\ud83d\ude00"}`, `{"name":"\\ud800"}`} {
		var out map[string]string
		if err := strictJSON([]byte(input), &out); err != nil {
			t.Fatal("valid Unicode refused", err)
		}
	}
}

func TestWireFieldNamesAreExact(t *testing.T) {
	for _, input := range []string{
		`{"Request":{"state":"missing","data_base64":""},"response":{"state":"present","data_base64":""}}`,
		`{"request":{"state":"missing","data_base64":""},"Request":{"state":"redacted","data_base64":""},"response":{"state":"present","data_base64":""}}`,
	} {
		var out CaptureInput
		if strictJSON([]byte(input), &out) == nil {
			t.Fatal("case alias overwrote or substituted a protocol field")
		}
	}
}

func TestMissingOrNullResponseBytesAreNotAnEmptySavedResult(t *testing.T) {
	for _, input := range []string{`{"request_state":"present"}`, `{"request_state":"present","data_base64":null}`} {
		var out struct {
			RequestState string `json:"request_state"`
			Data         string `json:"data_base64"`
		}
		if strictJSON([]byte(input), &out) == nil {
			t.Fatal("absent response became zero-byte result")
		}
	}
	var out struct {
		RequestState string `json:"request_state"`
		Data         string `json:"data_base64"`
	}
	if strictJSON([]byte(`{"request_state":"present","data_base64":""}`), &out) != nil {
		t.Fatal("explicit complete zero-byte result refused")
	}
}
