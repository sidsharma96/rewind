package access

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestAuthenticateRequestDirectTLS(t *testing.T) {
	token := strings.Repeat("a", 64)
	expiry := time.Unix(200, 0)
	var current atomic.Int64
	current.Store(expiry.Add(-time.Second).UnixNano())
	credentials, err := NewCredentials(1, []Credential{credentialFixture(token, "alice", expiry)}, func() time.Time {
		return time.Unix(0, current.Load())
	})
	if err != nil {
		t.Fatal(err)
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, err := credentials.AuthenticateRequest(r)
		if err != nil {
			if ctx != nil || !errors.Is(err, ErrUnauthenticated) {
				t.Error("HTTP authentication returned unexpected evidence or error")
			}
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		p, ok := principal(ctx)
		if !ok || p != "alice" {
			t.Error("HTTP authentication did not establish the configured principal")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = io.WriteString(w, p)
	})
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	client := server.Client() // Trusts this server's certificate and verifies TLS.
	for _, tc := range []struct {
		name   string
		values []string
		want   int
	}{
		{"valid", []string{"Bearer " + token}, http.StatusOK},
		{"scheme case", []string{"bEaReR " + token}, http.StatusOK},
		{"missing", nil, http.StatusUnauthorized},
		{"unknown token", []string{"Bearer " + strings.Repeat("b", 64)}, http.StatusUnauthorized},
		{"wrong scheme", []string{"Basic " + token}, http.StatusUnauthorized},
		{"uppercase token", []string{"Bearer " + strings.ToUpper(token)}, http.StatusUnauthorized},
		{"short token", []string{"Bearer " + token[:63]}, http.StatusUnauthorized},
		{"extra scheme space", []string{"Bearer  " + token}, http.StatusUnauthorized},
		{"duplicate valid header", []string{"Bearer " + token, "Bearer " + token}, http.StatusUnauthorized},
		{"mixed headers", []string{"Bearer " + token, "Bearer " + strings.Repeat("b", 64)}, http.StatusUnauthorized},
		{"combined header", []string{"Bearer " + token + ", Bearer " + token}, http.StatusUnauthorized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL, nil)
			if err != nil {
				t.Fatal(err)
			}
			for _, value := range tc.values {
				r.Header.Add("Authorization", value)
			}
			// Claimed identity headers cannot replace the verified token principal.
			r.Header.Set("X-Rewind-Principal", "mallory")
			r.Header.Set("X-Rewind-Owner", "bob")
			response, err := client.Do(r)
			if err != nil {
				t.Fatal("certificate-verifying TLS request failed:", err)
			}
			defer response.Body.Close()
			body, err := io.ReadAll(io.LimitReader(response.Body, 128))
			if err != nil || response.StatusCode != tc.want || (tc.want == http.StatusOK && string(body) != "alice") {
				t.Fatal("TLS authentication outcome or principal changed")
			}
		})
	}
	current.Store(expiry.UnixNano())
	r, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Authorization", "Bearer "+token)
	response, err := client.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatal("TLS request accepted a credential at its expiry boundary")
	}
}

func TestAuthenticateRequestRefusesPlaintextAndForwardedTLS(t *testing.T) {
	token := strings.Repeat("a", 64)
	now := time.Unix(100, 0)
	credentials, err := NewCredentials(1, []Credential{credentialFixture(token, "alice", now.Add(time.Hour))}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, err := credentials.AuthenticateRequest(r)
		if ctx != nil || !errors.Is(err, ErrUnauthenticated) {
			t.Error("plaintext request established authentication")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusUnauthorized)
	}))
	t.Cleanup(server.Close)
	for _, spoofTLS := range []bool{false, true} {
		r, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL, nil)
		if err != nil {
			t.Fatal(err)
		}
		r.Header.Set("Authorization", "Bearer "+token)
		if spoofTLS {
			r.Header.Set("X-Forwarded-Proto", "https")
			r.Header.Set("Forwarded", "proto=https")
			r.Header.Set("X-Forwarded-Ssl", "on")
		}
		response, err := server.Client().Do(r)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusUnauthorized {
			t.Fatal("forwarded TLS assertion bypassed the direct TLS requirement")
		}
	}
}

func TestAuthenticateRequestCanceledContext(t *testing.T) {
	token := strings.Repeat("a", 64)
	now := time.Unix(100, 0)
	credentials, err := NewCredentials(1, []Credential{credentialFixture(token, "alice", now.Add(time.Hour))}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	deadline, stop := context.WithDeadline(context.Background(), time.Unix(1, 0))
	defer stop()
	for _, tc := range []struct {
		ctx  context.Context
		want error
	}{{parent, context.Canceled}, {deadline, context.DeadlineExceeded}} {
		// Handcrafted server requests isolate cancellation classification; the
		// TLS handshake and certificate verification run in the network test above.
		r := httptest.NewRequestWithContext(tc.ctx, http.MethodGet, "https://rewind.example/", nil)
		r.Header.Set("Authorization", "Bearer "+token)
		if got, err := credentials.AuthenticateRequest(r); got != nil || !errors.Is(err, tc.want) {
			t.Fatal("request authentication lost its cancellation outcome")
		}
	}
	if ctx, err := credentials.AuthenticateRequest(nil); ctx != nil || !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("nil request did not fail closed")
	}
}
