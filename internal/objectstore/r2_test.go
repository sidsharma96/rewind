package objectstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// This opt-in probe writes two small synthetic objects in the explicitly approved
// development bucket. It never provisions, exposes, lists or deletes resources.
// Normal source/race checks need neither credentials nor a network connection.
func TestR2LiveImmutableObjects(t *testing.T) {
	if os.Getenv("REWIND_R2_LIVE_TEST") != "1" {
		t.Skip("set REWIND_R2_LIVE_TEST=1 with credentials scoped to rewind-dev")
	}
	endpoint := os.Getenv("REWIND_R2_ENDPOINT")
	access, secret := os.Getenv("REWIND_R2_ACCESS_KEY_ID"), os.Getenv("REWIND_R2_SECRET_ACCESS_KEY")
	if !liveEndpoint(endpoint) || access == "" || secret == "" {
		t.Fatal("configure the HTTPS R2 account endpoint and Rewind-specific S3 credentials locally")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	tenant := "rewind-live-" + strings.ToLower(rand.Text())
	newClient := func(dropReply bool) (*Store, *s3.Client, *lostPutReply) {
		transport := &http.Transport{
			DialContext:       (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 20 * time.Second}).DialContext,
			ForceAttemptHTTP2: true, MaxIdleConns: 2, MaxIdleConnsPerHost: 2, MaxConnsPerHost: 2,
			IdleConnTimeout: 20 * time.Second, TLSHandshakeTimeout: 5 * time.Second,
			ResponseHeaderTimeout: 10 * time.Second, MaxResponseHeaderBytes: 16 << 10,
		}
		t.Cleanup(transport.CloseIdleConnections)
		httpClient := &http.Client{Transport: transport, Timeout: 20 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
		loss := &lostPutReply{next: httpClient}
		var send s3.HTTPClient = httpClient
		if dropReply {
			send = loss
		}
		client := s3.New(s3.Options{Region: "auto", BaseEndpoint: aws.String(endpoint), UsePathStyle: true,
			HTTPClient: send, Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
				return aws.Credentials{AccessKeyID: access, SecretAccessKey: secret, Source: "explicit rewind-dev probe"}, nil
			}),
		})
		store, err := New(client, "rewind-dev", Limits{MaxBytes: 1024, MaxActive: 1, OperationTimeout: 20 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		return store, client, loss
	}
	first, client, _ := newClient(false)
	body := []byte("synthetic Rewind immutable-object probe\n")
	id := object(tenant, body)
	if err := first.Create(ctx, id, body); err != nil {
		t.Fatal("first create:", err)
	}
	if err := first.Create(ctx, id, body); err != nil {
		t.Fatal("identical retry:", err)
	}
	fresh, _, _ := newClient(false)
	if got, err := fresh.ReadExact(ctx, id, len(body)); err != nil || !bytes.Equal(got, body) {
		t.Fatal("fresh-client read:", err)
	}
	// Exercise R2's condition itself: the adapter deliberately cannot send wrong
	// bytes under this digest, so use the SDK directly for this negative attempt.
	k, err := key(id)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("rewind-dev"), Key: aws.String(k),
		Body: strings.NewReader("must not replace the original"), IfNoneMatch: aws.String("*"),
	}, attemptOptions(controlBodyLimit))
	var api smithy.APIError
	var status interface{ HTTPStatusCode() int }
	if !errors.As(err, &api) || api.ErrorCode() != "PreconditionFailed" || !errors.As(err, &status) || status.HTTPStatusCode() != 412 {
		t.Fatal("conditional overwrite did not return PreconditionFailed with HTTP 412")
	}
	if got, err := fresh.ReadExact(ctx, id, len(body)); err != nil || !bytes.Equal(got, body) {
		t.Fatal("original changed after rejected overwrite:", err)
	}
	t.Log("R2 rejected conditional overwrite; original bytes remain readable")

	// The real provider receives this PUT and replies successfully. The local HTTP
	// wrapper closes and discards that reply before the SDK sees it. This is a
	// controlled lost-reply experiment, not an actual network outage.
	interrupted, _, loss := newClient(true)
	other := []byte("synthetic Rewind locally discarded PUT reply\n")
	otherID := object(tenant, other)
	if err := interrupted.Create(ctx, otherID, other); err != nil {
		t.Fatal("lost-reply reconciliation:", err)
	}
	if !loss.discarded.Load() {
		t.Fatal("probe did not discard a successful PUT reply")
	}
	recovered, _, _ := newClient(false)
	if got, err := recovered.ReadExact(ctx, otherID, len(other)); err != nil || !bytes.Equal(got, other) {
		t.Fatal("fresh-client recovery:", err)
	}
	t.Logf("retained two synthetic immutable objects in rewind-dev, tenant %s", tenant)
	// A new client is not replacement of an ingest host, catalog recovery or a
	// failover test. No complete ingestion acknowledgment is exercised here.
}

// Restrict the live credential destination to a default Cloudflare R2 account
// endpoint. This development bucket has no jurisdiction-specific endpoint.
func liveEndpoint(value string) bool {
	u, err := url.Parse(value)
	if err != nil || u.Scheme != "https" || u.User != nil || u.Port() != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawPath != "" || u.Path != "" && u.Path != "/" {
		return false
	}
	account, ok := strings.CutSuffix(u.Host, ".r2.cloudflarestorage.com")
	if !ok || len(account) != 32 {
		return false
	}
	for _, c := range account {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

type lostPutReply struct {
	next      s3.HTTPClient
	discarded atomic.Bool
}

func (c *lostPutReply) Do(r *http.Request) (*http.Response, error) {
	response, err := c.next.Do(r)
	if err != nil || response == nil {
		return response, err
	}
	if r.Method == http.MethodPut && response.StatusCode >= 200 && response.StatusCode < 300 && c.discarded.CompareAndSwap(false, true) {
		if response.Body != nil {
			if err := response.Body.Close(); err != nil {
				return nil, err
			}
		}
		return nil, io.ErrUnexpectedEOF
	}
	return response, nil
}

func TestLiveEndpointRejectsOtherCredentialDestinations(t *testing.T) {
	good := "https://" + strings.Repeat("a", 32) + ".r2.cloudflarestorage.com"
	for _, value := range []string{good, good + "/"} {
		if !liveEndpoint(value) {
			t.Fatal("valid R2 endpoint refused")
		}
	}
	for _, value := range []string{"", strings.Replace(good, "https:", "http:", 1), good + ".example.com", good + ":8443", good + "?", good + "?x=1", good + "#fragment", good + "/bucket", strings.Replace(good, "https://", "https://user:secret@", 1), "https://not-an-account.r2.cloudflarestorage.com", "https://" + strings.Repeat("a", 32) + ".eu.r2.cloudflarestorage.com"} {
		if liveEndpoint(value) {
			t.Fatal("unsafe or unrelated endpoint accepted")
		}
	}
}

func TestSuccessfulPutReplyCanBeDiscardedLocally(t *testing.T) {
	remote := &backend{}
	_, client := fixture(t, remote)
	options := client.Options()
	loss := &lostPutReply{next: options.HTTPClient}
	options.HTTPClient = loss
	store, err := New(s3.New(options), "fixture-bucket", limits())
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("real SDK, controlled response loss")
	if err := store.Create(t.Context(), object("p", body), body); err != nil {
		t.Fatal(err)
	}
	if !loss.discarded.Load() {
		t.Fatal("successful PUT reply was not discarded")
	}
	if requests, puts, created := remote.counts(); requests != 3 || puts != 1 || created != 1 {
		t.Fatalf("requests=%d puts=%d created=%d", requests, puts, created)
	}
}
