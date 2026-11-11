package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sidsharma96/rewind/internal/httpapi"
)

type deliveryTransport func(*http.Request) (*http.Response, error)

func (f deliveryTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func deliveryFixture(t *testing.T, transport deliveryTransport) (*httpapi.Client, plan) {
	t.Helper()
	client, err := httpapi.NewClient("https://rewind.test", strings.Repeat("a", 64), &http.Client{Transport: transport, Timeout: time.Second}, 8192, 8192)
	if err != nil {
		t.Fatal(err)
	}
	return client, plan{Execution: httpapi.Execution{Project: httpapi.Opaque("demo"), Trace: strings.Repeat("1", 32), Span: strings.Repeat("2", 16)}}
}
func deliveryReply(t *testing.T, r *http.Request, p plan) *http.Response {
	t.Helper()
	kind := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
	receipt := httpapi.Receipt{Execution: p.Execution, Kind: kind, Digest: strings.Repeat("3", 64), Size: 7,
		Run: httpapi.Opaque("run"), Instance: httpapi.Opaque("alice"), Submitter: httpapi.Opaque("alice"),
		Accepted: "2026-01-01T00:00:00Z", Expires: "2026-01-02T00:00:00Z"}
	data, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(string(data)))}
}

func TestDeliverRecordsOverlapsAndJoins(t *testing.T) {
	entered := make(chan string, 2)
	release := make(chan struct{})
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	var p plan
	var active, peak atomic.Int32
	client, saved := deliveryFixture(t, func(r *http.Request) (*http.Response, error) {
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
		}
		entered <- r.URL.Path
		select {
		case <-release:
		case <-r.Context().Done():
			return nil, r.Context().Err()
		}
		return deliveryReply(t, r, p), nil
	})
	p = saved
	done := make(chan error, 1)
	go func() {
		span, capture, err := deliverRecords(ctx, client, p)
		if err == nil && (span.Kind != "span" || capture.Kind != "capture" || span.Execution != p.Execution || capture.Execution != p.Execution) {
			err = errors.New("lost paired receipts")
		}
		done <- err
	}()
	joined := false
	defer func() {
		cancel()
		if !joined {
			<-done
		}
	}()
	for range 2 {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("independent deliveries did not overlap")
		}
	}
	close(release)
	err := <-done
	joined = true
	if err != nil || active.Load() != 0 || peak.Load() != 2 {
		t.Fatal("delivery failed or returned before join", err, active.Load(), peak.Load())
	}
}

func TestDeliverRecordsFailureStillJoinsAcceptedPeer(t *testing.T) {
	for _, kind := range []string{"span", "capture"} {
		t.Run(kind, func(t *testing.T) {
			siblingEntered := make(chan struct{})
			siblingDone := make(chan struct{})
			var saved plan
			client, p := deliveryFixture(t, func(r *http.Request) (*http.Response, error) {
				if strings.HasSuffix(r.URL.Path, "/"+kind) {
					select {
					case <-siblingEntered:
					case <-r.Context().Done():
						return nil, r.Context().Err()
					}
					return &http.Response{StatusCode: 403, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"code":"denied","outcome":"refused"}`))}, nil
				}
				close(siblingEntered)
				defer close(siblingDone)
				return deliveryReply(t, r, saved), nil
			})
			saved = p
			span, capture, err := deliverRecords(t.Context(), client, p)
			if err == nil || !strings.Contains(err.Error(), kind+" delivery") {
				t.Fatal("lost failure", err)
			}
			if kind == "span" && capture.Kind != "capture" || kind == "capture" && span.Kind != "span" {
				t.Fatal("lost accepted peer receipt")
			}
			select {
			case <-siblingDone:
			default:
				t.Fatal("returned before sibling finished")
			}
		})
	}
}

func TestDeliverRecordsCanceledBeforeStart(t *testing.T) {
	var calls atomic.Int32
	client, p := deliveryFixture(t, func(r *http.Request) (*http.Response, error) { calls.Add(1); return nil, errors.New("must not call") })
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := deliverRecords(ctx, client, p); !errors.Is(err, context.Canceled) || calls.Load() != 0 {
		t.Fatal("canceled delivery performed I/O", err, calls.Load())
	}
}

func TestDeliverRecordsParentCancellationJoinsBoth(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var started, finished atomic.Int32
	client, p := deliveryFixture(t, func(r *http.Request) (*http.Response, error) {
		if started.Add(1) == 2 {
			cancel()
		}
		<-r.Context().Done()
		finished.Add(1)
		return nil, r.Context().Err()
	})
	if _, _, err := deliverRecords(ctx, client, p); err == nil || finished.Load() != 2 {
		t.Fatal("parent cancellation did not join both calls", err, finished.Load())
	}
}
