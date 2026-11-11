package ingest

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type traceReadObjects struct {
	Objects
	read func(context.Context, ObjectID, int) ([]byte, error)
}

func (o traceReadObjects) ReadExact(ctx context.Context, id ObjectID, size int) ([]byte, error) {
	return o.read(ctx, id, size)
}

func TestTraceIndependentReadsOverlapAndStayOrdered(t *testing.T) {
	r, objects, catalog := traceFixture(t, 4)
	r.spans.store.active = make(chan struct{}, 2)
	a := addTraceSpan(t, r, catalog, 20)
	b := addTraceSpan(t, r, catalog, 30)
	catalog.mu.Lock()
	captureOnly := a
	captureOnly.Reference.Identity.Span[0] = 25
	captureOnly.Reference.Identity.Kind = CaptureRecord
	catalog.rows[captureOnly.Reference.Identity] = captureOnly
	catalog.mu.Unlock()
	// Exactly two records fit. The next available key belongs on the next page.
	addTraceSpan(t, r, catalog, 40)
	r.limits.MaxStoredBytes = a.Reference.Size + b.Reference.Size
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	entered := make(chan ObjectID, 3)
	release := make(chan struct{})
	secondFinished := make(chan struct{})
	var active, peak atomic.Int32
	r.spans.store.objects = traceReadObjects{Objects: objects, read: func(ctx context.Context, id ObjectID, size int) ([]byte, error) {
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
		}
		entered <- id
		if id.Digest == a.Reference.Digest {
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		data, err := objects.ReadExact(ctx, id, size)
		if id.Digest == b.Reference.Digest {
			close(secondFinished)
		}
		return data, err
	}}
	type outcome struct {
		page TracePage
		err  error
	}
	done := make(chan outcome, 1)
	go func() {
		p, err := r.ReadTrace(ctx, sampleID.Tenant, sampleID.Trace, TraceCursor{})
		done <- outcome{p, err}
	}()
	var once sync.Once
	joined := false
	defer func() {
		cancel()
		once.Do(func() { close(release) })
		if !joined {
			<-done
		}
	}()
	seen := make(map[ObjectID]bool)
	for len(seen) < 2 {
		select {
		case id := <-entered:
			seen[id] = true
		case <-ctx.Done():
			t.Fatal("independent object reads did not overlap")
		}
	}
	select {
	case <-secondFinished:
	case <-ctx.Done():
		t.Fatal("second object did not finish while first remained blocked")
	}
	once.Do(func() { close(release) })
	result := <-done
	joined = true
	if result.err != nil || len(result.page.Items) != 3 || result.page.Items[0].Receipt != a || result.page.Items[1].State != TraceNoSpan || result.page.Items[2].Receipt != b || result.page.Next.After() != b.Reference.Identity.Span {
		t.Fatalf("ordered bounded page lost: %v %+v", result.err, result.page)
	}
	if peak.Load() != 2 || active.Load() != 0 || len(r.spans.store.active) != 0 || len(entered) != 0 {
		t.Fatal("parallelism, byte boundary or cleanup failed", peak.Load(), active.Load(), len(r.spans.store.active), len(entered))
	}
}

func TestTraceReadFailureCancelsAndJoinsSibling(t *testing.T) {
	for _, fault := range []string{"object", "cancel"} {
		t.Run(fault, func(t *testing.T) {
			r, objects, catalog := traceFixture(t, 2)
			r.spans.store.active = make(chan struct{}, 2)
			a := addTraceSpan(t, r, catalog, 20)
			addTraceSpan(t, r, catalog, 30)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			siblingEntered := make(chan struct{})
			siblingDone := make(chan struct{})
			broken := errors.New("controlled read failure")
			r.spans.store.objects = traceReadObjects{Objects: objects, read: func(ctx context.Context, id ObjectID, size int) ([]byte, error) {
				if id.Digest == a.Reference.Digest {
					select {
					case <-siblingEntered:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
					if fault == "cancel" {
						cancel()
						return nil, ctx.Err()
					}
					return nil, broken
				}
				close(siblingEntered)
				<-ctx.Done()
				close(siblingDone)
				return nil, ctx.Err()
			}}
			r.limits.OperationTimeout = 2 * time.Second
			page, err := r.ReadTrace(ctx, sampleID.Tenant, sampleID.Trace, TraceCursor{})
			want := broken
			if fault == "cancel" {
				want = context.Canceled
			}
			if !errors.Is(err, want) || !reflect.DeepEqual(page, TracePage{}) {
				t.Fatalf("wrong failure/partial page: %v %+v", err, page)
			}
			select {
			case <-siblingDone:
			default:
				t.Fatal("return before sibling joined")
			}
			if len(r.spans.store.active) != 0 {
				t.Fatal("admission leaked")
			}
		})
	}
}

func TestTraceReadFallsBackWithoutSpareAdmission(t *testing.T) {
	for _, occupied := range []bool{false, true} {
		t.Run(map[bool]string{false: "one-slot", true: "occupied-slot"}[occupied], func(t *testing.T) {
			r, objects, catalog := traceFixture(t, 2)
			if occupied {
				r.spans.store.active = make(chan struct{}, 2)
				r.spans.store.active <- struct{}{}
				defer r.spans.store.end()
			}
			a := addTraceSpan(t, r, catalog, 20)
			b := addTraceSpan(t, r, catalog, 30)
			var active, peak atomic.Int32
			r.spans.store.objects = traceReadObjects{Objects: objects, read: func(ctx context.Context, id ObjectID, size int) ([]byte, error) {
				n := active.Add(1)
				defer active.Add(-1)
				for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
				}
				return objects.ReadExact(ctx, id, size)
			}}
			page, err := r.ReadTrace(t.Context(), sampleID.Tenant, sampleID.Trace, TraceCursor{})
			if err != nil || len(page.Items) != 2 || page.Items[0].Receipt != a || page.Items[1].Receipt != b || peak.Load() != 1 {
				t.Fatal("fallback failed", err, peak.Load())
			}
			want := 0
			if occupied {
				want = 1
			}
			if len(r.spans.store.active) != want {
				t.Fatal("borrowed admission was not restored")
			}
		})
	}
}
