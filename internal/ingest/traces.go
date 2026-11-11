package ingest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sidsharma96/rewind/internal/record"
	"github.com/sidsharma96/rewind/internal/traceid"
)

// TraceCatalog discovers accepted execution keys, including capture-only records
// and retained expired identities. Registration alone is not a record. ScanTrace
// checks current project-reader permission even for an empty result, returns at
// most limit distinct nonzero IDs in increasing byte order strictly after after,
// and releases all catalog locks before returning. Zero after starts the scan.
// Each call selects current membership, not a snapshot spanning later calls.
// Implementations honor cancellation and are safe for concurrent calls. The
// logical result limit is not a hard bound on physical database work.
type TraceCatalog interface {
	ScanTrace(ctx context.Context, tenant string, trace traceid.Trace, after traceid.Span, limit int, principal string) ([]traceid.Span, error)
}

// TraceLimits is trusted application configuration, with no production defaults.
// A page consumes at most MaxCandidates executions and MaxStoredBytes object
// bytes. One extra candidate is requested as lookahead. Per-span codec limits
// also apply: aggregate expansion/graph work is at most MaxCandidates times those
// limits, not an exact heap or hard CPU bound. OperationTimeout spans the page;
// bounded in-memory decoding checks cancellation between codec calls.
type TraceLimits struct {
	MaxCandidates    int
	MaxStoredBytes   int
	OperationTimeout time.Duration
}

// TraceCursor is an internal scope-bound position, never authority or a public
// token encoding. Zero starts a request; zero Next means no more keys were
// observed in that selection, never that the trace is complete. Restarting at
// zero discovers late arrivals behind an earlier position.
type TraceCursor struct {
	tenant string
	trace  traceid.Trace
	after  traceid.Span
}

func (c TraceCursor) IsZero() bool { return c == (TraceCursor{}) }

// ResumeTrace constructs a validated position, not reader authority or a
// membership snapshot. Transport encoding belongs to the caller.
func ResumeTrace(tenant string, trace traceid.Trace, after traceid.Span) (TraceCursor, error) {
	if !validRegistrationID(tenant) || !trace.Valid() || !after.Valid() {
		return TraceCursor{}, ErrInvalid
	}
	return TraceCursor{tenant: tenant, trace: trace, after: after}, nil
}

// After is zero for the initial/exhausted position. It conveys no permission.
func (c TraceCursor) After() traceid.Span { return c.after }

type TraceState uint8

const (
	TraceAvailable TraceState = iota + 1
	TraceNoSpan               // No final span at lookup; not proof no execution happened.
	TraceExpired              // Final-span expiry was confirmed by the catalog.
)

// TraceItem has an owned Span and the same authoritative Receipt used to fetch
// it only when State is TraceAvailable. Unavailable items carry just Execution
// and State. No captured bodies, inferred completion or causal ordering is added.
type TraceItem struct {
	Execution Execution
	State     TraceState
	Receipt   Receipt
	Span      record.Span
}

type TracePage struct {
	Items []TraceItem
	Next  TraceCursor
}

// TraceReader shares its Spans coordinator's admission slot through all catalog,
// object and decoding work. A spare slot permits one joined helper worker;
// otherwise reads stay serial. It owns no queues or local cache.
// The caller owns returned graphs and must bound results it retains after return.
type TraceReader struct {
	spans   *Spans
	catalog TraceCatalog
	limits  TraceLimits
}

func NewTraceReader(spans *Spans, limits TraceLimits) (*TraceReader, error) {
	if spans == nil || limits.MaxCandidates <= 0 || limits.MaxCandidates == int(^uint(0)>>1) || limits.MaxStoredBytes <= 0 || limits.OperationTimeout <= 0 {
		return nil, ErrInvalid
	}
	// Discovery must use the same authoritative catalog as final-span lookup.
	catalog, ok := spans.catalog.(TraceCatalog)
	if !ok {
		return nil, ErrInvalid
	}
	return &TraceReader{spans: spans, catalog: catalog, limits: limits}, nil
}

// ReadTrace returns one bounded current page in span-ID byte order. A cursor
// does not freeze membership: new IDs ahead may appear; new IDs behind require
// restarting. Every request and item checks current authority/eligibility. A
// failure returns a zero page, never partial success or a consumed cursor.
func (r *TraceReader) ReadTrace(ctx context.Context, tenant string, trace traceid.Trace, cursor TraceCursor) (TracePage, error) {
	if !validRegistrationID(tenant) || !trace.Valid() || (!cursor.IsZero() && (cursor.tenant != tenant || cursor.trace != trace || !cursor.after.Valid())) {
		return TracePage{}, ErrInvalid
	}
	s := r.spans.store
	if err := s.admit(ctx); err != nil {
		return TracePage{}, err
	}
	defer s.end()
	ctx, cancel := context.WithTimeout(ctx, r.limits.OperationTimeout)
	defer cancel()
	principal, err := s.authorizer.AuthorizeRead(ctx, tenant)
	if err != nil {
		return TracePage{}, fmt.Errorf("authorize trace reader: %w", err)
	}
	if !validRegistrationID(principal) {
		return TracePage{}, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return TracePage{}, err
	}
	keys, err := r.catalog.ScanTrace(ctx, tenant, trace, cursor.after, r.limits.MaxCandidates+1, principal)
	if err != nil {
		return TracePage{}, fmt.Errorf("scan trace: %w", err)
	}
	if len(keys) > r.limits.MaxCandidates+1 {
		return TracePage{}, ErrIntegrity
	}
	previous := cursor.after
	for _, key := range keys {
		if !key.Valid() || bytes.Compare(key[:], previous[:]) <= 0 {
			return TracePage{}, ErrIntegrity
		}
		previous = key
	}
	page := TracePage{}
	used, consumed := 0, 0
items:
	for _, key := range keys {
		if consumed == r.limits.MaxCandidates {
			break
		}
		if err := ctx.Err(); err != nil {
			return TracePage{}, err
		}
		execution := Execution{Tenant: tenant, Trace: trace, Span: key}
		item := TraceItem{Execution: execution}
		receipt, err := s.lookup(ctx, spanIdentity(execution))
		switch {
		case errors.Is(err, ErrNotFound):
			item.State = TraceNoSpan
		case errors.Is(err, ErrExpired):
			item.State = TraceExpired
		case err != nil:
			return TracePage{}, err
		default:
			size := receipt.Reference.Size
			if size > r.limits.MaxStoredBytes {
				return TracePage{}, ErrTooLarge
			}
			if size > r.limits.MaxStoredBytes-used {
				// This key fits a fresh page. Leave it unconsumed for the next call.
				break items
			}
			item.State, item.Receipt = TraceAvailable, receipt
			used += size
		}
		page.Items = append(page.Items, item)
		consumed++
	}
	if consumed < len(keys) {
		// A nonempty key always fits an empty page or returns ErrTooLarge above.
		page.Next = TraceCursor{tenant: tenant, trace: trace, after: keys[consumed-1]}
	}
	if err := r.loadSpans(ctx, page.Items); err != nil {
		return TracePage{}, err
	}
	return page, nil
}

// loadSpans receives only authorized, budgeted receipts. Each worker writes
// disjoint item slots; join completes before any graph or admission slot escapes.
// The page's slot covers one worker. Taking a spare slot for the second preserves
// the global work bound, with serial fallback rather than a queue or ErrBusy.
func (r *TraceReader) loadSpans(ctx context.Context, items []TraceItem) error {
	available := 0
	for _, item := range items {
		if item.State == TraceAvailable {
			available++
		}
	}
	workers := 1
	if available > 1 {
		if err := r.spans.store.admit(ctx); err == nil {
			workers = 2
			defer r.spans.store.end()
		} else if !errors.Is(err, ErrBusy) {
			return err
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var first error
	var once sync.Once
	load := func(start int) {
		position := 0
		for i := range items {
			if items[i].State != TraceAvailable {
				continue
			}
			assigned := position%workers == start
			position++
			if !assigned {
				continue
			}
			if err := ctx.Err(); err != nil {
				once.Do(func() { first = err; cancel() })
				return
			}
			item := &items[i]
			stored, err := r.spans.store.readObject(ctx, item.Receipt.Reference)
			if err == nil {
				item.Span, err = r.spans.decodeSpan(ctx, item.Execution, stored)
			}
			if err != nil {
				once.Do(func() { first = err; cancel() })
				return
			}
		}
	}
	var joined sync.WaitGroup
	if workers == 2 {
		joined.Go(func() { load(1) })
	}
	load(0)
	joined.Wait()
	if first != nil {
		return first
	}
	return ctx.Err()
}
