package ingest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
// sequential object and decoding work. It owns no workers, queues or local cache.
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
			stored, err := s.readObject(ctx, receipt.Reference)
			if err != nil {
				return TracePage{}, err
			}
			span, err := r.spans.decodeSpan(ctx, execution, stored)
			if err != nil {
				return TracePage{}, err
			}
			item.State, item.Receipt, item.Span = TraceAvailable, receipt, span
			used += size
		}
		page.Items = append(page.Items, item)
		consumed++
	}
	if consumed < len(keys) {
		// A nonempty key always fits an empty page or returns ErrTooLarge above.
		page.Next = TraceCursor{tenant: tenant, trace: trace, after: keys[consumed-1]}
	}
	if err := ctx.Err(); err != nil {
		return TracePage{}, err
	}
	return page, nil
}
