package catalog

import (
	"bytes"
	"context"
	"errors"

	"github.com/sidsharma96/rewind/internal/ingest"
	"github.com/sidsharma96/rewind/internal/traceid"
)

var _ ingest.TraceCatalog = (*Catalog)(nil)

// ScanTrace selects current accepted execution keys, not registration-only
// bindings. Both kinds and expired markers count toward its logical limit. The
// primary-key prefix supports ordered seeking; LIMIT bounds returned identities,
// not MVCC visits, lock waits or an arbitrary planner's physical work. The caller
// must resolve each final span with Lookup for current permission and saved expiry.
func (c *Catalog) ScanTrace(ctx context.Context, tenant string, trace traceid.Trace, after traceid.Span, limit int, principal string) ([]traceid.Span, error) {
	if !validID(tenant) || !trace.Valid() || limit <= 0 || !validID(principal) {
		return nil, ingest.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case c.active <- struct{}{}:
	default:
		return nil, ingest.ErrBusy
	}
	defer func() { <-c.active }()
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	tx, err := begin(ctx, c.pool)
	if err != nil {
		return nil, backend("begin trace scan", errors.Join(err, ctx.Err()))
	}
	defer rollback(tx)
	if err := c.gate(ctx, tx, tenant, "SHARE"); err != nil {
		return nil, err
	}
	var allowed bool
	if err := tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM "+c.table("reader_grants")+" WHERE project=$1 AND principal_id=$2)", []byte(tenant), []byte(principal)).Scan(&allowed); err != nil {
		return nil, backend("check trace reader", err)
	}
	if !allowed {
		return nil, ingest.ErrDenied
	}
	rows, err := tx.Query(ctx, c.traceQuery(), []byte(tenant), trace[:], after[:], limit)
	if err != nil {
		return nil, backend("scan trace keys", err)
	}
	defer rows.Close()
	var keys []traceid.Span
	previous := after
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, backend("decode trace key", err)
		}
		key, err := traceid.SpanFromBytes(raw)
		if err != nil || bytes.Compare(key[:], previous[:]) <= 0 || len(keys) == limit {
			return nil, ingest.ErrIntegrity
		}
		keys = append(keys, key)
		previous = key
	}
	if err := rows.Err(); err != nil {
		return nil, backend("finish trace scan", err)
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, backend("commit trace scan", errors.Join(err, ctx.Err()))
	}
	return keys, nil
}

func (c *Catalog) traceQuery() string {
	return "SELECT span_id FROM " + c.table("records") + " WHERE project=$1 AND trace_id=$2 AND span_id>$3 GROUP BY span_id ORDER BY span_id LIMIT $4"
}
