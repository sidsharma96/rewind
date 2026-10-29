package catalog

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sidsharma96/rewind/internal/ingest"
)

func validID(s string) bool { return len(s) > 0 && len(s) <= 256 }
func recordKind(id ingest.Identity) (string, error) {
	if !validID(id.Tenant) || !id.Trace.Valid() || !id.Span.Valid() {
		return "", ingest.ErrInvalid
	}
	switch id.Kind {
	case ingest.SpanRecord:
		return "span", nil
	case ingest.CaptureRecord:
		return "capture", nil
	default:
		return "", ingest.ErrInvalid
	}
}

// Lookup checks the current project-reader grant and returns one original receipt.
// Live and already expired records use shared binding locks. A first expiry
// discovery ends that transaction and restarts once with an exclusive binding lock
// and fresh checks. One admission slot/deadline spans both attempts; no goroutines,
// background retries or object requests are owned. This does not implement Publish.
func (c *Catalog) Lookup(ctx context.Context, id ingest.Identity, principal string) (ingest.Receipt, error) {
	if _, err := recordKind(id); err != nil || !validID(principal) {
		return ingest.Receipt{}, ingest.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return ingest.Receipt{}, err
	}
	select {
	case c.active <- struct{}{}:
	default:
		return ingest.Receipt{}, ingest.ErrBusy
	}
	defer func() { <-c.active }()
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	receipt, restart, err := c.lookupAttempt(ctx, id, principal, false)
	if err != nil || !restart {
		return receipt, err
	}
	receipt, _, err = c.lookupAttempt(ctx, id, principal, true)
	return receipt, err
}

// Each attempt owns and closes its transaction. A restart is returned only after
// successful rollback releases all shared locks. Expiry may involve a write: commit
// must be confirmed before ErrExpired, even though the caller asked for a read.
func (c *Catalog) lookupAttempt(ctx context.Context, id ingest.Identity, principal string, exclusive bool) (ingest.Receipt, bool, error) {
	tx, err := begin(ctx, c.pool)
	if err != nil {
		return ingest.Receipt{}, false, backend("begin lookup", errors.Join(err, ctx.Err()))
	}
	defer rollback(tx)
	receipt, restart, err := c.lookupLocked(ctx, tx, id, principal, exclusive)
	if restart || err != nil && !errors.Is(err, ingest.ErrExpired) {
		if cleanup := rollback(tx); cleanup != nil {
			return ingest.Receipt{}, false, backend("rollback lookup", errors.Join(cleanup, ctx.Err()))
		}
		return ingest.Receipt{}, restart, err
	}
	if commitErr := tx.Commit(ctx); commitErr != nil {
		return ingest.Receipt{}, false, backend("commit lookup", errors.Join(commitErr, ctx.Err()))
	}
	return receipt, false, err
}

// lookupLocked never acquires a weaker lock and then upgrades it. The caller holds
// all locks until commit/rollback; every writer must take this binding exclusively.
func (c *Catalog) lookupLocked(ctx context.Context, tx pgx.Tx, id ingest.Identity, principal string, exclusive bool) (ingest.Receipt, bool, error) {
	if err := c.gate(ctx, tx, id.Tenant, "SHARE"); err != nil {
		return ingest.Receipt{}, false, err
	}
	var allowed bool
	err := tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM "+c.table("reader_grants")+" WHERE project=$1 AND principal_id=$2)", []byte(id.Tenant), []byte(principal)).Scan(&allowed)
	if err != nil {
		return ingest.Receipt{}, false, backend("check reader", err)
	}
	if !allowed {
		return ingest.Receipt{}, false, ingest.ErrDenied
	}
	mode := "SHARE"
	if exclusive {
		mode = "UPDATE"
	}
	args := []any{[]byte(id.Tenant), id.Trace[:], id.Span[:]}
	var run, instance []byte
	err = tx.QueryRow(ctx, "SELECT run_id,instance_id FROM "+c.table("bindings")+" WHERE project=$1 AND trace_id=$2 AND span_id=$3 FOR "+mode, args...).Scan(&run, &instance)
	if errors.Is(err, pgx.ErrNoRows) {
		return ingest.Receipt{}, false, ingest.ErrNotFound
	}
	if err != nil {
		return ingest.Receipt{}, false, backend("lock lookup binding", err)
	}
	kind, err := recordKind(id)
	if err != nil {
		return ingest.Receipt{}, false, err
	}
	args = append(args, kind)
	receipt, lifetime, err := c.record(ctx, tx, id, args)
	if err != nil {
		return ingest.Receipt{}, false, err
	}
	if receipt.Attribution.Owner != (ingest.Owner{Run: string(run), Instance: string(instance)}) {
		return ingest.Receipt{}, false, ingest.ErrIntegrity
	}
	if lifetime.Expired() {
		return ingest.Receipt{}, false, ingest.ErrExpired
	}
	var now time.Time
	if err = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return ingest.Receipt{}, false, backend("read lookup time", err)
	}
	observed, err := lifetime.ObserveExpiry(now)
	if err != nil {
		return ingest.Receipt{}, false, ingest.ErrIntegrity
	}
	if !observed.Expired() {
		return receipt, false, nil
	}
	if !exclusive {
		return ingest.Receipt{}, true, nil
	}
	tag, err := tx.Exec(ctx, "UPDATE "+c.table("records")+" SET expired=true WHERE project=$1 AND trace_id=$2 AND span_id=$3 AND kind=$4", args...)
	if err != nil {
		return ingest.Receipt{}, false, backend("save record expiry", err)
	}
	if tag.RowsAffected() != 1 {
		return ingest.Receipt{}, false, ingest.ErrIntegrity
	}
	return ingest.Receipt{}, false, ingest.ErrExpired
}

func (c *Catalog) record(ctx context.Context, tx pgx.Tx, id ingest.Identity, args []any) (ingest.Receipt, ingest.Lifetime, error) {
	var digest, run, instance, submitter []byte
	var size int64
	var expired bool
	var accepted, expires, until time.Time
	err := tx.QueryRow(ctx, "SELECT digest,size,owner_run,owner_instance,submitted_by,accepted_at,expires_at,marker_until,expired FROM "+c.table("records")+" WHERE project=$1 AND trace_id=$2 AND span_id=$3 AND kind=$4", args...).Scan(&digest, &size, &run, &instance, &submitter, &accepted, &expires, &until, &expired)
	if errors.Is(err, pgx.ErrNoRows) {
		return ingest.Receipt{}, ingest.Lifetime{}, ingest.ErrNotFound
	}
	if err != nil {
		return ingest.Receipt{}, ingest.Lifetime{}, backend("read record", err)
	}
	if len(digest) != 32 || size < 0 || int64(int(size)) != size || !validID(string(run)) || !validID(string(instance)) || !validID(string(submitter)) || accepted.IsZero() || !expires.After(accepted) {
		return ingest.Receipt{}, ingest.Lifetime{}, ingest.ErrIntegrity
	}
	lifetime, err := ingest.RestoreLifetime(expires, until, expired)
	if err != nil {
		return ingest.Receipt{}, ingest.Lifetime{}, ingest.ErrIntegrity
	}
	receipt := ingest.Receipt{Reference: ingest.Reference{Identity: id, Size: int(size)}, Attribution: ingest.Attribution{Owner: ingest.Owner{Run: string(run), Instance: string(instance)}, SubmittedBy: string(submitter)}, AcceptedAt: accepted.UTC(), ExpiresAt: expires.UTC()}
	copy(receipt.Reference.Digest[:], digest)
	return receipt, lifetime, nil
}
