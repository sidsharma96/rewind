package catalog

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sidsharma96/rewind/internal/ingest"
)

var _ ingest.Catalog = (*Catalog)(nil)

// CheckWrite checks current writer authority, binding, run and record expiry. It
// reserves nothing and creates neither a binding nor a record. Observed expiry
// and elapsed run closure must be saved before confirming their refusal.
func (c *Catalog) CheckWrite(ctx context.Context, id ingest.Identity, writer ingest.Attribution) error {
	_, err := c.write(ctx, id, writer, nil)
	return err
}

// Publish accepts an already-created immutable object reference. It performs no
// object I/O and compares exact references, as the current opaque-byte Store does.
// Decoded logical-content retry comparison remains a separate integration step.
// Current authority is checked before every outcome. Success follows commit;
// unexpected errors, including lost commit replies, never confirm refusal.
func (c *Catalog) Publish(ctx context.Context, ref ingest.Reference, writer ingest.Attribution) (ingest.Receipt, error) {
	if ref.Size < 0 {
		return ingest.Receipt{}, ingest.ErrInvalid
	}
	return c.write(ctx, ref.Identity, writer, &ref)
}

// A nil proposal is the nonreserving pre-upload check; a nonnil proposal is final
// publication. One admission slot/deadline covers the ordinary transaction and
// any separate run-closure transaction. No lock is upgraded in place.
func (c *Catalog) write(ctx context.Context, id ingest.Identity, writer ingest.Attribution, proposal *ingest.Reference) (ingest.Receipt, error) {
	if _, err := recordKind(id); err != nil || !validID(writer.Owner.Run) || !validID(writer.Owner.Instance) || !validID(writer.SubmittedBy) {
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
	tx, err := begin(ctx, c.pool)
	if err != nil {
		return ingest.Receipt{}, backend("begin record write", errors.Join(err, ctx.Err()))
	}
	defer rollback(tx)
	receipt, closeRun, err := c.writeLocked(ctx, tx, id, writer, proposal)
	if err != nil && !errors.Is(err, ingest.ErrExpired) {
		if cleanup := rollback(tx); cleanup != nil {
			return ingest.Receipt{}, backend("rollback record write", errors.Join(cleanup, ctx.Err()))
		}
		if closeRun != nil {
			if cleanup := c.closeElapsed(ctx, id.Tenant, []runInfo{*closeRun}); cleanup != nil {
				return ingest.Receipt{}, backend("record delivery closure", cleanup)
			}
		}
		return ingest.Receipt{}, err
	}
	// ErrExpired can represent a write to the saved expiry flag. Confirm that
	// state before returning the domain refusal, just as Lookup does.
	if commitErr := tx.Commit(ctx); commitErr != nil {
		return ingest.Receipt{}, backend("commit record write", errors.Join(commitErr, ctx.Err()))
	}
	return receipt, err
}

func (c *Catalog) writeLocked(ctx context.Context, tx pgx.Tx, id ingest.Identity, writer ingest.Attribution, proposal *ingest.Reference) (ingest.Receipt, *runInfo, error) {
	run, now, err := c.lockWriter(ctx, tx, id, writer)
	if err != nil {
		return ingest.Receipt{}, nil, err
	}
	if err = run.admission.CheckRecord(now); err != nil {
		if run.phase == "completed" && !now.Before(run.timeUntil) {
			return ingest.Receipt{}, &run, err
		}
		return ingest.Receipt{}, nil, err
	}
	kind, err := recordKind(id)
	if err != nil {
		return ingest.Receipt{}, nil, err
	}
	args := []any{[]byte(id.Tenant), id.Trace[:], id.Span[:], kind}
	original, lifetime, err := c.record(ctx, tx, id, args)
	if errors.Is(err, ingest.ErrNotFound) {
		if proposal == nil {
			return ingest.Receipt{}, nil, nil
		}
		receipt, err := c.insertRecord(ctx, tx, *proposal, writer, kind, now)
		return receipt, nil, err
	}
	if err != nil {
		return ingest.Receipt{}, nil, err
	}
	if original.Attribution.Owner != writer.Owner {
		return ingest.Receipt{}, nil, ingest.ErrIntegrity
	}
	observed, err := lifetime.ObserveExpiry(now)
	if err != nil {
		return ingest.Receipt{}, nil, ingest.ErrIntegrity
	}
	if observed.Expired() {
		if !lifetime.Expired() {
			tag, err := tx.Exec(ctx, "UPDATE "+c.table("records")+" SET expired=true WHERE project=$1 AND trace_id=$2 AND span_id=$3 AND kind=$4", args...)
			if err != nil {
				return ingest.Receipt{}, nil, backend("save write expiry", err)
			}
			if tag.RowsAffected() != 1 {
				return ingest.Receipt{}, nil, ingest.ErrIntegrity
			}
		}
		return ingest.Receipt{}, nil, ingest.ErrExpired
	}
	if proposal == nil {
		return ingest.Receipt{}, nil, nil
	}
	if original.Reference != *proposal {
		return ingest.Receipt{}, nil, ingest.ErrConflict
	}
	original.Duplicate = true
	return original, nil, nil
}

// The shared project gate orders grant/run/policy changes with this decision.
// Lock the existing binding exclusively BEFORE reading run eligibility/time or
// record state. A missing binding is denial, never authority to create one.
func (c *Catalog) lockWriter(ctx context.Context, tx pgx.Tx, id ingest.Identity, writer ingest.Attribution) (runInfo, time.Time, error) {
	if err := c.gate(ctx, tx, id.Tenant, "SHARE"); err != nil {
		return runInfo{}, time.Time{}, err
	}
	var allowed bool
	grantQuery := "SELECT EXISTS(SELECT 1 FROM " + c.table("writer_grants") + `
        WHERE project=$1 AND run_id=$2 AND instance_id=$3 AND principal_id=$4)`
	err := tx.QueryRow(ctx, grantQuery, []byte(id.Tenant), []byte(writer.Owner.Run),
		[]byte(writer.Owner.Instance), []byte(writer.SubmittedBy)).Scan(&allowed)
	if err != nil {
		return runInfo{}, time.Time{}, backend("check writer", err)
	}
	if !allowed {
		return runInfo{}, time.Time{}, ingest.ErrDenied
	}
	var run, instance []byte
	bindingQuery := "SELECT run_id,instance_id FROM " + c.table("bindings") + `
        WHERE project=$1 AND trace_id=$2 AND span_id=$3 FOR UPDATE`
	err = tx.QueryRow(ctx, bindingQuery, []byte(id.Tenant), id.Trace[:], id.Span[:]).Scan(&run, &instance)
	if errors.Is(err, pgx.ErrNoRows) {
		return runInfo{}, time.Time{}, ingest.ErrDenied
	}
	if err != nil {
		return runInfo{}, time.Time{}, backend("lock write binding", err)
	}
	if string(run) != writer.Owner.Run || string(instance) != writer.Owner.Instance {
		return runInfo{}, time.Time{}, ingest.ErrDenied
	}
	state, err := c.run(ctx, tx, id.Tenant, writer.Owner.Run)
	if err != nil {
		return runInfo{}, time.Time{}, err
	}
	var now time.Time
	if err = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return runInfo{}, time.Time{}, backend("read write time", err)
	}
	return state, now.UTC(), nil
}

func (c *Catalog) insertRecord(ctx context.Context, tx pgx.Tx, ref ingest.Reference, writer ingest.Attribution, kind string, accepted time.Time) (ingest.Receipt, error) {
	var payloadUS, markerUS int64
	err := tx.QueryRow(ctx, "SELECT payload_retention_us,marker_window_us FROM "+c.table("projects")+" WHERE id=$1", []byte(ref.Identity.Tenant)).Scan(&payloadUS, &markerUS)
	if err != nil {
		return ingest.Receipt{}, backend("read publication policy", err)
	}
	const maxMicros = int64(^uint64(0)>>1) / int64(time.Microsecond)
	if payloadUS <= 0 || payloadUS > maxMicros || markerUS <= 0 || markerUS > maxMicros {
		return ingest.Receipt{}, ingest.ErrIntegrity
	}
	expires := accepted.Add(time.Duration(payloadUS) * time.Microsecond)
	if accepted.IsZero() || !expires.After(accepted) {
		return ingest.Receipt{}, ingest.ErrIntegrity
	}
	lifetime, err := ingest.NewLifetime(expires, time.Duration(markerUS)*time.Microsecond)
	if err != nil {
		return ingest.Receipt{}, ingest.ErrIntegrity
	}
	query := "INSERT INTO " + c.table("records") + `
  (project,trace_id,span_id,kind,digest,size,owner_run,owner_instance,
   submitted_by,accepted_at,expires_at,marker_until)
  VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`
	tag, err := tx.Exec(ctx, query, []byte(ref.Identity.Tenant), ref.Identity.Trace[:], ref.Identity.Span[:], kind, ref.Digest[:], int64(ref.Size), []byte(writer.Owner.Run), []byte(writer.Owner.Instance), []byte(writer.SubmittedBy), accepted, expires, lifetime.MarkerUntil())
	if err != nil {
		return ingest.Receipt{}, backend("insert record", err)
	}
	if tag.RowsAffected() != 1 {
		return ingest.Receipt{}, ingest.ErrIntegrity
	}
	return ingest.Receipt{Reference: ref, Attribution: writer, AcceptedAt: accepted, ExpiresAt: expires}, nil
}
