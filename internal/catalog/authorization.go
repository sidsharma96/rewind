package catalog

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/sidsharma96/rewind/internal/ingest"
)

// WriterFor derives the owner from the registered binding, never a request label.
// It checks the current writer grant under the shared project gate. It does not
// inspect records or replace publication's run, expiry and permission checks.
func (c *Catalog) WriterFor(ctx context.Context, execution ingest.Execution, principal string) (ingest.Attribution, error) {
	if !validExecution(execution) || !validID(principal) {
		return ingest.Attribution{}, ingest.ErrInvalid
	}
	var writer ingest.Attribution
	err := c.permission(ctx, execution.Tenant, func(ctx context.Context, tx pgx.Tx) error {
		var run, instance []byte
		query := "SELECT b.run_id,b.instance_id FROM " + c.table("bindings") + " b JOIN " + c.table("writer_grants") + ` g
            ON g.project=b.project AND g.run_id=b.run_id AND g.instance_id=b.instance_id
            WHERE b.project=$1 AND b.trace_id=$2 AND b.span_id=$3 AND g.principal_id=$4`
		err := tx.QueryRow(ctx, query, []byte(execution.Tenant), execution.Trace[:], execution.Span[:], []byte(principal)).Scan(&run, &instance)
		if errors.Is(err, pgx.ErrNoRows) {
			return ingest.ErrDenied
		}
		if err != nil {
			return backend("check authenticated writer", err)
		}
		if !validID(string(run)) || !validID(string(instance)) {
			return ingest.ErrIntegrity
		}
		writer = ingest.Attribution{Owner: ingest.Owner{Run: string(run), Instance: string(instance)}, SubmittedBy: principal}
		return nil
	})
	if err != nil {
		return ingest.Attribution{}, err
	}
	return writer, nil
}

// CheckReader checks only the explicit project-reader grant. Owning, writing or
// registering executions never implies read access. Lookup repeats this check.
func (c *Catalog) CheckReader(ctx context.Context, project, principal string) error {
	if !validID(project) || !validID(principal) {
		return ingest.ErrInvalid
	}
	return c.permission(ctx, project, func(ctx context.Context, tx pgx.Tx) error {
		var allowed bool
		err := tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM "+c.table("reader_grants")+" WHERE project=$1 AND principal_id=$2)", []byte(project), []byte(principal)).Scan(&allowed)
		if err != nil {
			return backend("check authenticated reader", err)
		}
		if !allowed {
			return ingest.ErrDenied
		}
		return nil
	})
}

// CheckCreator checks exact-call or separately granted trusted-host authority.
// It creates no grant or binding. RegisterOwnership repeats permission and run
// checks atomically with the binding decision, including on retries.
func (c *Catalog) CheckCreator(ctx context.Context, execution ingest.Execution, owner ingest.Owner, principal string) error {
	r := ingest.Registration{Execution: execution, Owner: owner, RegisteredBy: principal}
	if !validRegistration(r) {
		return ingest.ErrInvalid
	}
	return c.permission(ctx, execution.Tenant, func(ctx context.Context, tx pgx.Tx) error {
		return c.checkCreator(ctx, tx, r)
	})
}

func validExecution(e ingest.Execution) bool {
	return validID(e.Tenant) && e.Trace.Valid() && e.Span.Valid()
}

// permission owns one bounded preliminary policy transaction. Its callback never
// performs object I/O or changes policy. The project gate orders it with trusted
// grant changes, but a successful check is not a lease for a later operation.
func (c *Catalog) permission(ctx context.Context, project string, check func(context.Context, pgx.Tx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case c.active <- struct{}{}:
	default:
		return ingest.ErrBusy
	}
	defer func() { <-c.active }()
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	tx, err := begin(ctx, c.pool)
	if err != nil {
		return backend("begin authentication policy check", errors.Join(err, ctx.Err()))
	}
	defer rollback(tx)
	err = c.gate(ctx, tx, project, "SHARE")
	if err == nil {
		err = check(ctx, tx)
	}
	if err != nil {
		if cleanup := rollback(tx); cleanup != nil {
			return backend("rollback authentication policy check", errors.Join(cleanup, ctx.Err()))
		}
		return err
	}
	return backend("commit authentication policy check", tx.Commit(ctx))
}
