// Package catalog supplies PostgreSQL ownership registration and record publication/lookup.
// It does not implement credential verification, run controls or object I/O.
// The installed schema also stores each project's trusted retention policy.
package catalog

import (
	"context"
	_ "embed"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sidsharma96/rewind/internal/ingest"
)

//go:embed schema.sql
var schemaSQL string

// Limits bounds admitted operations (including pool wait) and each whole request.
// Rollback cleanup may take up to two additional seconds after cancellation.
type Limits struct {
	MaxActive        int
	OperationTimeout time.Duration
}

// Catalog borrows its pool; the caller configures credentials, primary/durability
// and pool limits, and closes it after operations finish. Schema is trusted local
// configuration. All application table references are explicitly qualified.
// The pool owns asynchronous cleanup of discarded connections; its owner must
// call Close at shutdown to wait for those resources. Only trusted service code
// may access the database; telemetry is not a principal.
type Catalog struct {
	pool    *pgxpool.Pool
	schema  string
	active  chan struct{}
	timeout time.Duration
}

var _ ingest.OwnershipCatalog = (*Catalog)(nil)

func New(pool *pgxpool.Pool, schema string, limits Limits) (*Catalog, error) {
	if pool == nil || !validSchema(schema) || limits.MaxActive <= 0 || limits.OperationTimeout <= 0 {
		return nil, ingest.ErrInvalid
	}
	return &Catalog{pool: pool, schema: schema, active: make(chan struct{}, limits.MaxActive), timeout: limits.OperationTimeout}, nil
}

func validSchema(s string) bool {
	if s == "" || len(s) > 63 {
		return false
	}
	for i, ch := range s {
		if !(ch == '_' || ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || i > 0 && ch >= '0' && ch <= '9') {
			return false
		}
	}
	return true
}
func (c *Catalog) table(name string) string { return pgx.Identifier{c.schema, name}.Sanitize() }

// Install creates a NEW owned schema and its required tables atomically.
// Existing schemas are refused, never altered or dropped. Caller bounds ctx and
// controls installation separately from serving requests; this is not migration.
func Install(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	if pool == nil || !validSchema(schema) {
		return ingest.ErrInvalid
	}
	tx, err := begin(ctx, pool)
	if err != nil {
		return backend("begin schema install", err)
	}
	defer rollback(tx)
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err = tx.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		return backend("create owned schema", err)
	}
	if _, err = tx.Exec(ctx, "SET LOCAL search_path TO "+quoted); err != nil {
		return backend("select owned schema", err)
	}
	if _, err = tx.Exec(ctx, schemaSQL); err != nil {
		return backend("install tables", err)
	}
	return backend("commit schema install", tx.Commit(ctx))
}

// backendError keeps provider text (which can contain connection configuration)
// out of Error(), while preserving errors.Is/As for callers inspecting the cause.
type backendError struct {
	operation string
	cause     error
}

func (e *backendError) Error() string { return "catalog: " + e.operation + " failed" }
func (e *backendError) Unwrap() error {
	// Provider/control failures must not masquerade as a confirmed domain refusal.
	if errors.Is(e.cause, ingest.ErrDenied) || errors.Is(e.cause, ingest.ErrOwnershipConflict) || errors.Is(e.cause, ingest.ErrExpired) || errors.Is(e.cause, ingest.ErrNotFound) || errors.Is(e.cause, ingest.ErrConflict) {
		return nil
	}
	return e.cause
}
func backend(op string, err error) error {
	if err == nil {
		return nil
	}
	return &backendError{op, err}
}

// begin enforces minimum PostgreSQL commit prerequisites; remote placement and
// failover policy still belong to the operator. Never rely on a caller's
// synchronous_commit setting, which could acknowledge before local WAL flush.
// Preserve remote_apply when the operator already requires that stronger mode.
func begin(ctx context.Context, pool *pgxpool.Pool) (pgx.Tx, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, err
	}
	var durable bool
	var commitLevel string
	err = tx.QueryRow(ctx, "SELECT current_setting('fsync')='on' AND NOT pg_is_in_recovery() AND current_setting('transaction_read_only')='off', current_setting('synchronous_commit')").Scan(&durable, &commitLevel)
	if err == nil && !durable {
		err = errors.New("writable primary with fsync enabled required")
	}
	if err == nil && commitLevel != "on" && commitLevel != "remote_apply" {
		_, err = tx.Exec(ctx, "SET LOCAL synchronous_commit TO on")
	}
	if err != nil {
		return nil, errors.Join(err, rollback(tx))
	}
	return tx, nil
}

func rollback(tx pgx.Tx) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := tx.Rollback(ctx)
	if errors.Is(err, pgx.ErrTxClosed) {
		return nil
	}
	return err
}
func validRegistration(r ingest.Registration) bool {
	if !r.Execution.Trace.Valid() || !r.Execution.Span.Valid() {
		return false
	}
	// The remaining opaque identity bytes need not be valid UTF-8.
	for _, s := range []string{r.Execution.Tenant, r.Owner.Run, r.Owner.Instance, r.RegisteredBy} {
		if s == "" || len(s) > 256 {
			return false
		}
	}
	return true
}

// RegisterOwnership checks current creator permission and both affected runs
// under the shared project gate. Eligibility uses fresh database time after the
// binding lock; success waits for commit. Unexpected errors remain uncertain to
// Registrar, including commit/closure failure. No background retries are owned.
func (c *Catalog) RegisterOwnership(ctx context.Context, r ingest.Registration) (ingest.Registration, bool, error) {
	if !validRegistration(r) {
		return ingest.Registration{}, false, ingest.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return ingest.Registration{}, false, err
	}
	select {
	case c.active <- struct{}{}:
	default:
		return ingest.Registration{}, false, ingest.ErrBusy
	}
	defer func() { <-c.active }()
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	tx, err := begin(ctx, c.pool)
	if err != nil {
		return ingest.Registration{}, false, backend("begin registration", errors.Join(err, ctx.Err()))
	}
	defer rollback(tx)
	original, duplicate, expired, err := c.register(ctx, tx, r)
	if err != nil {
		if cleanup := rollback(tx); cleanup != nil {
			return ingest.Registration{}, false, backend("rollback registration", errors.Join(cleanup, ctx.Err()))
		}
		if len(expired) > 0 {
			if closeErr := c.closeElapsed(ctx, r.Execution.Tenant, expired); closeErr != nil {
				return ingest.Registration{}, false, backend("record closure", errors.Join(closeErr, ctx.Err()))
			}
		}
		if ctx.Err() != nil && !errors.Is(err, ingest.ErrDenied) && !errors.Is(err, ingest.ErrOwnershipConflict) {
			err = backend("registration", errors.Join(err, ctx.Err()))
		}
		return ingest.Registration{}, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return ingest.Registration{}, false, backend("commit registration", errors.Join(err, ctx.Err()))
	}
	return original, duplicate, nil
}

func (c *Catalog) gate(ctx context.Context, tx pgx.Tx, project string, mode string) error {
	var id []byte
	err := tx.QueryRow(ctx, "SELECT id FROM "+c.table("projects")+" WHERE id=$1 FOR "+mode, []byte(project)).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return ingest.ErrDenied
	}
	return backend("lock project", err)
}

type runInfo struct {
	id            string
	phase         string
	at, timeUntil time.Time
	admission     ingest.RunAdmission
}

func (c *Catalog) run(ctx context.Context, tx pgx.Tx, project, id string) (runInfo, error) {
	var phase string
	var at, until *time.Time
	err := tx.QueryRow(ctx, "SELECT phase,completed_at,until_at FROM "+c.table("runs")+" WHERE project=$1 AND run_id=$2", []byte(project), []byte(id)).Scan(&phase, &at, &until)
	if errors.Is(err, pgx.ErrNoRows) {
		return runInfo{}, ingest.ErrDenied
	}
	if err != nil {
		return runInfo{}, backend("read run", err)
	}
	state := ingest.NewRunAdmission()
	info := runInfo{id: id, phase: phase}
	if at != nil || until != nil {
		if at == nil || until == nil || at.IsZero() || !until.After(*at) {
			return runInfo{}, ingest.ErrIntegrity
		}
		duration := until.Sub(*at)
		if !at.Add(duration).Equal(*until) {
			return runInfo{}, ingest.ErrIntegrity
		}
		state, err = state.Complete(*at, duration)
		if err != nil {
			return runInfo{}, ingest.ErrIntegrity
		}
		info.at, info.timeUntil = at.UTC(), until.UTC()
	}
	switch phase {
	case "running":
		if at != nil {
			return runInfo{}, ingest.ErrIntegrity
		}
	case "completed":
		if at == nil {
			return runInfo{}, ingest.ErrIntegrity
		}
	case "closed":
		if at == nil {
			return runInfo{}, ingest.ErrIntegrity
		}
		state, err = state.CloseElapsed(*until)
	case "revoked":
		state, err = state.Revoke()
	default:
		return runInfo{}, ingest.ErrIntegrity
	}
	if err != nil {
		return runInfo{}, ingest.ErrIntegrity
	}
	info.admission = state
	return info, nil
}

func (c *Catalog) register(ctx context.Context, tx pgx.Tx, r ingest.Registration) (ingest.Registration, bool, []runInfo, error) {
	fail := func(err error) (ingest.Registration, bool, []runInfo, error) {
		return ingest.Registration{}, false, nil, err
	}
	if err := c.gate(ctx, tx, r.Execution.Tenant, "SHARE"); err != nil {
		return fail(err)
	}
	args := []any{[]byte(r.Execution.Tenant), r.Execution.Trace[:], r.Execution.Span[:], []byte(r.Owner.Run), []byte(r.Owner.Instance), []byte(r.RegisteredBy)}
	var allowed bool
	err := tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM "+c.table("creator_grants")+" WHERE project=$1 AND trace_id=$2 AND span_id=$3 AND run_id=$4 AND instance_id=$5 AND principal_id=$6)", args...).Scan(&allowed)
	if err != nil {
		return fail(backend("check creator", err))
	}
	if !allowed {
		return fail(ingest.ErrDenied)
	}
	tag, err := tx.Exec(ctx, "INSERT INTO "+c.table("bindings")+"(project,trace_id,span_id,run_id,instance_id,registered_by) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING", args...)
	if err != nil {
		return fail(backend("insert binding", err))
	}
	var run, instance, registrar []byte
	err = tx.QueryRow(ctx, "SELECT run_id,instance_id,registered_by FROM "+c.table("bindings")+" WHERE project=$1 AND trace_id=$2 AND span_id=$3 FOR UPDATE", args[:3]...).Scan(&run, &instance, &registrar)
	if err != nil {
		return fail(backend("lock binding", err))
	}
	original := ingest.Registration{Execution: r.Execution, Owner: ingest.Owner{Run: string(run), Instance: string(instance)}, RegisteredBy: string(registrar)}
	if !validRegistration(original) {
		return fail(ingest.ErrIntegrity)
	}
	identical := tag.RowsAffected() == 0 && original.Owner == r.Owner
	proposed, err := c.run(ctx, tx, r.Execution.Tenant, r.Owner.Run)
	if err != nil {
		return fail(err)
	}
	runs := []runInfo{proposed}
	if original.Owner.Run != r.Owner.Run {
		old, err := c.run(ctx, tx, r.Execution.Tenant, original.Owner.Run)
		if err != nil {
			return fail(err)
		}
		runs = append(runs, old)
	}
	var now time.Time
	if err := tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return fail(backend("read eligibility time", err))
	}
	var expired []runInfo
	var admissionErr error
	for _, run := range runs {
		if err := run.admission.CheckRegistration(now, identical); err != nil {
			admissionErr = err
		}
		if run.phase == "completed" && !now.Before(run.timeUntil) {
			expired = append(expired, run)
		}
	}
	if admissionErr != nil {
		return ingest.Registration{}, false, expired, admissionErr
	}
	if original.Owner != r.Owner {
		return fail(ingest.ErrOwnershipConflict)
	}
	return original, identical, nil, nil
}

func (c *Catalog) closeElapsed(ctx context.Context, project string, observed []runInfo) error {
	tx, err := begin(ctx, c.pool)
	if err != nil {
		return backend("begin closure", err)
	}
	defer rollback(tx)
	if err = c.gate(ctx, tx, project, "UPDATE"); err != nil {
		return backend("closure project unavailable", err)
	}
	for _, old := range observed {
		current, err := c.run(ctx, tx, project, old.id)
		if err != nil {
			return backend("recheck closure", err)
		}
		if current.phase == "revoked" || current.phase == "closed" {
			continue
		}
		if current.phase != "completed" || !current.at.Equal(old.at) || !current.timeUntil.Equal(old.timeUntil) {
			return backend("closure observation changed", ingest.ErrIntegrity)
		}
		if _, err = current.admission.CloseElapsed(old.timeUntil); err != nil {
			return backend("validate closure", err)
		}
		if _, err = tx.Exec(ctx, "UPDATE "+c.table("runs")+" SET phase='closed' WHERE project=$1 AND run_id=$2", []byte(project), []byte(old.id)); err != nil {
			return backend("save closure", err)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return backend("commit closure", err)
	}
	return nil
}
