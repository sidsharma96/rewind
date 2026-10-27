package catalog

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sidsharma96/rewind/internal/ingest"
)

type fixture struct {
	c    *Catalog
	pool *pgxpool.Pool
	r    ingest.Registration
}

func database(t *testing.T) fixture {
	t.Helper()
	dsn := os.Getenv("REWIND_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("set REWIND_TEST_POSTGRES to an explicitly disposable test database")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal("invalid test database configuration")
	}
	cfg.MaxConns = 8
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "8000"
	token := make([]byte, 12)
	if _, err = rand.Read(token); err != nil {
		t.Fatal(err)
	}
	schema := "rewind_test_" + hex.EncodeToString(token)
	cfg.ConnConfig.RuntimeParams["application_name"] = schema
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal("open test pool failed")
	}
	t.Cleanup(pool.Close)
	if err = Install(t.Context(), pool, schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := pool.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); err != nil {
			t.Error("remove owned test schema failed")
		}
	})
	c, err := New(pool, schema, Limits{4, 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return fixture{c, pool, ingest.Registration{Execution: ingest.Execution{Tenant: "project", Trace: "trace", Span: "42"}, Owner: ingest.Owner{Run: "run", Instance: "alice"}, RegisteredBy: "host-one"}}
}
func (f fixture) seed(t *testing.T, r ingest.Registration) {
	t.Helper()
	ctx := t.Context()
	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(tx)
	if _, err = tx.Exec(ctx, "INSERT INTO "+f.c.table("projects")+" VALUES($1) ON CONFLICT DO NOTHING", []byte(r.Execution.Tenant)); err != nil {
		t.Fatal(err)
	}
	if err = f.c.gate(ctx, tx, r.Execution.Tenant, "UPDATE"); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, "INSERT INTO "+f.c.table("runs")+"(project,run_id,phase) VALUES($1,$2,'running') ON CONFLICT DO NOTHING", []byte(r.Execution.Tenant), []byte(r.Owner.Run)); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, "INSERT INTO "+f.c.table("creator_grants")+" VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING", []byte(r.Execution.Tenant), []byte(r.Execution.Trace), []byte(r.Execution.Span), []byte(r.Owner.Run), []byte(r.Owner.Instance), []byte(r.RegisteredBy)); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}
func (f fixture) policy(t *testing.T, sql string, args ...any) {
	t.Helper()
	tx := f.holdPolicy(t)
	defer rollback(tx)
	if _, err := tx.Exec(t.Context(), sql, args...); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
}
func (f fixture) holdPolicy(t *testing.T) pgx.Tx {
	t.Helper()
	tx, err := f.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err = f.c.gate(t.Context(), tx, f.r.Execution.Tenant, "UPDATE"); err != nil {
		rollback(tx)
		t.Fatal(err)
	}
	t.Cleanup(func() { rollback(tx) })
	return tx
}
func blocked(t *testing.T, pool *pgxpool.Pool, holder uint32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
	defer cancel()
	for {
		var yes bool
		err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1::int=ANY(pg_blocking_pids(pid)))", int(holder)).Scan(&yes)
		if err != nil {
			t.Fatal("expected lock wait was not observed")
		}
		if yes {
			return
		}
	}
}

type registrationResult struct {
	original  ingest.Registration
	duplicate bool
	err       error
}

func start(t *testing.T, c *Catalog, ctx context.Context, r ingest.Registration) <-chan registrationResult {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan registrationResult, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		o, d, e := c.RegisterOwnership(ctx, r)
		done <- registrationResult{o, d, e}
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(12 * time.Second):
			t.Error("registration goroutine did not stop")
		}
	})
	return done
}

func receive(t *testing.T, done <-chan registrationResult) registrationResult {
	t.Helper()
	select {
	case r := <-done:
		return r
	case <-time.After(12 * time.Second):
		t.Fatal("registration did not finish")
		return registrationResult{}
	}
}
func TestRegistrationPersistenceAndCurrentPermission(t *testing.T) {
	f := database(t)
	f.seed(t, f.r)
	got, duplicate, err := f.c.RegisterOwnership(t.Context(), f.r)
	if err != nil || duplicate || got != f.r {
		t.Fatalf("first = %+v %v %v", got, duplicate, err)
	}
	other := f.r
	other.RegisteredBy = "host-two"
	f.seed(t, other)
	freshPool, err := pgxpool.NewWithConfig(t.Context(), f.pool.Config())
	if err != nil {
		t.Fatal("new pool failed")
	}
	defer freshPool.Close()
	fresh, err := New(freshPool, f.c.schema, Limits{2, 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	got, duplicate, err = fresh.RegisterOwnership(t.Context(), other)
	if err != nil || !duplicate || got != f.r {
		t.Fatalf("new pool duplicate = %+v %v %v", got, duplicate, err)
	}
	changed := other
	changed.Owner.Instance = "bob"
	f.seed(t, changed)
	if _, _, err = fresh.RegisterOwnership(t.Context(), changed); !errors.Is(err, ingest.ErrOwnershipConflict) {
		t.Fatalf("changed owner = %v", err)
	}
	f.policy(t, "DELETE FROM "+f.c.table("creator_grants")+" WHERE principal_id=$1", []byte(other.RegisteredBy))
	if _, _, err = fresh.RegisterOwnership(t.Context(), other); !errors.Is(err, ingest.ErrDenied) {
		t.Fatalf("removed grant = %v", err)
	}
	if err = Install(t.Context(), f.pool, f.c.schema); err == nil {
		t.Fatal("existing schema was overwritten")
	}
	if _, _, err = f.c.RegisterOwnership(t.Context(), f.r); err != nil {
		t.Fatal("failed installation damaged original schema")
	}
}
func TestRunDrainAndPersistedClosure(t *testing.T) {
	f := database(t)
	f.seed(t, f.r)
	if _, _, err := f.c.RegisterOwnership(t.Context(), f.r); err != nil {
		t.Fatal(err)
	}
	f.policy(t, "UPDATE "+f.c.table("runs")+" SET phase='completed',completed_at=clock_timestamp()-interval '1 minute',until_at=clock_timestamp()+interval '1 hour'")
	if got, dup, err := f.c.RegisterOwnership(t.Context(), f.r); err != nil || !dup || got != f.r {
		t.Fatalf("open drain retry: %+v %v %v", got, dup, err)
	}
	newCall := f.r
	newCall.Execution.Span = "new-after-completion"
	f.seed(t, newCall)
	if _, _, err := f.c.RegisterOwnership(t.Context(), newCall); !errors.Is(err, ingest.ErrDenied) {
		t.Fatalf("new drain binding = %v", err)
	}
	var count int
	if err := f.pool.QueryRow(t.Context(), "SELECT count(*) FROM "+f.c.table("bindings")).Scan(&count); err != nil || count != 1 {
		t.Fatal("refusal left tentative binding")
	}
	changed := f.r
	changed.Owner.Run = "other-running-run"
	f.seed(t, changed)
	if _, _, err := f.c.RegisterOwnership(t.Context(), changed); !errors.Is(err, ingest.ErrDenied) {
		t.Fatalf("bypassed original completed run: %v", err)
	}
	f.policy(t, "UPDATE "+f.c.table("runs")+" SET phase='completed',completed_at=clock_timestamp()-interval '20 minutes',until_at=clock_timestamp()-interval '10 minutes' WHERE run_id=$1", []byte(f.r.Owner.Run))
	if _, _, err := f.c.RegisterOwnership(t.Context(), f.r); !errors.Is(err, ingest.ErrDenied) {
		t.Fatalf("elapsed window = %v", err)
	}
	tx, err := f.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(tx)
	saved, err := f.c.run(t.Context(), tx, f.r.Execution.Tenant, f.r.Owner.Run)
	if err != nil || saved.phase != "closed" {
		t.Fatalf("closure was not persisted: %+v %v", saved, err)
	}
	if err = saved.admission.CheckRegistration(saved.timeUntil.Add(-time.Minute), true); !errors.Is(err, ingest.ErrDenied) {
		t.Fatalf("restored closure reopened at earlier time: %v", err)
	}
}
func TestOpaqueBinaryIdentityAndInputBounds(t *testing.T) {
	f := database(t)
	r := f.r
	blob := "\x00\xff" + strings.Repeat("x", 254)
	r.Execution = ingest.Execution{Tenant: blob, Trace: blob, Span: blob}
	r.Owner = ingest.Owner{Run: blob, Instance: blob}
	r.RegisteredBy = blob
	f.seed(t, r)
	got, dup, err := f.c.RegisterOwnership(t.Context(), r)
	if err != nil || dup || got != r {
		t.Fatalf("opaque bytes were not preserved: %v", err)
	}
	for _, span := range []string{"", strings.Repeat("x", 257)} {
		bad := r
		bad.Execution.Span = span
		if _, _, err := f.c.RegisterOwnership(t.Context(), bad); !errors.Is(err, ingest.ErrInvalid) {
			t.Fatal("unbounded input accepted")
		}
	}
	unknown := r
	unknown.Execution.Tenant = "unknown"
	if _, _, err := f.c.RegisterOwnership(t.Context(), unknown); !errors.Is(err, ingest.ErrDenied) {
		t.Fatalf("unknown project = %v", err)
	}
}
func TestConcurrentFirstRegistrationAndDifferentCall(t *testing.T) {
	for _, commit := range []bool{true, false} {
		name := "rollback"
		if commit {
			name = "commit"
		}
		t.Run(name, func(t *testing.T) {
			f := database(t)
			f.seed(t, f.r)
			second := f.r
			second.RegisteredBy = "host-two"
			f.seed(t, second)
			another := second
			another.Execution.Span = "99"
			f.seed(t, another)
			tx, err := f.pool.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer rollback(tx)
			if err = f.c.gate(t.Context(), tx, f.r.Execution.Tenant, "SHARE"); err != nil {
				t.Fatal(err)
			}
			_, err = tx.Exec(t.Context(), "INSERT INTO "+f.c.table("bindings")+" VALUES($1,$2,$3,$4,$5,$6)", []byte(f.r.Execution.Tenant), []byte(f.r.Execution.Trace), []byte(f.r.Execution.Span), []byte(f.r.Owner.Run), []byte(f.r.Owner.Instance), []byte(f.r.RegisteredBy))
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := start(t, f.c, ctx, second)
			blocked(t, f.pool, tx.Conn().PgConn().PID())
			if _, _, err = f.c.RegisterOwnership(t.Context(), another); err != nil {
				t.Fatalf("different call could not proceed: %v", err)
			}
			if commit {
				err = tx.Commit(t.Context())
			} else {
				err = tx.Rollback(t.Context())
			}
			if err != nil {
				t.Fatal(err)
			}
			got := receive(t, done)
			want := second
			if commit {
				want = f.r
			}
			if got.err != nil || got.duplicate != commit || got.original != want {
				t.Fatalf("first race = %+v", got)
			}
		})
	}
}
func TestGrantReadAfterProjectWait(t *testing.T) {
	f := database(t)
	f.seed(t, f.r)
	tx := f.holdPolicy(t)
	if _, err := tx.Exec(t.Context(), "DELETE FROM "+f.c.table("creator_grants")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := start(t, f.c, ctx, f.r)
	blocked(t, f.pool, tx.Conn().PgConn().PID())
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := receive(t, done); !errors.Is(got.err, ingest.ErrDenied) {
		t.Fatalf("stale permission = %+v", got)
	}
}
func TestCanceledWaitReleasesAdmission(t *testing.T) {
	f := database(t)
	f.seed(t, f.r)
	limited, err := New(f.pool, f.c.schema, Limits{1, 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	tx := f.holdPolicy(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := start(t, limited, ctx, f.r)
	blocked(t, f.pool, tx.Conn().PgConn().PID())
	if _, _, err = limited.RegisterOwnership(t.Context(), f.r); !errors.Is(err, ingest.ErrBusy) {
		t.Fatalf("unbounded admission: %v", err)
	}
	cancel()
	if got := receive(t, done); !errors.Is(got.err, context.Canceled) {
		t.Fatalf("canceled wait = %+v", got)
	}
	if err = tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, _, err = limited.RegisterOwnership(t.Context(), f.r); err != nil {
		t.Fatalf("admission leaked: %v", err)
	}
	// pgxpool delegates unhealthy-connection destruction asynchronously to
	// puddle. Observe its completion; an immediate counter sample is not a join.
	settled, cancelSettled := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelSettled()
	for f.pool.Stat().AcquiredConns() != 0 {
		if settled.Err() != nil {
			t.Fatal("pool connection cleanup did not settle")
		}
		runtime.Gosched()
	}
}
func TestBackendErrorRedactionAndClassification(t *testing.T) {
	cause := errors.New("private provider details")
	err := backend("commit registration", cause)
	if strings.Contains(err.Error(), cause.Error()) || !errors.Is(err, cause) {
		t.Fatal("provider text exposed or cause lost")
	}
	for _, cause := range []error{ingest.ErrDenied, ingest.ErrOwnershipConflict, ingest.ErrExpired} {
		if errors.Is(backend("unexpected driver failure", cause), cause) {
			t.Fatal("unexpected failure classified as confirmed refusal")
		}
	}
	if _, err := New(nil, "owned", Limits{1, time.Second}); !errors.Is(err, ingest.ErrInvalid) {
		t.Fatal("nil pool accepted")
	}
	if err := Install(t.Context(), nil, "public; DROP SCHEMA public"); !errors.Is(err, ingest.ErrInvalid) {
		t.Fatal("invalid setup accepted")
	}
}

func TestTransactionCommitDurabilityMode(t *testing.T) {
	for _, mode := range []string{"off", "remote_apply"} {
		t.Run(mode, func(t *testing.T) {
			f := database(t)
			f.seed(t, f.r)
			expected := "on"
			if mode == "remote_apply" {
				expected = mode
			}
			// Assert the effective setting at the write boundary. The pool may default
			// to asynchronous commit, or already require the stronger remote_apply.
			sql := "CREATE FUNCTION " + pgx.Identifier{f.c.schema, "require_commit_flush"}.Sanitize() + "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF current_setting('synchronous_commit') <> '" + expected + "' THEN RAISE EXCEPTION 'unexpected commit durability mode'; END IF; RETURN NEW; END $$"
			if _, err := f.pool.Exec(t.Context(), sql); err != nil {
				t.Fatal(err)
			}
			sql = "CREATE TRIGGER require_commit_flush BEFORE INSERT ON " + f.c.table("bindings") + " FOR EACH ROW EXECUTE FUNCTION " + pgx.Identifier{f.c.schema, "require_commit_flush"}.Sanitize() + "()"
			if _, err := f.pool.Exec(t.Context(), sql); err != nil {
				t.Fatal(err)
			}
			cfg := f.pool.Config()
			cfg.ConnConfig.RuntimeParams["synchronous_commit"] = mode
			configured, err := pgxpool.NewWithConfig(t.Context(), cfg)
			if err != nil {
				t.Fatal("open configured test pool failed")
			}
			defer configured.Close()
			c, err := New(configured, f.c.schema, Limits{1, 10 * time.Second})
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err = c.RegisterOwnership(t.Context(), f.r); err != nil {
				t.Fatalf("commit durability mode: %v", err)
			}
		})
	}
}

func TestRunPolicyReadAfterProjectWait(t *testing.T) {
	for _, phase := range []string{"completed", "revoked"} {
		for _, existing := range []bool{false, true} {
			name := phase + "/new"
			if existing {
				name = phase + "/existing"
			}
			t.Run(name, func(t *testing.T) {
				f := database(t)
				f.seed(t, f.r)
				if existing {
					if _, _, err := f.c.RegisterOwnership(t.Context(), f.r); err != nil {
						t.Fatal(err)
					}
				}
				tx := f.holdPolicy(t)
				sql := "UPDATE " + f.c.table("runs") + " SET phase='revoked'"
				if phase == "completed" {
					sql = "UPDATE " + f.c.table("runs") + " SET phase='completed',completed_at=clock_timestamp(),until_at=clock_timestamp()+interval '10 minutes'"
				}
				if _, err := tx.Exec(t.Context(), sql); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := start(t, f.c, ctx, f.r)
				blocked(t, f.pool, tx.Conn().PgConn().PID())
				if err := tx.Commit(t.Context()); err != nil {
					t.Fatal(err)
				}
				got := receive(t, done)
				if phase == "completed" && existing {
					if got.err != nil || !got.duplicate || got.original != f.r {
						t.Fatalf("existing drain retry: %+v", got)
					}
				} else if !errors.Is(got.err, ingest.ErrDenied) {
					t.Fatalf("closed run policy ignored: %+v", got)
				}
			})
		}
	}
}

// This optional case requires a SEPARATE disposable server deliberately started
// with fsync=off. The ordinary test database is never reconfigured.
func TestUnsafeServerRefusesTransactions(t *testing.T) {
	dsn := os.Getenv("REWIND_TEST_UNSAFE_POSTGRES")
	if dsn == "" {
		t.Skip("separate fsync=off disposable server not supplied")
	}
	pool, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatal("open unsafe test pool failed")
	}
	defer pool.Close()
	tx, err := begin(t.Context(), pool)
	if err == nil {
		rollback(tx)
		t.Fatal("unsafe server accepted")
	}
	if pool.Stat().AcquiredConns() != 0 {
		t.Fatal("rejected transaction was not released")
	}
}

// Controlled identity and discarded APPLICATION reply, not a real credential
// verifier or a dropped PostgreSQL protocol COMMIT response.
type fixedCreator string

func (p fixedCreator) AuthorizeRegistration(ctx context.Context, _ ingest.Execution, _ ingest.Owner) (string, error) {
	return string(p), ctx.Err()
}

type discardReply struct {
	*Catalog
	cause error
}

func (c discardReply) RegisterOwnership(ctx context.Context, r ingest.Registration) (ingest.Registration, bool, error) {
	o, d, err := c.Catalog.RegisterOwnership(ctx, r)
	if err != nil {
		return o, d, err
	}
	return ingest.Registration{}, false, c.cause
}
func TestDiscardedApplicationReplyResolvesWithFreshPool(t *testing.T) {
	f := database(t)
	f.seed(t, f.r)
	lost := errors.New("application response discarded")
	first, err := ingest.NewRegistrar(fixedCreator(f.r.RegisteredBy), discardReply{f.c, lost}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = first.Register(t.Context(), f.r.Execution, f.r.Owner); !errors.Is(err, ingest.ErrRegistrationUncertain) || !errors.Is(err, lost) {
		t.Fatalf("discarded reply = %v", err)
	}
	other := f.r
	other.RegisteredBy = "host-two"
	f.seed(t, other)
	pool, err := pgxpool.NewWithConfig(t.Context(), f.pool.Config())
	if err != nil {
		t.Fatal("replacement pool failed")
	}
	defer pool.Close()
	replacement, err := New(pool, f.c.schema, Limits{1, 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := ingest.NewRegistrar(fixedCreator(other.RegisteredBy), replacement, 1)
	if err != nil {
		t.Fatal(err)
	}
	got, err := fresh.Register(t.Context(), other.Execution, other.Owner)
	if err != nil || !got.Duplicate || got.Registration != f.r {
		t.Fatalf("fresh retry lost original attribution: %+v %v", got, err)
	}
}
