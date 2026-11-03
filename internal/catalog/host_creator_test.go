package catalog

import (
	"errors"
	"testing"

	"github.com/sidsharma96/rewind/internal/ingest"
	"github.com/sidsharma96/rewind/internal/traceid"
)

func hostCreator(t *testing.T, f fixture, r ingest.Registration) {
	t.Helper()
	f.policy(t, "INSERT INTO "+f.c.table("host_creator_grants")+" VALUES($1,$2,$3,$4)",
		[]byte(r.Execution.Tenant), []byte(r.Owner.Run), []byte(r.Owner.Instance), []byte(r.RegisteredBy))
}

func hostOnly(t *testing.T, f fixture) {
	t.Helper()
	f.seed(t, f.r)
	f.policy(t, "DELETE FROM "+f.c.table("creator_grants"))
	hostCreator(t, f, f.r)
}

func TestHostCreatorFreshExecutionsAndSeparatePermissions(t *testing.T) {
	f := database(t)
	hostOnly(t, f)
	for _, execution := range []ingest.Execution{
		{Tenant: f.r.Execution.Tenant, Trace: traceid.Trace{0xff, 0x80, 1}, Span: traceid.Span{0x90, 1}},
		{Tenant: f.r.Execution.Tenant, Trace: traceid.Trace{0xff, 0x80, 1}, Span: traceid.Span{0x90, 2}},
	} {
		r := f.r
		r.Execution = execution
		got, duplicate, err := f.c.RegisterOwnership(t.Context(), r)
		if err != nil || duplicate || got != r {
			t.Fatalf("fresh host registration = %+v %v %v", got, duplicate, err)
		}
		id := ingest.Identity{Tenant: execution.Tenant, Trace: execution.Trace, Span: execution.Span, Kind: ingest.SpanRecord}
		if err := f.c.CheckWrite(t.Context(), id, ingest.Attribution{Owner: r.Owner, SubmittedBy: r.RegisteredBy}); !errors.Is(err, ingest.ErrDenied) {
			t.Fatalf("host creator gained writer permission: %v", err)
		}
		if _, err := f.c.Lookup(t.Context(), id, r.RegisteredBy); !errors.Is(err, ingest.ErrDenied) {
			t.Fatalf("host creator gained reader permission: %v", err)
		}
	}
	var grants int
	if err := f.pool.QueryRow(t.Context(), "SELECT count(*) FROM "+f.c.table("creator_grants")).Scan(&grants); err != nil || grants != 0 {
		t.Fatalf("registration synthesized exact grants: %d %v", grants, err)
	}
}

func TestHostCreatorRejectsOtherScopesAndWriterOnly(t *testing.T) {
	f := database(t)
	f.seed(t, f.r)
	otherProject := f.r
	otherProject.Execution.Tenant = "other-project"
	f.seed(t, otherProject)
	otherRun := f.r
	otherRun.Owner.Run = "other-run"
	f.seed(t, otherRun)
	f.policy(t, "DELETE FROM "+f.c.table("creator_grants"))
	hostCreator(t, f, f.r)
	f.policy(t, "INSERT INTO "+f.c.table("writer_grants")+" VALUES($1,$2,$3,$4)",
		[]byte(f.r.Execution.Tenant), []byte(f.r.Owner.Run), []byte(f.r.Owner.Instance), []byte("writer-only"))
	otherInstance := f.r
	otherInstance.Owner.Instance = "bob"
	otherPrincipal := f.r
	otherPrincipal.RegisteredBy = "writer-only"
	for _, test := range []struct {
		name string
		r    ingest.Registration
	}{
		{"project", otherProject}, {"run", otherRun}, {"instance", otherInstance}, {"writer-only", otherPrincipal},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, _, err := f.c.RegisterOwnership(t.Context(), test.r); !errors.Is(err, ingest.ErrDenied) {
				t.Fatalf("wrong creator scope = %v", err)
			}
		})
	}
	var bindings int
	if err := f.pool.QueryRow(t.Context(), "SELECT count(*) FROM "+f.c.table("bindings")).Scan(&bindings); err != nil || bindings != 0 {
		t.Fatalf("denied registration left bindings: %d %v", bindings, err)
	}
}

func TestHostCreatorRevocationPrecedesDuplicateAndConflict(t *testing.T) {
	f := database(t)
	hostOnly(t, f)
	if _, _, err := f.c.RegisterOwnership(t.Context(), f.r); err != nil {
		t.Fatal(err)
	}
	changed := f.r
	changed.Owner.Instance = "bob"
	hostCreator(t, f, changed)
	if _, _, err := f.c.RegisterOwnership(t.Context(), changed); !errors.Is(err, ingest.ErrOwnershipConflict) {
		t.Fatalf("authorized owner conflict = %v", err)
	}
	f.policy(t, "DELETE FROM "+f.c.table("host_creator_grants"))
	for _, r := range []ingest.Registration{f.r, changed} {
		if _, _, err := f.c.RegisterOwnership(t.Context(), r); !errors.Is(err, ingest.ErrDenied) {
			t.Fatalf("revocation exposed registration outcome = %v", err)
		}
	}
	// An independently provisioned exact-call grant remains sufficient. The
	// scoped grant was never copied into one, so removing it alone denied above.
	f.seed(t, f.r)
	if got, duplicate, err := f.c.RegisterOwnership(t.Context(), f.r); err != nil || !duplicate || got != f.r {
		t.Fatalf("exact grant no longer works: %+v %v %v", got, duplicate, err)
	}
}

func TestHostCreatorObeysRunCompletionAndClosure(t *testing.T) {
	f := database(t)
	hostOnly(t, f)
	if _, _, err := f.c.RegisterOwnership(t.Context(), f.r); err != nil {
		t.Fatal(err)
	}
	changed := f.r
	changed.Owner.Run = "other-running-run"
	f.seed(t, changed)
	f.policy(t, "DELETE FROM "+f.c.table("creator_grants"))
	hostCreator(t, f, changed)
	f.policy(t, "UPDATE "+f.c.table("runs")+" SET phase='completed',completed_at=clock_timestamp()-interval '1 minute',until_at=clock_timestamp()+interval '1 hour' WHERE run_id=$1", []byte(f.r.Owner.Run))
	if got, duplicate, err := f.c.RegisterOwnership(t.Context(), f.r); err != nil || !duplicate || got != f.r {
		t.Fatalf("open drain duplicate = %+v %v %v", got, duplicate, err)
	}
	fresh := f.r
	fresh.Execution.Span = traceid.Span{0xb0, 99}
	for _, r := range []ingest.Registration{fresh, changed} {
		if _, _, err := f.c.RegisterOwnership(t.Context(), r); !errors.Is(err, ingest.ErrDenied) {
			t.Fatalf("host creator bypassed original run completion = %v", err)
		}
	}
	var bindings int
	if err := f.pool.QueryRow(t.Context(), "SELECT count(*) FROM "+f.c.table("bindings")).Scan(&bindings); err != nil || bindings != 1 {
		t.Fatalf("drain refusal left tentative binding: %d %v", bindings, err)
	}
	f.policy(t, "UPDATE "+f.c.table("runs")+" SET completed_at=clock_timestamp()-interval '20 minutes',until_at=clock_timestamp()-interval '10 minutes' WHERE run_id=$1", []byte(f.r.Owner.Run))
	if _, _, err := f.c.RegisterOwnership(t.Context(), f.r); !errors.Is(err, ingest.ErrDenied) {
		t.Fatalf("elapsed host retry = %v", err)
	}
	var phase string
	if err := f.pool.QueryRow(t.Context(), "SELECT phase FROM "+f.c.table("runs")+" WHERE run_id=$1", []byte(f.r.Owner.Run)).Scan(&phase); err != nil || phase != "closed" {
		t.Fatalf("elapsed host run not saved closed: %q %v", phase, err)
	}
	f.policy(t, "UPDATE "+f.c.table("runs")+" SET phase='revoked' WHERE run_id=$1", []byte(changed.Owner.Run))
	changed.Execution.Span = traceid.Span{0xb0, 100}
	if _, _, err := f.c.RegisterOwnership(t.Context(), changed); !errors.Is(err, ingest.ErrDenied) {
		t.Fatalf("revoked host run = %v", err)
	}
}

func TestHostCreatorReadsGrantAfterProjectWait(t *testing.T) {
	f := database(t)
	hostOnly(t, f)
	tx := f.holdPolicy(t)
	if _, err := tx.Exec(t.Context(), "DELETE FROM "+f.c.table("host_creator_grants")); err != nil {
		t.Fatal(err)
	}
	done := start(t, f.c, t.Context(), f.r)
	blocked(t, f.pool, tx.Conn().PgConn().PID())
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := receive(t, done); !errors.Is(got.err, ingest.ErrDenied) {
		t.Fatalf("stale host grant accepted registration: %+v", got)
	}
}
