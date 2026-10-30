package catalog

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sidsharma96/rewind/internal/ingest"
	"github.com/sidsharma96/rewind/internal/traceid"
)

func (f fixture) grantWriter(t *testing.T, writer ingest.Attribution) {
	t.Helper()
	f.policy(t, "INSERT INTO "+f.c.table("writer_grants")+" VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING", []byte(f.r.Execution.Tenant), []byte(writer.Owner.Run), []byte(writer.Owner.Instance), []byte(writer.SubmittedBy))
}
func (f fixture) writer(t *testing.T) ingest.Attribution {
	t.Helper()
	f.seed(t, f.r)
	if _, _, err := f.c.RegisterOwnership(t.Context(), f.r); err != nil {
		t.Fatal(err)
	}
	writer := ingest.Attribution{Owner: f.r.Owner, SubmittedBy: "collector"}
	f.grantWriter(t, writer)
	f.policy(t, "INSERT INTO "+f.c.table("reader_grants")+" VALUES($1,$2) ON CONFLICT DO NOTHING", []byte(f.r.Execution.Tenant), []byte("reader"))
	return writer
}
func (f fixture) reference(kind ingest.Kind, body string) ingest.Reference {
	return ingest.Reference{Identity: ingest.Identity{Tenant: f.r.Execution.Tenant, Trace: f.r.Execution.Trace, Span: f.r.Execution.Span, Kind: kind}, Digest: sha256.Sum256([]byte(body)), Size: len(body)}
}
func freshCatalog(t *testing.T, f fixture) *Catalog {
	t.Helper()
	pool, err := pgxpool.NewWithConfig(t.Context(), f.pool.Config())
	if err != nil {
		t.Fatal("open fresh publication pool")
	}
	t.Cleanup(pool.Close)
	c, err := New(pool, f.c.schema, Limits{4, 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func TestPublishOriginalReceiptAndIndependentKinds(t *testing.T) {
	f := database(t)
	writer := f.writer(t)
	for _, kind := range []ingest.Kind{ingest.CaptureRecord, ingest.SpanRecord} {
		ref := f.reference(kind, "same object")
		if err := f.c.CheckWrite(t.Context(), ref.Identity, writer); err != nil {
			t.Fatal(err)
		}
		requireLookup(t, f.c, ref.Identity, ingest.Receipt{}, ingest.ErrNotFound)
		first, err := f.c.Publish(t.Context(), ref, writer)
		if err != nil || first.Duplicate || first.Reference != ref || first.Attribution != writer || first.AcceptedAt.IsZero() || first.ExpiresAt.Sub(first.AcceptedAt) != ingest.DefaultPayloadRetention || first.AcceptedAt.Nanosecond()%1000 != 0 {
			t.Fatalf("publication receipt invalid: %+v %v", first, err)
		}
		var marker time.Time
		if err := f.pool.QueryRow(t.Context(), "SELECT marker_until FROM "+f.c.table("records")+" WHERE kind=$1", map[ingest.Kind]string{ingest.SpanRecord: "span", ingest.CaptureRecord: "capture"}[kind]).Scan(&marker); err != nil || marker.Sub(first.ExpiresAt) != ingest.DefaultMarkerWindow {
			t.Fatal("marker deadline did not use original policy")
		}
		other := writer
		other.SubmittedBy = "another-collector"
		f.grantWriter(t, other)
		f.policy(t, "UPDATE "+f.c.table("projects")+" SET payload_retention_us=1,marker_window_us=1")
		retry, err := freshCatalog(t, f).Publish(t.Context(), ref, other)
		want := first
		want.Duplicate = true
		if err != nil || retry != want {
			t.Fatal("retry changed reference, attribution or deadlines")
		}
		changed := ref
		changed.Digest = sha256.Sum256([]byte("different"))
		if receipt, err := f.c.Publish(t.Context(), changed, writer); !errors.Is(err, ingest.ErrConflict) || receipt != (ingest.Receipt{}) {
			t.Fatal("changed reference replaced original")
		}
		requireLookup(t, freshCatalog(t, f), ref.Identity, first, nil)
		f.policy(t, "UPDATE "+f.c.table("projects")+" SET payload_retention_us=$1,marker_window_us=$2", ingest.DefaultPayloadRetention.Microseconds(), ingest.DefaultMarkerWindow.Microseconds())
	}
}

func TestPublishAuthorityBeforeEveryOutcome(t *testing.T) {
	f := database(t)
	writer := f.writer(t)
	ref := f.reference(ingest.SpanRecord, "original")
	if _, err := f.c.Publish(t.Context(), ref, writer); err != nil {
		t.Fatal(err)
	}
	wrong := writer
	wrong.Owner.Instance = "bob"
	f.grantWriter(t, wrong)
	for _, a := range []ingest.Attribution{wrong, {Owner: writer.Owner, SubmittedBy: "ungranted"}} {
		if err := f.c.CheckWrite(t.Context(), ref.Identity, a); !errors.Is(err, ingest.ErrDenied) {
			t.Fatal("precheck omitted writer/owner checks")
		}
		if _, err := f.c.Publish(t.Context(), ref, a); !errors.Is(err, ingest.ErrDenied) {
			t.Fatal("duplicate disclosed before authority")
		}
	}
	if _, err := f.c.Lookup(t.Context(), ref.Identity, writer.SubmittedBy); !errors.Is(err, ingest.ErrDenied) {
		t.Fatal("writer gained reader grant")
	}
	absent := ref
	absent.Identity.Span = traceid.Span{99}
	if _, err := f.c.Publish(t.Context(), absent, writer); !errors.Is(err, ingest.ErrDenied) {
		t.Fatal("publication created missing binding")
	}
	f.policy(t, "DELETE FROM "+f.c.table("writer_grants"))
	changed := ref
	changed.Size++
	for _, proposal := range []ingest.Reference{ref, changed, absent} {
		if _, err := f.c.Publish(t.Context(), proposal, writer); !errors.Is(err, ingest.ErrDenied) {
			t.Fatal("revoked writer learned duplicate/conflict/absence")
		}
	}
}

func TestPublishSerializesSameBinding(t *testing.T) {
	for _, different := range []bool{false, true} {
		t.Run(map[bool]string{false: "identical", true: "conflicting"}[different], func(t *testing.T) {
			f := database(t)
			writer := f.writer(t)
			ref := f.reference(ingest.SpanRecord, "first")
			holder, err := f.pool.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer rollback(holder)
			if err := f.c.gate(t.Context(), holder, ref.Identity.Tenant, "SHARE"); err != nil {
				t.Fatal(err)
			}
			if _, err := holder.Exec(t.Context(), "SELECT 1 FROM "+f.c.table("bindings")+" FOR UPDATE"); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			type result struct {
				receipt ingest.Receipt
				err     error
			}
			done := make(chan result, 2)
			var wg sync.WaitGroup
			for i := 0; i < 2; i++ {
				proposal := ref
				if i == 1 && different {
					proposal.Digest = sha256.Sum256([]byte("second"))
				}
				wg.Add(1)
				go func() { defer wg.Done(); r, e := f.c.Publish(ctx, proposal, writer); done <- result{r, e} }()
			}
			t.Cleanup(func() { cancel(); wg.Wait() })
			waitCtx, waitCancel := context.WithTimeout(t.Context(), 4*time.Second)
			defer waitCancel()
			for {
				var waiting int
				// PostgreSQL may queue the second tuple waiter behind the first,
				// so both need not name holder as their DIRECT blocker. Observe
				// both binding-lock waits in this fixture's own application.
				err := f.pool.QueryRow(waitCtx, "SELECT count(*) FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE '%bindings%FOR UPDATE%'", f.c.schema).Scan(&waiting)
				if err != nil {
					t.Fatal("both publication attempts did not wait on binding")
				}
				if waiting == 2 {
					break
				}
			}
			if err := holder.Commit(t.Context()); err != nil {
				t.Fatal(err)
			}
			originals, duplicates, conflicts := 0, 0, 0
			for i := 0; i < 2; i++ {
				r := <-done
				switch {
				case errors.Is(r.err, ingest.ErrConflict):
					conflicts++
				case r.err != nil:
					t.Fatal(r.err)
				case r.receipt.Duplicate:
					duplicates++
				default:
					originals++
				}
			}
			if originals != 1 || (different && (conflicts != 1 || duplicates != 0)) || (!different && (duplicates != 1 || conflicts != 0)) {
				t.Fatal("concurrent publications did not preserve one original")
			}
		})
	}
}

func TestPublicationSavesExpiryAndClosure(t *testing.T) {
	for _, precheck := range []bool{false, true} {
		f := database(t)
		original := f.seedRecord(t, ingest.SpanRecord, false, true)
		writer := f.writer(t)
		var err error
		if precheck {
			err = f.c.CheckWrite(t.Context(), original.Reference.Identity, writer)
		} else {
			_, err = f.c.Publish(t.Context(), original.Reference, writer)
		}
		if !errors.Is(err, ingest.ErrExpired) {
			t.Fatal("past record not refused")
		}
		var saved bool
		if err := f.pool.QueryRow(t.Context(), "SELECT expired FROM "+f.c.table("records")).Scan(&saved); err != nil || !saved {
			t.Fatal("expiry refusal was not saved")
		}
		requireLookup(t, freshCatalog(t, f), original.Reference.Identity, ingest.Receipt{}, ingest.ErrExpired)
	}
	f := database(t)
	original := f.seedRecord(t, ingest.SpanRecord, true, false)
	writer := f.writer(t)
	if _, err := f.c.Publish(t.Context(), original.Reference, writer); !errors.Is(err, ingest.ErrExpired) {
		t.Fatal("saved expiry revived under earlier clock fixture")
	}
	f2 := database(t)
	writer2 := f2.writer(t)
	ref := f2.reference(ingest.CaptureRecord, "late")
	f2.policy(t, "UPDATE "+f2.c.table("runs")+" SET phase='completed',completed_at='2020-01-01',until_at='2020-01-02'")
	if _, err := f2.c.Publish(t.Context(), ref, writer2); !errors.Is(err, ingest.ErrDenied) {
		t.Fatal("elapsed delivery accepted")
	}
	var phase string
	if err := f2.pool.QueryRow(t.Context(), "SELECT phase FROM "+f2.c.table("runs")).Scan(&phase); err != nil || phase != "closed" {
		t.Fatal("delivery refusal did not save closure")
	}
	f2.policy(t, "UPDATE "+f2.c.table("runs")+" SET completed_at='2200-01-01',until_at='2200-01-02'")
	if err := freshCatalog(t, f2).CheckWrite(t.Context(), ref.Identity, writer2); !errors.Is(err, ingest.ErrDenied) {
		t.Fatal("closed state revived with future deadline fixture")
	}
}

func TestPublicationFailureNeverConfirmsExpiryOrAcceptance(t *testing.T) {
	for _, operation := range []string{"insert", "expiry"} {
		for _, stage := range []string{"statement", "commit"} {
			t.Run(operation+"-"+stage, func(t *testing.T) {
				f := database(t)
				writer := f.writer(t)
				ref := f.reference(ingest.SpanRecord, "body")
				event := "INSERT"
				if operation == "expiry" {
					ref = f.seedRecord(t, ingest.SpanRecord, false, true).Reference
					event = "UPDATE"
				}
				fn := pgx.Identifier{f.c.schema, "fail_publication"}.Sanitize()
				prefix, suffix := "CREATE TRIGGER fail_publication BEFORE ", " FOR EACH ROW EXECUTE FUNCTION "
				if stage == "commit" {
					prefix, suffix = "CREATE CONSTRAINT TRIGGER fail_publication AFTER ", " DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION "
				}
				if _, err := f.pool.Exec(t.Context(), "CREATE FUNCTION "+fn+"() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic failure'; END $$; "+prefix+event+" ON "+f.c.table("records")+suffix+fn+"()"); err != nil {
					t.Fatal(err)
				}
				receipt, err := f.c.Publish(t.Context(), ref, writer)
				if err == nil || errors.Is(err, ingest.ErrExpired) || errors.Is(err, ingest.ErrConflict) || errors.Is(err, ingest.ErrDenied) || receipt != (ingest.Receipt{}) {
					t.Fatal("failed statement/commit confirmed domain outcome")
				}
				var rows int
				if operation == "insert" {
					if err := f.pool.QueryRow(t.Context(), "SELECT count(*) FROM "+f.c.table("records")).Scan(&rows); err != nil || rows != 0 {
						t.Fatal("failed insert persisted")
					}
				} else {
					var expired bool
					if err := f.pool.QueryRow(t.Context(), "SELECT expired FROM "+f.c.table("records")).Scan(&expired); err != nil || expired {
						t.Fatal("failed expiry persisted")
					}
				}
			})
		}
	}
}

func TestPublicationInputAndCancellationBeforeDatabase(t *testing.T) {
	c := &Catalog{}
	id := ingest.Identity{Tenant: "p", Trace: traceid.Trace{1}, Span: traceid.Span{2}}
	writer := ingest.Attribution{Owner: ingest.Owner{Run: "r", Instance: "i"}, SubmittedBy: "principal"}
	if err := c.CheckWrite(t.Context(), id, ingest.Attribution{}); !errors.Is(err, ingest.ErrInvalid) {
		t.Fatal("invalid attribution reached database")
	}
	if _, err := c.Publish(t.Context(), ingest.Reference{Identity: id, Size: -1}, writer); !errors.Is(err, ingest.ErrInvalid) {
		t.Fatal("negative size reached database")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := c.Publish(ctx, ingest.Reference{Identity: id}, writer); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled publication reached database")
	}
}

func TestPublicationBoundedAdmissionAndFreshGrantAfterWait(t *testing.T) {
	f := database(t)
	writer := f.writer(t)
	ref := f.reference(ingest.SpanRecord, "waiting")
	c, err := New(f.pool, f.c.schema, Limits{1, 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	holder := f.holdPolicy(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() { defer close(finished); _, err := c.Publish(ctx, ref, writer); done <- err }()
	t.Cleanup(func() { cancel(); <-finished })
	blocked(t, f.pool, holder.Conn().PgConn().PID())
	if err := c.CheckWrite(t.Context(), ref.Identity, writer); !errors.Is(err, ingest.ErrBusy) {
		t.Fatal("waiting publication did not count against active bound")
	}
	if _, err := holder.Exec(t.Context(), "DELETE FROM "+f.c.table("writer_grants")); err != nil {
		t.Fatal(err)
	}
	if err := holder.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, ingest.ErrDenied) {
		t.Fatal("publication used grant snapshot from before gate wait")
	}
	if err := c.CheckWrite(t.Context(), ref.Identity, writer); !errors.Is(err, ingest.ErrDenied) {
		t.Fatal("completed operation retained admission slot")
	}
}

func TestPublicationClosureFailureIsOperational(t *testing.T) {
	f := database(t)
	writer := f.writer(t)
	ref := f.reference(ingest.SpanRecord, "late")
	f.policy(t, "UPDATE "+f.c.table("runs")+" SET phase='completed',completed_at='2020-01-01',until_at='2020-01-02'")
	fn := pgx.Identifier{f.c.schema, "fail_closure"}.Sanitize()
	if _, err := f.pool.Exec(t.Context(), "CREATE FUNCTION "+fn+"() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic closure failure'; END $$; CREATE TRIGGER fail_closure BEFORE UPDATE ON "+f.c.table("runs")+" FOR EACH ROW EXECUTE FUNCTION "+fn+"()"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.c.Publish(t.Context(), ref, writer); err == nil || errors.Is(err, ingest.ErrDenied) {
		t.Fatal("failed closure save confirmed a deadline refusal")
	}
	var phase string
	if err := f.pool.QueryRow(t.Context(), "SELECT phase FROM "+f.c.table("runs")).Scan(&phase); err != nil || phase != "completed" {
		t.Fatal("failed closure mutated state")
	}
}
