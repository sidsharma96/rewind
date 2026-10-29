package catalog

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sidsharma96/rewind/internal/ingest"
	"github.com/sidsharma96/rewind/internal/traceid"
)

// Trusted fixture setup, not record acceptance or a producer-facing grant API.
func (f fixture) seedRecord(t *testing.T, kind ingest.Kind, expired bool, past bool) ingest.Receipt {
	t.Helper()
	f.seed(t, f.r)
	if _, _, err := f.c.RegisterOwnership(t.Context(), f.r); err != nil {
		t.Fatal(err)
	}
	id := ingest.Identity{Tenant: f.r.Execution.Tenant, Trace: f.r.Execution.Trace, Span: f.r.Execution.Span, Kind: kind}
	name, err := recordKind(id)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2020, 1, 1, 0, 0, 0, 123456000, time.UTC)
	expires := at.Add(200 * 365 * 24 * time.Hour)
	if past {
		expires = at.Add(7 * 24 * time.Hour)
	}
	digest := sha256.Sum256([]byte("synthetic record"))
	receipt := ingest.Receipt{Reference: ingest.Reference{Identity: id, Digest: digest, Size: 16}, Attribution: ingest.Attribution{Owner: f.r.Owner, SubmittedBy: "collector-original"}, AcceptedAt: at, ExpiresAt: expires}
	tx := f.holdPolicy(t)
	defer rollback(tx)
	if _, err = tx.Exec(t.Context(), "INSERT INTO "+f.c.table("reader_grants")+" VALUES($1,$2) ON CONFLICT DO NOTHING", []byte(id.Tenant), []byte("reader")); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(t.Context(), "INSERT INTO "+f.c.table("records")+" VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)", []byte(id.Tenant), id.Trace[:], id.Span[:], name, digest[:], receipt.Reference.Size, []byte(f.r.Owner.Run), []byte(f.r.Owner.Instance), []byte(receipt.Attribution.SubmittedBy), at, expires, expires.Add(24*time.Hour), expired)
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	return receipt
}
func requireLookup(t *testing.T, c *Catalog, id ingest.Identity, want ingest.Receipt, wantErr error) {
	t.Helper()
	r, err := c.Lookup(t.Context(), id, "reader")
	if !errors.Is(err, wantErr) || r != want {
		t.Fatalf("lookup=%+v,%v want=%+v,%v", r, err, want, wantErr)
	}
}
func TestLookupOriginalReceiptAndFreshPool(t *testing.T) {
	f := database(t)
	original := f.seedRecord(t, ingest.SpanRecord, false, false)
	capture := f.seedRecord(t, ingest.CaptureRecord, false, false)
	for _, want := range []ingest.Receipt{original, capture} {
		requireLookup(t, f.c, want.Reference.Identity, want, nil)
	}
	// Current policy and run write authority do not rewrite receipts or reader grants.
	f.policy(t, "UPDATE "+f.c.table("projects")+" SET payload_retention_us=1,marker_window_us=1")
	f.policy(t, "UPDATE "+f.c.table("runs")+" SET phase='revoked'")
	fresh, err := pgxpool.NewWithConfig(t.Context(), f.pool.Config())
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	c, err := New(fresh, f.c.schema, Limits{4, time.Second * 5})
	if err != nil {
		t.Fatal(err)
	}
	requireLookup(t, c, original.Reference.Identity, original, nil)
	missing := original.Reference.Identity
	missing.Span = traceid.Span{101}
	requireLookup(t, c, missing, ingest.Receipt{}, ingest.ErrNotFound)
	// Registration/ownership alone does not authorize reading, including absence.
	if _, err = c.Lookup(t.Context(), missing, f.r.RegisteredBy); !errors.Is(err, ingest.ErrDenied) {
		t.Fatal("owner/creator gained reader access")
	}
}

func TestLookupSharesBindingWhileRegistrationWaits(t *testing.T) {
	f := database(t)
	original := f.seedRecord(t, ingest.SpanRecord, false, false)
	id := original.Reference.Identity
	first, err := f.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(first)
	if _, restart, err := f.c.lookupLocked(t.Context(), first, id, "reader", false); err != nil || restart {
		t.Fatal("first shared lookup failed")
	}
	second, err := f.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(second)
	if _, restart, err := f.c.lookupLocked(t.Context(), second, id, "reader", false); err != nil || restart {
		t.Fatal("second shared lookup did not overlap")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second*2)
	defer cancel()
	if r, err := f.c.Lookup(ctx, id, "reader"); err != nil || r != original {
		t.Fatal("public lookup blocked behind shared readers")
	}
	writer := start(t, f.c, t.Context(), f.r)
	blocked(t, f.pool, first.Conn().PgConn().PID())
	if err = first.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	blocked(t, f.pool, second.Conn().PgConn().PID())
	if err = second.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if r := receive(t, writer); r.err != nil || !r.duplicate {
		t.Fatal("waiting registration failed")
	}
}

func TestLookupExpiryRestartRechecksPermissionAndState(t *testing.T) {
	for _, action := range []string{"deny", "saved_by_other", "clock_before_deadline"} {
		t.Run(action, func(t *testing.T) {
			f := database(t)
			original := f.seedRecord(t, ingest.SpanRecord, false, true)
			id := original.Reference.Identity
			r, restart, err := f.c.lookupAttempt(t.Context(), id, "reader", false)
			if err != nil || !restart || r != (ingest.Receipt{}) {
				t.Fatal("unsaved expiry not returned as restart")
			}
			// Successful policy update proves the shared attempt released its project gate.
			switch action {
			case "deny":
				f.policy(t, "DELETE FROM "+f.c.table("reader_grants"))
			case "saved_by_other":
				f.policy(t, "UPDATE "+f.c.table("records")+" SET expired=true")
			case "clock_before_deadline":
				// Synthetic replacement fixture state, not a production timestamp mutation.
				// It exercises a second attempt that now finds a live record without moving
				// the machine clock. Actual catalog decisions must preserve original dates.
				f.policy(t, "UPDATE "+f.c.table("records")+" SET expires_at=expires_at+interval '200 years',marker_until=marker_until+interval '200 years'")
			}
			r, restart, err = f.c.lookupAttempt(t.Context(), id, "reader", true)
			want := ingest.ErrExpired
			if action == "deny" {
				want = ingest.ErrDenied
			}
			if action == "clock_before_deadline" {
				want = nil
			}
			if !errors.Is(err, want) || restart {
				t.Fatalf("recheck=%v,%v", restart, err)
			}
			var saved bool
			if err = f.pool.QueryRow(t.Context(), "SELECT expired FROM "+f.c.table("records")).Scan(&saved); err != nil {
				t.Fatal(err)
			}
			if saved != (action == "saved_by_other") {
				t.Fatal("second attempt saved an unauthorized or currently live expiry")
			}
		})
	}
}

type lookupReply struct {
	receipt ingest.Receipt
	err     error
}

func startLookup(t *testing.T, c *Catalog, ctx context.Context, id ingest.Identity) <-chan lookupReply {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan lookupReply, 1)
	joined := make(chan struct{})
	go func() { defer close(joined); r, e := c.Lookup(ctx, id, "reader"); done <- lookupReply{r, e} }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Error("lookup goroutine did not stop")
		}
	})
	return done
}
func lookupReplyAt(t *testing.T, done <-chan lookupReply) lookupReply {
	t.Helper()
	select {
	case r := <-done:
		return r
	case <-t.Context().Done():
		t.Fatal("lookup canceled")
	case <-time.After(12 * time.Second):
		t.Fatal("lookup did not finish")
	}
	return lookupReply{}
}
func TestConcurrentExpiryReadersSaveAndFreshPoolRefuses(t *testing.T) {
	f := database(t)
	original := f.seedRecord(t, ingest.SpanRecord, false, true)
	peer := f.seedRecord(t, ingest.CaptureRecord, false, false)
	one := startLookup(t, f.c, t.Context(), original.Reference.Identity)
	two := startLookup(t, f.c, t.Context(), original.Reference.Identity)
	for _, done := range []<-chan lookupReply{one, two} {
		r := lookupReplyAt(t, done)
		if !errors.Is(r.err, ingest.ErrExpired) || r.receipt != (ingest.Receipt{}) {
			t.Fatal("concurrent reader did not confirm expiry")
		}
	}
	fresh, err := pgxpool.NewWithConfig(t.Context(), f.pool.Config())
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	c, err := New(fresh, f.c.schema, Limits{2, 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	requireLookup(t, c, original.Reference.Identity, ingest.Receipt{}, ingest.ErrExpired)
	requireLookup(t, c, peer.Reference.Identity, peer, nil)
	// Inspect originals under a trusted test query after operations have completed.
	var at, expires, until time.Time
	var saved bool
	err = f.pool.QueryRow(t.Context(), "SELECT accepted_at,expires_at,marker_until,expired FROM "+f.c.table("records")+" WHERE kind='span'").Scan(&at, &expires, &until, &saved)
	if err != nil || !saved || !at.Equal(original.AcceptedAt) || !expires.Equal(original.ExpiresAt) || !until.Equal(expires.Add(24*time.Hour)) {
		t.Fatal("expiry changed original dates or failed to save")
	}
}
func TestSavedExpiryRefusesEarlierClockAndCatalogCorruption(t *testing.T) {
	f := database(t)
	original := f.seedRecord(t, ingest.CaptureRecord, true, false)
	// Stored expiry with a future deadline represents previously saved expiry after
	// a false forward clock observation; today's earlier clock must not revive it.
	requireLookup(t, f.c, original.Reference.Identity, ingest.Receipt{}, ingest.ErrExpired)
	f.policy(t, "UPDATE "+f.c.table("records")+" SET owner_instance=$1", []byte("wrong-owner"))
	requireLookup(t, f.c, original.Reference.Identity, ingest.Receipt{}, ingest.ErrIntegrity)
}

func TestLookupExpiryWriteFailureDoesNotConfirmExpiry(t *testing.T) {
	for _, stage := range []string{"update", "commit"} {
		t.Run(stage, func(t *testing.T) {
			f := database(t)
			original := f.seedRecord(t, ingest.SpanRecord, false, true)
			fn := pgx.Identifier{f.c.schema, "fail_expiry"}.Sanitize()
			trigger := "CREATE TRIGGER fail_expiry BEFORE UPDATE ON "
			suffix := " FOR EACH ROW EXECUTE FUNCTION "
			if stage == "commit" {
				trigger = "CREATE CONSTRAINT TRIGGER fail_expiry AFTER UPDATE ON "
				suffix = " DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION "
			}
			_, err := f.pool.Exec(t.Context(), "CREATE FUNCTION "+fn+"() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic private failure'; END $$; "+trigger+f.c.table("records")+suffix+fn+"()")
			if err != nil {
				t.Fatal(err)
			}
			r, err := f.c.Lookup(t.Context(), original.Reference.Identity, "reader")
			if err == nil || errors.Is(err, ingest.ErrExpired) || errors.Is(err, ingest.ErrNotFound) || r != (ingest.Receipt{}) {
				t.Fatal("failed expiry save confirmed a result")
			}
			var saved bool
			if err = f.pool.QueryRow(t.Context(), "SELECT expired FROM "+f.c.table("records")).Scan(&saved); err != nil || saved {
				t.Fatal("failed expiry save changed state")
			}
		})
	}
}

func TestLookupAdmissionCancellationAndGrantAfterWait(t *testing.T) {
	f := database(t)
	original := f.seedRecord(t, ingest.SpanRecord, false, false)
	c, err := New(f.pool, f.c.schema, Limits{1, 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	policy := f.holdPolicy(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := startLookup(t, c, ctx, original.Reference.Identity)
	blocked(t, f.pool, policy.Conn().PgConn().PID())
	if _, err = c.Lookup(t.Context(), original.Reference.Identity, "reader"); !errors.Is(err, ingest.ErrBusy) {
		t.Fatal("lookup admission did not bound waiters")
	}
	cancel()
	if r := lookupReplyAt(t, done); !errors.Is(r.err, context.Canceled) {
		t.Fatal("lookup wait did not cancel")
	}
	if _, err = policy.Exec(t.Context(), "DELETE FROM "+f.c.table("reader_grants")); err != nil {
		t.Fatal(err)
	}
	waiting := startLookup(t, c, t.Context(), original.Reference.Identity)
	blocked(t, f.pool, policy.Conn().PgConn().PID())
	if err = policy.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if r := lookupReplyAt(t, waiting); !errors.Is(r.err, ingest.ErrDenied) {
		t.Fatal("lookup did not observe removed reader grant")
	}
}
func TestLookupRejectsInvalidInputBeforeDatabase(t *testing.T) {
	c, err := New(&pgxpool.Pool{}, "owned", Limits{1, time.Second})
	if err != nil {
		t.Fatal(err)
	}
	valid := ingest.Identity{Tenant: "project", Trace: traceid.Trace{1}, Span: traceid.Span{42}}
	for _, id := range []ingest.Identity{{}, {Tenant: "project", Trace: traceid.Trace{1}, Span: traceid.Span{42}, Kind: ingest.Kind(9)}} {
		if _, err = c.Lookup(t.Context(), id, "reader"); !errors.Is(err, ingest.ErrInvalid) {
			t.Fatal("invalid lookup identity accepted")
		}
	}
	if _, err = c.Lookup(t.Context(), valid, ""); !errors.Is(err, ingest.ErrInvalid) {
		t.Fatal("empty reader accepted")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err = c.Lookup(ctx, valid, "reader"); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled lookup touched database")
	}
}

func TestLookupExclusiveRestartUsesBoundedAdmission(t *testing.T) {
	f := database(t)
	original := f.seedRecord(t, ingest.SpanRecord, false, true)
	holder, err := f.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(holder)
	if _, restart, err := f.c.lookupLocked(t.Context(), holder, original.Reference.Identity, "reader", false); err != nil || !restart {
		t.Fatal("shared expiry observation failed")
	}
	c, err := New(f.pool, f.c.schema, Limits{1, 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	done := startLookup(t, c, t.Context(), original.Reference.Identity)
	blocked(t, f.pool, holder.Conn().PgConn().PID())
	if _, err = c.Lookup(t.Context(), original.Reference.Identity, "reader"); !errors.Is(err, ingest.ErrBusy) {
		t.Fatal("restart released admission slot while waiting")
	}
	reply := lookupReplyAt(t, done)
	if !errors.Is(reply.err, context.DeadlineExceeded) || errors.Is(reply.err, ingest.ErrExpired) {
		t.Fatal("exclusive wait failed to honor operation timeout")
	}
	if err = holder.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	requireLookup(t, c, original.Reference.Identity, ingest.Receipt{}, ingest.ErrExpired)
}
