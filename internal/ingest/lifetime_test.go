package ingest

import (
	"context"
	"errors"
	"testing"
	"time"
)

func testLifetime(t *testing.T) Lifetime {
	t.Helper()
	expires := time.Date(2026, 3, 8, 9, 30, 0, 0, time.UTC)
	lifetime, err := NewLifetime(expires, DefaultMarkerWindow)
	if err != nil {
		t.Fatal(err)
	}
	return lifetime
}

func TestLifetimeBoundaryAndStableDeadlines(t *testing.T) {
	lifetime := testLifetime(t)
	expires, until := lifetime.PayloadExpiresAt(), lifetime.MarkerUntil()
	// Explicit expected date: exactly 24 elapsed hours, not a local calendar day.
	if !until.Equal(time.Date(2026, 3, 9, 9, 30, 0, 0, time.UTC)) {
		t.Fatalf("marker deadline=%v", until)
	}
	for _, tc := range []struct {
		at   time.Time
		want RetentionPhase
	}{
		{expires.Add(-time.Nanosecond), BeforeExpiry},
		{expires, WithinMarkerWindow},
		{until.Add(-time.Nanosecond), WithinMarkerWindow},
		{until, AfterMarkerWindow},
		{until.Add(time.Hour), AfterMarkerWindow},
		{expires.In(time.FixedZone("same instant", -7*60*60)), WithinMarkerWindow},
	} {
		got, err := lifetime.PhaseAt(tc.at)
		if err != nil || got != tc.want {
			t.Fatalf("phase at %v=%v,%v; want %v", tc.at, got, err, tc.want)
		}
	}
	if !lifetime.PayloadExpiresAt().Equal(expires) || !lifetime.MarkerUntil().Equal(until) {
		t.Fatal("classification moved original deadlines")
	}
	custom, err := NewLifetime(expires, 2*time.Hour)
	if err != nil || !custom.MarkerUntil().Equal(expires.Add(2*time.Hour)) {
		t.Fatalf("explicit window=%+v,%v", custom, err)
	}
}

func TestLifetimeRejectsUnspecifiedValues(t *testing.T) {
	expires := testLifetime(t).PayloadExpiresAt()
	for _, tc := range []struct {
		expires time.Time
		window  time.Duration
	}{
		{time.Time{}, DefaultMarkerWindow},
		{expires, 0},
		{expires, -time.Hour},
	} {
		if _, err := NewLifetime(tc.expires, tc.window); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid lifetime=%v", err)
		}
	}
	if phase, err := (Lifetime{}).PhaseAt(expires); phase != InvalidPhase || !errors.Is(err, ErrInvalid) {
		t.Fatalf("zero lifetime=%v,%v", phase, err)
	}
	if phase, err := testLifetime(t).PhaseAt(time.Time{}); phase != InvalidPhase || !errors.Is(err, ErrInvalid) {
		t.Fatalf("unspecified now=%v,%v", phase, err)
	}
}

// lifetimeCatalog is a sequential test model for one record's retained marker.
// It has no expiry worker or real clock; moving now does not remove any object
// or marker. Expiry state belongs to this retained fixture value, not the coordinator.
// Native catalog clock/isolation/recovery guarantees are not modeled.
type lifetimeCatalog struct {
	*modelCatalog
	id       Identity
	lifetime Lifetime
	now      time.Time
	calls    int
	saveErr  error // injected failure before saving expiry
	replyErr error // injected failure after saving expiry, before confirming it
}

func (c *lifetimeCatalog) expired(ctx context.Context, id Identity) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if id != c.id {
		return false, nil
	}
	if _, exists := c.rows[id]; !exists {
		return false, nil
	}
	next, err := c.lifetime.ObserveExpiry(c.now)
	if err != nil {
		return false, err
	}
	if !c.lifetime.Expired() && next.Expired() {
		if c.saveErr != nil {
			return false, c.saveErr
		}
		c.lifetime = next // controlled save under modelCatalog.mu, not remote storage
		if c.replyErr != nil {
			return false, c.replyErr
		}
	}
	return c.lifetime.Expired(), nil
}

func (c *lifetimeCatalog) CheckWrite(ctx context.Context, id Identity, a Attribution) error {
	c.calls++
	return c.modelCatalog.CheckWrite(ctx, id, a)
}
func (c *lifetimeCatalog) Publish(ctx context.Context, ref Reference, a Attribution) (Receipt, error) {
	c.calls++
	return c.modelCatalog.Publish(ctx, ref, a)
}
func (c *lifetimeCatalog) Lookup(ctx context.Context, id Identity, principal string) (Receipt, error) {
	c.calls++
	return c.modelCatalog.Lookup(ctx, id, principal)
}

func newLifetimeCatalog(t *testing.T, catalog *modelCatalog, lifetime Lifetime) *lifetimeCatalog {
	t.Helper()
	catalog.now = lifetime.PayloadExpiresAt().Add(-DefaultPayloadRetention)
	c := &lifetimeCatalog{modelCatalog: catalog, id: captureID(), lifetime: lifetime, now: lifetime.PayloadExpiresAt().Add(-time.Hour)}
	catalog.expiry = func(id Identity) error {
		expired, err := c.expired(t.Context(), id)
		if err != nil {
			return err
		}
		if expired {
			return ErrExpired
		}
		return nil
	}
	return c
}

func TestRetainedExpiredIdentityRefusesRetryAndRead(t *testing.T) {
	_, o, catalog := fixture(t)
	lifetime := testLifetime(t)
	c := newLifetimeCatalog(t, catalog, lifetime)
	s, err := New(authorizeFunc(allow), o, c, Limits{1024, 2})
	if err != nil {
		t.Fatal(err)
	}
	// The unrelated span has no expiry in this controlled fixture.
	if _, err := s.Write(t.Context(), sampleID, []byte("span metadata")); err != nil {
		t.Fatal(err)
	}
	data := []byte("captured bytes")
	catalog.afterPublish = func(context.Context) error { return errors.New("lost commit response") }
	if _, err := s.Write(t.Context(), c.id, data); !errors.Is(err, ErrUncertain) {
		t.Fatalf("lost reply=%v", err)
	}
	catalog.afterPublish = nil
	fresh, err := New(authorizeFunc(allow), o, c, Limits{1024, 2})
	if err != nil {
		t.Fatal(err)
	}
	if r, err := fresh.Write(t.Context(), c.id, data); err != nil || !r.Duplicate {
		t.Fatalf("live retry=%+v,%v", r, err)
	}
	original, err := catalog.Lookup(t.Context(), c.id, "reader-a")
	if err != nil {
		t.Fatal(err)
	}
	for _, now := range []time.Time{lifetime.PayloadExpiresAt(), lifetime.MarkerUntil().Add(-time.Nanosecond), lifetime.MarkerUntil()} {
		c.now = now
		reads := o.reads.Load()
		if got, err := fresh.Read(t.Context(), c.id); !errors.Is(err, ErrExpired) || errors.Is(err, ErrNotFound) || got != nil {
			t.Fatalf("expired read=%q,%v", got, err)
		}
		if o.reads.Load() != reads {
			t.Fatal("expired lookup reached object read")
		}
		for _, contents := range [][]byte{data, []byte("changed captured bytes")} {
			if r, err := fresh.Write(t.Context(), c.id, contents); !errors.Is(err, ErrExpired) || errors.Is(err, ErrUncertain) || r != (Receipt{}) {
				t.Fatalf("expired write=%+v,%v", r, err)
			}
		}
		got := catalog.rows[c.id]
		if got != original || !c.lifetime.PayloadExpiresAt().Equal(lifetime.PayloadExpiresAt()) || !c.lifetime.MarkerUntil().Equal(lifetime.MarkerUntil()) {
			t.Fatalf("refusal changed reference/deadlines=%+v", got)
		}
		requireRead(t, fresh, sampleID, []byte("span metadata"))
	}
	// Even at the marker-window boundary, keeping a marker is allowed. The phase
	// alone does not erase it or force the catalog to accept the identity again.
	denied := errors.New("tenant denied")
	untrusted, err := New(authorizeFunc(func(context.Context, string) error { return denied }), o, c, Limits{1024, 2})
	if err != nil {
		t.Fatal(err)
	}
	calls, creates, reads := c.calls, o.creates.Load(), o.reads.Load()
	if _, err := untrusted.Write(t.Context(), c.id, data); !errors.Is(err, denied) {
		t.Fatalf("denied expired write=%v", err)
	}
	if _, err := untrusted.Read(t.Context(), c.id); !errors.Is(err, denied) {
		t.Fatalf("denied expired read=%v", err)
	}
	if c.calls != calls || o.creates.Load() != creates || o.reads.Load() != reads {
		t.Fatal("unauthorized expiry request reached storage")
	}
}

func TestExpiryCheckBeforeAndAfterUpload(t *testing.T) {
	for _, duringUpload := range []bool{false, true} {
		t.Run(map[bool]string{false: "already expired", true: "expires during upload"}[duringUpload], func(t *testing.T) {
			testExpiryCheck(t, duringUpload)
		})
	}
}

func testExpiryCheck(t *testing.T, duringUpload bool) {
	t.Helper()
	_, o, catalog := fixture(t)
	lifetime := testLifetime(t)
	c := newLifetimeCatalog(t, catalog, lifetime)
	s, err := New(authorizeFunc(allow), o, c, Limits{1024, 2})
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("captured bytes")
	r, err := s.Write(t.Context(), c.id, data)
	if err != nil {
		t.Fatal(err)
	}
	if duringUpload {
		// Different bytes require a new object. Expiry during that upload must
		// still refuse publication, without replacing the original reference.
		data = []byte("late changed capture")
		o.afterCreate = func(context.Context) error {
			c.now = lifetime.PayloadExpiresAt()
			return nil
		}
	} else {
		c.now = lifetime.PayloadExpiresAt()
		// Simulate reclamation of this unique, idle fixture object only.
		// This is not a production deletion protocol; no readers race.
		o.mu.Lock()
		delete(o.data, ObjectID{Tenant: c.id.Tenant, Digest: r.Reference.Digest})
		o.mu.Unlock()
	}
	creates, calls := o.creates.Load(), c.calls
	if _, err := s.Write(t.Context(), c.id, data); !errors.Is(err, ErrExpired) || errors.Is(err, ErrUncertain) {
		t.Fatalf("expired retry=%v", err)
	}
	if duringUpload {
		if o.creates.Load() != creates+1 || o.count() != 2 || c.calls != calls+2 {
			t.Fatal("expiry after preliminary check did not reach final refusal with an orphan")
		}
	} else if o.creates.Load() != creates || o.count() != 0 || c.calls != calls+1 {
		t.Fatal("known expired identity reached upload or publication")
	}
	if ref := catalog.rows[c.id]; ref != r {
		t.Fatalf("expired retry changed acceptance=%+v", ref)
	}
}
