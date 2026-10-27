package ingest

import (
	"context"
	"errors"
	"testing"
	"time"
)

func testDeliveryWindow(t *testing.T) DeliveryWindow {
	t.Helper()
	completed := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	w, err := NewDeliveryWindow(completed, DefaultDeliveryWindow)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func TestDeliveryWindowExactBoundaryAndFixedDeadline(t *testing.T) {
	w := testDeliveryWindow(t)
	if !w.Until().Equal(time.Date(2026, 10, 26, 12, 10, 0, 0, time.UTC)) {
		t.Fatalf("default deadline: %v", w.Until())
	}
	for _, tc := range []struct {
		at   time.Time
		want bool
	}{
		{w.CompletedAt().Add(-time.Nanosecond), false},
		{w.CompletedAt(), true},
		{w.Until().Add(-time.Nanosecond), true},
		{w.Until(), false},
		{w.Until().Add(time.Hour), false},
		{w.CompletedAt().In(time.FixedZone("same instant", -7*60*60)), true},
	} {
		original := w
		got, err := w.AllowsAt(tc.at)
		if err != nil || got != tc.want || w != original {
			t.Fatalf("at %v: allowed=%v err=%v changed=%v", tc.at, got, err, w != original)
		}
	}
	custom, err := NewDeliveryWindow(w.CompletedAt(), 30*time.Second)
	if err != nil || !custom.Until().Equal(w.CompletedAt().Add(30*time.Second)) {
		t.Fatalf("explicit custom window: %+v, %v", custom, err)
	}
	// A new policy does not mutate the original accepted completion value.
	if w.Until().Sub(w.CompletedAt()) != 10*time.Minute {
		t.Fatal("custom policy moved original deadline")
	}
}

func TestDeliveryWindowRejectsUnspecifiedPolicyOrTime(t *testing.T) {
	w := testDeliveryWindow(t)
	for _, tc := range []struct {
		completed time.Time
		duration  time.Duration
	}{{time.Time{}, DefaultDeliveryWindow}, {w.CompletedAt(), 0}, {w.CompletedAt(), -time.Minute}} {
		if got, err := NewDeliveryWindow(tc.completed, tc.duration); !errors.Is(err, ErrInvalid) || got != (DeliveryWindow{}) {
			t.Fatalf("invalid window: %+v, %v", got, err)
		}
	}
	if allowed, err := (DeliveryWindow{}).AllowsAt(w.CompletedAt()); allowed || !errors.Is(err, ErrInvalid) {
		t.Fatalf("zero window: %v, %v", allowed, err)
	}
	if allowed, err := w.AllowsAt(time.Time{}); allowed || !errors.Is(err, ErrInvalid) {
		t.Fatalf("unspecified time: %v, %v", allowed, err)
	}
}

// This is one completed project/run in the controlled catalog, not a lifecycle
// adapter. All accesses to its time and window use catalog.mu. Other registered
// runs remain running in this fixture; no production unknown-run fallback exists.
type deliveryModel struct {
	catalog *modelCatalog
	window  DeliveryWindow
	now     time.Time
}

func deliveryFixture(t *testing.T) (*Store, *modelObjects, *modelCatalog, *deliveryModel) {
	t.Helper()
	s, o, c := fixture(t)
	w := testDeliveryWindow(t)
	m := &deliveryModel{catalog: c, window: w, now: w.CompletedAt()}
	c.writeWindow = func(id Identity, a Attribution) error {
		if id.Tenant != sampleID.Tenant || a.Owner.Run != sampleOwner.Run {
			return nil // A different known run has not completed in this model.
		}
		allowed, err := m.window.AllowsAt(m.now)
		if err != nil {
			return err
		}
		if !allowed {
			return ErrDenied
		}
		return nil
	}
	return s, o, c, m
}

func (m *deliveryModel) advance(at time.Time) {
	m.catalog.mu.Lock()
	m.now = at
	m.catalog.mu.Unlock()
}

func TestDeliveryWindowClosesBothKindsWithoutChangingAcceptedRecords(t *testing.T) {
	for _, id := range []Identity{sampleID, captureID()} {
		s, o, c, m := deliveryFixture(t)
		originalWindow := m.window
		c.afterPublish = func(context.Context) error { return errors.New("lost commit response") }
		if _, err := s.Write(t.Context(), id, []byte("original")); !errors.Is(err, ErrUncertain) {
			t.Fatalf("lost reply: %v", err)
		}
		c.afterPublish = nil
		fresh := newStore(t, o, c, authorizeFunc(allow), Limits{1024, 1})
		m.advance(m.window.Until().Add(-time.Nanosecond))
		if r, err := fresh.Write(t.Context(), id, []byte("original")); err != nil || !r.Duplicate || r.Attribution != (Attribution{sampleOwner, "principal-a"}) {
			t.Fatalf("retry before deadline: %+v, %v", r, err)
		}
		m.advance(m.window.Until())
		creates, pubs := o.creates.Load(), c.publications.Load()
		for _, data := range [][]byte{[]byte("original"), []byte("changed")} {
			if r, err := fresh.Write(t.Context(), id, data); !errors.Is(err, ErrDenied) || errors.Is(err, ErrConflict) || errors.Is(err, ErrUncertain) || r != (Receipt{}) {
				t.Fatalf("after deadline: %+v, %v", r, err)
			}
		}
		if o.creates.Load() != creates || c.publications.Load() != pubs || m.window != originalWindow {
			t.Fatal("late retry uploaded, published or moved the deadline")
		}
		requireRead(t, fresh, id, []byte("original"))
	}
}

func TestDeliveryWindowDeadlineDuringUploadRefusesPublication(t *testing.T) {
	for _, kind := range []Kind{SpanRecord, CaptureRecord} {
		for _, state := range []string{"new", "duplicate", "conflict"} {
			t.Run(map[Kind]string{SpanRecord: "span", CaptureRecord: "capture"}[kind]+"/"+state, func(t *testing.T) {
				s, o, c, m := deliveryFixture(t)
				id := sampleID
				id.Kind = kind
				data := []byte("original")
				if state != "new" {
					if _, err := s.Write(t.Context(), id, data); err != nil {
						t.Fatal(err)
					}
				}
				if state == "conflict" {
					data = []byte("changed")
				}
				before, pubs := c.count(), c.publications.Load()
				m.advance(m.window.Until().Add(-time.Nanosecond))
				entered, release := make(chan struct{}), make(chan struct{})
				o.afterCreate = func(ctx context.Context) error {
					close(entered)
					select {
					case <-release:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				done := make(chan error, 1)
				go func() { _, err := s.Write(t.Context(), id, data); done <- err }()
				<-entered
				m.advance(m.window.Until())
				close(release)
				if err := <-done; !errors.Is(err, ErrDenied) || errors.Is(err, ErrConflict) || errors.Is(err, ErrUncertain) {
					t.Fatalf("publication after deadline: %v", err)
				}
				if c.count() != before || c.publications.Load() != pubs+1 || o.count() == 0 {
					t.Fatal("late publication changed acceptance or removed uploaded bytes")
				}
				if state != "new" {
					requireRead(t, s, id, []byte("original"))
				}
			})
		}
	}
}

func TestDeliveryWindowDoesNotRestoreRevokedPermission(t *testing.T) {
	for _, duringUpload := range []bool{false, true} {
		s, o, c, m := deliveryFixture(t)
		m.advance(m.window.CompletedAt().Add(time.Minute))
		revoke := func() { setWriter(c, sampleID.Tenant, Attribution{sampleOwner, "principal-a"}, false) }
		if duringUpload {
			o.afterCreate = func(context.Context) error { revoke(); return nil }
		} else {
			revoke()
		}
		if _, err := s.Write(t.Context(), captureID(), nil); !errors.Is(err, ErrDenied) || errors.Is(err, ErrUncertain) || c.count() != 0 {
			t.Fatalf("revoked during window: %v", err)
		}
		if !duringUpload && o.creates.Load() != 0 {
			t.Fatal("revoked before upload still created an object")
		}
	}
}

func TestDeliveryWindowDoesNotCloseAnotherRunOrProject(t *testing.T) {
	_, o, c, m := deliveryFixture(t)
	m.advance(m.window.Until())
	for _, tc := range []struct {
		id    Identity
		owner Owner
	}{
		{Identity{Tenant: sampleID.Tenant, Trace: sampleID.Trace, Span: "another-run-call"}, Owner{Run: "another-run", Instance: sampleOwner.Instance}},
		{Identity{Tenant: "another-project", Trace: sampleID.Trace, Span: sampleID.Span}, sampleOwner},
	} {
		a := Attribution{tc.owner, "other-principal"}
		provision(t, c, tc.id, a)
		s := newStore(t, o, c, writerAuth(a), Limits{1024, 1})
		if _, err := s.Write(t.Context(), tc.id, nil); err != nil {
			t.Fatalf("independent run/project closed: %v", err)
		}
	}
}

func TestDeliveryWindowInvalidCatalogTimeFailsClosed(t *testing.T) {
	s, o, c, m := deliveryFixture(t)
	m.advance(time.Time{})
	if _, err := s.Write(t.Context(), sampleID, nil); !errors.Is(err, ErrInvalid) || errors.Is(err, ErrUncertain) || o.creates.Load()+c.publications.Load() != 0 {
		t.Fatalf("invalid preliminary time: %v", err)
	}
	m.advance(m.window.CompletedAt())
	o.afterCreate = func(context.Context) error { m.advance(time.Time{}); return nil }
	if _, err := s.Write(t.Context(), sampleID, nil); !errors.Is(err, ErrInvalid) || !errors.Is(err, ErrUncertain) || c.count() != 0 {
		t.Fatalf("invalid publication time: %v", err)
	}
}

func TestDeliveryWindowConfirmedPublicationStaysSuccessful(t *testing.T) {
	s, _, c, m := deliveryFixture(t)
	m.advance(m.window.Until().Add(-time.Nanosecond))
	c.afterPublish = func(context.Context) error { m.advance(m.window.Until()); return nil }
	if r, err := s.Write(t.Context(), sampleID, []byte("accepted before deadline")); err != nil || r.Duplicate {
		t.Fatalf("confirmed publication changed after deadline: %+v, %v", r, err)
	}
	requireRead(t, s, sampleID, []byte("accepted before deadline"))
}
