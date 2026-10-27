package ingest

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRememberedExpiryAndDiscardedObservation(t *testing.T) {
	original := testLifetime(t)
	expires, until := original.PayloadExpiresAt(), original.MarkerUntil()
	before := expires.Add(-time.Nanosecond)
	live, err := original.ObserveExpiry(before)
	if err != nil || live != original || live.Expired() {
		t.Fatal("early observation changed state")
	}
	expired, err := original.ObserveExpiry(expires)
	if err != nil || !expired.Expired() || original.Expired() {
		t.Fatal("exact expiry must change only the returned copy")
	}
	for _, now := range []time.Time{before, expires.Add(-24 * time.Hour), expires, until, until.Add(time.Hour)} {
		got, err := expired.ObserveExpiry(now)
		if err != nil || got != expired || !got.PayloadExpiresAt().Equal(expires) || !got.MarkerUntil().Equal(until) {
			t.Fatal("saved expiry/deadlines changed")
		}
	}
	// A clock predicate alone, or discarding the transition, permits earlier time.
	phase, err := expired.PhaseAt(before)
	if err != nil || phase != BeforeExpiry {
		t.Fatal("pure deadline classification changed meaning")
	}
	unsaved, err := original.ObserveExpiry(before)
	if err != nil || unsaved.Expired() {
		t.Fatal("discarded copy falsely established saved expiry")
	}
	for _, tc := range []struct {
		lifetime Lifetime
		now      time.Time
	}{{Lifetime{}, expires}, {original, time.Time{}}, {expired, time.Time{}}} {
		got, err := tc.lifetime.ObserveExpiry(tc.now)
		if !errors.Is(err, ErrInvalid) || got != (Lifetime{}) {
			t.Fatal("unspecified value accepted")
		}
	}
}

func TestSavedExpiryRefusesFreshCoordinatorAfterClockCorrection(t *testing.T) {
	for _, kind := range []Kind{SpanRecord, CaptureRecord} {
		t.Run(map[Kind]string{SpanRecord: "span", CaptureRecord: "capture"}[kind], func(t *testing.T) {
			_, o, catalog := fixture(t)
			c := newLifetimeCatalog(t, catalog, testLifetime(t))
			c.id.Kind = kind
			s := newStore(t, o, c, authorizeFunc(allow), Limits{1024, 2})
			data := []byte("retained bytes")
			original, err := s.Write(t.Context(), c.id, data)
			if err != nil {
				t.Fatal(err)
			}
			other := c.id
			if kind == SpanRecord {
				other.Kind = CaptureRecord
			} else {
				other.Kind = SpanRecord
			}
			if _, err = s.Write(t.Context(), other, []byte("peer stays live")); err != nil {
				t.Fatal(err)
			}
			c.now = c.lifetime.PayloadExpiresAt()
			if _, err = s.Read(t.Context(), c.id); !errors.Is(err, ErrExpired) {
				t.Fatal("expiry not confirmed")
			}
			saved := c.lifetime
			c.now = c.now.Add(-time.Minute)
			fresh := newStore(t, o, c, authorizeFunc(allow), Limits{1024, 2})
			reads, creates := o.reads.Load(), o.creates.Load()
			if data, err := fresh.Read(t.Context(), c.id); !errors.Is(err, ErrExpired) || data != nil {
				t.Fatal("clock correction revived read")
			}
			for _, body := range [][]byte{data, []byte("changed bytes")} {
				if r, err := fresh.Write(t.Context(), c.id, body); !errors.Is(err, ErrExpired) || errors.Is(err, ErrUncertain) || r != (Receipt{}) {
					t.Fatal("clock correction revived write")
				}
			}
			if o.reads.Load() != reads || o.creates.Load() != creates || c.rows[c.id] != original || c.lifetime != saved {
				t.Fatal("refused operation accessed object or changed original state")
			}
			requireRead(t, fresh, other, []byte("peer stays live"))
		})
	}
}

func TestExpirySaveFailureAndLostReply(t *testing.T) {
	for _, saved := range []bool{false, true} {
		t.Run(map[bool]string{false: "before_save", true: "saved_reply_lost"}[saved], func(t *testing.T) {
			_, o, catalog := fixture(t)
			c := newLifetimeCatalog(t, catalog, testLifetime(t))
			s := newStore(t, o, c, authorizeFunc(allow), Limits{1024, 2})
			data := []byte("capture")
			original, err := s.Write(t.Context(), c.id, data)
			if err != nil {
				t.Fatal(err)
			}
			problem := errors.New("expiry save unavailable")
			c.now = c.lifetime.PayloadExpiresAt()
			if saved {
				c.replyErr = problem
			} else {
				c.saveErr = problem
			}
			reads, creates := o.reads.Load(), o.creates.Load()
			if data, err := s.Read(t.Context(), c.id); !errors.Is(err, problem) || errors.Is(err, ErrExpired) || errors.Is(err, ErrNotFound) || data != nil {
				t.Fatal("uncertain expiry returned a confirmed outcome")
			}
			if o.reads.Load() != reads || o.creates.Load() != creates || c.lifetime.Expired() != saved || c.rows[c.id] != original {
				t.Fatal("expiry failure changed receipt or touched objects")
			}
			if !saved {
				if r, err := s.Write(t.Context(), c.id, data); !errors.Is(err, problem) || errors.Is(err, ErrExpired) || r != (Receipt{}) {
					t.Fatal("failed preliminary expiry save returned confirmed result")
				}
				if o.creates.Load() != creates {
					t.Fatal("failed preliminary expiry save reached upload")
				}
			}
			c.saveErr = nil
			c.replyErr = nil
			c.now = c.now.Add(-time.Minute)
			fresh := newStore(t, o, c, authorizeFunc(allow), Limits{1024, 2})
			if saved {
				if _, err = fresh.Read(t.Context(), c.id); !errors.Is(err, ErrExpired) {
					t.Fatal("saved expiry lost after uncertain reply")
				}
			} else {
				requireRead(t, fresh, c.id, data)
			}
		})
	}
}

func TestExpiryPublishFailureIsUncertain(t *testing.T) {
	for _, saved := range []bool{false, true} {
		t.Run(map[bool]string{false: "before_save", true: "saved_reply_lost"}[saved], func(t *testing.T) {
			_, o, catalog := fixture(t)
			c := newLifetimeCatalog(t, catalog, testLifetime(t))
			s := newStore(t, o, c, authorizeFunc(allow), Limits{1024, 2})
			original, err := s.Write(t.Context(), c.id, []byte("capture"))
			if err != nil {
				t.Fatal(err)
			}
			problem := errors.New("expiry commit reply unavailable")
			o.afterCreate = func(context.Context) error {
				c.now = c.lifetime.PayloadExpiresAt()
				if saved {
					c.replyErr = problem
				} else {
					c.saveErr = problem
				}
				return nil
			}
			r, err := s.Write(t.Context(), c.id, []byte("changed during upload"))
			if !errors.Is(err, ErrUncertain) || !errors.Is(err, problem) || errors.Is(err, ErrExpired) || r != (Receipt{}) {
				t.Fatal("uncertain expiry publish returned a confirmed result")
			}
			if c.lifetime.Expired() != saved || c.rows[c.id] != original {
				t.Fatal("expiry publish failure changed original acceptance")
			}
		})
	}
}

func TestDeniedRequestCannotSaveExpiry(t *testing.T) {
	_, o, catalog := fixture(t)
	c := newLifetimeCatalog(t, catalog, testLifetime(t))
	s := newStore(t, o, c, authorizeFunc(allow), Limits{1024, 2})
	if _, err := s.Write(t.Context(), c.id, []byte("capture")); err != nil {
		t.Fatal(err)
	}
	c.now = c.lifetime.PayloadExpiresAt()
	// The caller authenticates, but current catalog reader/writer grants were removed.
	delete(c.readers, readerGrant{c.id.Tenant, "reader-a"})
	delete(c.writers, writerGrant{c.id.Tenant, Attribution{sampleOwner, "principal-a"}})
	if _, err := s.Read(t.Context(), c.id); !errors.Is(err, ErrDenied) {
		t.Fatal("denied reader was allowed")
	}
	if _, err := s.Write(t.Context(), c.id, []byte("capture")); !errors.Is(err, ErrDenied) {
		t.Fatal("denied writer was allowed")
	}
	if c.lifetime.Expired() {
		t.Fatal("unauthorized request saved expiry")
	}
}
