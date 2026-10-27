package ingest

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
)

func captureID() Identity {
	id := sampleID
	id.Kind = CaptureRecord
	return id
}

func requireRead(t *testing.T, s *Store, id Identity, want []byte) {
	t.Helper()
	got, err := s.Read(t.Context(), id)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("read kind %d = %q, %v; want %q", id.Kind, got, err, want)
	}
}

func TestSpanAndCaptureHaveSeparateAcceptance(t *testing.T) {
	for _, order := range []struct {
		name          string
		first, second Identity
	}{
		{"span first", sampleID, captureID()},
		{"capture first", captureID(), sampleID},
	} {
		t.Run(order.name, func(t *testing.T) {
			s, o, c := fixture(t)
			// Equal content must not turn accepting the peer into a duplicate.
			data := []byte("same opaque bytes")
			r, err := s.Write(t.Context(), order.first, data)
			if err != nil || r.Duplicate || r.Reference.Identity != order.first {
				t.Fatalf("accept first = %+v, %v", r, err)
			}
			fresh := newStore(t, o, c, authorizeFunc(allow), Limits{1024, 2})
			requireRead(t, fresh, order.first, data)
			if _, err := fresh.Read(t.Context(), order.second); !errors.Is(err, ErrNotFound) {
				t.Fatalf("absent peer = %v", err)
			}
			if r, err := fresh.Write(t.Context(), order.first, data); err != nil || !r.Duplicate {
				t.Fatalf("retry while peer absent = %+v, %v", r, err)
			}
			if _, err := fresh.Write(t.Context(), order.first, []byte("changed first record")); !errors.Is(err, ErrConflict) {
				t.Fatalf("conflict while peer absent = %v", err)
			}
			requireRead(t, fresh, order.first, data)
			r, err = fresh.Write(t.Context(), order.second, data)
			if err != nil || r.Duplicate || r.Reference.Identity != order.second {
				t.Fatalf("accept second = %+v, %v", r, err)
			}
			if c.count() != 2 || o.count() != 2 {
				// One object is shared by the accepted peers; the conflict left an orphan.
				t.Fatalf("accepted=%d objects=%d; want 2 accepted records, 1 shared object, 1 orphan", c.count(), o.count())
			}
			for _, id := range []Identity{order.first, order.second} {
				requireRead(t, fresh, id, data)
				r, err := fresh.Write(t.Context(), id, data)
				if err != nil || !r.Duplicate {
					t.Fatalf("retry kind %d = %+v, %v", id.Kind, r, err)
				}
			}
		})
	}
}

func TestSecondRecordFailuresPreserveAcceptedPeer(t *testing.T) {
	for _, order := range []struct {
		name          string
		first, second Identity
	}{
		{"span first", sampleID, captureID()},
		{"capture first", captureID(), sampleID},
	} {
		t.Run(order.name, func(t *testing.T) { testSecondRecordFailures(t, order.first, order.second) })
	}
}

func testSecondRecordFailures(t *testing.T, firstID, secondID Identity) {
	t.Helper()
	broken := errors.New("interrupted second record")
	for _, tc := range []struct {
		name      string
		configure func(*modelObjects, *modelCatalog)
		wantErr   error
		committed bool
	}{
		{"upload fails", func(o *modelObjects, _ *modelCatalog) {
			o.afterCreate = func(context.Context) error { return broken }
		}, broken, false},
		{"publication fails", func(_ *modelObjects, c *modelCatalog) {
			c.beforePublish = func(context.Context) error { return broken }
		}, ErrUncertain, false},
		{"publication reply lost", func(_ *modelObjects, c *modelCatalog) {
			c.afterPublish = func(context.Context) error { return broken }
		}, ErrUncertain, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, o, c := fixture(t)
			first, second := []byte("first record"), []byte("second record")
			if _, err := s.Write(t.Context(), firstID, first); err != nil {
				t.Fatal(err)
			}
			// Accepting one record must not imply that the peer exists.
			if _, err := s.Read(t.Context(), secondID); !errors.Is(err, ErrNotFound) {
				t.Fatalf("missing peer = %v", err)
			}
			tc.configure(o, c)
			if r, err := s.Write(t.Context(), secondID, second); !errors.Is(err, tc.wantErr) || r != (Receipt{}) {
				t.Fatalf("second record result = %+v, %v", r, err)
			}
			requireRead(t, s, firstID, first)
			o.afterCreate, c.beforePublish, c.afterPublish = nil, nil, nil
			fresh := newStore(t, o, c, authorizeFunc(allow), Limits{1024, 2})
			if tc.committed {
				requireRead(t, fresh, secondID, second)
			} else if _, err := fresh.Read(t.Context(), secondID); !errors.Is(err, ErrNotFound) {
				t.Fatalf("unpublished peer read = %v", err)
			}
			r, err := fresh.Write(t.Context(), secondID, second)
			if err != nil || r.Duplicate != tc.committed {
				t.Fatalf("second record retry = %+v, %v", r, err)
			}
			if _, err := fresh.Write(t.Context(), secondID, []byte("different bytes")); !errors.Is(err, ErrConflict) {
				t.Fatalf("changed peer = %v", err)
			}
			requireRead(t, fresh, firstID, first)
			requireRead(t, fresh, secondID, second)
			if c.count() != 2 {
				t.Fatalf("accepted=%d; want span and original capture", c.count())
			}
		})
	}
}

func TestConcurrentSpanAndCaptureRetriesStaySeparate(t *testing.T) {
	s, o, c := fixture(t)
	start := make(chan struct{})
	type result struct {
		id      Identity
		receipt Receipt
		err     error
	}
	results := make(chan result, 16)
	var workers sync.WaitGroup
	for range 8 {
		for _, id := range []Identity{sampleID, captureID()} {
			workers.Go(func() {
				<-start
				data := []byte("span metadata")
				if id.Kind == CaptureRecord {
					data = []byte("captured bodies")
				}
				r, err := s.Write(t.Context(), id, data)
				results <- result{id, r, err}
			})
		}
	}
	close(start)
	workers.Wait()
	close(results)
	accepted := map[Kind]int{}
	for r := range results {
		if r.err != nil || r.receipt.Reference.Identity != r.id {
			t.Fatalf("write = %+v", r)
		}
		if !r.receipt.Duplicate {
			accepted[r.id.Kind]++
		}
	}
	if accepted[SpanRecord] != 1 || accepted[CaptureRecord] != 1 || c.count() != 2 || o.count() != 2 {
		t.Fatalf("new records=%v catalog=%d objects=%d", accepted, c.count(), o.count())
	}
	requireRead(t, s, sampleID, []byte("span metadata"))
	requireRead(t, s, captureID(), []byte("captured bodies"))
}

func TestCaptureAuthorizationAndKindValidationPrecedeStorage(t *testing.T) {
	s, o, c := fixture(t)
	denied := errors.New("tenant denied")
	authCalls := 0
	s.authorizer = authorizeFunc(func(context.Context, string) error { authCalls++; return denied })
	for _, id := range []Identity{captureID(), {Tenant: "other-tenant", Trace: sampleID.Trace, Span: sampleID.Span, Kind: CaptureRecord}} {
		if _, err := s.Write(t.Context(), id, nil); !errors.Is(err, denied) {
			t.Fatalf("denied write = %v", err)
		}
		if _, err := s.Read(t.Context(), id); !errors.Is(err, denied) {
			t.Fatalf("denied read = %v", err)
		}
	}
	if authCalls != 4 {
		t.Fatalf("authorization calls=%d", authCalls)
	}
	invalid := captureID()
	invalid.Kind = Kind(255)
	if _, err := s.Write(t.Context(), invalid, nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid write = %v", err)
	}
	if _, err := s.Read(t.Context(), invalid); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid read = %v", err)
	}
	if authCalls != 4 || o.creates.Load()+o.reads.Load()+c.publications.Load()+c.lookups.Load() != 0 {
		t.Fatal("invalid or unauthorized capture reached capabilities")
	}
}
