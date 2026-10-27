package ingest

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestReceiptPreservesFirstTimesAcrossRetryAndPolicyChange(t *testing.T) {
	for _, firstID := range []Identity{sampleID, captureID()} {
		t.Run(map[Kind]string{SpanRecord: "span first", CaptureRecord: "capture first"}[firstID.Kind], func(t *testing.T) {
			first, objects, catalog := fixture(t)
			accepted := catalog.now
			bytes := []byte("original record")
			catalog.afterPublish = func(context.Context) error { return errors.New("saved receipt reply lost") }
			if r, err := first.Write(t.Context(), firstID, bytes); !errors.Is(err, ErrUncertain) || r != (Receipt{}) {
				t.Fatalf("lost reply: %+v %v", r, err)
			}
			catalog.afterPublish = nil
			catalog.mu.Lock()
			catalog.now = accepted.Add(24 * time.Hour)
			catalog.retention = 30 * 24 * time.Hour
			catalog.mu.Unlock()
			// Another authorized submitting principal still receives original attribution
			// and times. A replacement coordinator owns no copy of the earlier receipt.
			later := Attribution{sampleOwner, "collector-b"}
			setWriter(catalog, firstID.Tenant, later, true)
			fresh := newStore(t, objects, catalog, writerAuth(later), Limits{1024, 2})
			retry, err := fresh.Write(t.Context(), firstID, bytes)
			if err != nil || !retry.Duplicate || !retry.AcceptedAt.Equal(accepted) || !retry.ExpiresAt.Equal(accepted.Add(7*24*time.Hour)) || retry.Attribution.SubmittedBy != "principal-a" {
				t.Fatalf("retry changed original result: %+v %v", retry, err)
			}
			lookup, err := catalog.Lookup(t.Context(), firstID, "reader-a")
			if err != nil || lookup.Duplicate || !lookup.AcceptedAt.Equal(retry.AcceptedAt) || !lookup.ExpiresAt.Equal(retry.ExpiresAt) {
				t.Fatalf("lookup times: %+v %v", lookup, err)
			}
			if _, err := fresh.Write(t.Context(), firstID, []byte("changed contents")); !errors.Is(err, ErrConflict) {
				t.Fatal(err)
			}
			after, err := catalog.Lookup(t.Context(), firstID, "reader-a")
			if err != nil || after != lookup {
				t.Fatalf("conflict changed receipt: %+v %v", after, err)
			}
			peer := captureID()
			if firstID.Kind == CaptureRecord {
				peer = sampleID
			}
			saved, err := fresh.Write(t.Context(), peer, bytes)
			if err != nil || saved.Duplicate || !saved.AcceptedAt.Equal(accepted.Add(24*time.Hour)) || !saved.ExpiresAt.Equal(saved.AcceptedAt.Add(30*24*time.Hour)) {
				t.Fatalf("peer did not receive separate current policy: %+v %v", saved, err)
			}
			again, err := catalog.Lookup(t.Context(), firstID, "reader-a")
			if err != nil || again != lookup {
				t.Fatalf("matching peer changed original receipt: %+v %v", again, err)
			}
		})
	}
}

func TestReceiptMalformedTimesFailClosed(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(Receipt) Receipt
	}{
		{"missing acceptance", func(r Receipt) Receipt { r.AcceptedAt = time.Time{}; return r }},
		{"missing expiry", func(r Receipt) Receipt { r.ExpiresAt = time.Time{}; return r }},
		{"equal instants", func(r Receipt) Receipt { r.ExpiresAt = r.AcceptedAt; return r }},
		{"expiry before acceptance", func(r Receipt) Receipt { r.ExpiresAt = r.AcceptedAt.Add(-time.Nanosecond); return r }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, duplicate := range []bool{false, true} {
				store, _, catalog := fixture(t)
				if duplicate {
					if _, err := store.Write(t.Context(), sampleID, nil); err != nil {
						t.Fatal(err)
					}
				}
				catalog.changeReceipt = tc.change
				if got, err := store.Write(t.Context(), sampleID, nil); !errors.Is(err, ErrIntegrity) || !errors.Is(err, ErrUncertain) || got != (Receipt{}) {
					t.Fatalf("publish duplicate=%v: %+v %v", duplicate, got, err)
				}
			}
			store, objects, catalog := fixture(t)
			if _, err := store.Write(t.Context(), sampleID, nil); err != nil {
				t.Fatal(err)
			}
			catalog.changeReceipt = tc.change
			if got, err := store.Read(t.Context(), sampleID); !errors.Is(err, ErrIntegrity) || got != nil || objects.reads.Load() != 0 {
				t.Fatalf("malformed lookup reached object: %q %v", got, err)
			}
		})
	}
}

func TestReceiptDoesNotUseIngestClockOrDefaultDuration(t *testing.T) {
	store, _, catalog := fixture(t)
	// Deliberately old supplied instants and a custom duration. The controlled
	// catalog decides availability; the coordinator must not sample its own clock.
	catalog.now = time.Date(2001, 1, 1, 12, 0, 0, 0, time.UTC)
	catalog.retention = 2 * time.Hour
	got, err := store.Write(t.Context(), sampleID, []byte("bytes"))
	if err != nil || !got.AcceptedAt.Equal(catalog.now) || !got.ExpiresAt.Equal(catalog.now.Add(2*time.Hour)) {
		t.Fatalf("custom catalog times rejected: %+v %v", got, err)
	}
	requireRead(t, store, sampleID, []byte("bytes"))
}
