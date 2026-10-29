package ingest

import (
	"errors"
	"testing"
	"time"

	"github.com/sidsharma96/rewind/internal/traceid"
)

func TestRunElapsedClosureSurvivesEarlierClock(t *testing.T) {
	w := testDeliveryWindow(t)
	running := NewRunAdmission()
	completed, err := running.Complete(w.CompletedAt(), DefaultDeliveryWindow)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		state RunAdmission
		at    time.Time
		want  error
	}{
		{RunAdmission{}, w.Until(), ErrInvalid},
		{RunAdmission{phase: runClosed}, w.Until(), ErrInvalid}, {completed, time.Time{}, ErrInvalid},
		{running, w.Until(), ErrDenied}, {completed, w.Until().Add(-time.Nanosecond), ErrDenied},
	} {
		got, err := tc.state.CloseElapsed(tc.at)
		if !errors.Is(err, tc.want) || got != (RunAdmission{}) {
			t.Fatalf("refused closure = %+v, %v", got, err)
		}
	}
	closed, err := completed.CloseElapsed(w.Until())
	if err != nil || closed.phase != runClosed || closed.window != completed.window {
		t.Fatalf("closure: %+v, %v", closed, err)
	}
	// No persisted closure: the original time predicate alone permits earlier time.
	earlier := w.Until().Add(-time.Minute)
	if err := completed.CheckRecord(earlier); err != nil {
		t.Fatal(err)
	}
	for _, at := range []time.Time{earlier, w.CompletedAt().Add(-time.Hour), w.Until(), w.Until().Add(time.Hour)} {
		for _, same := range []bool{false, true} {
			if err := closed.CheckRegistration(at, same); !errors.Is(err, ErrDenied) {
				t.Fatalf("closed registration: %v", err)
			}
		}
		if err := closed.CheckRecord(at); !errors.Is(err, ErrDenied) {
			t.Fatalf("closed record: %v", err)
		}
		again, err := closed.CloseElapsed(at)
		if err != nil || again != closed {
			t.Fatalf("closure retry: %+v, %v", again, err)
		}
		again, err = closed.Complete(at, time.Hour)
		if err != nil || again != closed {
			t.Fatalf("completion reopened window: %+v, %v", again, err)
		}
	}
	revoked, err := closed.Revoke()
	if err != nil || revoked.phase != runRevoked || revoked.window != closed.window {
		t.Fatalf("revoke closed: %+v %v", revoked, err)
	}
	again, err := revoked.CloseElapsed(earlier)
	if err != nil || again != revoked {
		t.Fatalf("closure replaced revocation: %+v %v", again, err)
	}
	if _, err := revoked.Complete(earlier, time.Hour); !errors.Is(err, ErrDenied) {
		t.Fatal("revocation reopened")
	}
	// False forward time can close early: correction cannot undo saved closure.
	future, err := completed.CloseElapsed(w.Until().Add(time.Hour))
	if err != nil || future.CheckRecord(earlier) != ErrDenied {
		t.Fatal("forward observation did not stay closed")
	}
}

func TestSavedRunClosureRefusesFreshCoordinatorsAfterClockCorrection(t *testing.T) {
	for _, kind := range []Kind{SpanRecord, CaptureRecord} {
		t.Run(map[Kind]string{SpanRecord: "span", CaptureRecord: "capture"}[kind], func(t *testing.T) {
			s, o, c := fixture(t)
			id := sampleID
			id.Kind = kind
			contents := []byte("accepted before closure")
			accepted, err := s.Write(t.Context(), id, contents)
			if err != nil {
				t.Fatal(err)
			}
			w := testDeliveryWindow(t)
			key := runKey{id.Tenant, sampleOwner.Run}
			completed, err := completeModelRun(c, key, w.CompletedAt(), DefaultDeliveryWindow)
			if err != nil {
				t.Fatal(err)
			}
			// Trusted fixture policy operation under the same mutex; no SQL/durability.
			c.mu.Lock()
			closed, err := completed.CloseElapsed(w.Until())
			if err == nil {
				c.runs[key] = closed
			}
			c.now = w.Until().Add(-time.Minute)
			c.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			fresh := newStore(t, o, c, authorizeFunc(allow), Limits{1024, 1})
			beforeObjects, beforeRows := o.count(), c.count()
			for _, data := range [][]byte{contents, []byte("changed")} {
				if _, err := fresh.Write(t.Context(), id, data); !errors.Is(err, ErrDenied) {
					t.Fatalf("closed write: %v", err)
				}
			}
			another := id
			another.Span = traceid.Span{100}
			if _, err := fresh.Write(t.Context(), another, contents); !errors.Is(err, ErrDenied) {
				t.Fatalf("unregistered write: %v", err)
			}
			r := newRegistrar(t, trustedRegistrar("trusted-host-1"), c, 1)
			if _, err := r.Register(t.Context(), sampleExecution, sampleOwner); !errors.Is(err, ErrDenied) || errors.Is(err, ErrRegistrationUncertain) {
				t.Fatalf("closed registration: %v", err)
			}
			if o.count() != beforeObjects || c.count() != beforeRows || c.runs[key] != closed || c.rows[id] != accepted {
				t.Fatal("closed attempts changed saved state")
			}
			if _, err := fresh.Read(t.Context(), id); err != nil {
				t.Fatalf("closure changed reader grant: %v", err)
			}
		})
	}
}
