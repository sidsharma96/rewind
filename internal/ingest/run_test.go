package ingest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sidsharma96/rewind/internal/traceid"
)

func TestRunAdmissionTransitionsAndDeadline(t *testing.T) {
	w := testDeliveryWindow(t)
	running := NewRunAdmission()
	completed, err := running.Complete(w.CompletedAt(), DefaultDeliveryWindow)
	if err != nil {
		t.Fatal(err)
	}
	revoked, err := completed.Revoke()
	if err != nil || revoked.window != completed.window {
		t.Fatalf("revocation erased completion: %+v, %v", revoked, err)
	}
	for _, tc := range []struct {
		name          string
		state         RunAdmission
		at            time.Time
		newCall, same error
	}{
		{"running", running, w.CompletedAt(), nil, nil},
		{"completion", completed, w.CompletedAt(), ErrDenied, nil},
		{"before deadline", completed, w.Until().Add(-time.Nanosecond), ErrDenied, nil},
		{"deadline", completed, w.Until(), ErrDenied, ErrDenied},
		{"before completion", completed, w.CompletedAt().Add(-time.Nanosecond), ErrDenied, ErrDenied},
		{"revoked", revoked, w.CompletedAt(), ErrDenied, ErrDenied},
		{"unknown", RunAdmission{}, w.CompletedAt(), ErrInvalid, ErrInvalid},
		{"unknown clock", completed, time.Time{}, ErrInvalid, ErrInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.state.CheckRegistration(tc.at, false); !errors.Is(err, tc.newCall) {
				t.Fatalf("new registration: %v, want %v", err, tc.newCall)
			}
			if err := tc.state.CheckRegistration(tc.at, true); !errors.Is(err, tc.same) {
				t.Fatalf("identical registration: %v, want %v", err, tc.same)
			}
			if err := tc.state.CheckRecord(tc.at); !errors.Is(err, tc.same) {
				t.Fatalf("record admission: %v, want %v", err, tc.same)
			}
		})
	}
	for _, at := range []time.Time{w.CompletedAt().Add(-time.Hour), w.Until().Add(time.Hour), {}} {
		retry, err := completed.Complete(at, 30*time.Minute)
		if err != nil || retry != completed {
			t.Fatalf("completion retry moved original: %+v, %v", retry, err)
		}
	}
	if got, err := revoked.Complete(w.Until(), DefaultDeliveryWindow); !errors.Is(err, ErrDenied) || got != (RunAdmission{}) {
		t.Fatalf("revoked run reopened: %+v, %v", got, err)
	}
	if got, err := revoked.Revoke(); err != nil || got != revoked {
		t.Fatalf("revocation retry changed original: %+v, %v", got, err)
	}
	if _, err := (RunAdmission{}).Revoke(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unknown state revoked successfully: %v", err)
	}
	if _, err := (RunAdmission{}).Complete(w.CompletedAt(), DefaultDeliveryWindow); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unknown state completed successfully: %v", err)
	}
	for _, duration := range []time.Duration{0, -time.Minute} {
		if _, err := running.Complete(w.CompletedAt(), duration); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid first-completion policy: %v", err)
		}
	}
}

// Trusted test control only: these helpers do not authenticate a run controller
// or model provider durability. They serialize state with the catalog decision.
func completeModelRun(c *modelCatalog, key runKey, at time.Time, duration time.Duration) (RunAdmission, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	original, exists := c.runs[key]
	if !exists {
		return RunAdmission{}, ErrDenied
	}
	next, err := original.Complete(at, duration)
	if err == nil {
		c.runs[key] = next
	}
	return next, err
}

func revokeModelRun(c *modelCatalog, key runKey) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	original, exists := c.runs[key]
	if !exists {
		return ErrDenied
	}
	next, err := original.Revoke()
	if err == nil {
		c.runs[key] = next
	}
	return err
}

func setCreator(c *modelCatalog, e Execution, o Owner, principal string, allowed bool) {
	c.mu.Lock()
	c.creators[creatorGrant{e, o, principal}] = allowed
	c.mu.Unlock()
}

func TestRunRegistrationLostReplyResolvesDuringDrain(t *testing.T) {
	_, _, c := fixture(t)
	w := testDeliveryWindow(t)
	key := runKey{sampleID.Tenant, sampleOwner.Run}
	e := sampleExecution
	e.Span = traceid.Span{103}
	setCreator(c, e, sampleOwner, "trusted-host-1", true)
	setCreator(c, e, sampleOwner, "trusted-host-2", true)
	first := newRegistrar(t, trustedRegistrar("trusted-host-1"), c, 1)
	broken := errors.New("registration committed; reply lost")
	c.afterRegistration = func(context.Context) error { return broken }
	if _, err := first.Register(t.Context(), e, sampleOwner); !errors.Is(err, ErrRegistrationUncertain) || !errors.Is(err, broken) {
		t.Fatalf("lost reply: %v", err)
	}
	c.afterRegistration = nil
	original := c.bindings[e]
	completed, err := completeModelRun(c, key, w.CompletedAt(), DefaultDeliveryWindow)
	if err != nil {
		t.Fatal(err)
	}
	fresh := newRegistrar(t, trustedRegistrar("trusted-host-2"), c, 1)
	for _, at := range []time.Time{w.CompletedAt(), w.Until().Add(-time.Nanosecond)} {
		c.mu.Lock()
		c.now = at
		c.mu.Unlock()
		r, err := fresh.Register(t.Context(), e, sampleOwner)
		if err != nil || !r.Duplicate || r.Registration != original || r.Registration.RegisteredBy != "trusted-host-1" {
			t.Fatalf("retry during drain changed attribution: %+v, %v", r, err)
		}
		got, err := completeModelRun(c, key, at, time.Hour)
		if err != nil || got != completed || len(c.bindings) != 2 {
			t.Fatalf("retry extended completion or created binding: %+v, %v", got, err)
		}
	}
	setCreator(c, e, sampleOwner, "trusted-host-2", false)
	if _, err := fresh.Register(t.Context(), e, sampleOwner); !errors.Is(err, ErrDenied) || errors.Is(err, ErrRegistrationUncertain) {
		t.Fatalf("missing current creator permission: %v", err)
	}
	setCreator(c, e, sampleOwner, "trusted-host-2", true)
	c.mu.Lock()
	c.now = w.Until()
	c.mu.Unlock()
	if r, err := fresh.Register(t.Context(), e, sampleOwner); !errors.Is(err, ErrDenied) || errors.Is(err, ErrRegistrationUncertain) || r != (RegistrationReceipt{}) {
		t.Fatalf("deadline retry: %+v, %v", r, err)
	}
	if c.bindings[e] != original || c.runs[key] != completed {
		t.Fatal("refused retry changed original binding or deadline")
	}
}

func TestRunCompletedRefusesNewOrChangedRegistrations(t *testing.T) {
	for _, revoked := range []bool{false, true} {
		_, _, c := fixture(t)
		w := testDeliveryWindow(t)
		key := runKey{sampleID.Tenant, sampleOwner.Run}
		if _, err := completeModelRun(c, key, w.CompletedAt(), DefaultDeliveryWindow); err != nil {
			t.Fatal(err)
		}
		if revoked {
			if err := revokeModelRun(c, key); err != nil {
				t.Fatal(err)
			}
		}
		original := c.bindings[sampleExecution]
		r := newRegistrar(t, trustedRegistrar("trusted-host-1"), c, 1)
		otherRun := Owner{Run: "another-running-run", Instance: sampleOwner.Instance}
		c.runs[runKey{sampleID.Tenant, otherRun.Run}] = NewRunAdmission()
		newCall := sampleExecution
		newCall.Span = traceid.Span{102}
		cases := []struct {
			e Execution
			o Owner
		}{
			{newCall, sampleOwner},
			{sampleExecution, Owner{Run: sampleOwner.Run, Instance: "changed-instance"}},
			{sampleExecution, otherRun}, // Cannot bypass the original run's closure.
		}
		if revoked {
			cases = append(cases, struct {
				e Execution
				o Owner
			}{sampleExecution, sampleOwner})
		}
		for _, tc := range cases {
			setCreator(c, tc.e, tc.o, "trusted-host-1", true)
			rcpt, err := r.Register(t.Context(), tc.e, tc.o)
			if !errors.Is(err, ErrDenied) || errors.Is(err, ErrOwnershipConflict) || errors.Is(err, ErrRegistrationUncertain) || rcpt != (RegistrationReceipt{}) {
				t.Fatalf("closed run disclosed registration outcome: %+v, %v", rcpt, err)
			}
		}
		if len(c.bindings) != 1 || c.bindings[sampleExecution] != original {
			t.Fatal("closed run accepted or changed a binding")
		}
	}
}

func TestRunTransitionBetweenAuthorizationAndRegistration(t *testing.T) {
	for _, transition := range []string{"complete", "revoke", "deadline", "creator revoked"} {
		for _, identical := range []bool{false, true} {
			t.Run(transition+map[bool]string{false: "/new", true: "/identical"}[identical], func(t *testing.T) {
				_, _, c := fixture(t)
				key := runKey{sampleID.Tenant, sampleOwner.Run}
				e := sampleExecution
				if !identical {
					e.Span = traceid.Span{101}
				}
				setCreator(c, e, sampleOwner, "trusted-host-1", true)
				if transition == "deadline" {
					if _, err := completeModelRun(c, key, c.now, DefaultDeliveryWindow); err != nil {
						t.Fatal(err)
					}
				}
				entered, release := make(chan struct{}), make(chan struct{})
				c.beforeRegistration = func(ctx context.Context) error {
					close(entered)
					select {
					case <-release:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				r := newRegistrar(t, trustedRegistrar("trusted-host-1"), c, 1)
				type result struct {
					r   RegistrationReceipt
					err error
				}
				done := make(chan result, 1)
				go func() { rcpt, err := r.Register(t.Context(), e, sampleOwner); done <- result{rcpt, err} }()
				<-entered
				switch transition {
				case "complete":
					if _, err := completeModelRun(c, key, c.now, DefaultDeliveryWindow); err != nil {
						t.Fatal(err)
					}
				case "revoke":
					if err := revokeModelRun(c, key); err != nil {
						t.Fatal(err)
					}
				case "deadline":
					c.mu.Lock()
					c.now = c.runs[key].window.Until()
					c.mu.Unlock()
				case "creator revoked":
					setCreator(c, e, sampleOwner, "trusted-host-1", false)
				}
				close(release)
				got := <-done
				if transition == "complete" && identical {
					if got.err != nil || !got.r.Duplicate || got.r.Registration != c.bindings[e] {
						t.Fatalf("completed identical retry: %+v, %v", got.r, got.err)
					}
				} else if !errors.Is(got.err, ErrDenied) || errors.Is(got.err, ErrRegistrationUncertain) || got.r != (RegistrationReceipt{}) {
					t.Fatalf("closed in-flight registration: %+v, %v", got.r, got.err)
				}
				if len(c.bindings) != 1 {
					t.Fatal("in-flight registration created a binding after closure")
				}
			})
		}
	}
}

func TestRunRegistrationUnknownStateAndPolicyErrorsFailClosed(t *testing.T) {
	for _, missing := range []bool{false, true} {
		s, o, c := fixture(t)
		key := runKey{sampleID.Tenant, sampleOwner.Run}
		if missing {
			delete(c.runs, key)
		} else {
			c.runs[key] = RunAdmission{}
		}
		r := newRegistrar(t, trustedRegistrar("trusted-host-1"), c, 1)
		if got, err := r.Register(t.Context(), sampleExecution, sampleOwner); err == nil || got != (RegistrationReceipt{}) {
			t.Fatalf("unknown state admitted: %+v, %v", got, err)
		}
		if _, err := s.Write(t.Context(), sampleID, nil); err == nil || o.creates.Load()+c.publications.Load() != 0 {
			t.Fatalf("unknown state uploaded or published: %v", err)
		}
	}
	_, _, c := fixture(t)
	broken := errors.New("authoritative permission state unavailable")
	c.policyErr = broken
	r := newRegistrar(t, trustedRegistrar("trusted-host-1"), c, 1)
	if _, err := r.Register(t.Context(), sampleExecution, sampleOwner); !errors.Is(err, broken) || !errors.Is(err, ErrRegistrationUncertain) {
		t.Fatalf("policy outage lost uncertainty/cause: %v", err)
	}
}

func TestRunRevocationClosesDeliveryWithoutClosingReadAccess(t *testing.T) {
	s, o, c, _ := deliveryFixture(t)
	for _, id := range []Identity{sampleID, captureID()} {
		if _, err := s.Write(t.Context(), id, []byte("accepted evidence")); err != nil {
			t.Fatal(err)
		}
	}
	if err := revokeModelRun(c, runKey{sampleID.Tenant, sampleOwner.Run}); err != nil {
		t.Fatal(err)
	}
	creates, pubs := o.creates.Load(), c.publications.Load()
	for _, id := range []Identity{sampleID, captureID()} {
		if _, err := s.Write(t.Context(), id, []byte("accepted evidence")); !errors.Is(err, ErrDenied) || errors.Is(err, ErrUncertain) {
			t.Fatalf("revoked run delivery: %v", err)
		}
		requireRead(t, s, id, []byte("accepted evidence"))
	}
	if o.creates.Load() != creates || c.publications.Load() != pubs {
		t.Fatal("known revoked delivery reached upload/publication")
	}
}

func TestRunCompletionStopsNewCallsButDrainsExistingCapture(t *testing.T) {
	s, o, c := fixture(t)
	key := runKey{sampleID.Tenant, sampleOwner.Run}
	if _, err := s.Write(t.Context(), sampleID, []byte("span accepted before completion")); err != nil {
		t.Fatal(err)
	}
	completed, err := completeModelRun(c, key, c.now, DefaultDeliveryWindow)
	if err != nil {
		t.Fatal(err)
	}
	newCall := sampleExecution
	newCall.Span = traceid.Span{100}
	setCreator(c, newCall, sampleOwner, "trusted-host-1", true)
	r := newRegistrar(t, trustedRegistrar("trusted-host-1"), c, 1)
	if _, err := r.Register(t.Context(), newCall, sampleOwner); !errors.Is(err, ErrDenied) || errors.Is(err, ErrRegistrationUncertain) {
		t.Fatalf("new call after completion: %v", err)
	}
	if got, err := r.Register(t.Context(), sampleExecution, sampleOwner); err != nil || !got.Duplicate || got.Registration != c.bindings[sampleExecution] {
		t.Fatalf("existing call retry during drain: %+v, %v", got, err)
	}
	if got, err := s.Write(t.Context(), captureID(), []byte("queued capture")); err != nil || got.Duplicate {
		t.Fatalf("queued capture not drained: %+v, %v", got, err)
	}
	c.mu.Lock()
	c.now = completed.window.Until()
	c.mu.Unlock()
	creates := o.creates.Load()
	if _, err := s.Write(t.Context(), captureID(), []byte("queued capture")); !errors.Is(err, ErrDenied) || o.creates.Load() != creates {
		t.Fatalf("retry at deadline: %v", err)
	}
	requireRead(t, s, sampleID, []byte("span accepted before completion"))
	requireRead(t, s, captureID(), []byte("queued capture"))
}

func TestRunRegistrationConflictRequiresBothRunsOpen(t *testing.T) {
	for _, originalOpen := range []bool{false, true} {
		for _, proposedOpen := range []bool{false, true} {
			_, _, c := fixture(t)
			key := runKey{sampleID.Tenant, sampleOwner.Run}
			other := Owner{Run: "other-run", Instance: sampleOwner.Instance}
			otherKey := runKey{sampleID.Tenant, other.Run}
			c.runs[otherKey] = NewRunAdmission()
			for _, tc := range []struct {
				key  runKey
				open bool
			}{{key, originalOpen}, {otherKey, proposedOpen}} {
				if !tc.open {
					if _, err := completeModelRun(c, tc.key, c.now, DefaultDeliveryWindow); err != nil {
						t.Fatal(err)
					}
				}
			}
			setCreator(c, sampleExecution, other, "trusted-host-1", true)
			r := newRegistrar(t, trustedRegistrar("trusted-host-1"), c, 1)
			_, err := r.Register(t.Context(), sampleExecution, other)
			want := ErrDenied
			if originalOpen && proposedOpen {
				want = ErrOwnershipConflict
			}
			if !errors.Is(err, want) || errors.Is(err, ErrRegistrationUncertain) || len(c.bindings) != 1 || c.bindings[sampleExecution].Owner != sampleOwner {
				t.Fatalf("run pair originalOpen=%v proposedOpen=%v: %v", originalOpen, proposedOpen, err)
			}
		}
	}
}

func TestRunRegistrationConfirmedBeforeRevocationStaysSuccessful(t *testing.T) {
	_, _, c := fixture(t)
	key := runKey{sampleID.Tenant, sampleOwner.Run}
	e := sampleExecution
	e.Span = traceid.Span{104}
	setCreator(c, e, sampleOwner, "trusted-host-1", true)
	c.afterRegistration = func(context.Context) error { return revokeModelRun(c, key) }
	r := newRegistrar(t, trustedRegistrar("trusted-host-1"), c, 1)
	got, err := r.Register(t.Context(), e, sampleOwner)
	if err != nil || got.Duplicate || got.Registration != c.bindings[e] {
		t.Fatalf("confirmed registration changed after revocation: %+v, %v", got, err)
	}
}

func TestRunRevocationDuringUploadRefusesPublication(t *testing.T) {
	s, o, c, _ := deliveryFixture(t)
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
	go func() { _, err := s.Write(t.Context(), captureID(), []byte("in flight")); done <- err }()
	<-entered
	if err := revokeModelRun(c, runKey{sampleID.Tenant, sampleOwner.Run}); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; !errors.Is(err, ErrDenied) || errors.Is(err, ErrUncertain) || c.count() != 0 || o.count() != 1 {
		t.Fatalf("run revoked during upload: err=%v accepted=%d objects=%d", err, c.count(), o.count())
	}
}
