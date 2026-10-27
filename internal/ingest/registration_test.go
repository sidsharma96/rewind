package ingest

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

var (
	sampleExecution = Execution{Tenant: "tenant-a", Trace: "trace-1", Span: "span-42"}
	sampleOwner     = Owner{Run: "run-17", Instance: "planner-7"}
)

type registrationAuthFunc func(context.Context, Execution, Owner) (string, error)

func (f registrationAuthFunc) AuthorizeRegistration(ctx context.Context, e Execution, o Owner) (string, error) {
	return f(ctx, e, o)
}

// This fixture models atomic immutable bindings, not PostgreSQL or credentials.
// The test authorizer represents the trusted creator; no agent can become that
// creator just by providing the strings passed to Register.
type modelOwnershipCatalog struct {
	mu           sync.Mutex
	bindings     map[Execution]Registration
	calls        atomic.Int64
	before       func(context.Context) error
	after        func(context.Context) error
	changeResult func(Registration, bool) (Registration, bool)
}

func (m *modelOwnershipCatalog) RegisterOwnership(ctx context.Context, proposed Registration) (Registration, bool, error) {
	m.calls.Add(1)
	if m.before != nil {
		if err := m.before(ctx); err != nil {
			return Registration{}, false, err
		}
	}
	m.mu.Lock()
	if err := ctx.Err(); err != nil {
		m.mu.Unlock()
		return Registration{}, false, err
	}
	original, duplicate := m.bindings[proposed.Execution]
	if duplicate && original.Owner != proposed.Owner {
		m.mu.Unlock()
		return Registration{}, false, ErrOwnershipConflict
	}
	if !duplicate {
		original = proposed
		m.bindings[proposed.Execution] = proposed
	}
	m.mu.Unlock()
	if m.after != nil {
		if err := m.after(ctx); err != nil {
			return Registration{}, false, err
		}
	}
	if m.changeResult != nil {
		original, duplicate = m.changeResult(original, duplicate)
	}
	return original, duplicate, nil
}

func (m *modelOwnershipCatalog) snapshot() map[Execution]Registration {
	m.mu.Lock()
	defer m.mu.Unlock()
	copy := make(map[Execution]Registration, len(m.bindings))
	for e, reg := range m.bindings {
		copy[e] = reg
	}
	return copy
}

func trustedRegistrar(principal string) registrationAuthFunc {
	return func(ctx context.Context, _ Execution, _ Owner) (string, error) { return principal, ctx.Err() }
}

func newRegistrar(t *testing.T, auth RegistrationAuthorizer, catalog OwnershipCatalog, maxActive int) *Registrar {
	t.Helper()
	r, err := NewRegistrar(auth, catalog, maxActive)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func registrationFixture(t *testing.T) (*Registrar, *modelOwnershipCatalog) {
	t.Helper()
	c := &modelOwnershipCatalog{bindings: make(map[Execution]Registration)}
	return newRegistrar(t, trustedRegistrar("trusted-host-1"), c, 32), c
}

func TestRegistrationFailureBoundaries(t *testing.T) {
	broken := errors.New("catalog connection interrupted")
	for _, afterCommit := range []bool{false, true} {
		t.Run(map[bool]string{false: "before commit", true: "lost commit reply"}[afterCommit], func(t *testing.T) {
			r, c := registrationFixture(t)
			fail := func(context.Context) error { return broken }
			if afterCommit {
				c.after = fail
			} else {
				c.before = fail
			}
			if receipt, err := r.Register(t.Context(), sampleExecution, sampleOwner); !errors.Is(err, ErrRegistrationUncertain) || !errors.Is(err, broken) || receipt != (RegistrationReceipt{}) {
				t.Fatalf("interrupted registration=%+v,%v", receipt, err)
			}
			if len(c.snapshot()) != map[bool]int{false: 0, true: 1}[afterCommit] {
				t.Fatal("wrong surviving catalog state")
			}
			c.before, c.after = nil, nil
			// A new authorized registrar can resolve a retry. It must not replace
			// the principal recorded by a registration whose reply was lost.
			fresh := newRegistrar(t, trustedRegistrar("trusted-host-2"), c, 1)
			receipt, err := fresh.Register(t.Context(), sampleExecution, sampleOwner)
			wantPrincipal := "trusted-host-2"
			if afterCommit {
				wantPrincipal = "trusted-host-1"
			}
			want := Registration{sampleExecution, sampleOwner, wantPrincipal}
			if err != nil || receipt.Duplicate != afterCommit || receipt.Registration != want || c.snapshot()[sampleExecution] != want {
				t.Fatalf("fresh retry=%+v,%v", receipt, err)
			}
		})
	}
}

func TestRegistrationPreservesOwnerAndOriginalAttribution(t *testing.T) {
	r, c := registrationFixture(t)
	first, err := r.Register(t.Context(), sampleExecution, sampleOwner)
	if err != nil || first.Duplicate {
		t.Fatalf("first=%+v,%v", first, err)
	}
	fresh := newRegistrar(t, trustedRegistrar("trusted-host-2"), c, 1)
	if retry, err := fresh.Register(t.Context(), sampleExecution, sampleOwner); err != nil || !retry.Duplicate || retry.Registration != first.Registration {
		t.Fatalf("authorized retry changed attribution=%+v,%v", retry, err)
	}
	for _, owner := range []Owner{{Run: "run-18", Instance: sampleOwner.Instance}, {Run: sampleOwner.Run, Instance: "planner-8"}} {
		if receipt, err := fresh.Register(t.Context(), sampleExecution, owner); !errors.Is(err, ErrOwnershipConflict) || errors.Is(err, ErrRegistrationUncertain) || receipt != (RegistrationReceipt{}) {
			t.Fatalf("changed owner=%+v,%v", receipt, err)
		}
	}
	if c.snapshot()[sampleExecution] != first.Registration || len(c.snapshot()) != 1 {
		t.Fatal("retry or conflict changed original registration")
	}
	// Identical trace/span IDs in a different tenant name a separate execution.
	other := sampleExecution
	other.Tenant = "tenant-b"
	if receipt, err := r.Register(t.Context(), other, sampleOwner); err != nil || receipt.Duplicate || len(c.snapshot()) != 2 {
		t.Fatalf("separate tenant=%+v,%v", receipt, err)
	}
}

func TestRegistrationRequiresTrustedExecutionCreator(t *testing.T) {
	r, c := registrationFixture(t)
	denied := errors.New("not authorized to register this execution and owner")
	var authCalls int
	// This represents an agent with write permission, but no registration grant.
	agent := newRegistrar(t, registrationAuthFunc(func(context.Context, Execution, Owner) (string, error) {
		authCalls++
		return "", denied
	}), c, 1)
	for _, registered := range []bool{false, true} {
		if registered {
			if _, err := r.Register(t.Context(), sampleExecution, sampleOwner); err != nil {
				t.Fatal(err)
			}
		}
		calls := c.calls.Load()
		for _, owner := range []Owner{sampleOwner, {Run: sampleOwner.Run, Instance: "worker-1"}} {
			if receipt, err := agent.Register(t.Context(), sampleExecution, owner); !errors.Is(err, denied) || errors.Is(err, ErrRegistrationUncertain) || receipt != (RegistrationReceipt{}) {
				t.Fatalf("unauthorized claim=%+v,%v", receipt, err)
			}
		}
		if c.calls.Load() != calls {
			t.Fatal("unauthorized registration reached catalog")
		}
	}
	if authCalls != 4 || c.snapshot()[sampleExecution].Owner != sampleOwner {
		t.Fatal("failed authorization leaked admission slot or changed ownership")
	}
}

func TestConcurrentRegistrationsKeepOneOwner(t *testing.T) {
	r, c := registrationFixture(t)
	other := Owner{Run: sampleOwner.Run, Instance: "worker-1"}
	type result struct {
		owner   Owner
		receipt RegistrationReceipt
		err     error
	}
	start := make(chan struct{})
	results := make(chan result, 16)
	var workers sync.WaitGroup
	for range 8 {
		for _, owner := range []Owner{sampleOwner, other} {
			workers.Go(func() {
				<-start
				receipt, err := r.Register(t.Context(), sampleExecution, owner)
				results <- result{owner, receipt, err}
			})
		}
	}
	close(start)
	workers.Wait()
	close(results)
	var winner Owner
	created, duplicates, conflicts := 0, 0, 0
	for result := range results {
		switch {
		case errors.Is(result.err, ErrOwnershipConflict):
			conflicts++
		case result.err != nil:
			t.Fatal(result.err)
		case result.receipt.Duplicate:
			duplicates++
		default:
			created++
			winner = result.owner
		}
	}
	if created != 1 || duplicates != 7 || conflicts != 8 || len(c.snapshot()) != 1 || c.snapshot()[sampleExecution].Owner != winner {
		t.Fatalf("created=%d duplicates=%d conflicts=%d winner=%+v", created, duplicates, conflicts, winner)
	}
	// Both candidates here are privileged registration attempts. An ordinary
	// agent never enters this race; the authorizer must refuse it beforehand.
}

func TestRegistrationAdmissionAndCancellation(t *testing.T) {
	_, c := registrationFixture(t)
	entered := make(chan struct{})
	var once sync.Once
	auth := registrationAuthFunc(func(ctx context.Context, _ Execution, _ Owner) (string, error) {
		blocked := false
		once.Do(func() { close(entered); blocked = true })
		if blocked {
			<-ctx.Done()
		}
		return "trusted-host-1", ctx.Err()
	})
	r := newRegistrar(t, auth, c, 1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := r.Register(ctx, sampleExecution, sampleOwner); done <- err }()
	<-entered
	if _, err := r.Register(t.Context(), sampleExecution, sampleOwner); !errors.Is(err, ErrBusy) {
		t.Fatalf("occupied slot=%v", err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) || errors.Is(err, ErrRegistrationUncertain) || c.calls.Load() != 0 {
		t.Fatalf("canceled authorization=%v", err)
	}
	if _, err := r.Register(t.Context(), sampleExecution, sampleOwner); err != nil {
		t.Fatalf("slot not released=%v", err)
	}
}

func TestRegistrationCancellationAtCatalogBoundary(t *testing.T) {
	for _, boundary := range []string{"after authorization", "catalog returns cancellation", "after confirmed commit"} {
		t.Run(boundary, func(t *testing.T) {
			_, c := registrationFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			auth := trustedRegistrar("trusted-host-1")
			switch boundary {
			case "after authorization":
				auth = func(context.Context, Execution, Owner) (string, error) { cancel(); return "trusted-host-1", nil }
			case "catalog returns cancellation":
				c.before = func(context.Context) error { cancel(); return ctx.Err() }
			case "after confirmed commit":
				c.after = func(context.Context) error { cancel(); return nil }
			}
			r := newRegistrar(t, auth, c, 1)
			receipt, err := r.Register(ctx, sampleExecution, sampleOwner)
			switch boundary {
			case "after authorization":
				if !errors.Is(err, context.Canceled) || errors.Is(err, ErrRegistrationUncertain) || c.calls.Load() != 0 || receipt != (RegistrationReceipt{}) {
					t.Fatalf("canceled before catalog=%+v,%v", receipt, err)
				}
			case "catalog returns cancellation":
				if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrRegistrationUncertain) || receipt != (RegistrationReceipt{}) {
					t.Fatalf("unknown registration=%+v,%v", receipt, err)
				}
			case "after confirmed commit":
				if err != nil || receipt.Registration.Execution != sampleExecution || ctx.Err() == nil {
					t.Fatalf("confirmed registration=%+v,%v", receipt, err)
				}
			}
		})
	}
}

func TestRegistrationRejectsInvalidCatalogResult(t *testing.T) {
	for _, change := range []func(Registration) Registration{
		func(r Registration) Registration { r.Execution.Tenant = "other"; return r },
		func(r Registration) Registration { r.Execution.Trace = "other"; return r },
		func(r Registration) Registration { r.Execution.Span = "other"; return r },
		func(r Registration) Registration { r.Owner.Run = "other"; return r },
		func(r Registration) Registration { r.Owner.Instance = "other"; return r },
		func(r Registration) Registration { r.RegisteredBy = ""; return r },
		func(r Registration) Registration { r.RegisteredBy = strings.Repeat("x", maxIdentityBytes+1); return r },
		func(r Registration) Registration { r.RegisteredBy = "different-creator"; return r },
	} {
		r, c := registrationFixture(t)
		c.changeResult = func(reg Registration, duplicate bool) (Registration, bool) { return change(reg), duplicate }
		if receipt, err := r.Register(t.Context(), sampleExecution, sampleOwner); !errors.Is(err, ErrRegistrationUncertain) || !errors.Is(err, ErrIntegrity) || receipt != (RegistrationReceipt{}) {
			t.Fatalf("invalid catalog result=%+v,%v", receipt, err)
		}
		if c.snapshot()[sampleExecution] != (Registration{sampleExecution, sampleOwner, "trusted-host-1"}) {
			t.Fatal("invalid reply erased actual registration")
		}
	}
}

func TestRegistrationBoundsBeforeCapabilities(t *testing.T) {
	r, c := registrationFixture(t)
	authCalls := 0
	r.authorizer = registrationAuthFunc(func(ctx context.Context, _ Execution, _ Owner) (string, error) {
		authCalls++
		return "trusted-host-1", ctx.Err()
	})
	for part := range 5 {
		for _, bad := range []string{"", strings.Repeat("x", maxIdentityBytes+1)} {
			e, o := sampleExecution, sampleOwner
			fields := []*string{&e.Tenant, &e.Trace, &e.Span, &o.Run, &o.Instance}
			*fields[part] = bad
			if _, err := r.Register(t.Context(), e, o); !errors.Is(err, ErrInvalid) {
				t.Fatalf("invalid registration=%v", err)
			}
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := r.Register(ctx, sampleExecution, sampleOwner); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled=%v", err)
	}
	if authCalls != 0 || c.calls.Load() != 0 {
		t.Fatal("invalid input reached capabilities")
	}
	for _, bad := range []string{"", strings.Repeat("x", maxIdentityBytes+1)} {
		r.authorizer = trustedRegistrar(bad)
		if _, err := r.Register(t.Context(), sampleExecution, sampleOwner); !errors.Is(err, ErrInvalid) || c.calls.Load() != 0 {
			t.Fatalf("invalid authenticated principal=%v", err)
		}
	}
	r.authorizer = trustedRegistrar("trusted-host-1")
	if _, err := r.Register(t.Context(), sampleExecution, sampleOwner); err != nil {
		t.Fatalf("admission slot leaked=%v", err)
	}
}

func TestNewRegistrarRequiresCapabilitiesAndPositiveLimit(t *testing.T) {
	_, c := registrationFixture(t)
	for _, limit := range []int{0, -1} {
		if _, err := NewRegistrar(trustedRegistrar("host"), c, limit); !errors.Is(err, ErrInvalid) {
			t.Fatalf("limit=%d: %v", limit, err)
		}
	}
	if _, err := NewRegistrar(nil, c, 1); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil authorizer=%v", err)
	}
	if _, err := NewRegistrar(trustedRegistrar("host"), nil, 1); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil catalog=%v", err)
	}
}

func TestRegistrationAuthorizesExactExecutionAndOwner(t *testing.T) {
	_, c := registrationFixture(t)
	denied := errors.New("outside creator grant")
	auth := registrationAuthFunc(func(ctx context.Context, e Execution, o Owner) (string, error) {
		if e != sampleExecution || o != sampleOwner {
			return "", denied
		}
		return "trusted-host-1", ctx.Err()
	})
	r := newRegistrar(t, auth, c, 1)
	for field := range 5 {
		e, o := sampleExecution, sampleOwner
		parts := []*string{&e.Tenant, &e.Trace, &e.Span, &o.Run, &o.Instance}
		*parts[field] = "outside-grant"
		if _, err := r.Register(t.Context(), e, o); !errors.Is(err, denied) || c.calls.Load() != 0 {
			t.Fatalf("request outside grant reached catalog=%v", err)
		}
	}
	if receipt, err := r.Register(t.Context(), sampleExecution, sampleOwner); err != nil || receipt.Registration.Owner != sampleOwner || c.calls.Load() != 1 {
		t.Fatalf("authorized exact binding=%+v,%v", receipt, err)
	}
}
