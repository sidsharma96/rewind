package producer_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/sidsharma96/rewind/internal/ingest"
	"github.com/sidsharma96/rewind/internal/producer"
	"github.com/sidsharma96/rewind/internal/traceid"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// This controlled capability tests the helper's ID and retry mechanics. Native
// catalog/access integration tests establish actual credential and grant checks.
type registrations struct {
	mu        sync.Mutex
	principal string
	checks    int
	proposals []ingest.Registration
	saved     map[ingest.Execution]ingest.Registration
	loseReply bool
}

func (r *registrations) AuthorizeRegistration(_ context.Context, _ ingest.Execution, _ ingest.Owner) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.checks++
	return r.principal, nil
}

func (r *registrations) RegisterOwnership(_ context.Context, proposed ingest.Registration) (ingest.Registration, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.proposals = append(r.proposals, proposed)
	if original, ok := r.saved[proposed.Execution]; ok {
		if original.Owner != proposed.Owner {
			return ingest.Registration{}, false, ingest.ErrOwnershipConflict
		}
		return original, true, nil
	}
	r.saved[proposed.Execution] = proposed
	if r.loseReply {
		r.loseReply = false
		return ingest.Registration{}, false, errors.New("discarded committed registration reply")
	}
	return proposed, false, nil
}

func registrar(t *testing.T) (*ingest.Registrar, *registrations) {
	t.Helper()
	state := &registrations{principal: "trusted-host", saved: make(map[ingest.Execution]ingest.Registration)}
	r, err := ingest.NewRegistrar(state, state, 1)
	if err != nil {
		t.Fatal(err)
	}
	return r, state
}

func tracer(t *testing.T) trace.Tracer {
	t.Helper()
	p := sdktrace.NewTracerProvider()
	t.Cleanup(func() {
		if err := p.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return p.Tracer("rewind-producer-test")
}

func execution(project string, sc trace.SpanContext) ingest.Execution {
	return ingest.Execution{Tenant: project, Trace: traceid.Trace(sc.TraceID()), Span: traceid.Span(sc.SpanID())}
}

func TestHostRegistersActualSDKRootAndChildIDs(t *testing.T) {
	r, state := registrar(t)
	assignedOwner := ingest.Owner{Run: "run-one", Instance: "bob-one"}
	wantOwner := assignedOwner
	h, err := producer.NewHost(r, "assigned-project", assignedOwner)
	if err != nil {
		t.Fatal(err)
	}
	// Reassigning the caller's local owner does not reconfigure the trusted host.
	assignedOwner.Run = "other-run"
	assignedOwner.Instance = "other-instance"
	tr := tracer(t)
	parent, root := tr.Start(t.Context(), "handoff")
	defer root.End()
	_, child := tr.Start(parent, "calculate")
	defer child.End()
	rootID, childID := root.SpanContext(), child.SpanContext()
	if !rootID.IsValid() || !childID.IsValid() || rootID.TraceID() != childID.TraceID() || rootID.SpanID() == childID.SpanID() {
		t.Fatal("SDK did not produce distinct executions in the same trace")
	}
	for _, sc := range []trace.SpanContext{rootID, childID} {
		got, err := h.Register(t.Context(), sc)
		want := ingest.Registration{Execution: execution("assigned-project", sc), Owner: wantOwner, RegisteredBy: "trusted-host"}
		if err != nil || got.Duplicate || got.Registration != want {
			t.Fatalf("SDK registration = %+v %v; want %+v", got, err, want)
		}
	}
	if !root.SpanContext().Equal(rootID) || !child.SpanContext().Equal(childID) {
		t.Fatal("registration changed the SDK span contexts")
	}
	if len(state.saved) != 2 || len(state.proposals) != 2 {
		t.Fatal("registration did not preserve two independent SDK executions")
	}
}

func TestHostUncertainRegistrationRetriesSameSDKIDs(t *testing.T) {
	r, state := registrar(t)
	state.loseReply = true
	owner := ingest.Owner{Run: "run-one", Instance: "alice-one"}
	h, err := producer.NewHost(r, "project", owner)
	if err != nil {
		t.Fatal(err)
	}
	_, span := tracer(t).Start(t.Context(), "handoff")
	defer span.End()
	sc := span.SpanContext()
	got, err := h.Register(t.Context(), sc)
	if !errors.Is(err, ingest.ErrRegistrationUncertain) || got != (ingest.RegistrationReceipt{}) {
		t.Fatalf("lost registration reply = %+v %v", got, err)
	}
	want := ingest.Registration{Execution: execution("project", sc), Owner: owner, RegisteredBy: "trusted-host"}
	if state.saved[want.Execution] != want {
		t.Fatal("controlled lost reply did not retain the original binding")
	}
	state.mu.Lock()
	state.principal = "replacement-host"
	state.mu.Unlock()
	got, err = h.Register(t.Context(), sc)
	if err != nil || !got.Duplicate || got.Registration != want {
		t.Fatalf("uncertain retry = %+v %v; want original %+v", got, err, want)
	}
	if len(state.saved) != 1 || len(state.proposals) != 2 || state.proposals[0].Execution != state.proposals[1].Execution || state.proposals[1].RegisteredBy != "replacement-host" {
		t.Fatal("retry changed execution identity or bypassed current authorization")
	}
}

func TestHostRejectsInvalidContextsAndZeroHostBeforeCatalog(t *testing.T) {
	r, state := registrar(t)
	h, err := producer.NewHost(r, "project", ingest.Owner{Run: "run", Instance: "instance"})
	if err != nil {
		t.Fatal(err)
	}
	_, span := tracer(t).Start(t.Context(), "valid")
	defer span.End()
	valid := span.SpanContext()
	for _, sc := range []trace.SpanContext{
		{},
		trace.NewSpanContext(trace.SpanContextConfig{TraceID: valid.TraceID()}),
		trace.NewSpanContext(trace.SpanContextConfig{SpanID: valid.SpanID()}),
	} {
		if got, err := h.Register(t.Context(), sc); !errors.Is(err, ingest.ErrInvalid) || got != (ingest.RegistrationReceipt{}) {
			t.Fatalf("invalid span context = %+v %v", got, err)
		}
	}
	for _, empty := range []*producer.Host{nil, {}} {
		if got, err := empty.Register(t.Context(), valid); !errors.Is(err, ingest.ErrInvalid) || got != (ingest.RegistrationReceipt{}) {
			t.Fatalf("zero host = %+v %v", got, err)
		}
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := h.Register(canceled, valid); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled registration = %v", err)
	}
	if state.checks != 0 || len(state.proposals) != 0 {
		t.Fatal("invalid or canceled input reached authorization/catalog")
	}
}

func TestNewHostRejectsInvalidConfigurationBeforeCatalog(t *testing.T) {
	r, state := registrar(t)
	owner := ingest.Owner{Run: "run", Instance: "instance"}
	tooLong := strings.Repeat("x", 257)
	for _, test := range []struct {
		name    string
		r       *ingest.Registrar
		project string
		owner   ingest.Owner
	}{
		{"nil registrar", nil, "project", owner},
		{"empty project", r, "", owner},
		{"long project", r, tooLong, owner},
		{"empty run", r, "project", ingest.Owner{Instance: owner.Instance}},
		{"long run", r, "project", ingest.Owner{Run: tooLong, Instance: owner.Instance}},
		{"empty instance", r, "project", ingest.Owner{Run: owner.Run}},
		{"long instance", r, "project", ingest.Owner{Run: owner.Run, Instance: tooLong}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if h, err := producer.NewHost(test.r, test.project, test.owner); h != nil || !errors.Is(err, ingest.ErrInvalid) {
				t.Fatalf("invalid host configuration = %v %v", h, err)
			}
		})
	}
	if state.checks != 0 || len(state.proposals) != 0 {
		t.Fatal("invalid constructor configuration reached authorization/catalog")
	}
}
