package ingest

import (
	"context"
	"errors"
	"fmt"
)

var (
	ErrOwnershipConflict     = errors.New("execution already registered to a different owner")
	ErrRegistrationUncertain = errors.New("ownership registration outcome unknown; retry the same execution and owner")
)

// Execution names the call shared by span and capture records, without a Kind.
// IDs are opaque internal values, not a public format or validated OTLP encoding.
type Execution struct {
	Tenant string
	Trace  string
	Span   string
}

// Owner identifies one agent instance within one run. Its project scope comes
// from Execution.Tenant. A stable agent name does not confer this authority.
type Owner struct {
	Run      string
	Instance string
}

// Registration binds both record kinds to the same owner. RegisteredBy is the
// authenticated principal that originally established the binding, not the owner
// label or credentials. It must survive duplicate deliveries unchanged.
type Registration struct {
	Execution    Execution
	Owner        Owner
	RegisteredBy string
}

type RegistrationReceipt struct {
	Registration Registration
	Duplicate    bool
}

// RegistrationAuthorizer authorizes a trusted execution creator to bind this
// exact execution to this owner, returning its authenticated principal identity.
// Authorizing ordinary agent writes or only tenant membership is insufficient.
// Implementations must obtain authority from trusted request context, not labels,
// honor cancellation and be safe for concurrent calls. No authentication is
// implemented here; controlled tests supply this capability.
type RegistrationAuthorizer interface {
	AuthorizeRegistration(context.Context, Execution, Owner) (principal string, err error)
}

// OwnershipCatalog atomically creates an immutable execution-to-owner binding or
// returns the original registration for an identical execution/owner retry. A
// retry by another authorized registrar preserves the original RegisteredBy.
// Different owners return ErrOwnershipConflict without changing the original.
// All other errors may hide a committed registration. The adapter must enforce
// applicable authoritative registration permissions before returning an outcome.
// Success requires durable recovery of the binding and original attribution.
// Implementations honor cancellation, are safe for concurrent calls and must not
// include credentials in errors. This is not yet wired into record publication.
type OwnershipCatalog interface {
	RegisterOwnership(context.Context, Registration) (original Registration, duplicate bool, err error)
}

// Registrar coordinates one ownership registration, not record acceptance. It has
// no local bindings, workers, queues, retries or deletion. Replacing it retains
// only state in its catalog. Its active limit is independent of Store's limit:
// combined catalog concurrency can reach the sum of both configured limits.
type Registrar struct {
	authorizer RegistrationAuthorizer
	catalog    OwnershipCatalog
	active     chan struct{}
}

func NewRegistrar(authorizer RegistrationAuthorizer, catalog OwnershipCatalog, maxActive int) (*Registrar, error) {
	if authorizer == nil || catalog == nil || maxActive <= 0 {
		return nil, ErrInvalid
	}
	return &Registrar{authorizer: authorizer, catalog: catalog, active: make(chan struct{}, maxActive)}, nil
}

// Register establishes ownership before either record kind arrives. It does not
// upload bytes, publish span metadata or enforce ownership in the tenant-only
// Store. An error is not proof that an earlier attempt did not register the call.
func (r *Registrar) Register(ctx context.Context, execution Execution, owner Owner) (RegistrationReceipt, error) {
	for _, part := range []string{execution.Tenant, execution.Trace, execution.Span, owner.Run, owner.Instance} {
		if !validRegistrationID(part) {
			return RegistrationReceipt{}, ErrInvalid
		}
	}
	if err := ctx.Err(); err != nil {
		return RegistrationReceipt{}, err
	}
	select {
	case r.active <- struct{}{}:
	default:
		return RegistrationReceipt{}, ErrBusy
	}
	defer func() { <-r.active }()

	principal, err := r.authorizer.AuthorizeRegistration(ctx, execution, owner)
	if err != nil {
		return RegistrationReceipt{}, fmt.Errorf("authorize ownership registration: %w", err)
	}
	if !validRegistrationID(principal) {
		return RegistrationReceipt{}, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return RegistrationReceipt{}, err
	}
	proposed := Registration{Execution: execution, Owner: owner, RegisteredBy: principal}
	original, duplicate, err := r.catalog.RegisterOwnership(ctx, proposed)
	if err != nil {
		if errors.Is(err, ErrOwnershipConflict) {
			return RegistrationReceipt{}, fmt.Errorf("register execution owner: %w", err)
		}
		return RegistrationReceipt{}, fmt.Errorf("register execution owner: %w", errors.Join(ErrRegistrationUncertain, err))
	}
	if original.Execution != execution || original.Owner != owner || !validRegistrationID(original.RegisteredBy) || (!duplicate && original != proposed) {
		return RegistrationReceipt{}, fmt.Errorf("register execution owner: %w", errors.Join(ErrRegistrationUncertain, ErrIntegrity))
	}
	// Confirmed registration remains success after cancellation, like publication.
	return RegistrationReceipt{Registration: original, Duplicate: duplicate}, nil
}

func validRegistrationID(id string) bool { return id != "" && len(id) <= maxIdentityBytes }
