package access

import (
	"context"

	"github.com/sidsharma96/rewind/internal/ingest"
)

// Permissions is the current catalog policy needed by authenticated requests.
// Implementations check exact principals, not telemetry labels. These preliminary
// checks grant no acceptance: registration, publication and lookup must repeat
// their current policy checks at their own authoritative operation boundaries.
type Permissions interface {
	WriterFor(context.Context, ingest.Execution, string) (ingest.Attribution, error)
	CheckReader(context.Context, string, string) error
	CheckCreator(context.Context, ingest.Execution, ingest.Owner, string) error
}

// Authorizer joins verified credential context to current catalog permissions.
// It has no identity injection method, policy cache, worker or credential store.
// Credentials and Sessions alone construct its private identity contexts.
type Authorizer struct{ permissions Permissions }

var _ ingest.Authorizer = (*Authorizer)(nil)
var _ ingest.RegistrationAuthorizer = (*Authorizer)(nil)

func NewAuthorizer(permissions Permissions) (*Authorizer, error) {
	if permissions == nil {
		return nil, ErrInvalidConfig
	}
	return &Authorizer{permissions: permissions}, nil
}

func verifiedPrincipal(ctx context.Context) (string, error) {
	if ctx == nil {
		return "", ErrUnauthenticated
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	p, ok := principal(ctx)
	if !ok {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		return "", ErrUnauthenticated
	}
	return p, nil
}

func (a *Authorizer) AuthorizeWrite(ctx context.Context, execution ingest.Execution) (ingest.Attribution, error) {
	p, err := verifiedPrincipal(ctx)
	if err != nil {
		return ingest.Attribution{}, err
	}
	if identity, ok := ctx.Value(credentialKey{}).(authenticatedPrincipal); !ok || identity.readOnly {
		return ingest.Attribution{}, ingest.ErrDenied
	}
	writer, err := a.permissions.WriterFor(ctx, execution, p)
	if err != nil {
		return ingest.Attribution{}, err
	}
	if writer.SubmittedBy != p || !boundedIdentity(writer.Owner.Run) || !boundedIdentity(writer.Owner.Instance) {
		return ingest.Attribution{}, ingest.ErrIntegrity
	}
	return writer, nil
}

func (a *Authorizer) AuthorizeRead(ctx context.Context, project string) (string, error) {
	p, err := verifiedPrincipal(ctx)
	if err != nil {
		return "", err
	}
	if err := a.permissions.CheckReader(ctx, project, p); err != nil {
		return "", err
	}
	return p, nil
}

func (a *Authorizer) AuthorizeRegistration(ctx context.Context, execution ingest.Execution, owner ingest.Owner) (string, error) {
	p, err := verifiedPrincipal(ctx)
	if err != nil {
		return "", err
	}
	if identity, ok := ctx.Value(credentialKey{}).(authenticatedPrincipal); !ok || identity.readOnly {
		return "", ingest.ErrDenied
	}
	if err := a.permissions.CheckCreator(ctx, execution, owner, p); err != nil {
		return "", err
	}
	return p, nil
}

func boundedIdentity(s string) bool { return len(s) > 0 && len(s) <= 256 }
