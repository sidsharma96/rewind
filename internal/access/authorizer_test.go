package access

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/sidsharma96/rewind/internal/ingest"
	"github.com/sidsharma96/rewind/internal/traceid"
)

type permissionCall struct {
	operation string
	principal string
	project   string
	execution ingest.Execution
	owner     ingest.Owner
}

type permissionProbe struct {
	calls  []permissionCall
	writer ingest.Attribution
	err    error
}

func (p *permissionProbe) WriterFor(_ context.Context, execution ingest.Execution, principal string) (ingest.Attribution, error) {
	p.calls = append(p.calls, permissionCall{operation: "write", principal: principal, execution: execution})
	return p.writer, p.err
}

func (p *permissionProbe) CheckReader(_ context.Context, project, principal string) error {
	p.calls = append(p.calls, permissionCall{operation: "read", principal: principal, project: project})
	return p.err
}

func (p *permissionProbe) CheckCreator(_ context.Context, execution ingest.Execution, owner ingest.Owner, principal string) error {
	p.calls = append(p.calls, permissionCall{operation: "register", principal: principal, execution: execution, owner: owner})
	return p.err
}

type authorizerFixture struct {
	now         time.Time
	expires     time.Time
	ctx         context.Context
	execution   ingest.Execution
	owner       ingest.Owner
	permissions *permissionProbe
	authorizer  *Authorizer
}

func newAuthorizerFixture(t *testing.T) *authorizerFixture {
	t.Helper()
	f := &authorizerFixture{
		now: time.Unix(100, 0), expires: time.Unix(200, 0),
		execution: ingest.Execution{Tenant: "project", Trace: traceid.Trace{1}, Span: traceid.Span{2}},
		owner:     ingest.Owner{Run: "run", Instance: "alice-instance"},
	}
	f.permissions = &permissionProbe{writer: ingest.Attribution{Owner: f.owner, SubmittedBy: "collector"}}
	var err error
	f.authorizer, err = NewAuthorizer(f.permissions)
	if err != nil {
		t.Fatal(err)
	}
	token := strings.Repeat("a", 64)
	credentials, err := NewCredentials(1, []Credential{credentialFixture(token, "collector", f.expires)}, func() time.Time { return f.now })
	if err != nil {
		t.Fatal(err)
	}
	f.ctx, err = credentials.Authenticate(context.Background(), token)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestAuthorizerUsesVerifiedPrincipalAndExactScope(t *testing.T) {
	f := newAuthorizerFixture(t)
	writer, err := f.authorizer.AuthorizeWrite(f.ctx, f.execution)
	if err != nil || writer != f.permissions.writer {
		t.Fatal("verified writer authority changed")
	}
	reader, err := f.authorizer.AuthorizeRead(f.ctx, f.execution.Tenant)
	if err != nil || reader != "collector" {
		t.Fatal("verified reader principal changed")
	}
	registrar, err := f.authorizer.AuthorizeRegistration(f.ctx, f.execution, f.owner)
	if err != nil || registrar != "collector" {
		t.Fatal("verified registration principal changed")
	}
	want := []permissionCall{
		{operation: "write", principal: "collector", execution: f.execution},
		{operation: "read", principal: "collector", project: f.execution.Tenant},
		{operation: "register", principal: "collector", execution: f.execution, owner: f.owner},
	}
	if len(f.permissions.calls) != len(want) {
		t.Fatal("protected operations skipped their current permission checks")
	}
	for i, call := range f.permissions.calls {
		if call != want[i] {
			t.Fatal("permission check used a claimed identity or different scope")
		}
	}
	// A later policy refusal must be queried again, not hidden by a previous
	// successful authentication or operation for this same principal.
	f.permissions.err = ingest.ErrDenied
	if got, err := f.authorizer.AuthorizeWrite(f.ctx, f.execution); got != (ingest.Attribution{}) || !errors.Is(err, ingest.ErrDenied) || len(f.permissions.calls) != 4 {
		t.Fatal("authorizer cached an earlier permission")
	}
}

func TestAuthorizerUnverifiedExpiredAndCanceledCannotReachPermissions(t *testing.T) {
	type requestLabel string
	for _, tc := range []string{"nil", "plain", "claimed principal", "expired", "canceled", "deadline"} {
		t.Run(tc, func(t *testing.T) {
			f := newAuthorizerFixture(t)
			ctx := f.ctx
			want := ErrUnauthenticated
			switch tc {
			case "nil":
				ctx = nil
			case "plain":
				ctx = context.Background()
			case "claimed principal":
				ctx = context.WithValue(context.Background(), requestLabel("principal"), "collector")
			case "expired":
				f.now = f.expires
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				want = context.Canceled
			case "deadline":
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(ctx, time.Unix(1, 0))
				defer cancel()
				want = context.DeadlineExceeded
			}
			if got, err := f.authorizer.AuthorizeWrite(ctx, f.execution); got != (ingest.Attribution{}) || !errors.Is(err, want) {
				t.Fatal("unverified write did not fail without authority")
			}
			if got, err := f.authorizer.AuthorizeRead(ctx, f.execution.Tenant); got != "" || !errors.Is(err, want) {
				t.Fatal("unverified read did not fail without authority")
			}
			if got, err := f.authorizer.AuthorizeRegistration(ctx, f.execution, f.owner); got != "" || !errors.Is(err, want) {
				t.Fatal("unverified registration did not fail without authority")
			}
			if len(f.permissions.calls) != 0 {
				t.Fatal("unverified identity reached current permission queries")
			}
		})
	}
}

func TestAuthorizerRejectsMalformedWriterAttribution(t *testing.T) {
	for _, tc := range []struct {
		name   string
		writer ingest.Attribution
	}{
		{"wrong submitter", ingest.Attribution{Owner: ingest.Owner{Run: "run", Instance: "alice"}, SubmittedBy: "other-principal"}},
		{"missing submitter", ingest.Attribution{Owner: ingest.Owner{Run: "run", Instance: "alice"}}},
		{"missing run", ingest.Attribution{Owner: ingest.Owner{Instance: "alice"}, SubmittedBy: "collector"}},
		{"missing instance", ingest.Attribution{Owner: ingest.Owner{Run: "run"}, SubmittedBy: "collector"}},
		{"oversized run", ingest.Attribution{Owner: ingest.Owner{Run: strings.Repeat("r", 257), Instance: "alice"}, SubmittedBy: "collector"}},
		{"oversized instance", ingest.Attribution{Owner: ingest.Owner{Run: "run", Instance: strings.Repeat("界", 86)}, SubmittedBy: "collector"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newAuthorizerFixture(t)
			f.permissions.writer = tc.writer
			got, err := f.authorizer.AuthorizeWrite(f.ctx, f.execution)
			if got != (ingest.Attribution{}) || !errors.Is(err, ingest.ErrIntegrity) || errors.Is(err, ingest.ErrDenied) || len(f.permissions.calls) != 1 {
				t.Fatal("malformed permission response became usable authority or a fabricated refusal")
			}
		})
	}
}

func TestAuthorizerPreservesCancellationDuringEvidenceCheck(t *testing.T) {
	f := newAuthorizerFixture(t)
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	evidence := ctx.Value(credentialKey{}).(authenticatedPrincipal)
	evidence.now = func() time.Time {
		cancel()
		return f.now
	}
	ctx = context.WithValue(ctx, credentialKey{}, evidence)
	if got, err := f.authorizer.AuthorizeWrite(ctx, f.execution); got != (ingest.Attribution{}) || !errors.Is(err, context.Canceled) || len(f.permissions.calls) != 0 {
		t.Fatal("cancellation during evidence check became identity refusal or policy access")
	}
}

func TestAuthorizerPreservesPermissionFailures(t *testing.T) {
	unavailable := errors.New("controlled policy service unavailable")
	for _, failure := range []error{ingest.ErrDenied, unavailable, context.Canceled, context.DeadlineExceeded} {
		f := newAuthorizerFixture(t)
		f.permissions.err = failure
		if got, err := f.authorizer.AuthorizeWrite(f.ctx, f.execution); got != (ingest.Attribution{}) || !errors.Is(err, failure) || errors.Is(err, ingest.ErrDenied) != errors.Is(failure, ingest.ErrDenied) {
			t.Fatal("write changed a permission failure or leaked partial authority")
		}
		if got, err := f.authorizer.AuthorizeRead(f.ctx, f.execution.Tenant); got != "" || !errors.Is(err, failure) || errors.Is(err, ingest.ErrDenied) != errors.Is(failure, ingest.ErrDenied) {
			t.Fatal("read changed a permission failure or leaked partial authority")
		}
		if got, err := f.authorizer.AuthorizeRegistration(f.ctx, f.execution, f.owner); got != "" || !errors.Is(err, failure) || errors.Is(err, ingest.ErrDenied) != errors.Is(failure, ingest.ErrDenied) {
			t.Fatal("registration changed a permission failure or leaked partial authority")
		}
		if len(f.permissions.calls) != 3 {
			t.Fatal("failure test did not reach each permission operation")
		}
	}
	if got, err := NewAuthorizer(nil); got != nil || !errors.Is(err, ErrInvalidConfig) {
		t.Fatal("missing permission provider was accepted")
	}
}
