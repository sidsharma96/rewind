// Package ingest coordinates immutable object writes with catalog publication.
// Separate catalog and objectstore packages provide partial backend adapters.
// Controlled-backend tests exercise the protocol, not remote durability.
package ingest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
)

var (
	ErrInvalid       = errors.New("invalid ingest input or configuration")
	ErrTooLarge      = errors.New("record exceeds byte limit")
	ErrBusy          = errors.New("ingest operation limit reached")
	ErrConflict      = errors.New("record identity already has different contents")
	ErrExpired       = errors.New("record expired; retained identity prevents acceptance")
	ErrUncertain     = errors.New("catalog publication outcome unknown; retry the same identity and contents")
	ErrNotFound      = errors.New("record not found in catalog")
	ErrObjectMissing = errors.New("accepted record object missing")
	ErrIntegrity     = errors.New("accepted record reference or contents failed integrity check")
	ErrDenied        = errors.New("record operation not authorized")
)

const maxIdentityBytes = 256

// Kind separates immutable span metadata from one logical capture for that span.
// It is an internal catalog key component, not a wire or persisted enum.
type Kind uint8

const (
	SpanRecord Kind = iota
	CaptureRecord
)

// Identity names a record associated with an execution, not its content. Kind
// distinguishes the span and its capture; it does not create another execution.
// These strings are opaque internal identifiers; their external encoding and
// authentication are not defined here.
type Identity struct {
	Tenant string
	Trace  string
	Span   string
	Kind   Kind
}

// ObjectID scopes content addressing to a tenant. Adapters must not collapse the
// tenant scope or treat a known digest as authorization.
type ObjectID struct {
	Tenant string
	Digest [sha256.Size]byte
}

// Reference is the catalog's accepted identity and immutable object location.
// This is an internal contract, not a persisted or public wire format.
type Reference struct {
	Identity Identity
	Digest   [sha256.Size]byte
	Size     int
}

type Receipt struct {
	Reference   Reference
	Attribution Attribution
	Duplicate   bool
}

// Attribution separates the execution owner from the authenticated submitter.
// It comes from trusted authorization, never Write's caller or telemetry labels.
// The catalog retains the original value separately for each accepted record.
type Attribution struct {
	Owner       Owner
	SubmittedBy string
}

// Authorizer checks exact execution write authority or explicit project-reader
// permission before storage access. Returned identities must come from trusted
// request context and grants. A collector needs explicit authority for the owner.
// The catalog must also check current grants at each operation's atomic boundary.
// Implementations honor cancellation and are safe for concurrent calls.
type Authorizer interface {
	AuthorizeWrite(context.Context, Execution) (Attribution, error)
	AuthorizeRead(context.Context, string) (principal string, err error)
}

// Objects provides immutable content storage. Create succeeds only after complete
// durable bytes are verified, or identical bytes already exist. It must never
// overwrite different contents. Read must bound allocation and transferred bytes
// by maxBytes, returning ErrTooLarge rather than a truncated successful result.
// Missing objects return ErrObjectMissing, not the catalog-only ErrNotFound.
// Implementations honor cancellation, are safe for concurrent calls and neither
// retain nor mutate Create's arguments. Read transfers ownership of its result.
type Objects interface {
	Create(ctx context.Context, id ObjectID, contents []byte) error
	Read(ctx context.Context, id ObjectID, maxBytes int) ([]byte, error)
}

// Catalog enforces current grants and the immutable binding established through
// OwnershipCatalog in the same authoritative state. CheckWrite checks the writer
// and registered owner, then expiry, allowing both absent and live records. It
// reserves nothing. Publish repeats those checks atomically with acceptance,
// before returning duplicate/conflict/expiry outcomes. Both kinds share one owner
// but neither requires the peer record. Reference and original Attribution must
// be committed together. Identical retries return the original attribution even
// when a different authorized submitter retries; legacy unattributed rows fail closed.
// When completion state exists, CheckWrite and Publish must also enforce its fixed
// delivery deadline; a DeliveryWindow alone does not establish that trusted state.
// Lookup checks the explicit project-reader grant before exposing any record state.
// It returns original attribution with Duplicate=false; only absence is ErrNotFound.
// ErrDenied, ErrConflict and ErrExpired confirm refusal of this Publish attempt;
// all other Publish errors may hide a commit. No error proves earlier absence.
// Implementations honor cancellation, are safe for concurrent calls, and must not
// include captured bytes or credentials in errors. See engineering/AUTHORIZATION.md.
type Catalog interface {
	CheckWrite(context.Context, Identity, Attribution) error
	Publish(context.Context, Reference, Attribution) (Receipt, error)
	Lookup(context.Context, Identity, string) (Receipt, error)
}

// Limits is trusted application configuration, not a negotiated service SLA.
// MaxActive includes authorization, object operations and catalog operations.
type Limits struct {
	MaxRecordBytes int
	MaxActive      int
}

// Store has no local accepted-record cache, workers or retry queue. Replacing it
// leaves acceptance state exclusively in the supplied storage capabilities.
type Store struct {
	authorizer Authorizer
	objects    Objects
	catalog    Catalog
	limits     Limits
	active     chan struct{}
}

func New(authorizer Authorizer, objects Objects, catalog Catalog, limits Limits) (*Store, error) {
	if authorizer == nil || objects == nil || catalog == nil || limits.MaxRecordBytes <= 0 || limits.MaxActive <= 0 {
		return nil, ErrInvalid
	}
	return &Store{authorizer, objects, catalog, limits, make(chan struct{}, limits.MaxActive)}, nil
}

// Write accepts one immutable record. The caller must not mutate
// contents until return. Equality is exact opaque bytes for this experiment;
// the typed schema/encoding needed for production equality remains undesigned.
// Either kind may arrive first; no capture-body validation is performed.
// Do not expose this primitive
// directly as an attachment API. An error is never proof that a record with this
// identity was not accepted by an earlier attempt. A preliminary lookup can refuse
// work before upload, but only final publication decides acceptance.
func (s *Store) Write(ctx context.Context, id Identity, contents []byte) (Receipt, error) {
	if len(contents) > s.limits.MaxRecordBytes {
		return Receipt{}, ErrTooLarge
	}
	if err := s.begin(ctx, id); err != nil {
		return Receipt{}, err
	}
	defer s.end()

	writer, err := s.authorizer.AuthorizeWrite(ctx, executionOf(id))
	if err != nil {
		return Receipt{}, fmt.Errorf("authorize record writer: %w", err)
	}
	if !validAttribution(writer) {
		return Receipt{}, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	if err := s.catalog.CheckWrite(ctx, id, writer); err != nil {
		return Receipt{}, fmt.Errorf("check record before upload: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	// The check reserves nothing: publication rechecks authority, expiry and conflicts.
	owned := bytes.Clone(contents)
	ref := Reference{Identity: id, Digest: sha256.Sum256(owned), Size: len(owned)}
	object := ObjectID{Tenant: id.Tenant, Digest: ref.Digest}
	if err := s.objects.Create(ctx, object, owned); err != nil {
		return Receipt{}, fmt.Errorf("create immutable record object: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	receipt, err := s.catalog.Publish(ctx, ref, writer)
	if err != nil {
		if errors.Is(err, ErrConflict) || errors.Is(err, ErrExpired) || errors.Is(err, ErrDenied) {
			return Receipt{}, fmt.Errorf("publish record: %w", err)
		}
		return Receipt{}, fmt.Errorf("publish record: %w", errors.Join(ErrUncertain, err))
	}
	if receipt.Reference != ref || !validAttribution(receipt.Attribution) || receipt.Attribution.Owner != writer.Owner || (!receipt.Duplicate && receipt.Attribution != writer) {
		return Receipt{}, fmt.Errorf("publish record: %w", errors.Join(ErrUncertain, ErrIntegrity))
	}
	// Once publication is confirmed, later cancellation cannot undo acceptance.
	return receipt, nil
}

// Read resolves one known record. It does not enumerate a trace, join span and
// capture, or establish replay readiness. Catalog errors, unavailable objects and
// integrity failures must not be treated as record absence.
func (s *Store) Read(ctx context.Context, id Identity) ([]byte, error) {
	if err := s.begin(ctx, id); err != nil {
		return nil, err
	}
	defer s.end()

	reader, err := s.authorizer.AuthorizeRead(ctx, id.Tenant)
	if err != nil {
		return nil, fmt.Errorf("authorize project reader: %w", err)
	}
	if !validRegistrationID(reader) {
		return nil, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	receipt, err := s.catalog.Lookup(ctx, id, reader)
	if err != nil {
		return nil, fmt.Errorf("lookup record: %w", err)
	}
	ref := receipt.Reference
	if ref.Identity != id || ref.Size < 0 || !validAttribution(receipt.Attribution) || receipt.Duplicate {
		return nil, ErrIntegrity
	}
	if ref.Size > s.limits.MaxRecordBytes {
		return nil, ErrTooLarge
	}
	contents, err := s.objects.Read(ctx, ObjectID{Tenant: id.Tenant, Digest: ref.Digest}, ref.Size)
	if err != nil {
		return nil, fmt.Errorf("read accepted record object: %w", err)
	}
	if len(contents) != ref.Size || sha256.Sum256(contents) != ref.Digest {
		return nil, ErrIntegrity
	}
	return contents, nil
}

func (s *Store) begin(ctx context.Context, id Identity) error {
	if id.Kind != SpanRecord && id.Kind != CaptureRecord {
		return ErrInvalid
	}
	for _, part := range []string{id.Tenant, id.Trace, id.Span} {
		if part == "" || len(part) > maxIdentityBytes {
			return ErrInvalid
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case s.active <- struct{}{}:
	default:
		return ErrBusy
	}
	return nil
}

func (s *Store) end() { <-s.active }

func executionOf(id Identity) Execution {
	return Execution{Tenant: id.Tenant, Trace: id.Trace, Span: id.Span}
}

func validAttribution(a Attribution) bool {
	return validRegistrationID(a.Owner.Run) && validRegistrationID(a.Owner.Instance) && validRegistrationID(a.SubmittedBy)
}
