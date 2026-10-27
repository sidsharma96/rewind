// Package ingest coordinates immutable object writes with catalog publication.
// It has no network transport or PostgreSQL/R2 adapter. Its controlled-backend
// tests exercise the protocol, not remote durability.
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
	Reference Reference
	Duplicate bool
}

// Authorizer must bind the requested tenant to trusted request identity.
// The coordinator calls it before accessing either storage capability.
// Implementations must honor cancellation and be safe for concurrent calls.
type Authorizer interface {
	Authorize(context.Context, string) error
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

// Catalog atomically publishes one record with its lookup reference. Kind is part
// of identity: accepting capture must not replace or conflict with span metadata.
// Either kind may be accepted first; publication must not require the peer record.
// This primitive does not authorize capture ownership.
// Publish returns duplicate=true only for an identical accepted reference. ErrConflict
// and ErrExpired mean this publication was refused without modifying the original.
// A retained expired identity refuses both identical and changed contents. Lookup
// returns ErrExpired while retaining that identity, not ErrNotFound. Every other
// Publish error may hide a committed transaction. Lookup reads authoritative committed
// state; only absence returns ErrNotFound. Implementations honor cancellation and
// are safe for concurrent calls. They must not include captured bytes in errors.
type Catalog interface {
	Publish(context.Context, Reference) (duplicate bool, err error)
	Lookup(context.Context, Identity) (Reference, error)
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

	if _, err := s.catalog.Lookup(ctx, id); err != nil && !errors.Is(err, ErrNotFound) {
		return Receipt{}, fmt.Errorf("check record before upload: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	// The lookup reserves nothing: publication must recheck expiry and conflicts.
	owned := bytes.Clone(contents)
	ref := Reference{Identity: id, Digest: sha256.Sum256(owned), Size: len(owned)}
	object := ObjectID{Tenant: id.Tenant, Digest: ref.Digest}
	if err := s.objects.Create(ctx, object, owned); err != nil {
		return Receipt{}, fmt.Errorf("create immutable record object: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	duplicate, err := s.catalog.Publish(ctx, ref)
	if err != nil {
		if errors.Is(err, ErrConflict) || errors.Is(err, ErrExpired) {
			return Receipt{}, fmt.Errorf("publish record: %w", err)
		}
		return Receipt{}, fmt.Errorf("publish record: %w", errors.Join(ErrUncertain, err))
	}
	// Once publication is confirmed, later cancellation cannot undo acceptance.
	return Receipt{Reference: ref, Duplicate: duplicate}, nil
}

// Read resolves one known record. It does not enumerate a trace, join span and
// capture, or establish replay readiness. Catalog errors, unavailable objects and
// integrity failures must not be treated as record absence.
func (s *Store) Read(ctx context.Context, id Identity) ([]byte, error) {
	if err := s.begin(ctx, id); err != nil {
		return nil, err
	}
	defer s.end()

	ref, err := s.catalog.Lookup(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("lookup record: %w", err)
	}
	if ref.Identity != id || ref.Size < 0 {
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
	if err := s.authorizer.Authorize(ctx, id.Tenant); err != nil {
		s.end()
		return fmt.Errorf("authorize tenant: %w", err)
	}
	return nil
}

func (s *Store) end() { <-s.active }
