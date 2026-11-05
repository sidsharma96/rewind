package ingest

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/sidsharma96/rewind/internal/record"
)

// SpanCatalog is a controlled coordinator capability, never a producer API.
// InspectSpan checks current writer/binding/run/expiry state and returns an
// original receipt (Duplicate=false), or found=false and a zero receipt. It grants
// no project-reader access. PublishSpan creates an absent record or returns its
// original duplicate; compare=true means a different reference still needs typed
// comparison, NOT acceptance of this retry. No object I/O occurs under its locks.
//
// ResolveSpan rechecks the same current authority, eligibility, saved expiry and
// exact accepted reference under ordinary publication locks before consuming the
// trusted coordinator's comparison result. A changed/missing reference is an
// integrity error. Equal returns the original duplicate; different conflicts.
// Read/parse/cleanup errors must never be passed as a comparison. No operation
// changes an existing reference, attribution or times. Publication errors may hide
// a commit, as with Catalog.Publish. Calls honor cancellation and are concurrent-safe.
type SpanCatalog interface {
	Catalog
	InspectSpan(context.Context, Identity, Attribution) (original Receipt, found bool, err error)
	PublishSpan(context.Context, Reference, Attribution) (original Receipt, compare bool, err error)
	ResolveSpan(context.Context, Reference, Attribution, bool) (Receipt, error)
}

// Spans joins typed final-span preparation to the existing storage protocol. One
// Store admission slot covers each complete call, including graph work, comparison
// and decoding. There are no workers, caches or independent preparation gates.
// Its authorizer remains a trusted capability; real credential verification is
// outside this internal coordinator. Known-record reads do not enumerate a trace.
type Spans struct {
	store   *Store
	catalog SpanCatalog
	codec   record.SpanCodec
}

func NewSpans(authorizer Authorizer, objects Objects, catalog SpanCatalog, limits Limits, codec record.SpanCodec) (*Spans, error) {
	if !codec.Configured() {
		return nil, ErrInvalid
	}
	store, err := New(authorizer, objects, catalog, limits)
	if err != nil {
		return nil, err
	}
	return &Spans{store: store, catalog: catalog, codec: codec}, nil
}

// WriteSpan accepts the explicit ended declaration, including an ended failure
// or unknown name/time. Every reachable input stays stable until return. The
// expected execution must match metadata; labels never provide write authority.
// Success preserves the original acceptance even for differently encoded but
// equivalent metadata. A conflicting retry never changes the original record.
func (s *Spans) WriteSpan(ctx context.Context, execution Execution, ended record.EndedSpan) (Receipt, error) {
	id := spanIdentity(execution)
	if err := s.store.begin(ctx, id); err != nil {
		return Receipt{}, err
	}
	defer s.store.end()
	view, err := ended.View()
	if err != nil {
		return Receipt{}, err
	}
	if !matchesExecution(view, execution) {
		return Receipt{}, ErrInvalid
	}
	writer, err := s.store.authorizer.AuthorizeWrite(ctx, execution)
	if err != nil {
		return Receipt{}, fmt.Errorf("authorize span writer: %w", err)
	}
	if !validAttribution(writer) {
		return Receipt{}, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	original, found, err := s.catalog.InspectSpan(ctx, id, writer)
	if err != nil {
		return Receipt{}, fmt.Errorf("inspect span before preparation: %w", err)
	}
	if (!found && original != (Receipt{})) || (found && (!validOriginal(original, id, writer) || original.Duplicate)) {
		return Receipt{}, ErrIntegrity
	}
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	prepared, stored, err := s.codec.Prepare(ended)
	if err != nil {
		return Receipt{}, fmt.Errorf("prepare ended span: %w", preparationError(err))
	}
	if !matchesExecution(prepared, execution) {
		return Receipt{}, ErrInvalid
	}
	if len(stored) > s.store.limits.MaxRecordBytes {
		return Receipt{}, ErrTooLarge
	}
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	ref := Reference{Identity: id, Digest: sha256.Sum256(stored), Size: len(stored)}
	if found {
		return s.resolve(ctx, original, writer, ref, prepared)
	}
	if err := s.store.objects.Create(ctx, ObjectID{Tenant: id.Tenant, Digest: ref.Digest}, stored); err != nil {
		return Receipt{}, fmt.Errorf("create span object: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	accepted, compare, err := s.catalog.PublishSpan(ctx, ref, writer)
	if err != nil {
		if errors.Is(err, ErrConflict) || errors.Is(err, ErrExpired) || errors.Is(err, ErrDenied) {
			return Receipt{}, fmt.Errorf("publish span: %w", err)
		}
		return Receipt{}, fmt.Errorf("publish span: %w", errors.Join(ErrUncertain, err))
	}
	if !validOriginal(accepted, id, writer) || (!accepted.Duplicate && accepted.Attribution != writer) ||
		(compare && (!accepted.Duplicate || accepted.Reference == ref)) || (!compare && accepted.Reference != ref) {
		return Receipt{}, errors.Join(ErrUncertain, ErrIntegrity)
	}
	if compare {
		// A concurrent publisher won after our absent preflight. Compare once;
		// its accepted reference cannot be replaced, but authority/expiry can change.
		return s.resolve(ctx, accepted, writer, ref, prepared)
	}
	return accepted, nil // Confirmed publication survives later cancellation.
}

func (s *Spans) resolve(ctx context.Context, original Receipt, writer Attribution, proposed Reference, prepared record.Span) (Receipt, error) {
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	ref := original.Reference
	if ref.Size > s.store.limits.MaxRecordBytes {
		return Receipt{}, ErrTooLarge
	}
	equal := ref == proposed
	if !equal {
		stored, err := s.store.objects.ReadExact(ctx, ObjectID{Tenant: ref.Identity.Tenant, Digest: ref.Digest}, ref.Size)
		if err != nil {
			return Receipt{}, fmt.Errorf("read span for retry comparison: %w", err)
		}
		if len(stored) != ref.Size || sha256.Sum256(stored) != ref.Digest {
			return Receipt{}, ErrIntegrity
		}
		accepted, err := s.codec.Decode(stored)
		if err != nil {
			return Receipt{}, fmt.Errorf("decode span for retry comparison: %w", err)
		}
		if !matchesExecution(accepted, executionOf(ref.Identity)) {
			return Receipt{}, ErrIntegrity
		}
		equal, err = s.codec.Compare(accepted, prepared)
		if err != nil {
			return Receipt{}, fmt.Errorf("compare span retry: %w", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	resolved, err := s.catalog.ResolveSpan(ctx, ref, writer, equal)
	if err != nil {
		return Receipt{}, fmt.Errorf("resolve span retry: %w", err)
	}
	want := original
	want.Duplicate = true
	if !equal || resolved != want {
		return Receipt{}, ErrIntegrity
	}
	return resolved, nil
}

// ReadSpan requires explicit project-reader permission and validates the saved
// execution against the authorized known-record key. The returned graph is owned
// by the caller; every error returns a zero Span. Absence, unsupported format,
// configured limits and storage failures remain distinct errors.
func (s *Spans) ReadSpan(ctx context.Context, execution Execution) (record.Span, error) {
	id := spanIdentity(execution)
	if err := s.store.begin(ctx, id); err != nil {
		return record.Span{}, err
	}
	defer s.store.end()
	stored, err := s.store.read(ctx, id)
	if err != nil {
		return record.Span{}, err
	}
	return s.decodeSpan(ctx, execution, stored)
}

// decodeSpan is shared by known-record and trace-page reads. Its caller holds
// the Store slot and has verified these bytes against this authorized execution.
func (s *Spans) decodeSpan(ctx context.Context, execution Execution, stored []byte) (record.Span, error) {
	if err := ctx.Err(); err != nil {
		return record.Span{}, err
	}
	view, err := s.codec.Decode(stored)
	if err != nil {
		return record.Span{}, fmt.Errorf("decode saved span: %w", err)
	}
	if !matchesExecution(view, execution) {
		return record.Span{}, ErrIntegrity
	}
	if err := ctx.Err(); err != nil {
		return record.Span{}, err
	}
	return view, nil
}

func spanIdentity(e Execution) Identity {
	return Identity{Tenant: e.Tenant, Trace: e.Trace, Span: e.Span, Kind: SpanRecord}
}

func matchesExecution(span record.Span, e Execution) bool {
	trace, id, err := span.IDs()
	return err == nil && trace == e.Trace && id == e.Span
}

func validOriginal(r Receipt, id Identity, writer Attribution) bool {
	return validReceiptTimes(r) && r.Reference.Identity == id && r.Reference.Size >= 0 &&
		validAttribution(r.Attribution) && r.Attribution.Owner == writer.Owner
}

// preparationError classifies only local preparation before object I/O. Errors
// decoding an accepted object deliberately never acquire this input-refusal tag.
func preparationError(err error) error {
	if errors.Is(err, record.ErrEnvelopeTooLarge) {
		return errors.Join(ErrTooLarge, err)
	}
	return errors.Join(ErrInvalid, err)
}
