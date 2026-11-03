package ingest

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/sidsharma96/rewind/internal/record"
)

// CaptureCatalog is a trusted typed-capture publisher, not a producer API.
// Its three operations have the same current-authority, saved-expiry, immutable
// reference and outside-lock comparison contract as SpanCatalog, for CaptureRecord
// only. A malformed/unsupported accepted capture is an error, never a conflict or
// uncertain new publication. Legacy opaque captures are not silently reinterpreted.
type CaptureCatalog interface {
	Catalog
	InspectCapture(context.Context, Identity, Attribution) (Receipt, bool, error)
	PublishCapture(context.Context, Reference, Attribution) (Receipt, bool, error)
	ResolveCapture(context.Context, Reference, Attribution, bool) (Receipt, error)
}

// Captures borrows the existing typed-span Store so span writes, trace discovery,
// capture writes/reads and playback share one complete-operation admission bound.
// It requires no saved span row, owns no workers or cache, and verifies no real
// credentials. Codecs and the stored-record cap independently bound actual data.
type Captures struct {
	store   *Store
	catalog CaptureCatalog
	codec   record.CaptureCodec
}

func NewCaptures(spans *Spans, codec record.CaptureCodec) (*Captures, error) {
	if spans == nil || !codec.Configured() {
		return nil, ErrInvalid
	}
	catalog, ok := spans.catalog.(CaptureCatalog)
	if !ok {
		return nil, ErrInvalid
	}
	return &Captures{store: spans.store, catalog: catalog, codec: codec}, nil
}

// WriteCapture accepts one immutable capture observation, in either arrival order
// with its final span. Every reachable body remains stable until return. A changed
// state or byte conflicts; equivalent formatting/compression keeps the original.
func (s *Captures) WriteCapture(ctx context.Context, execution Execution, capture record.Capture) (Receipt, error) {
	id := captureIdentity(execution)
	if err := s.store.begin(ctx, id); err != nil {
		return Receipt{}, err
	}
	defer s.store.end()
	if !matchesCapture(capture, execution) {
		return Receipt{}, ErrInvalid
	}
	writer, err := s.store.authorizer.AuthorizeWrite(ctx, execution)
	if err != nil {
		return Receipt{}, fmt.Errorf("authorize capture writer: %w", err)
	}
	if !validAttribution(writer) {
		return Receipt{}, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	original, found, err := s.catalog.InspectCapture(ctx, id, writer)
	if err != nil {
		return Receipt{}, fmt.Errorf("inspect capture before preparation: %w", err)
	}
	if (!found && original != (Receipt{})) || (found && (!validOriginal(original, id, writer) || original.Duplicate)) {
		return Receipt{}, ErrIntegrity
	}
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	prepared, stored, err := s.codec.Prepare(capture)
	if err != nil {
		return Receipt{}, fmt.Errorf("prepare capture: %w", err)
	}
	if !matchesCapture(prepared, execution) {
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
		return Receipt{}, fmt.Errorf("create capture object: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	accepted, compare, err := s.catalog.PublishCapture(ctx, ref, writer)
	if err != nil {
		if errors.Is(err, ErrConflict) || errors.Is(err, ErrExpired) || errors.Is(err, ErrDenied) {
			return Receipt{}, fmt.Errorf("publish capture: %w", err)
		}
		return Receipt{}, fmt.Errorf("publish capture: %w", errors.Join(ErrUncertain, err))
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

func (s *Captures) resolve(ctx context.Context, original Receipt, writer Attribution, proposed Reference, prepared record.Capture) (Receipt, error) {
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
			return Receipt{}, fmt.Errorf("read capture for retry comparison: %w", err)
		}
		if len(stored) != ref.Size || sha256.Sum256(stored) != ref.Digest {
			return Receipt{}, ErrIntegrity
		}
		accepted, err := s.codec.Decode(stored)
		if err != nil {
			return Receipt{}, fmt.Errorf("decode accepted capture for retry comparison: %w", err)
		}
		if !matchesCapture(accepted, executionOf(ref.Identity)) {
			return Receipt{}, ErrIntegrity
		}
		equal, err = s.codec.Compare(accepted, prepared)
		if err != nil {
			return Receipt{}, fmt.Errorf("compare capture retry: %w", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	resolved, err := s.catalog.ResolveCapture(ctx, ref, writer, equal)
	if err != nil {
		return Receipt{}, fmt.Errorf("resolve capture retry: %w", err)
	}
	want := original
	want.Duplicate = true
	if !equal || resolved != want {
		return Receipt{}, ErrIntegrity
	}
	return resolved, nil
}

// CaptureRead joins owned decoded bodies to the exact original authorized receipt.
// A valid capture can describe unavailable bodies. Storage/codec errors return zero.
type CaptureRead struct {
	Receipt Receipt
	Capture record.Capture
}

// SavedResult is a complete saved response, not a complete captured pair. Its
// required RequestState preserves missing/redacted/truncated input evidence.
// Bytes are owned by the caller, including a valid zero-byte response.
type SavedResult struct {
	Execution    Execution
	Receipt      Receipt
	Bytes        []byte
	RequestState record.BodyState
}

var ErrResultUnavailable = errors.New("complete saved response unavailable")

func (s *Captures) ReadCapture(ctx context.Context, execution Execution) (CaptureRead, error) {
	if err := s.store.begin(ctx, captureIdentity(execution)); err != nil {
		return CaptureRead{}, err
	}
	defer s.store.end()
	return s.readCapture(ctx, execution)
}

// Playback returns only a Present response. It takes no incoming request or tool
// client, matches no new request, infers no tool exception/status and has no live
// fallback. Unavailable response evidence is inspectable through ReadCapture.
func (s *Captures) Playback(ctx context.Context, execution Execution) (SavedResult, error) {
	if err := s.store.begin(ctx, captureIdentity(execution)); err != nil {
		return SavedResult{}, err
	}
	defer s.store.end()
	read, err := s.readCapture(ctx, execution)
	if err != nil {
		return SavedResult{}, err
	}
	if read.Capture.Response.State != record.BodyPresent {
		return SavedResult{}, ErrResultUnavailable
	}
	return SavedResult{Execution: execution, Receipt: read.Receipt, Bytes: read.Capture.Response.Data, RequestState: read.Capture.Request.State}, nil
}

// readCapture holds the caller's admission slot through verified decoding.
func (s *Captures) readCapture(ctx context.Context, execution Execution) (CaptureRead, error) {
	receipt, err := s.store.lookup(ctx, captureIdentity(execution))
	if err != nil {
		return CaptureRead{}, err
	}
	stored, err := s.store.readObject(ctx, receipt.Reference)
	if err != nil {
		return CaptureRead{}, err
	}
	if err := ctx.Err(); err != nil {
		return CaptureRead{}, err
	}
	capture, err := s.codec.Decode(stored)
	if err != nil {
		return CaptureRead{}, fmt.Errorf("decode saved capture: %w", err)
	}
	if !matchesCapture(capture, execution) {
		return CaptureRead{}, ErrIntegrity
	}
	if err := ctx.Err(); err != nil {
		return CaptureRead{}, err
	}
	return CaptureRead{Receipt: receipt, Capture: capture}, nil
}

func captureIdentity(e Execution) Identity {
	return Identity{Tenant: e.Tenant, Trace: e.Trace, Span: e.Span, Kind: CaptureRecord}
}
func matchesCapture(v record.Capture, e Execution) bool {
	return v.Trace == e.Trace && v.Span == e.Span
}
