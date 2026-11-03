package catalog

import (
	"context"

	"github.com/sidsharma96/rewind/internal/ingest"
)

var _ ingest.SpanCatalog = (*Catalog)(nil)

type writeOperation uint8

const (
	checkWrite writeOperation = iota
	publishRecord
	inspectSpan
	publishSpan
	resolveEqualSpan
	resolveDifferentSpan
)

// InspectSpan returns the original receipt to trusted retry coordination, not to
// a writer client. All writer/binding/run/expiry checks share the publication
// transaction. Absence reserves nothing; newly observed expiry must commit.
func (c *Catalog) InspectSpan(ctx context.Context, id ingest.Identity, writer ingest.Attribution) (ingest.Receipt, bool, error) {
	if id.Kind != ingest.SpanRecord {
		return ingest.Receipt{}, false, ingest.ErrInvalid
	}
	original, err := c.write(ctx, id, writer, inspectSpan, nil)
	if err != nil {
		return ingest.Receipt{}, false, err
	}
	return original, original != (ingest.Receipt{}), nil
}

// PublishSpan inserts absent records or discovers an original reference under
// the normal publication locks. compare=true is only an internal candidate: the
// caller must read/verify/compare outside locks then ResolveSpan before confirming
// this differently encoded retry. No new content replaces an accepted reference.
func (c *Catalog) PublishSpan(ctx context.Context, ref ingest.Reference, writer ingest.Attribution) (ingest.Receipt, bool, error) {
	if ref.Identity.Kind != ingest.SpanRecord || ref.Size < 0 {
		return ingest.Receipt{}, false, ingest.ErrInvalid
	}
	original, err := c.write(ctx, ref.Identity, writer, publishSpan, &ref)
	if err != nil {
		return ingest.Receipt{}, false, err
	}
	return original, original.Reference != ref, nil
}

// ResolveSpan consumes only the trusted coordinator's successful comparison.
// It repeats authority, run, expiry and reference checks; it never uploads,
// creates an absent record, changes an original receipt or grants read access.
func (c *Catalog) ResolveSpan(ctx context.Context, expected ingest.Reference, writer ingest.Attribution, equal bool) (ingest.Receipt, error) {
	if expected.Identity.Kind != ingest.SpanRecord || expected.Size < 0 {
		return ingest.Receipt{}, ingest.ErrInvalid
	}
	operation := resolveDifferentSpan
	if equal {
		operation = resolveEqualSpan
	}
	return c.write(ctx, expected.Identity, writer, operation, &expected)
}
