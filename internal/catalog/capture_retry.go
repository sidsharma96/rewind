package catalog

import (
	"context"

	"github.com/sidsharma96/rewind/internal/ingest"
)

var _ ingest.CaptureCatalog = (*Catalog)(nil)

// InspectCapture returns the original receipt to trusted retry coordination, not to
// a writer client. All writer/binding/run/expiry checks share the publication
// transaction. Absence reserves nothing; newly observed expiry must commit.
func (c *Catalog) InspectCapture(ctx context.Context, id ingest.Identity, writer ingest.Attribution) (ingest.Receipt, bool, error) {
	if id.Kind != ingest.CaptureRecord {
		return ingest.Receipt{}, false, ingest.ErrInvalid
	}
	original, err := c.write(ctx, id, writer, inspectTyped, nil)
	if err != nil {
		return ingest.Receipt{}, false, err
	}
	return original, original != (ingest.Receipt{}), nil
}

// PublishCapture inserts absent records or discovers an original reference under
// the normal publication locks. compare=true is only an internal candidate: the
// caller must read/verify/compare outside locks then ResolveCapture before confirming
// this differently encoded retry. No new content replaces an accepted reference.
func (c *Catalog) PublishCapture(ctx context.Context, ref ingest.Reference, writer ingest.Attribution) (ingest.Receipt, bool, error) {
	if ref.Identity.Kind != ingest.CaptureRecord || ref.Size < 0 {
		return ingest.Receipt{}, false, ingest.ErrInvalid
	}
	original, err := c.write(ctx, ref.Identity, writer, publishTyped, &ref)
	if err != nil {
		return ingest.Receipt{}, false, err
	}
	return original, original.Reference != ref, nil
}

// ResolveCapture consumes only the trusted coordinator's successful comparison.
// It repeats authority, run, expiry and reference checks; it never uploads,
// creates an absent record, changes an original receipt or grants read access.
func (c *Catalog) ResolveCapture(ctx context.Context, expected ingest.Reference, writer ingest.Attribution, equal bool) (ingest.Receipt, error) {
	if expected.Identity.Kind != ingest.CaptureRecord || expected.Size < 0 {
		return ingest.Receipt{}, ingest.ErrInvalid
	}
	operation := resolveDifferentTyped
	if equal {
		operation = resolveEqualTyped
	}
	return c.write(ctx, expected.Identity, writer, operation, &expected)
}
