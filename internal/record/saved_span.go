package record

import (
	"errors"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

var ErrSavedSpan = errors.New("invalid saved span")

// SpanCodec saves one declared ended span, with inline context, as metadata JSON
// under the version-1 span envelope. Capture bodies and catalog attribution are
// separate. It preserves blank names, unknown numeric values and reported times;
// it does not promise complete OTLP semantic conformance or authenticate input.
// Configuration is immutable. Callers bound concurrent operations separately.
type SpanCodec struct {
	metadata MetadataCodec
	envelope EnvelopeCodec
}

func NewSpanCodec(metadata MetadataCodec, envelope EnvelopeCodec) (SpanCodec, error) {
	if !metadata.valid() || !envelope.valid() {
		return SpanCodec{}, ErrSavedSpan
	}
	return SpanCodec{metadata: metadata, envelope: envelope}, nil
}

// Configured checks immutable limits, not span validity or authorization.
func (c SpanCodec) Configured() bool { return c.metadata.valid() && c.envelope.valid() }

// Prepare checks the borrowed graph before copying, normalizes only the owned
// copy, and returns independent prepared metadata and stored bytes. Keep all
// reachable input stable until return. Limits bound visits/bytes/depth, not exact
// heap usage or elapsed CPU. Every error returns no partial graph or bytes.
func (c SpanCodec) Prepare(ended EndedSpan) (Span, []byte, error) {
	view, err := ended.View()
	if err != nil {
		return Span{}, nil, err
	}
	packet, err := view.OTLP()
	if err != nil {
		return Span{}, nil, err
	}
	if err := c.metadata.Check(packet); err != nil {
		return Span{}, nil, err
	}
	owned := proto.Clone(packet).(*tracepb.TracesData)
	if err := c.metadata.NormalizeOwned(owned); err != nil {
		return Span{}, nil, err
	}
	prepared, err := c.oneSpan(owned)
	if err != nil {
		return Span{}, nil, err
	}
	payload, err := c.metadata.Encode(owned)
	if err != nil {
		return Span{}, nil, err
	}
	stored, err := c.envelope.Encode(SpanObject, payload)
	if err != nil {
		return Span{}, nil, err
	}
	return prepared, stored, nil
}

// Decode requires a span-kind authorized catalog lookup and bytes verified
// against its exact size/digest. The caller cross-checks the returned execution
// IDs with that catalog key. It refuses batches rather than selecting their first
// span. Success owns the complete graph; errors expose no partial record. Saved
// attribute order must already be normalized; decoding never repairs evidence.
func (c SpanCodec) Decode(stored []byte) (Span, error) {
	payload, err := c.envelope.Decode(SpanObject, stored)
	if err != nil {
		return Span{}, err
	}
	packet, err := c.metadata.Decode(payload)
	if err != nil {
		return Span{}, err
	}
	return c.oneSpan(packet)
}

// Compare retains the metadata codec's checked typed-value rules. An error is
// neither equality nor a conflict. Both prepared graphs stay stable until return.
func (c SpanCodec) Compare(a, b Span) (bool, error) { return c.metadata.Compare(a, b) }

func (c SpanCodec) oneSpan(packet *tracepb.TracesData) (Span, error) {
	if err := c.metadata.Check(packet); err != nil {
		return Span{}, err
	}
	if len(packet.ResourceSpans) != 1 || len(packet.ResourceSpans[0].ScopeSpans) != 1 ||
		len(packet.ResourceSpans[0].ScopeSpans[0].Spans) != 1 {
		return Span{}, ErrSavedSpan
	}
	view, err := SpanAt(packet, 0, 0, 0)
	if err != nil {
		return Span{}, err
	}
	if _, _, err := view.IDs(); err != nil {
		return Span{}, err
	}
	if n := len(view.Metadata.ParentSpanId); n != 0 && n != 8 {
		return Span{}, ErrSavedSpan
	}
	if !attributesOrdered(packet.ProtoReflect()) {
		return Span{}, ErrUnorderedMetadata
	}
	return view, nil
}
