package capturelab

import (
	spanrecord "github.com/sidsharma96/rewind/internal/record"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// spanComparisonProjection is a deliberately copying lab baseline, not the
// production retry comparator or a saved representation. The caller keeps the
// borrowed graph stable throughout. Only Resource, Scope and Status acquire an
// empty message when absent; no other message presence is changed. Full equality,
// semantic admission, capture bodies and saved-version rules remain separate.
func spanComparisonProjection(in spanrecord.Span) (*tracepb.TracesData, error) {
	if _, _, err := in.IDs(); err != nil {
		return nil, err
	}
	wrapped, err := in.OTLP()
	if err != nil {
		return nil, err
	}
	// Validate BEFORE Clone: arbitrary borrowed graphs may contain cycles,
	// excessive nesting, duplicate keys or unsupported unknown wire fields.
	budget := maxMetadata
	if err := validateOTLPMessage(wrapped.ProtoReflect(), 0, &budget); err != nil {
		return nil, err
	}
	out := proto.Clone(wrapped).(*tracepb.TracesData)
	if err := normalizeOwnedOTLP(out); err != nil {
		return nil, err
	}
	rs := out.ResourceSpans[0]
	ss := rs.ScopeSpans[0]
	if rs.Resource == nil {
		rs.Resource = &resourcepb.Resource{}
	}
	if ss.Scope == nil {
		ss.Scope = &commonpb.InstrumentationScope{}
	}
	if ss.Spans[0].Status == nil {
		ss.Spans[0].Status = &tracepb.Status{}
	}
	return out, nil
}
