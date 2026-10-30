package capturelab

import (
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"strings"
)

// The experiment uses the runtime implementation; caller ownership is unchanged.
func normalizeOwnedOTLP(in *tracepb.TracesData) error {
	return labMetadataError(metadataCodec.NormalizeOwned(in))
}

// Independent key comparator for the lab's sort-strategy experiment and oracle.
func compareAttributeKeys(a, b *commonpb.KeyValue) int {
	return strings.Compare(a.GetKey(), b.GetKey())
}
