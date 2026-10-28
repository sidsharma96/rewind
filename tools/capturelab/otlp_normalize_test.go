package capturelab

import (
	"slices"
	"strings"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// normalizeOwnedOTLP exercises only attribute ordering, not full record validity
// or retry equality. The caller must exclusively own the entire reachable graph,
// including slice backing arrays. Use a fresh decode or explicitly clone a
// retained caller's message before calling; do not share it concurrently.
// Duplicate entries are retained, with no relative-order or meaning guarantee.
func normalizeOwnedOTLP(in *tracepb.TracesData) error {
	if in == nil {
		return errInvalid
	}
	budget := maxMetadata
	if err := validateOTLPMessage(in.ProtoReflect(), 0, &budget); err != nil {
		return err // Validate the complete graph before reordering any collection.
	}
	orderOTLPAttributes(in.ProtoReflect())
	return nil
}

func compareAttributeKeys(a, b *commonpb.KeyValue) int {
	return strings.Compare(a.GetKey(), b.GetKey())
}

// The preceding read-only validation bounds this second walk, including shared
// submessages, and refuses cycles. Only the pinned schema's keyed lists sort;
// array values and all other repeated fields retain their sequence order.
func orderOTLPAttributes(m protoreflect.Message) {
	var attrs []*commonpb.KeyValue
	switch v := m.Interface().(type) {
	case *resourcepb.Resource:
		attrs = v.GetAttributes()
	case *commonpb.InstrumentationScope:
		attrs = v.GetAttributes()
	case *tracepb.Span:
		attrs = v.GetAttributes()
	case *tracepb.Span_Event:
		attrs = v.GetAttributes()
	case *tracepb.Span_Link:
		attrs = v.GetAttributes()
	case *commonpb.KeyValueList:
		attrs = v.GetValues()
	}
	if !slices.IsSortedFunc(attrs, compareAttributeKeys) {
		slices.SortFunc(attrs, compareAttributeKeys)
	}
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if fd.Kind() != protoreflect.MessageKind {
			return true
		}
		if fd.IsList() {
			list := v.List()
			for i := 0; i < list.Len(); i++ {
				orderOTLPAttributes(list.Get(i).Message())
			}
		} else {
			orderOTLPAttributes(v.Message())
		}
		return true
	})
}
