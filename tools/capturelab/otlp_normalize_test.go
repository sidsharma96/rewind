package capturelab

import (
	"errors"
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
// Repeated keys are refused before mutation, even when their values match.
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
	attrs := otlpAttributeList(m)
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

// Keep the validator and ordering walk on the same pinned keyed containers.
func otlpAttributeList(m protoreflect.Message) []*commonpb.KeyValue {
	switch v := m.Interface().(type) {
	case *resourcepb.Resource:
		return v.GetAttributes()
	case *commonpb.InstrumentationScope:
		return v.GetAttributes()
	case *tracepb.Span:
		return v.GetAttributes()
	case *tracepb.Span_Event:
		return v.GetAttributes()
	case *tracepb.Span_Link:
		return v.GetAttributes()
	case *commonpb.KeyValueList:
		return v.GetValues()
	}
	return nil
}

var errDuplicateAttribute = errors.New("repeated experimental attribute key")

func validateUniqueAttributeKeys(attrs []*commonpb.KeyValue) error {
	if len(attrs) < 2 {
		return nil
	}
	// The caller has already bounded this message's known fields. Store only
	// key references, never attribute values or copies of their contents.
	seen := make(map[string]struct{}, len(attrs))
	for _, attr := range attrs {
		key := attr.GetKey()
		if _, ok := seen[key]; ok {
			return errDuplicateAttribute
		}
		seen[key] = struct{}{}
	}
	return nil
}
