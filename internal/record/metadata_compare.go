package record

import (
	"slices"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Compare checks both stable complete span views before comparing their typed
// metadata without copying or mutation. Attributes must already be ordered.
// Only absent Resource, Scope and Status read as local empty messages; all other
// presence and sequence distinctions remain evidence. A false result with an
// error is unusable as a conflict decision. Bodies, versions, authorization and
// catalog retry resolution are outside this metadata operation.
func (c MetadataCodec) Compare(a, b Span) (bool, error) {
	if !c.valid() {
		return false, ErrMetadataInvalid
	}
	for _, in := range []Span{a, b} {
		if _, _, err := in.IDs(); err != nil {
			return false, err
		}
		wrapped, err := in.OTLP()
		if err != nil {
			return false, err
		}
		if err := c.Check(wrapped); err != nil {
			return false, err
		}
		if !attributesOrdered(wrapped.ProtoReflect()) {
			return false, ErrUnorderedMetadata
		}
	}

	am, bm := a.Metadata.ProtoReflect(), b.Metadata.ProtoReflect()
	fields := am.Descriptor().Fields()
	status := fields.ByName("status")
	if status == nil || status.Kind() != protoreflect.MessageKind || status.IsList() ||
		status.Message().FullName() != "opentelemetry.proto.trace.v1.Status" {
		return false, ErrMetadataUnsupported
	}
	if a.Context.ResourceSchemaURL != b.Context.ResourceSchemaURL ||
		a.Context.ScopeSchemaURL != b.Context.ScopeSchemaURL {
		return false, nil
	}
	ar, br := a.Context.Resource, b.Context.Resource
	if ar == nil {
		ar = &resourcepb.Resource{}
	}
	if br == nil {
		br = &resourcepb.Resource{}
	}
	if !proto.Equal(ar, br) {
		return false, nil
	}
	as, bs := a.Context.Scope, b.Context.Scope
	if as == nil {
		as = &commonpb.InstrumentationScope{}
	}
	if bs == nil {
		bs = &commonpb.InstrumentationScope{}
	}
	if !proto.Equal(as, bs) {
		return false, nil
	}
	ast, bst := a.Metadata.Status, b.Metadata.Status
	if ast == nil {
		ast = &tracepb.Status{}
	}
	if bst == nil {
		bst = &tracepb.Status{}
	}
	if !proto.Equal(ast, bst) {
		return false, nil
	}
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if field == status {
			continue // Only this position has the special Status presence rule.
		}
		if am.Has(field) != bm.Has(field) || !am.Get(field).Equal(bm.Get(field)) {
			return false, nil
		}
	}
	return true, nil
}

// The preceding complete guard bounds this second walk and refuses malformed
// pointers/cycles. Inspect exactly the keyed lists used by the normalizer; sequence
// order in arrays, events and links remains significant. No mutation occurs.
func attributesOrdered(m protoreflect.Message) bool {
	if !slices.IsSortedFunc(otlpAttributeList(m), compareAttributeKeys) {
		return false
	}
	ordered := true
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if fd.Kind() != protoreflect.MessageKind {
			return true
		}
		if fd.IsList() {
			list := v.List()
			for i := 0; i < list.Len(); i++ {
				if !attributesOrdered(list.Get(i).Message()) {
					ordered = false
					return false
				}
			}
		} else {
			ordered = attributesOrdered(v.Message())
		}
		return ordered
	})
	return ordered
}
