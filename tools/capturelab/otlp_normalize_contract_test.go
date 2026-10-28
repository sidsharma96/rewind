package capturelab

import (
	"errors"
	"slices"
	"strings"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// Explicit fixture paths are independent of the implementation's reflective
// traversal. Every keyed container gets an intentionally different permutation.
func normalizationFixture() (*tracepb.TracesData, []*[]*commonpb.KeyValue) {
	in := otlpFixture()
	rs := in.ResourceSpans[0]
	ss := rs.ScopeSpans[0]
	span := ss.Spans[0]
	array := &commonpb.ArrayValue{Values: []*commonpb.AnyValue{otlpInt(3), otlpInt(1)}}
	inner := &commonpb.KeyValueList{Values: sortFixture(3, "reverse")}
	outer := &commonpb.KeyValueList{Values: []*commonpb.KeyValue{
		{Key: "z", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: array}}},
		{Key: "a", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{Values: []*commonpb.AnyValue{
			otlpString("first"), {Value: &commonpb.AnyValue_KvlistValue{KvlistValue: inner}}, otlpString("last"),
		}}}}},
	}}
	lists := []*[]*commonpb.KeyValue{&rs.Resource.Attributes, &ss.Scope.Attributes, &span.Attributes, &span.Events[0].Attributes, &span.Links[0].Attributes}
	for i, list := range lists {
		*list = sortFixture(5, "reverse")
		// Retain typed values but avoid identical permutations at every path.
		(*list)[0], (*list)[i] = (*list)[i], (*list)[0]
	}
	span.Attributes = append(span.Attributes, &commonpb.KeyValue{Key: "nested", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{KvlistValue: outer}}})
	// Multiple sequence members make accidental event/link/group sorting visible.
	span.Events = append([]*tracepb.Span_Event{{Name: "z-event"}}, span.Events...)
	span.Links = append([]*tracepb.Span_Link{{TraceState: "z-link"}}, span.Links...)
	ss.Spans = append([]*tracepb.Span{{Name: "z-span"}}, ss.Spans...)
	rs.ScopeSpans = append([]*tracepb.ScopeSpans{{SchemaUrl: "z-scope"}}, rs.ScopeSpans...)
	in.ResourceSpans = append([]*tracepb.ResourceSpans{{SchemaUrl: "z-resource"}}, in.ResourceSpans...)
	return in, append(lists, &outer.Values, &inner.Values)
}

// Insertion sort is a small independent oracle, not the selected implementation.
func oracleAttributeOrder(lists []*[]*commonpb.KeyValue) {
	for _, p := range lists {
		a := *p
		for i := 1; i < len(a); i++ {
			for j := i; j > 0 && a[j].Key < a[j-1].Key; j-- {
				a[j], a[j-1] = a[j-1], a[j]
			}
		}
	}
}

func TestOTLPNormalizeOwned(t *testing.T) {
	in, lists := normalizationFixture()
	want, oracle := normalizationFixture()
	oracleAttributeOrder(oracle)
	backing := make([]**commonpb.KeyValue, len(lists))
	pairs := make([][]*commonpb.KeyValue, len(lists))
	for i, list := range lists {
		backing[i] = &(*list)[0]
		pairs[i] = slices.Clone(*list)
	}
	if err := normalizeOwnedOTLP(in); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(in, want) {
		t.Fatal("normalization differs from oracle or changed sequence/value/context")
	}
	for i, list := range lists {
		if backing[i] != &(*list)[0] {
			t.Fatal("replaced collection backing array")
		}
		for _, pair := range pairs[i] {
			if !slices.Contains(*list, pair) {
				t.Fatal("cloned or discarded an original pair")
			}
		}
	}
	once := proto.Clone(in)
	if err := normalizeOwnedOTLP(in); err != nil || !proto.Equal(in, once) {
		t.Fatal("not idempotent")
	}
}

func TestOTLPNormalizeOwnershipBoundary(t *testing.T) {
	caller, _ := normalizationFixture()
	original := proto.Clone(caller)
	owned := proto.Clone(caller).(*tracepb.TracesData)
	if err := normalizeOwnedOTLP(owned); err != nil || !proto.Equal(caller, original) || proto.Equal(caller, owned) {
		t.Fatal("explicit snapshot did not isolate retaining caller")
	}
	encoded, err := encodeOTLPJSON(caller)
	if err != nil || !proto.Equal(caller, original) {
		t.Fatal("encoder changed caller")
	}
	decoded, err := decodeOTLPJSON(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if err := normalizeOwnedOTLP(decoded); err != nil || !proto.Equal(decoded, owned) || !proto.Equal(caller, original) {
		t.Fatal("fresh decode normalization lost data or changed caller")
	}
}

func TestOTLPNormalizeRefusalIsNonMutating(t *testing.T) {
	_, positions := otlpUnknownFixture()
	for i, position := range positions {
		t.Run(position.name, func(t *testing.T) {
			in, targets := otlpUnknownFixture()
			targets[i].message.ProtoReflect().SetUnknown(unknownWire([]byte("future")))
			before := proto.Clone(in)
			if err := normalizeOwnedOTLP(in); !errors.Is(err, errUnsupportedMetadata) || !proto.Equal(in, before) {
				t.Fatal("unsupported field refusal mutated message or returned wrong error")
			}
		})
	}
	in, _ := normalizationFixture()
	in.ResourceSpans = append(in.ResourceSpans, &tracepb.ResourceSpans{SchemaUrl: strings.Repeat("x", maxMetadata)})
	before := proto.Clone(in)
	if err := normalizeOwnedOTLP(in); !errors.Is(err, errInvalid) || !proto.Equal(in, before) {
		t.Fatal("oversize refusal mutated message")
	}
	cycle, lists := normalizationFixture()
	value := &commonpb.AnyValue{}
	value.Value = &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{Values: []*commonpb.AnyValue{value}}}
	*lists[0] = append(*lists[0], &commonpb.KeyValue{Key: "cycle", Value: value})
	order := slices.Clone(*lists[0])
	if err := normalizeOwnedOTLP(cycle); !errors.Is(err, errInvalid) || !slices.Equal(*lists[0], order) || value.GetArrayValue().Values[0] != value {
		t.Fatal("cycle refusal mutated message")
	}
	if !errors.Is(normalizeOwnedOTLP(nil), errInvalid) {
		t.Fatal("nil input accepted")
	}
}

func TestOTLPNormalizeSharedEntries(t *testing.T) {
	in, lists := normalizationFixture()
	// Exclusive ownership can include two internal references to a unique list.
	*lists[1] = *lists[0]
	original := slices.Clone(*lists[0])
	if err := normalizeOwnedOTLP(in); err != nil {
		t.Fatal(err)
	}
	if len(*lists[0]) != len(original) || !slices.IsSortedFunc(*lists[0], compareAttributeKeys) || !slices.Equal(*lists[0], *lists[1]) {
		t.Fatal("shared keyed collection lost entries or ordering")
	}
	for _, pair := range original {
		if !slices.Contains(*lists[0], pair) {
			t.Fatal("original pair discarded")
		}
	}
}

func FuzzOTLPNormalize(f *testing.F) {
	f.Add([]byte{3, 1, 4, 1, 5})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, swaps []byte) {
		if len(swaps) > 128 {
			t.Skip()
		}
		in, lists := normalizationFixture()
		for _, list := range lists {
			for i, b := range swaps {
				j := i % len(*list)
				k := int(b) % len(*list)
				(*list)[j], (*list)[k] = (*list)[k], (*list)[j]
			}
		}
		want, oracle := normalizationFixture()
		oracleAttributeOrder(oracle)
		if err := normalizeOwnedOTLP(in); err != nil || !proto.Equal(in, want) {
			t.Fatal("permutation changed contents or sequence order")
		}
	})
}

func TestOTLPNormalizeKeyOrdering(t *testing.T) {
	in := otlpFixture()
	span := in.ResourceSpans[0].ScopeSpans[0].Spans[0]
	span.Attributes = nil
	for _, key := range []string{"é", "z", "a", "A", "", "a\x00z", "a\x00a"} {
		span.Attributes = append(span.Attributes, &commonpb.KeyValue{Key: key, Value: otlpString(key)})
	}
	want := slices.Clone(span.Attributes)
	oracleAttributeOrder([]*[]*commonpb.KeyValue{&want})
	if err := normalizeOwnedOTLP(in); err != nil || !slices.Equal(span.Attributes, want) {
		t.Fatal("keys did not use case-sensitive Go string order")
	}
}

func BenchmarkOTLPAttributeOwnership(b *testing.B) {
	for _, strategy := range []string{"owned", "clone"} {
		b.Run(strategy, func(b *testing.B) {
			in, lists := normalizationFixture()
			original := make([][]*commonpb.KeyValue, len(lists))
			for i, p := range lists {
				original[i] = slices.Clone(*p)
			}
			b.ReportAllocs()
			for b.Loop() {
				work := in
				if strategy == "clone" {
					work = proto.Clone(in).(*tracepb.TracesData)
				} else {
					// Restore the same unsorted lists without new graph allocation.
					// This pointer-copy cost is included in the owned result.
					for i, p := range lists {
						copy(*p, original[i])
					}
				}
				if err := normalizeOwnedOTLP(work); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
