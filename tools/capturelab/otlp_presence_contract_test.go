package capturelab

import (
	"bytes"
	"slices"
	"strings"
	"testing"

	spanrecord "github.com/sidsharma96/rewind/internal/record"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// Each bit independently supplies one empty descriptive message. Every other
// field, including both grouping schema URLs, stays identical across masks.
func presenceFixture(mask uint8) *tracepb.TracesData {
	in := otlpFixture()
	rs := in.ResourceSpans[0]
	ss := rs.ScopeSpans[0]
	rs.Resource, ss.Scope, ss.Spans[0].Status = nil, nil, nil
	if mask&1 != 0 {
		rs.Resource = &resourcepb.Resource{}
	}
	if mask&2 != 0 {
		ss.Scope = &commonpb.InstrumentationScope{}
	}
	if mask&4 != 0 {
		ss.Spans[0].Status = &tracepb.Status{}
	}
	return in
}

func projectPresence(t testing.TB, in *tracepb.TracesData) *tracepb.TracesData {
	t.Helper()
	view, err := spanrecord.SpanAt(in, 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	out, err := spanComparisonProjection(view)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestOTLPComparisonPresence(t *testing.T) {
	var projections []*tracepb.TracesData
	var encodings [][]byte
	for mask := uint8(0); mask < 8; mask++ {
		in := presenceFixture(mask)
		before := proto.Clone(in)
		encoded, err := encodeOTLPJSON(in)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := decodeOTLPJSON(encoded)
		if err != nil || !proto.Equal(decoded, before) {
			t.Fatalf("mask %d: saved representation changed: %v", mask, err)
		}
		out := projectPresence(t, decoded)
		if !proto.Equal(decoded, before) || !proto.Equal(in, before) {
			t.Fatal("comparison changed the original presence or attribute order")
		}
		if !proto.Equal(out, projectPresence(t, out)) {
			t.Fatal("comparison projection is not idempotent")
		}
		projections = append(projections, out)
		encodings = append(encodings, encoded)
	}
	for i := range projections {
		for j := range projections {
			if !proto.Equal(projections[i], projections[j]) {
				t.Fatalf("presence masks %d and %d did not compare equally", i, j)
			}
			if i != j && bytes.Equal(encodings[i], encodings[j]) {
				t.Fatal("saved encoder erased a presence distinction")
			}
		}
	}
	t.Log("8 presence forms: 64 projected comparisons equal; all original forms round-trip intact")
}

func TestOTLPComparisonKeepsInformation(t *testing.T) {
	cases := []struct {
		name   string
		change func(*tracepb.ResourceSpans, *tracepb.ScopeSpans, *tracepb.Span)
	}{
		{"resource-attribute", func(r *tracepb.ResourceSpans, _ *tracepb.ScopeSpans, _ *tracepb.Span) {
			r.Resource.Attributes = []*commonpb.KeyValue{{Key: "x", Value: otlpInt(0)}}
		}},
		{"resource-count", func(r *tracepb.ResourceSpans, _ *tracepb.ScopeSpans, _ *tracepb.Span) {
			r.Resource.DroppedAttributesCount = 1
		}},
		{"resource-entity", func(r *tracepb.ResourceSpans, _ *tracepb.ScopeSpans, _ *tracepb.Span) {
			r.Resource.EntityRefs = []*commonpb.EntityRef{{Type: "service"}}
		}},
		{"resource-url", func(r *tracepb.ResourceSpans, _ *tracepb.ScopeSpans, _ *tracepb.Span) { r.SchemaUrl += "/changed" }},
		{"scope-name", func(_ *tracepb.ResourceSpans, s *tracepb.ScopeSpans, _ *tracepb.Span) { s.Scope.Name = "library" }},
		{"scope-version", func(_ *tracepb.ResourceSpans, s *tracepb.ScopeSpans, _ *tracepb.Span) { s.Scope.Version = "1" }},
		{"scope-attribute", func(_ *tracepb.ResourceSpans, s *tracepb.ScopeSpans, _ *tracepb.Span) {
			s.Scope.Attributes = []*commonpb.KeyValue{{Key: "x", Value: otlpInt(0)}}
		}},
		{"scope-count", func(_ *tracepb.ResourceSpans, s *tracepb.ScopeSpans, _ *tracepb.Span) {
			s.Scope.DroppedAttributesCount = 1
		}},
		{"scope-url", func(_ *tracepb.ResourceSpans, s *tracepb.ScopeSpans, _ *tracepb.Span) { s.SchemaUrl += "/changed" }},
		{"status-message", func(_ *tracepb.ResourceSpans, _ *tracepb.ScopeSpans, p *tracepb.Span) {
			p.Status.Message = "diagnostic"
		}},
		{"status-ok", func(_ *tracepb.ResourceSpans, _ *tracepb.ScopeSpans, p *tracepb.Span) {
			p.Status.Code = tracepb.Status_STATUS_CODE_OK
		}},
		{"status-unknown", func(_ *tracepb.ResourceSpans, _ *tracepb.ScopeSpans, p *tracepb.Span) { p.Status.Code = 99 }},
		{"status-negative", func(_ *tracepb.ResourceSpans, _ *tracepb.ScopeSpans, p *tracepb.Span) { p.Status.Code = -1 }},
		{"kind", func(_ *tracepb.ResourceSpans, _ *tracepb.ScopeSpans, p *tracepb.Span) {
			p.Kind = tracepb.Span_SPAN_KIND_INTERNAL
		}},
		{"span-attribute", func(_ *tracepb.ResourceSpans, _ *tracepb.ScopeSpans, p *tracepb.Span) {
			p.Attributes[0].Value = otlpInt(13)
		}},
	}
	want := projectPresence(t, presenceFixture(0))
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := presenceFixture(7)
			r := in.ResourceSpans[0]
			s := r.ScopeSpans[0]
			tc.change(r, s, s.Spans[0])
			before := proto.Clone(in)
			if proto.Equal(want, projectPresence(t, in)) || !proto.Equal(before, in) {
				t.Fatal("comparison erased information or mutated its input")
			}
		})
	}
}

func TestOTLPComparisonIsNotRecursiveEmptying(t *testing.T) {
	in := presenceFixture(0)
	span := in.ResourceSpans[0].ScopeSpans[0].Spans[0]
	span.Attributes = []*commonpb.KeyValue{{Key: "value"}}
	want := projectPresence(t, in)
	span.Attributes[0].Value = &commonpb.AnyValue{}
	if proto.Equal(want, projectPresence(t, in)) {
		t.Fatal("absent and empty AnyValue were equated")
	}
	span.Attributes[0].Value = &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{}}}
	if proto.Equal(want, projectPresence(t, in)) {
		t.Fatal("unselected and empty typed array values were equated")
	}
}

func TestOTLPComparisonOrderingAndOwnership(t *testing.T) {
	batch, _ := normalizationFixture()
	view, err := spanrecord.SpanAt(batch, 1, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	in, err := view.OTLP()
	if err != nil {
		t.Fatal(err)
	}
	want := projectPresence(t, in)
	span := in.ResourceSpans[0].ScopeSpans[0].Spans[0]
	slices.Reverse(span.Attributes)
	before := proto.Clone(in)
	out := projectPresence(t, in)
	if !proto.Equal(want, out) || !proto.Equal(before, in) {
		t.Fatal("attribute order affected comparison or caller data was reordered")
	}
	// The returned graph must be exclusively owned, including nested values.
	out.ResourceSpans[0].Resource.Attributes[0].Value = otlpString("changed")
	out.ResourceSpans[0].ScopeSpans[0].Scope.Attributes[0].Key = "changed"
	out.ResourceSpans[0].ScopeSpans[0].Spans[0].Name = "changed"
	if !proto.Equal(want, projectPresence(t, in)) {
		t.Fatal("projection shares mutable state with its source")
	}
	for _, sequence := range []string{"array", "events", "links"} {
		t.Run(sequence, func(t *testing.T) {
			in := presenceFixture(7)
			p := in.ResourceSpans[0].ScopeSpans[0].Spans[0]
			p.Attributes = []*commonpb.KeyValue{{Key: "array", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{Values: []*commonpb.AnyValue{otlpInt(1), otlpInt(2)}}}}}}
			p.Events = append(p.Events, &tracepb.Span_Event{Name: "second"})
			p.Links = append(p.Links, &tracepb.Span_Link{TraceState: "second"})
			want := projectPresence(t, in)
			switch sequence {
			case "array":
				slices.Reverse(p.Attributes[0].Value.GetArrayValue().Values)
			case "events":
				slices.Reverse(p.Events)
			case "links":
				slices.Reverse(p.Links)
			}
			if proto.Equal(want, projectPresence(t, in)) {
				t.Fatal("sequence order was erased")
			}
		})
	}
}

func TestOTLPComparisonKindDefaultIsNotInternal(t *testing.T) {
	in := presenceFixture(0)
	p := in.ResourceSpans[0].ScopeSpans[0].Spans[0]
	p.Kind = tracepb.Span_SPAN_KIND_UNSPECIFIED
	want := projectPresence(t, in)
	p.Kind = tracepb.Span_SPAN_KIND_INTERNAL
	if proto.Equal(want, projectPresence(t, in)) {
		t.Fatal("zero kind was silently interpreted as INTERNAL")
	}
}

func TestOTLPComparisonRefusesBeforeCopy(t *testing.T) {
	for _, name := range []string{"unknown-resource", "unknown-scope", "unknown-status", "duplicate", "oversize", "cycle", "missing-span", "bad-id", "nil-array-arm", "nil-list-entry"} {
		t.Run(name, func(t *testing.T) {
			in := presenceFixture(7)
			r := in.ResourceSpans[0]
			s := r.ScopeSpans[0]
			p := s.Spans[0]
			view, err := spanrecord.SpanAt(in, 0, 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			switch name {
			case "unknown-resource":
				r.Resource.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
			case "unknown-scope":
				s.Scope.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
			case "unknown-status":
				p.Status.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
			case "duplicate":
				p.Attributes = []*commonpb.KeyValue{{Key: "x"}, {Key: "x"}}
			case "oversize":
				p.Name = strings.Repeat("x", maxMetadata+1)
			case "cycle":
				a := &commonpb.ArrayValue{}
				v := &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: a}}
				a.Values = []*commonpb.AnyValue{v}
				p.Attributes = []*commonpb.KeyValue{{Key: "cycle", Value: v}}
			case "missing-span":
				view.Metadata = nil
			case "bad-id":
				p.SpanId = []byte{1}
			case "nil-array-arm":
				p.Attributes = []*commonpb.KeyValue{{Key: "array", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{}}}}
			case "nil-list-entry":
				p.Events = []*tracepb.Span_Event{nil}
			}
			// Clone is safe only for the acyclic, structurally valid cases.
			var before proto.Message
			if name != "cycle" && name != "nil-array-arm" && name != "nil-list-entry" {
				before = proto.Clone(in)
			}
			if out, err := spanComparisonProjection(view); err == nil || out != nil {
				t.Fatal("invalid input returned a usable projection")
			}
			if before != nil && !proto.Equal(before, in) {
				t.Fatal("refusal modified the borrowed input")
			}
		})
	}
}

func TestOTLPCodecMalformedMessagePointers(t *testing.T) {
	for _, name := range []string{"array-arm", "kvlist-arm", "event-entry"} {
		t.Run(name, func(t *testing.T) {
			in := presenceFixture(7)
			p := in.ResourceSpans[0].ScopeSpans[0].Spans[0]
			v := &commonpb.AnyValue{}
			switch name {
			case "array-arm":
				v.Value = &commonpb.AnyValue_ArrayValue{}
			case "kvlist-arm":
				v.Value = &commonpb.AnyValue_KvlistValue{}
			case "event-entry":
				p.Events = []*tracepb.Span_Event{nil}
			}
			p.Attributes = []*commonpb.KeyValue{{Key: "value", Value: v}}
			if b, err := encodeOTLPJSON(in); err == nil || b != nil {
				t.Fatal("encoder silently converted an invalid message pointer")
			}
			if err := normalizeOwnedOTLP(in); err == nil {
				t.Fatal("normalizer accepted an invalid message pointer")
			}
			if p.Attributes[0].Value != v || (name == "event-entry" && p.Events[0] != nil) ||
				(name == "array-arm" && v.GetArrayValue() != nil) || (name == "kvlist-arm" && v.GetKvlistValue() != nil) {
				t.Fatal("refusal changed malformed input")
			}
		})
	}
}

func FuzzOTLPComparisonPresence(f *testing.F) {
	f.Add(uint8(0), uint8(7), int32(0))
	f.Add(uint8(3), uint8(4), int32(99))
	f.Add(uint8(7), uint8(0), int32(-2147483648))
	f.Fuzz(func(t *testing.T, left, right uint8, code int32) {
		a, b := presenceFixture(left), presenceFixture(right)
		// A zero code may be omitted or explicitly present. A nonzero code
		// requires a present Status and must remain distinct from zero.
		if code != 0 {
			b.ResourceSpans[0].ScopeSpans[0].Spans[0].Status = &tracepb.Status{Code: tracepb.Status_StatusCode(code)}
		}
		beforeA, beforeB := proto.Clone(a), proto.Clone(b)
		pa, pb := projectPresence(t, a), projectPresence(t, b)
		if proto.Equal(pa, pb) != (code == 0) || proto.Equal(pb, pa) != (code == 0) {
			t.Fatal("presence or unknown code did not follow the field-value oracle")
		}
		if !proto.Equal(pa, projectPresence(t, pa)) || !proto.Equal(pb, projectPresence(t, pb)) ||
			!proto.Equal(a, beforeA) || !proto.Equal(b, beforeB) {
			t.Fatal("projection changed input or was not idempotent")
		}
	})
}
