package capturelab

import (
	"errors"
	"testing"

	spanrecord "github.com/sidsharma96/rewind/internal/record"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

func TestSpanRecordInlineCodec(t *testing.T) {
	in := otlpFixture()
	rs := in.ResourceSpans[0]
	ss := rs.ScopeSpans[0]
	rs.Resource.EntityRefs = []*commonpb.EntityRef{{Type: "service", IdKeys: []string{"service.name"}, SchemaUrl: "https://entity.example/1"}}
	second := proto.Clone(ss.Spans[0]).(*tracepb.Span)
	second.Name = "executor"
	second.SpanId[7]++
	ss.Spans = append(ss.Spans, second)
	// The owner can normalize once before selecting read-only views. No full
	// graph copy is required by selection/reconstruction or the encoder.
	if err := normalizeOwnedOTLP(in); err != nil {
		t.Fatal(err)
	}
	before := proto.Clone(in)
	var encoded [][]byte
	var expected []*tracepb.TracesData
	for p := 0; p < 2; p++ {
		view, err := spanrecord.SpanAt(in, 0, 0, p)
		if err != nil {
			t.Fatal(err)
		}
		wrapped, err := view.OTLP()
		if err != nil {
			t.Fatal(err)
		}
		b, err := encodeOTLPJSON(wrapped)
		if err != nil {
			t.Fatal(err)
		}
		want := proto.Clone(in).(*tracepb.TracesData)
		want.ResourceSpans[0].ScopeSpans[0].Spans = want.ResourceSpans[0].ScopeSpans[0].Spans[p : p+1]
		encoded = append(encoded, b)
		expected = append(expected, want)
	}
	if !proto.Equal(in, before) {
		t.Fatal("selection/reconstruction/encoding changed input")
	}
	// Encoded records must not rely on the original packet or a context registry.
	rs.Resource.Attributes[0].Value = otlpString("changed")
	rs.Resource.EntityRefs[0].Type = "changed"
	rs.SchemaUrl = "changed"
	ss.Scope.Name = "changed"
	ss.SchemaUrl = "changed"
	ss.Spans[0].Name = "changed"
	ss.Spans[1].Name = "changed"
	for p, b := range encoded {
		decoded, err := decodeOTLPJSON(b)
		if err != nil || !proto.Equal(decoded, expected[p]) {
			t.Fatal("independent encoded span lost its inline context or metadata")
		}
	}
}

func TestSpanRecordOptionalContextCodec(t *testing.T) {
	for _, present := range []bool{false, true} {
		in := otlpFixture()
		rs := in.ResourceSpans[0]
		ss := rs.ScopeSpans[0]
		rs.Resource = nil
		ss.Scope = nil
		if present {
			rs.Resource = &resourcepb.Resource{}
			ss.Scope = &commonpb.InstrumentationScope{}
		}
		view, err := spanrecord.SpanAt(in, 0, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		wrapped, err := view.OTLP()
		if err != nil {
			t.Fatal(err)
		}
		b, err := encodeOTLPJSON(wrapped)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := decodeOTLPJSON(b)
		if err != nil || !proto.Equal(decoded, in) {
			t.Fatal("optional context presence or schema URLs lost")
		}
	}
}

func TestSpanRecordUnknownPolicy(t *testing.T) {
	_, positions := otlpUnknownFixture()
	for i, position := range positions {
		t.Run(position.name, func(t *testing.T) {
			in, targets := otlpUnknownFixture()
			targets[i].message.ProtoReflect().SetUnknown(unknownWire([]byte("future")))
			before := proto.Clone(in)
			view, err := spanrecord.SpanAt(in, 0, 0, 0)
			switch position.name {
			case "packet", "resource-spans", "scope-spans":
				if !errors.Is(err, spanrecord.ErrUnsupported) || view != (spanrecord.Span{}) {
					t.Fatal("wrapper projection dropped unsupported fields")
				}
			default:
				if err != nil {
					t.Fatal(err)
				}
				wrapped, err := view.OTLP()
				if err != nil {
					t.Fatal(err)
				}
				if b, err := encodeOTLPJSON(wrapped); !errors.Is(err, errUnsupportedMetadata) || b != nil {
					t.Fatal("retained descendant unknown fields bypassed encoder refusal")
				}
			}
			if !proto.Equal(in, before) {
				t.Fatal("unknown-field refusal mutated input")
			}
		})
	}
}

func FuzzSpanRecord(f *testing.F) {
	// A compact structural seed keeps mutations focused on grouping/context.
	// Full semantic validity is not under test here; rich typed fixtures above
	// cover the independently encoded valid-span path.
	seed := []byte(`{"resourceSpans":[
	 {"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"a"}}]},"schemaUrl":"https://resource/0","scopeSpans":[
	  {"scope":{"name":"sdk-a"},"schemaUrl":"https://scope/0/0","spans":[{"name":"planner"},{"name":"executor"}]},
	  {"scope":{"name":"sdk-b"},"schemaUrl":"https://scope/0/1","spans":[{"name":"worker"}]}]},
	 {"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"b"}}]},"schemaUrl":"https://resource/1","scopeSpans":[
	  {"scope":{"name":"sdk-c"},"schemaUrl":"https://scope/1/0","spans":[{"name":"collector"}]}]}
	]}`)
	f.Add(seed, uint8(0), uint8(0), uint8(0))
	f.Add(seed, uint8(1), uint8(1), uint8(1))
	f.Fuzz(func(t *testing.T, b []byte, resource, scope, span uint8) {
		in, err := decodeOTLPJSON(b)
		if err != nil || len(in.ResourceSpans) == 0 {
			return
		}
		r := int(resource) % len(in.ResourceSpans)
		rs := in.ResourceSpans[r]
		if rs == nil || len(rs.ScopeSpans) == 0 {
			return
		}
		s := int(scope) % len(rs.ScopeSpans)
		ss := rs.ScopeSpans[s]
		if ss == nil || len(ss.Spans) == 0 {
			return
		}
		p := int(span) % len(ss.Spans)
		if ss.Spans[p] == nil {
			return
		}
		before := proto.Clone(in)
		view, err := spanrecord.SpanAt(in, r, s, p)
		if err != nil {
			t.Fatal(err) // Strict JSON decoding cannot introduce wrapper unknowns.
		}
		out, err := view.OTLP()
		if err != nil || !proto.Equal(in, before) {
			t.Fatal("selection/reconstruction failed or mutated input")
		}
		want := proto.Clone(in).(*tracepb.TracesData)
		want.ResourceSpans = want.ResourceSpans[r : r+1]
		want.ResourceSpans[0].ScopeSpans = want.ResourceSpans[0].ScopeSpans[s : s+1]
		want.ResourceSpans[0].ScopeSpans[0].Spans = want.ResourceSpans[0].ScopeSpans[0].Spans[p : p+1]
		if !proto.Equal(out, want) {
			t.Fatal("inline context differs from clone-and-prune reference")
		}
		encoded, err := encodeOTLPJSON(out)
		if err != nil {
			return // Parsing/selection do not imply encoder or semantic validity.
		}
		decoded, err := decodeOTLPJSON(encoded)
		if err != nil || !proto.Equal(decoded, want) {
			t.Fatal("individually encoded span changed its context")
		}
	})
}
