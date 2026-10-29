package capturelab

import (
	"errors"
	"testing"

	spanrecord "github.com/sidsharma96/rewind/internal/record"
	"github.com/sidsharma96/rewind/internal/traceid"
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
	var traces []traceid.Trace
	var spans []traceid.Span
	for p := 0; p < 2; p++ {
		view, err := spanrecord.SpanAt(in, 0, 0, p)
		if err != nil {
			t.Fatal(err)
		}
		trace, span, err := view.IDs()
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
		traces = append(traces, trace)
		spans = append(spans, span)
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
	for _, span := range ss.Spans {
		clear(span.TraceId)
		clear(span.SpanId)
	}
	for p, b := range encoded {
		decoded, err := decodeOTLPJSON(b)
		if err != nil || !proto.Equal(decoded, expected[p]) {
			t.Fatal("independent encoded span lost its inline context or metadata")
		}
		view, err := spanrecord.SpanAt(decoded, 0, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		trace, span, err := view.IDs()
		if err != nil || trace != traces[p] || span != spans[p] {
			t.Fatal("encoded span changed its binary execution identity")
		}
	}
}

func TestSpanRecordTimingCodec(t *testing.T) {
	const max = ^uint64(0)
	for _, pair := range [][2]uint64{{20, 10}, {20, 20}, {20, 30}, {0, 0}, {0, max}, {max, 0}, {1, max}} {
		in := otlpFixture()
		metadata := in.ResourceSpans[0].ScopeSpans[0].Spans[0]
		metadata.StartTimeUnixNano, metadata.EndTimeUnixNano = pair[0], pair[1]
		before := proto.Clone(in)
		view, err := spanrecord.SpanAt(in, 0, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		n, available := view.DurationNanos()
		wrapped, err := view.OTLP()
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := encodeOTLPJSON(wrapped)
		if err != nil || !proto.Equal(in, before) {
			t.Fatal("encoding changed or refused the reported timing")
		}
		metadata.StartTimeUnixNano, metadata.EndTimeUnixNano = 1, 1
		decoded, err := decodeOTLPJSON(encoded)
		if err != nil || !proto.Equal(decoded, before) {
			t.Fatal("codec did not preserve the original reported timestamps")
		}
		view, err = spanrecord.SpanAt(decoded, 0, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		if got, ok := view.DurationNanos(); got != n || ok != available {
			t.Fatal("timing interpretation changed after codec round trip")
		}
	}
}

func TestDeclaredEndedSpanCodec(t *testing.T) {
	for _, status := range []tracepb.Status_StatusCode{tracepb.Status_STATUS_CODE_UNSET, tracepb.Status_STATUS_CODE_OK, tracepb.Status_STATUS_CODE_ERROR} {
		for _, times := range [][2]uint64{{0, 0}, {20, 10}, {10, 20}} {
			in := otlpFixture()
			metadata := in.ResourceSpans[0].ScopeSpans[0].Spans[0]
			metadata.Status = &tracepb.Status{Code: status}
			metadata.StartTimeUnixNano, metadata.EndTimeUnixNano = times[0], times[1]
			before := proto.Clone(in)
			selected, err := spanrecord.SpanAt(in, 0, 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			// This is the caller's final declaration, not an inference from the
			// status/timing fields and not storage acceptance or authentication.
			ended, err := selected.DeclareEnded()
			if err != nil {
				t.Fatal(err)
			}
			view, err := ended.View()
			if err != nil {
				t.Fatal(err)
			}
			wrapped, err := view.OTLP()
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := encodeOTLPJSON(wrapped)
			if err != nil || !proto.Equal(in, before) {
				t.Fatalf("status=%v times=%v: encoding changed or refused metadata: %v", status, times, err)
			}
			// Borrowed views end their use before the source changes. Only the
			// encoded bytes are an independent snapshot in this lab.
			metadata.Status.Code = tracepb.Status_STATUS_CODE_ERROR
			metadata.StartTimeUnixNano, metadata.EndTimeUnixNano = 1, 1
			clear(metadata.TraceId)
			in.ResourceSpans[0].ScopeSpans[0].Scope.Name = "changed"
			decoded, err := decodeOTLPJSON(encoded)
			if err != nil || !proto.Equal(decoded, before) {
				t.Fatalf("status=%v times=%v: saved metadata lost original contents", status, times)
			}
		}
	}
	// A final declaration cannot bypass the separate unsupported-field guard.
	in := otlpFixture()
	in.ResourceSpans[0].ScopeSpans[0].Spans[0].ProtoReflect().SetUnknown(unknownWire([]byte("future")))
	selected, err := spanrecord.SpanAt(in, 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	ended, err := selected.DeclareEnded()
	if err != nil {
		t.Fatal(err)
	}
	view, err := ended.View()
	if err != nil {
		t.Fatal(err)
	}
	wrapped, err := view.OTLP()
	if err != nil {
		t.Fatal(err)
	}
	if b, err := encodeOTLPJSON(wrapped); !errors.Is(err, errUnsupportedMetadata) || b != nil {
		t.Fatal("completion declaration bypassed unsupported-field refusal")
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
