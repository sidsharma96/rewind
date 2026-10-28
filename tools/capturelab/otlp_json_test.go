package capturelab

import (
	"bytes"
	"encoding/json"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// These fixed fixtures investigate a candidate JSON representation, not a saved
// format, semantic validator or OTLP receiver. proto.Equal is a structural
// round-trip oracle here, not the selected logical equality rule for retries.
func otlpInt(v int64) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: v}}
}

func otlpString(v string) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}
}

func otlpFixture() *tracepb.TracesData {
	return &tracepb.TracesData{ResourceSpans: []*tracepb.ResourceSpans{{
		Resource: &resourcepb.Resource{
			Attributes:             []*commonpb.KeyValue{{Key: "service.name", Value: otlpString("agent-runner")}},
			DroppedAttributesCount: 2,
		},
		SchemaUrl: "https://example.invalid/resource/v1",
		ScopeSpans: []*tracepb.ScopeSpans{{
			Scope: &commonpb.InstrumentationScope{
				Name: "rewind-test", Version: "0.0.synthetic",
				Attributes:             []*commonpb.KeyValue{{Key: "scope.attr", Value: otlpInt(7)}},
				DroppedAttributesCount: 1,
			},
			SchemaUrl: "https://example.invalid/scope/v1",
			Spans: []*tracepb.Span{{
				TraceId: []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
				SpanId:  []byte{8, 9, 10, 11, 12, 13, 14, 15}, ParentSpanId: []byte{1, 2, 3, 4, 5, 6, 7, 8},
				TraceState: "synthetic=1", Name: "planner", Kind: tracepb.Span_SPAN_KIND_CLIENT,
				StartTimeUnixNano: 1700000000000000001, EndTimeUnixNano: 1700000000000000100,
				Attributes: []*commonpb.KeyValue{
					{Key: "tokens-int", Value: otlpInt(12)},
					{Key: "tokens-text", Value: otlpString("12")},
					{Key: "metadata-bytes", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_BytesValue{BytesValue: []byte{0, 255, 7}}}},
				},
				DroppedAttributesCount: 1, DroppedEventsCount: 2, DroppedLinksCount: 3, Flags: 1,
				Status: &tracepb.Status{Code: tracepb.Status_STATUS_CODE_ERROR, Message: "synthetic failure"},
				Events: []*tracepb.Span_Event{{
					TimeUnixNano: 1700000000000000010, Name: "tool.completed",
					Attributes:             []*commonpb.KeyValue{{Key: "event.attr", Value: otlpString("event-value")}},
					DroppedAttributesCount: 1,
				}},
				Links: []*tracepb.Span_Link{{
					TraceId: []byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31},
					SpanId:  []byte{1, 2, 3, 4, 5, 6, 7, 8}, TraceState: "other=2", Flags: 1,
					Attributes:             []*commonpb.KeyValue{{Key: "link.attr", Value: otlpInt(9)}},
					DroppedAttributesCount: 1,
				}},
			}},
		}},
	}}}
}

func TestOTLPJSONPacketRoundTrip(t *testing.T) {
	in := otlpFixture()
	b, err := protojson.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out tracepb.TracesData
	if err := protojson.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(in, &out) {
		t.Fatal("candidate JSON changed known metadata or its context")
	}
}

func TestOTLPJSONValues(t *testing.T) {
	cases := []struct {
		name string
		in   *commonpb.AnyValue
	}{
		{"empty", &commonpb.AnyValue{}},
		{"text", otlpString("12")}, {"empty-text", otlpString("")},
		{"true", &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: true}}},
		{"false", &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: false}}},
		{"integer", otlpInt(12)}, {"zero-integer", otlpInt(0)},
		{"above-double-integer-precision", otlpInt(9007199254740993)},
		{"largest-integer", otlpInt(1<<63 - 1)}, {"smallest-integer", otlpInt(-1 << 63)},
		{"double", &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: 1.25}}},
		{"zero-double", &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: 0}}},
		{"bytes", &commonpb.AnyValue{Value: &commonpb.AnyValue_BytesValue{BytesValue: []byte{0, 255, 7}}}},
		{"empty-bytes", &commonpb.AnyValue{Value: &commonpb.AnyValue_BytesValue{BytesValue: []byte{}}}},
		{"array", &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{Values: []*commonpb.AnyValue{otlpInt(12), otlpString("12")}}}}},
		{"empty-array", &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{}}}},
		{"key-values", &commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{KvlistValue: &commonpb.KeyValueList{Values: []*commonpb.KeyValue{
			{Key: "count", Value: otlpInt(12)}, {Key: "label", Value: otlpString("12")},
		}}}}},
		{"empty-key-values", &commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{KvlistValue: &commonpb.KeyValueList{}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := protojson.Marshal(tc.in)
			if err != nil {
				t.Fatal(err)
			}
			var out commonpb.AnyValue
			if err := protojson.Unmarshal(b, &out); err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(tc.in, &out) {
				t.Fatal("candidate JSON changed value type, presence or contents")
			}
		})
	}
}

func TestOTLPJSONRepresentation(t *testing.T) {
	span := otlpFixture().ResourceSpans[0].ScopeSpans[0].Spans[0]
	b, err := protojson.Marshal(span)
	if err != nil {
		t.Fatal(err)
	}
	// Inspect selected fields, not whole JSON bytes or field order. These are
	// candidate ProtoJSON conventions, not an approved saved/OTLP wire format.
	var fields struct {
		TraceID string `json:"traceId"`
		StartNS string `json:"startTimeUnixNano"`
		Kind    string `json:"kind"`
		Attrs   []struct {
			Key   string                     `json:"key"`
			Value map[string]json.RawMessage `json:"value"`
		} `json:"attributes"`
	}
	if err := json.Unmarshal(b, &fields); err != nil {
		t.Fatal(err)
	}
	if fields.TraceID != "AAECAwQFBgcICQoLDA0ODw==" || fields.StartNS != "1700000000000000001" || fields.Kind != "SPAN_KIND_CLIENT" {
		t.Fatal("candidate ID, timestamp or enum representation changed")
	}
	for _, tc := range []struct{ key, field string }{{"tokens-int", "intValue"}, {"tokens-text", "stringValue"}} {
		found := false
		for _, attr := range fields.Attrs {
			if attr.Key != tc.key {
				continue
			}
			var value string
			if len(attr.Value) != 1 || json.Unmarshal(attr.Value[tc.field], &value) != nil || value != "12" {
				t.Fatal("typed field label or decimal/string representation changed")
			}
			found = true
		}
		if !found {
			t.Fatal("missing typed attribute")
		}
	}
}

func TestOTLPOrdinaryJSONOneofBoundary(t *testing.T) {
	for _, in := range []*commonpb.AnyValue{otlpInt(12), otlpString("12")} {
		b, err := json.Marshal(in)
		if err != nil {
			t.Fatal(err)
		}
		var out commonpb.AnyValue
		if err := json.Unmarshal(b, &out); err == nil {
			t.Fatal("plain JSON oneof decoding changed; revisit the simple baseline")
		}
	}
}

func TestOTLPJSONOwnership(t *testing.T) {
	in := otlpFixture()
	want := proto.Clone(in)
	b, err := protojson.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	saved := bytes.Clone(b)
	var out tracepb.TracesData
	if err := protojson.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	in.ResourceSpans[0].ScopeSpans[0].Spans[0].Name = "changed-input"
	in.ResourceSpans[0].ScopeSpans[0].Spans[0].Attributes[2].Value.GetBytesValue()[0] = 9
	if !bytes.Equal(b, saved) {
		t.Fatal("encoder retained mutable input")
	}
	clear(b)
	if !proto.Equal(&out, want) {
		t.Fatal("decoder retained input bytes or shared generated-message state")
	}
	afterInputChange := proto.Clone(in)
	out.ResourceSpans[0].ScopeSpans[0].Spans[0].Attributes[2].Value.GetBytesValue()[0] = 11
	if !proto.Equal(in, afterInputChange) {
		t.Fatal("decoded metadata shares mutable input state")
	}
}

func TestOTLPJSONUnknownWireFieldLoss(t *testing.T) {
	for _, nested := range []bool{false, true} {
		in := otlpFixture()
		var target proto.Message = in
		if nested {
			target = in.ResourceSpans[0].ScopeSpans[0].Spans[0].Attributes[0].Value
		}
		unknown := protowire.AppendTag(nil, 1000, protowire.VarintType)
		unknown = protowire.AppendVarint(unknown, 1)
		target.ProtoReflect().SetUnknown(unknown)
		b, err := protojson.Marshal(in)
		if err != nil {
			t.Fatal(err)
		}
		var out tracepb.TracesData
		if err := protojson.Unmarshal(b, &out); err != nil {
			t.Fatal(err)
		}
		if len(target.ProtoReflect().GetUnknown()) == 0 || proto.Equal(in, &out) || !proto.Equal(&out, otlpFixture()) {
			t.Fatal("revisit unknown-field loss boundary or known-field fidelity")
		}
	}
}

func TestOTLPJSONIsNotRecordValidation(t *testing.T) {
	var span tracepb.Span
	if err := protojson.Unmarshal([]byte(`{}`), &span); err != nil {
		t.Fatal(err)
	}
	if len(span.TraceId) != 0 || len(span.SpanId) != 0 {
		t.Fatal("decoder unexpectedly supplied missing execution identity")
	}
	if err := protojson.Unmarshal([]byte(`{"name":"planner","futureField":true}`), &span); err == nil {
		t.Fatal("default unknown JSON field policy changed; revisit candidate")
	}
}
