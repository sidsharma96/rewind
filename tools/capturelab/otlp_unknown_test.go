package capturelab

import (
	"bytes"
	"errors"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

type unknownTarget struct {
	name    string
	message proto.Message
}

// Every message-bearing part of the fixture can independently carry unknown
// wire fields. Use a fresh fixture per case so one refusal cannot mask another.
func otlpUnknownFixture() (*tracepb.TracesData, []unknownTarget) {
	in := otlpFixture()
	rs := in.ResourceSpans[0]
	ss := rs.ScopeSpans[0]
	span := ss.Spans[0]
	leaf := otlpString("known")
	pair := &commonpb.KeyValue{Key: "future-label", Value: leaf}
	kv := &commonpb.KeyValueList{Values: []*commonpb.KeyValue{pair}}
	kvValue := &commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{KvlistValue: kv}}
	array := &commonpb.ArrayValue{Values: []*commonpb.AnyValue{kvValue}}
	arrayValue := &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: array}}
	span.Attributes = append(span.Attributes, &commonpb.KeyValue{Key: "nested", Value: arrayValue})
	return in, []unknownTarget{
		{"packet", in}, {"resource-spans", rs}, {"resource", rs.Resource},
		{"resource-attribute", rs.Resource.Attributes[0]}, {"resource-value", rs.Resource.Attributes[0].Value},
		{"scope-spans", ss}, {"scope", ss.Scope},
		{"scope-attribute", ss.Scope.Attributes[0]}, {"scope-value", ss.Scope.Attributes[0].Value},
		{"span", span}, {"status", span.Status},
		{"span-attribute", span.Attributes[0]}, {"span-value", span.Attributes[0].Value},
		{"event", span.Events[0]}, {"event-attribute", span.Events[0].Attributes[0]}, {"event-value", span.Events[0].Attributes[0].Value},
		{"link", span.Links[0]}, {"link-attribute", span.Links[0].Attributes[0]}, {"link-value", span.Links[0].Attributes[0].Value},
		{"array-value", arrayValue}, {"array", array}, {"key-value-value", kvValue},
		{"key-value-list", kv}, {"nested-key-value", pair}, {"nested-leaf", leaf},
	}
}

func unknownWire(payload []byte) []byte {
	b := protowire.AppendTag(nil, 1000, protowire.BytesType)
	return protowire.AppendBytes(b, payload)
}

func TestOTLPCodecRefusesUnknownWire(t *testing.T) {
	_, names := otlpUnknownFixture()
	for i, name := range names {
		t.Run(name.name, func(t *testing.T) {
			in, targets := otlpUnknownFixture()
			clean := proto.Clone(in)
			m := targets[i].message.ProtoReflect()
			if m.Descriptor().Fields().ByNumber(1000) != nil {
				t.Fatal("fixture tag is no longer unknown; revisit the schema pin")
			}
			unknown := unknownWire([]byte("synthetic future field"))
			m.SetUnknown(unknown)
			want := proto.Clone(in)

			// Simple oracle: ProtoJSON alone loses this field. The candidate
			// must refuse before converting, rather than clear it and succeed.
			b, err := protojson.Marshal(in)
			if err != nil {
				t.Fatal(err)
			}
			var lost tracepb.TracesData
			if err := protojson.Unmarshal(b, &lost); err != nil || !proto.Equal(clean, &lost) || proto.Equal(in, &lost) {
				t.Fatal("baseline no longer demonstrates unknown-field loss")
			}
			if b, err := encodeOTLPJSON(in); !errors.Is(err, errUnsupportedMetadata) || b != nil {
				t.Fatalf("unsupported field returned usable bytes or wrong error: %v", err)
			}
			if !proto.Equal(in, want) || !bytes.Equal(m.GetUnknown(), unknown) {
				t.Fatal("refusal changed the original metadata")
			}

			// Exercise actual protobuf parsing, not only direct test injection.
			wire, err := proto.Marshal(in)
			if err != nil {
				t.Fatal(err)
			}
			var decoded tracepb.TracesData
			if err := proto.Unmarshal(wire, &decoded); err != nil || !proto.Equal(want, &decoded) {
				t.Fatal("binary route did not retain the unknown field")
			}
			if b, err := encodeOTLPJSON(&decoded); !errors.Is(err, errUnsupportedMetadata) || b != nil {
				t.Fatal("parsed unknown field escaped refusal")
			}
		})
	}
}

func TestOTLPCodecRefusesUnknownValueAlternative(t *testing.T) {
	in := otlpFixture()
	value := &commonpb.AnyValue{}
	// A future AnyValue field can exist on the wire while this generated
	// version sees no active known oneof alternative. Empty is not a substitute.
	wire := unknownWire([]byte("synthetic future value"))
	if err := proto.Unmarshal(wire, value); err != nil {
		t.Fatal(err)
	}
	if value.Value != nil || len(value.ProtoReflect().GetUnknown()) == 0 {
		t.Fatal("fixture no longer represents an unknown value field")
	}
	in.ResourceSpans[0].ScopeSpans[0].Spans[0].Attributes[0].Value = value
	if b, err := encodeOTLPJSON(in); !errors.Is(err, errUnsupportedMetadata) || b != nil {
		t.Fatal("unknown value was silently changed to empty")
	}
}

func TestOTLPCodecRefusesUnknownJSON(t *testing.T) {
	for _, b := range []string{
		`{"futureField":true}`,
		`{"resourceSpans":[{"futureField":true}]}`,
		`{"resourceSpans":[{"resource":{"futureField":true}}]}`,
		`{"resourceSpans":[{"scopeSpans":[{"scope":{"futureField":true}}]}]}`,
		`{"resourceSpans":[{"scopeSpans":[{"spans":[{"futureField":true}]}]}]}`,
		`{"resourceSpans":[{"scopeSpans":[{"spans":[{"attributes":[{"key":"known","value":{"futureField":true}}]}]}]}]}`,
		`{"resourceSpans":[{"scopeSpans":[{"spans":[{"attributes":[{"key":"known","value":{"arrayValue":{"values":[{"futureField":true}]}}}]}]}]}]}`,
	} {
		if out, err := decodeOTLPJSON([]byte(b)); err == nil || out != nil {
			t.Fatal("unknown JSON field returned a usable message")
		}
	}
}

func TestOTLPCodecAllowsNewAttributeNames(t *testing.T) {
	in := otlpFixture()
	span := in.ResourceSpans[0].ScopeSpans[0].Spans[0]
	span.Attributes = append(span.Attributes, &commonpb.KeyValue{Key: "futureField", Value: otlpString("new label")})
	b, err := encodeOTLPJSON(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := decodeOTLPJSON(b)
	if err != nil || !proto.Equal(in, out) {
		t.Fatal("ordinary new attribute name was refused or changed")
	}
}

func FuzzOTLPUnknownFieldRefusal(f *testing.F) {
	f.Add(uint8(0), []byte("root"))
	f.Add(uint8(24), []byte("nested"))
	f.Fuzz(func(t *testing.T, position uint8, payload []byte) {
		if len(payload) > 1024 {
			return // Bound this synthetic probe independently of wire sizes.
		}
		in, targets := otlpUnknownFixture()
		m := targets[int(position)%len(targets)].message.ProtoReflect()
		unknown := unknownWire(payload)
		m.SetUnknown(unknown)
		want := proto.Clone(in)
		if b, err := encodeOTLPJSON(in); !errors.Is(err, errUnsupportedMetadata) || b != nil {
			t.Fatal("unknown metadata did not fail without output")
		}
		if !proto.Equal(want, in) || !bytes.Equal(unknown, m.GetUnknown()) {
			t.Fatal("refusal mutated metadata")
		}
	})
}
