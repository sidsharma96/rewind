package record

import (
	"bytes"
	"errors"
	"math"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

func savedCodec(t testing.TB, threshold int) SpanCodec {
	t.Helper()
	m, err := NewMetadataCodec(MetadataLimits{16384, 32768, 64})
	if err != nil {
		t.Fatal(err)
	}
	p := DefaultCompressionPolicy()
	p.Threshold = threshold
	e, err := NewEnvelopeCodec(EnvelopeLimits{16384, 16384}, p)
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewSpanCodec(m, e)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func savedFixture() Span {
	return Span{Context: Context{Resource: &resourcepb.Resource{}, Scope: &commonpb.InstrumentationScope{Name: "agent"},
		ResourceSchemaURL: "https://resource.example/1", ScopeSchemaURL: "https://scope.example/1"},
		Metadata: &tracepb.Span{TraceId: bytes.Repeat([]byte{1}, 16), SpanId: bytes.Repeat([]byte{2}, 8),
			Kind: -99, Flags: 0xf0000001, StartTimeUnixNano: 20, EndTimeUnixNano: 10,
			Status: &tracepb.Status{Code: -77}, Links: []*tracepb.Span_Link{{}, {TraceId: make([]byte, 16), SpanId: make([]byte, 8), Flags: 0x80000000}},
			Attributes: []*commonpb.KeyValue{{Key: "z", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: math.Copysign(0, -1)}}}, {Key: "a"}}}}
}

func TestSavedSpanPreservesEvidenceAndOwnsOutput(t *testing.T) {
	for _, threshold := range []int{1, 32768} {
		c := savedCodec(t, threshold)
		in := savedFixture()
		before := proto.Clone(in.Metadata)
		ended, _ := in.DeclareEnded()
		prepared, stored, err := c.Prepare(ended)
		if err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(in.Metadata, before) || prepared.Metadata == in.Metadata || prepared.Context.Resource == in.Context.Resource {
			t.Fatal("preparation mutated or borrowed input")
		}
		if prepared.Metadata.Attributes[0].Key != "a" || (stored[6] == encodingGzip) != (threshold == 1) {
			t.Fatal("fixture did not exercise ordering/encoding")
		}
		got, err := c.Decode(stored)
		if err != nil {
			t.Fatal(err)
		}
		if equal, err := c.Compare(prepared, got); err != nil || !equal {
			t.Fatal("saved evidence changed", err)
		}
		if got.Metadata.Name != "" || got.Metadata.Kind != -99 || got.Metadata.Flags != 0xf0000001 ||
			got.Metadata.Status.Code != -77 || got.Metadata.EndTimeUnixNano != 10 || got.Context.Resource == nil ||
			!math.Signbit(got.Metadata.Attributes[1].Value.GetDoubleValue()) || len(got.Metadata.Links) != 2 ||
			len(got.Metadata.Links[0].TraceId) != 0 || len(got.Metadata.Links[1].TraceId) != 16 {
			t.Fatal("unknown, absent, zero-sign or anomalous evidence lost")
		}
		in.Metadata.Name = "changed after return"
		clear(stored)
		prepared.Metadata.Kind = 1
		if got.Metadata.Name != "" || got.Metadata.Kind != -99 {
			t.Fatal("decoded graph aliases input, prepared graph or stored buffer")
		}
	}
}

func TestSavedSpanRefusesBatchesAndInvalidGraphs(t *testing.T) {
	c := savedCodec(t, 32768)
	for _, change := range []func(*tracepb.TracesData){
		func(p *tracepb.TracesData) { p.ResourceSpans = append(p.ResourceSpans, p.ResourceSpans[0]) },
		func(p *tracepb.TracesData) {
			r := p.ResourceSpans[0]
			r.ScopeSpans = append(r.ScopeSpans, r.ScopeSpans[0])
		},
		func(p *tracepb.TracesData) {
			s := p.ResourceSpans[0].ScopeSpans[0]
			s.Spans = append(s.Spans, s.Spans[0])
		},
		func(p *tracepb.TracesData) { p.ResourceSpans = nil },
		func(p *tracepb.TracesData) { p.ResourceSpans[0].ScopeSpans[0].Spans[0].SpanId = make([]byte, 8) },
		func(p *tracepb.TracesData) { p.ResourceSpans[0].ScopeSpans[0].Spans[0].ParentSpanId = []byte{1} },
		func(p *tracepb.TracesData) {
			p.ResourceSpans[0].ScopeSpans[0].Spans[0].Attributes = append(p.ResourceSpans[0].ScopeSpans[0].Spans[0].Attributes, &commonpb.KeyValue{Key: "a"})
		},
	} {
		packet, _ := savedFixture().OTLP()
		change(packet)
		// Some malformed inputs are refused by the existing serializer first;
		// the rest must fail the typed decoder, never select the first span.
		payload, err := c.metadata.Encode(packet)
		if err != nil {
			continue
		}
		stored, err := c.envelope.Encode(SpanObject, payload)
		if err != nil {
			t.Fatal(err)
		}
		if got, err := c.Decode(stored); err == nil || got != (Span{}) {
			t.Fatal("invalid saved input exposed a partial span")
		}
	}
	ended, _ := savedFixture().DeclareEnded()
	view, _ := ended.View()
	view.Metadata.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	if got, b, err := c.Prepare(ended); !errors.Is(err, ErrMetadataUnsupported) || got != (Span{}) || b != nil {
		t.Fatal("unknown evidence silently lost before copy")
	}
	if got, b, err := c.Prepare(EndedSpan{}); err == nil || got != (Span{}) || b != nil {
		t.Fatal("zero declaration accepted")
	}
	if got, b, err := (SpanCodec{}).Prepare(ended); err == nil || got != (Span{}) || b != nil {
		t.Fatal("zero codec accepted")
	}
}

func TestSavedSpanDecodeDoesNotRepairAttributesOrLabels(t *testing.T) {
	c := savedCodec(t, 32768)
	packet, _ := savedFixture().OTLP() // z,a is deliberately not ordered.
	payload, err := c.metadata.Encode(packet)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := c.envelope.Encode(SpanObject, payload)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := c.Decode(stored); !errors.Is(err, ErrUnorderedMetadata) || got != (Span{}) {
		t.Fatal("saved unordered attributes were repaired")
	}
	for _, offset := range []int{4, 5, 6} {
		b := bytes.Clone(stored)
		b[offset] = 255
		if got, err := c.Decode(b); !errors.Is(err, ErrEnvelopeUnsupported) || got != (Span{}) {
			t.Fatal("unsupported label interpreted")
		}
	}
	stored[5] = byte(CaptureObject)
	if got, err := c.Decode(stored); !errors.Is(err, ErrEnvelopeKindMismatch) || got != (Span{}) {
		t.Fatal("capture interpreted as span")
	}
}

func FuzzSavedSpanDecode(f *testing.F) {
	c := savedCodec(f, 1)
	ended, _ := savedFixture().DeclareEnded()
	_, seed, err := c.Prepare(ended)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte("RWND"))
	f.Fuzz(func(t *testing.T, b []byte) {
		got, err := c.Decode(b)
		if err != nil {
			if got != (Span{}) {
				t.Fatal("partial result on error")
			}
			return
		}
		ended, err := got.DeclareEnded()
		if err != nil {
			t.Fatal(err)
		}
		prepared, stored, err := c.Prepare(ended)
		if err != nil {
			return
		} // Another serialization can exceed the configured output cap.
		again, err := c.Decode(stored)
		if err != nil {
			t.Fatal(err)
		}
		if equal, err := c.Compare(prepared, again); err != nil || !equal {
			t.Fatal("accepted saved record failed round trip", err)
		}
	})
}
