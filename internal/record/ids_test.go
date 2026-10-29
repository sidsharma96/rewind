package record_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/sidsharma96/rewind/internal/record"
	"github.com/sidsharma96/rewind/internal/traceid"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

func TestSpanIDsUseSelectedExecution(t *testing.T) {
	in := packetFixture()
	wantTrace := traceid.Trace{0: 0x80, 15: 0xab}
	wantSpan := traceid.Span{0: 0xff, 7: 0xcd}
	selected := in.ResourceSpans[1].ScopeSpans[0].Spans[1]
	selected.TraceId = bytes.Clone(wantTrace[:])
	selected.SpanId = bytes.Clone(wantSpan[:])
	selected.ParentSpanId = []byte{1, 2, 3, 4, 5, 6, 7, 8}
	selected.Links = []*tracepb.Span_Link{{TraceId: bytes.Repeat([]byte{1}, 16), SpanId: bytes.Repeat([]byte{2}, 8)}}
	selected.Attributes = []*commonpb.KeyValue{{Key: "span.id", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "a descriptive label"}}}}
	before := proto.Clone(in)
	view, err := record.SpanAt(in, 1, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	trace, span, err := view.IDs()
	if err != nil || trace != wantTrace || span != wantSpan {
		t.Fatalf("selected IDs=%v,%v err=%v", trace, span, err)
	}
	if !proto.Equal(in, before) {
		t.Fatal("identity extraction changed metadata")
	}
	// Mutating the original buffers after extraction cannot rewrite a prior key.
	clear(selected.TraceId)
	clear(selected.SpanId)
	if trace != wantTrace || span != wantSpan {
		t.Fatal("returned identity retained source buffers")
	}
	if a, b, err := view.IDs(); !errors.Is(err, traceid.ErrInvalid) || a != (traceid.Trace{}) || b != (traceid.Span{}) {
		t.Fatal("later extraction used stale identity instead of current metadata")
	}
}

func TestSpanIDsFailureReturnsNoPartialIdentity(t *testing.T) {
	validTrace := bytes.Repeat([]byte{1}, 16)
	validSpan := bytes.Repeat([]byte{2}, 8)
	for _, tc := range []struct {
		name string
		span *tracepb.Span
		want error
	}{
		{"nil span", nil, record.ErrInvalid},
		{"empty", &tracepb.Span{}, traceid.ErrInvalid},
		{"short trace", &tracepb.Span{TraceId: validTrace[:15], SpanId: validSpan}, traceid.ErrInvalid},
		{"long trace", &tracepb.Span{TraceId: bytes.Repeat([]byte{1}, 17), SpanId: validSpan}, traceid.ErrInvalid},
		{"zero trace", &tracepb.Span{TraceId: make([]byte, 16), SpanId: validSpan}, traceid.ErrInvalid},
		{"missing span", &tracepb.Span{TraceId: validTrace}, traceid.ErrInvalid},
		{"short span", &tracepb.Span{TraceId: validTrace, SpanId: validSpan[:7]}, traceid.ErrInvalid},
		{"long span", &tracepb.Span{TraceId: validTrace, SpanId: bytes.Repeat([]byte{2}, 9)}, traceid.ErrInvalid},
		{"zero span", &tracepb.Span{TraceId: validTrace, SpanId: make([]byte, 8)}, traceid.ErrInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var before *tracepb.Span
			if tc.span != nil {
				before = proto.Clone(tc.span).(*tracepb.Span)
			}
			trace, span, err := (record.Span{Metadata: tc.span}).IDs()
			if !errors.Is(err, tc.want) || trace != (traceid.Trace{}) || span != (traceid.Span{}) {
				t.Fatal("invalid span returned wrong error or partial identity")
			}
			if !proto.Equal(tc.span, before) {
				t.Fatal("failed extraction changed source")
			}
		})
	}
}

func TestSpanIDsDoNotClaimCompletionOrFullValidity(t *testing.T) {
	metadata := &tracepb.Span{TraceId: bytes.Repeat([]byte{1}, 16), SpanId: bytes.Repeat([]byte{2}, 8)}
	metadata.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	view := record.Span{Metadata: metadata}
	trace, span, err := view.IDs()
	if err != nil || !trace.Valid() || !span.Valid() || metadata.EndTimeUnixNano != 0 {
		t.Fatal("identity could not be extracted before completion")
	}
	if len(metadata.ProtoReflect().GetUnknown()) == 0 {
		t.Fatal("identity extraction discarded unsupported metadata")
	}
	// A stable owned key does not freeze the borrowed metadata or certify it.
	metadata.EndTimeUnixNano = 5
	if a, b, err := view.IDs(); err != nil || a != trace || b != span {
		t.Fatal("completion changed execution identity")
	}
}
