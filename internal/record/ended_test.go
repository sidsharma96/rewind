package record_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/sidsharma96/rewind/internal/record"
	"github.com/sidsharma96/rewind/internal/traceid"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

func TestDeclareEndedDoesNotDeriveCompletionFromMetadata(t *testing.T) {
	for _, status := range []tracepb.Status_StatusCode{tracepb.Status_STATUS_CODE_UNSET, tracepb.Status_STATUS_CODE_OK, tracepb.Status_STATUS_CODE_ERROR} {
		for _, times := range [][2]uint64{{0, 0}, {10, 0}, {0, 10}, {20, 10}, {10, 10}, {10, 20}} {
			metadata := &tracepb.Span{
				TraceId: bytes.Repeat([]byte{1}, 16), SpanId: bytes.Repeat([]byte{2}, 8),
				StartTimeUnixNano: times[0], EndTimeUnixNano: times[1],
				Status: &tracepb.Status{Code: status},
			}
			view := record.Span{Metadata: metadata}
			before := proto.Clone(metadata)
			// Identity is available before any final declaration. Neither a known
			// duration nor an OK/Error status supplies an EndedSpan value.
			if _, _, err := view.IDs(); err != nil {
				t.Fatal(err)
			}
			var undeclared record.EndedSpan
			if got, err := undeclared.View(); !errors.Is(err, record.ErrInvalid) || got != (record.Span{}) {
				t.Fatal("zero value exposed a final span")
			}
			// The call itself makes the producer's assertion. Unknown/reversed
			// time and an ended failure must not reject that assertion.
			ended, err := view.DeclareEnded()
			if err != nil {
				t.Fatalf("status=%v times=%v: %v", status, times, err)
			}
			got, err := ended.View()
			if err != nil || got != view || !proto.Equal(metadata, before) {
				t.Fatalf("status=%v times=%v: declaration lost or changed metadata", status, times)
			}
		}
	}
}

func TestDeclareEndedRefusesInvalidExecution(t *testing.T) {
	validTrace, validSpan := bytes.Repeat([]byte{1}, 16), bytes.Repeat([]byte{2}, 8)
	for _, tc := range []struct {
		name     string
		metadata *tracepb.Span
		want     error
	}{
		{"missing metadata", nil, record.ErrInvalid},
		{"missing IDs", &tracepb.Span{}, traceid.ErrInvalid},
		{"invalid trace", &tracepb.Span{TraceId: validTrace[:15], SpanId: validSpan}, traceid.ErrInvalid},
		{"zero trace", &tracepb.Span{TraceId: make([]byte, 16), SpanId: validSpan}, traceid.ErrInvalid},
		{"invalid span", &tracepb.Span{TraceId: validTrace, SpanId: validSpan[:7]}, traceid.ErrInvalid},
		{"zero span", &tracepb.Span{TraceId: validTrace, SpanId: make([]byte, 8)}, traceid.ErrInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ended, err := (record.Span{Metadata: tc.metadata}).DeclareEnded()
			if !errors.Is(err, tc.want) || ended != (record.EndedSpan{}) {
				t.Fatal("invalid execution returned a declaration or wrong error")
			}
			if view, err := ended.View(); !errors.Is(err, record.ErrInvalid) || view != (record.Span{}) {
				t.Fatal("failed declaration exposed a partial view")
			}
		})
	}
}

func TestEndedSpanBorrowsCompleteInlineContext(t *testing.T) {
	in := packetFixture()
	metadata := in.ResourceSpans[1].ScopeSpans[0].Spans[1]
	metadata.TraceId, metadata.SpanId = bytes.Repeat([]byte{1}, 16), bytes.Repeat([]byte{2}, 8)
	metadata.ProtoReflect().SetUnknown(unknownWire())
	before := proto.Clone(in)
	selected, err := record.SpanAt(in, 1, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	ended, err := selected.DeclareEnded()
	if err != nil {
		t.Fatal(err)
	}
	view, err := ended.View()
	if err != nil || view != selected || !proto.Equal(in, before) {
		t.Fatal("declaration copied, changed or lost inline context")
	}
	// Unknown metadata must remain for the encoder's independent refusal check;
	// declaring completion does not certify complete semantic validity.
	if len(view.Metadata.ProtoReflect().GetUnknown()) == 0 {
		t.Fatal("declaration discarded unsupported metadata")
	}
}
