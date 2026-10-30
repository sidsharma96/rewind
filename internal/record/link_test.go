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

func TestLinkTargetStates(t *testing.T) {
	traces := [][]byte{nil, make([]byte, 16), bytes.Repeat([]byte{1}, 16)}
	spans := [][]byte{nil, make([]byte, 8), bytes.Repeat([]byte{2}, 8)}
	for i, trace := range traces {
		for j, span := range spans {
			link := &tracepb.Span_Link{TraceId: trace, SpanId: span, TraceState: "vendor=value", Flags: ^uint32(0), DroppedAttributesCount: 2}
			before := proto.Clone(link)
			target, usable, err := record.TargetOfLink(link)
			want := i == 2 && j == 2
			if err != nil || usable != want {
				t.Fatalf("shape %d/%d: usable=%v err=%v", i, j, usable, err)
			}
			if want {
				if !bytes.Equal(target.Trace[:], trace) || !bytes.Equal(target.Span[:], span) {
					t.Fatal("wrong target")
				}
			} else if target != (record.LinkTarget{}) {
				t.Fatal("unavailable target leaked partial identity")
			}
			if !proto.Equal(link, before) {
				t.Fatal("classification changed source")
			}
		}
	}
	if target, usable, err := record.TargetOfLink(&tracepb.Span_Link{}); err != nil || usable || target != (record.LinkTarget{}) {
		t.Fatal("empty link did not remain valid evidence without a target")
	}
}

func TestLinkTargetRefusesMalformedShape(t *testing.T) {
	if target, usable, err := record.TargetOfLink(nil); !errors.Is(err, record.ErrInvalid) || usable || target != (record.LinkTarget{}) {
		t.Fatal("nil link became empty evidence")
	}
	// Check both widths before deciding unavailable: empty/zero on one side must
	// never hide malformed data on the other side.
	for _, trace := range [][]byte{nil, make([]byte, 16), bytes.Repeat([]byte{1}, 16), {1}, make([]byte, 17)} {
		for _, span := range [][]byte{nil, make([]byte, 8), bytes.Repeat([]byte{2}, 8), {1}, make([]byte, 9)} {
			if (len(trace) == 0 || len(trace) == 16) && (len(span) == 0 || len(span) == 8) {
				continue
			}
			target, usable, err := record.TargetOfLink(&tracepb.Span_Link{TraceId: trace, SpanId: span})
			if !errors.Is(err, traceid.ErrInvalid) || usable || target != (record.LinkTarget{}) {
				t.Fatal("malformed shape returned a usable or partial target")
			}
		}
	}
}

func TestLinkTargetOwnsIDsAndDoesNotCertifyMetadata(t *testing.T) {
	link := &tracepb.Span_Link{TraceId: bytes.Repeat([]byte{1}, 16), SpanId: bytes.Repeat([]byte{2}, 8)}
	link.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	before := proto.Clone(link)
	target, usable, err := record.TargetOfLink(link)
	if err != nil || !usable || !proto.Equal(before, link) {
		t.Fatal("target extraction erased unsupported fields or failed")
	}
	want := target
	clear(link.TraceId)
	clear(link.SpanId)
	if target != want || !target.Trace.Valid() || !target.Span.Valid() {
		t.Fatal("returned key borrowed mutable buffers")
	}
	if now, usable, err := record.TargetOfLink(link); err != nil || usable || now != (record.LinkTarget{}) {
		t.Fatal("classification cached a stale target")
	}
	// A valid link does not supply the containing execution's identity.
	view := record.Span{Metadata: &tracepb.Span{Links: []*tracepb.Span_Link{before.(*tracepb.Span_Link)}}}
	if _, _, err := view.IDs(); !errors.Is(err, traceid.ErrInvalid) {
		t.Fatal("link identity replaced invalid own IDs")
	}
}

func FuzzLinkTarget(f *testing.F) {
	f.Add([]byte{}, []byte{})
	f.Add(make([]byte, 16), make([]byte, 8))
	f.Add(bytes.Repeat([]byte{1}, 16), bytes.Repeat([]byte{2}, 8))
	f.Add([]byte{1}, []byte{})
	f.Fuzz(func(t *testing.T, trace, span []byte) {
		if len(trace) > 64 || len(span) > 64 {
			return
		}
		beforeTrace, beforeSpan := bytes.Clone(trace), bytes.Clone(span)
		target, usable, err := record.TargetOfLink(&tracepb.Span_Link{TraceId: trace, SpanId: span})
		shape := (len(trace) == 0 || len(trace) == 16) && (len(span) == 0 || len(span) == 8)
		nonzero := func(v []byte) bool {
			for _, b := range v {
				if b != 0 {
					return true
				}
			}
			return false
		}
		want := shape && len(trace) == 16 && len(span) == 8 && nonzero(trace) && nonzero(span)
		if (err != nil) != (!shape) || usable != want {
			t.Fatal("classification disagrees with byte-length/nonzero oracle")
		}
		if want {
			if !bytes.Equal(trace, target.Trace[:]) || !bytes.Equal(span, target.Span[:]) {
				t.Fatal("wrong copied target")
			}
		} else if target != (record.LinkTarget{}) {
			t.Fatal("partial target escaped")
		}
		if !bytes.Equal(trace, beforeTrace) || !bytes.Equal(span, beforeSpan) {
			t.Fatal("input changed")
		}
	})
}
