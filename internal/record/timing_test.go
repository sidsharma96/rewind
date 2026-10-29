package record_test

import (
	"math/big"
	"testing"

	"github.com/sidsharma96/rewind/internal/record"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

func TestSpanDurationNanos(t *testing.T) {
	if n, ok := (record.Span{}).DurationNanos(); ok || n != 0 {
		t.Fatal("missing span produced a duration")
	}
	const max = ^uint64(0)
	for _, tc := range []struct {
		name               string
		start, end, length uint64
		ok                 bool
	}{
		{"ordered", 20, 30, 10, true},
		{"reversed", 30, 20, 0, false},
		{"equal", 20, 20, 0, true},
		{"unknown both", 0, 0, 0, false},
		{"zero start", 0, 1, 0, false},
		{"zero end", 1, 0, 0, false},
		{"unknown start with large end", 0, max, 0, false},
		{"signed boundary", 1, 1<<63 + 1, 1 << 63, true},
		{"high timestamps", 1 << 63, 1<<63 + 1, 1, true},
		{"full nonzero range", 1, max, max - 1, true},
		{"last nanosecond", max - 1, max, 1, true},
		{"max equal", max, max, 0, true},
		{"max reversed", max, 0, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := packetFixture()
			metadata := in.ResourceSpans[0].ScopeSpans[0].Spans[0]
			metadata.StartTimeUnixNano, metadata.EndTimeUnixNano = tc.start, tc.end
			before := proto.Clone(in)
			view, err := record.SpanAt(in, 0, 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			n, ok := view.DurationNanos()
			if n != tc.length || ok != tc.ok {
				t.Fatalf("duration=%d,%v want=%d,%v", n, ok, tc.length, tc.ok)
			}
			if !proto.Equal(in, before) {
				t.Fatal("timing derivation changed original metadata")
			}
			// A returned numeric result is owned, but the view still borrows input.
			metadata.StartTimeUnixNano, metadata.EndTimeUnixNano = 5, 7
			if next, valid := view.DurationNanos(); !valid || next != 2 || n != tc.length || ok != tc.ok {
				t.Fatal("duration was cached or prior returned result changed")
			}
		})
	}
}

func FuzzSpanDurationNanos(f *testing.F) {
	const max = ^uint64(0)
	for _, pair := range [][2]uint64{{0, 0}, {0, 1}, {1, 0}, {20, 30}, {20, 20}, {30, 20}, {1, 1<<63 + 1}, {0, max}, {1, max}, {max, 0}, {max - 1, max}} {
		f.Add(pair[0], pair[1])
	}
	f.Fuzz(func(t *testing.T, start, end uint64) {
		metadata := &tracepb.Span{StartTimeUnixNano: start, EndTimeUnixNano: end}
		n, ok := (record.Span{Metadata: metadata}).DurationNanos()
		// Signed arbitrary-precision subtraction is independent of the runtime's
		// unsigned guard and cannot wrap or narrow at the uint64/int64 boundary.
		var a, b, difference big.Int
		difference.Sub(a.SetUint64(end), b.SetUint64(start))
		switch {
		case start == 0 || end == 0:
			if ok || n != 0 {
				t.Fatal("unknown endpoint produced a measured duration")
			}
		case difference.Sign() < 0:
			if ok || n != 0 {
				t.Fatal("negative mathematical interval became a numeric duration")
			}
		default:
			if !ok || !difference.IsUint64() || n != difference.Uint64() {
				t.Fatal("known ordered interval lost its exact unsigned difference")
			}
		}
		if metadata.StartTimeUnixNano != start || metadata.EndTimeUnixNano != end {
			t.Fatal("timing derivation changed input")
		}
	})
}
