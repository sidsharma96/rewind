package record

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

func testMetadataCodec(t *testing.T, limits MetadataLimits) MetadataCodec {
	t.Helper()
	c, err := NewMetadataCodec(limits)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestMetadataCodecConfiguration(t *testing.T) {
	for _, limits := range []MetadataLimits{{}, {1, 1, 0}, {1, 0, 1}, {0, 1, 1}, {-1, 1, 1}, {1, -1, 1}, {1, 1, -1}} {
		if c, err := NewMetadataCodec(limits); !errors.Is(err, ErrMetadataInvalid) || c != (MetadataCodec{}) {
			t.Fatal("invalid config created codec")
		}
	}
	var c MetadataCodec
	packet := &tracepb.TracesData{}
	if err := c.Check(packet); !errors.Is(err, ErrMetadataInvalid) {
		t.Fatal("zero check succeeded")
	}
	if err := c.NormalizeOwned(packet); !errors.Is(err, ErrMetadataInvalid) {
		t.Fatal("zero normalization succeeded")
	}
	if out, err := c.Encode(packet); !errors.Is(err, ErrMetadataInvalid) || out != nil {
		t.Fatal("zero encode succeeded")
	}
	if out, err := c.Decode([]byte(`{}`)); !errors.Is(err, ErrMetadataInvalid) || out != nil {
		t.Fatal("zero decode succeeded")
	}
	if equal, err := c.Compare(Span{}, Span{}); equal || !errors.Is(err, ErrMetadataInvalid) {
		t.Fatal("zero compare succeeded")
	}
}

func TestMetadataCodecLimitsAreSeparate(t *testing.T) {
	// Exactly one graph work unit is sufficient for an empty packet. Encoding
	// still needs two bytes, independent of that successful structural check.
	c := testMetadataCodec(t, MetadataLimits{MaxBytes: 1, MaxWork: 1, MaxDepth: 1})
	packet := &tracepb.TracesData{}
	if err := c.Check(packet); err != nil {
		t.Fatal(err)
	}
	if out, err := c.Encode(packet); !errors.Is(err, ErrMetadataInvalid) || out != nil {
		t.Fatal("output byte cap bypassed")
	}
	c = testMetadataCodec(t, MetadataLimits{MaxBytes: 2, MaxWork: 1, MaxDepth: 1})
	if out, err := c.Encode(packet); err != nil || string(out) != `{}` {
		t.Fatalf("exact independent bounds: %s, %v", out, err)
	}
	if out, err := c.Decode([]byte(`{} `)); out != nil || !errors.Is(err, ErrMetadataInvalid) {
		t.Fatal("input byte cap bypassed")
	}
	nested := &tracepb.TracesData{ResourceSpans: []*tracepb.ResourceSpans{{}}}
	if err := c.Check(nested); !errors.Is(err, ErrMetadataInvalid) {
		t.Fatal("graph work cap bypassed")
	}
	// Avoid MaxBytes+1 overflow even for a caller-selected maximum int.
	c = testMetadataCodec(t, MetadataLimits{MaxBytes: int(^uint(0) >> 1), MaxWork: 1, MaxDepth: 1})
	if out, err := c.Encode(packet); err != nil || string(out) != `{}` {
		t.Fatal("maximum byte cap overflowed")
	}
}

func TestMetadataJSONDepthBoundary(t *testing.T) {
	c := testMetadataCodec(t, MetadataLimits{MaxBytes: 4096, MaxWork: 4096, MaxDepth: 8})
	atDepth := strings.Repeat("[", 8) + "0" + strings.Repeat("]", 8)
	if c.checkJSON([]byte(atDepth)) != nil || c.checkJSON([]byte("["+atDepth+"]")) == nil {
		t.Fatal("JSON depth boundary changed")
	}
	if out, err := c.Decode([]byte(`{"resourceSpans":[],"resourceSpans":[]}`)); out != nil || err == nil {
		t.Fatal("duplicate JSON field accepted")
	}
}

func TestMetadataCodecIndependentConcurrentRequests(t *testing.T) {
	c := testMetadataCodec(t, MetadataLimits{MaxBytes: 4096, MaxWork: 4096, MaxDepth: 64})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Go(func() {
			packet := &tracepb.TracesData{ResourceSpans: []*tracepb.ResourceSpans{{ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{{
				TraceId: bytes.Repeat([]byte{1}, 16), SpanId: bytes.Repeat([]byte{2}, 8),
				Attributes: []*commonpb.KeyValue{{Key: "z"}, {Key: "a"}},
			}}}}}}}
			before := proto.Clone(packet)
			if err := c.NormalizeOwned(packet); err != nil {
				t.Error(err)
				return
			}
			out, err := c.Encode(packet)
			if err != nil {
				t.Error(err)
				return
			}
			decoded, err := c.Decode(out)
			if err != nil || !proto.Equal(packet, decoded) {
				t.Error("independent round trip failed")
				return
			}
			// Caller still owns the fresh decoded graph; no codec cache retains it.
			decoded.ResourceSpans[0].ScopeSpans[0].Spans[0].Name = "changed"
			if packet.ResourceSpans[0].ScopeSpans[0].Spans[0].Name != "" || proto.Equal(before, packet) {
				t.Error("ownership or ordering changed")
			}
		})
	}
	wg.Wait()
}
