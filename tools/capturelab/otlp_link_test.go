package capturelab

import (
	"bytes"
	"errors"
	"testing"

	spanrecord "github.com/sidsharma96/rewind/internal/record"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

func linkEvidenceFixture(traceState, spanState uint8) *tracepb.TracesData {
	in := otlpFixture()
	link := in.ResourceSpans[0].ScopeSpans[0].Spans[0].Links[0]
	switch traceState % 3 {
	case 0:
		link.TraceId = nil
	case 1:
		link.TraceId = make([]byte, 16)
	}
	switch spanState % 3 {
	case 0:
		link.SpanId = nil
	case 1:
		link.SpanId = make([]byte, 8)
	}
	return in
}

func checkLinkEvidence(t testing.TB, in *tracepb.TracesData, wantUsable bool) {
	t.Helper()
	if err := normalizeOwnedOTLP(in); err != nil {
		t.Fatal(err)
	}
	before, err := proto.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := encodeOTLPJSON(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := decodeOTLPJSON(encoded)
	if err != nil || !proto.Equal(in, out) {
		t.Fatalf("link evidence lost: %v", err)
	}
	p := out.ResourceSpans[0].ScopeSpans[0].Spans[0]
	target, usable, err := spanrecord.TargetOfLink(p.Links[0])
	if err != nil || usable != wantUsable || (!usable && target != (spanrecord.LinkTarget{})) {
		t.Fatal("decoded link target misclassified")
	}
	equal, err := compareSpanMetadata(comparisonView(t, in), comparisonView(t, out))
	if err != nil || !equal {
		t.Fatalf("roundtrip retry changed: %v", err)
	}
	after, err := proto.Marshal(in)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("encoding/classification/comparison changed evidence")
	}
}

func TestOTLPLinkEvidence(t *testing.T) {
	var fixtures []*tracepb.TracesData
	for trace := uint8(0); trace < 3; trace++ {
		for span := uint8(0); span < 3; span++ {
			in := linkEvidenceFixture(trace, span)
			checkLinkEvidence(t, in, trace == 2 && span == 2)
			fixtures = append(fixtures, in)
		}
	}
	for i, a := range fixtures {
		for j, b := range fixtures {
			if checkDirectComparison(t, a, b) != (i == j) {
				t.Fatal("absent, zero and valid IDs lost their retry distinction")
			}
		}
	}
	// Even completely empty received messages retain their list positions. A nil
	// pointer entry remains malformed and is not an empty message substitute.
	empty := otlpFixture()
	p := empty.ResourceSpans[0].ScopeSpans[0].Spans[0]
	p.Links = []*tracepb.Span_Link{{}, {}, p.Links[0]}
	checkLinkEvidence(t, empty, false)
	without := proto.Clone(empty).(*tracepb.TracesData)
	without.ResourceSpans[0].ScopeSpans[0].Spans[0].Links = without.ResourceSpans[0].ScopeSpans[0].Spans[0].Links[1:]
	if checkDirectComparison(t, empty, without) {
		t.Fatal("an empty link was discarded during comparison")
	}
	t.Log("9 target-ID shapes and empty link entries retain evidence and retry distinctions")
}

func TestOTLPLinkWithoutTargetStillComparesAllFields(t *testing.T) {
	for _, change := range []string{"attributes", "trace-state", "flags", "dropped-count", "remove", "order"} {
		t.Run(change, func(t *testing.T) {
			a := linkEvidenceFixture(0, 1)
			p := a.ResourceSpans[0].ScopeSpans[0].Spans[0]
			p.Links = append(p.Links, &tracepb.Span_Link{})
			b := proto.Clone(a).(*tracepb.TracesData)
			q := b.ResourceSpans[0].ScopeSpans[0].Spans[0]
			link := q.Links[0]
			switch change {
			case "attributes":
				link.Attributes[0].Value = otlpInt(10)
			case "trace-state":
				link.TraceState += "changed"
			case "flags":
				link.Flags ^= 1 << 31
			case "dropped-count":
				link.DroppedAttributesCount++
			case "remove":
				q.Links = q.Links[1:]
			case "order":
				q.Links[0], q.Links[1] = q.Links[1], q.Links[0]
			}
			if checkDirectComparison(t, a, b) {
				t.Fatal("unusable target hid changed link evidence")
			}
		})
	}
}

func TestOTLPLinkTargetDoesNotBypassValidation(t *testing.T) {
	for _, kind := range []string{"unknown", "nil-link", "wrong-trace-width", "wrong-span-width"} {
		t.Run(kind, func(t *testing.T) {
			in := linkEvidenceFixture(0, 0)
			p := in.ResourceSpans[0].ScopeSpans[0].Spans[0]
			want := errInvalid
			switch kind {
			case "unknown":
				p.Links[0].ProtoReflect().SetUnknown(unknownWire([]byte("future")))
				want = errUnsupportedMetadata
			case "nil-link":
				p.Links[0] = nil
			case "wrong-trace-width":
				p.Links[0].TraceId = []byte{1}
			case "wrong-span-width":
				p.Links[0].SpanId = make([]byte, 9)
			}
			if b, err := encodeOTLPJSON(in); b != nil || !errors.Is(err, want) {
				t.Fatalf("link bypassed codec refusal: %v", err)
			}
			good := otlpFixture()
			if err := normalizeOwnedOTLP(good); err != nil {
				t.Fatal(err)
			}
			good.ResourceSpans[0].SchemaUrl = "obvious mismatch"
			badView, goodView := comparisonView(t, in), comparisonView(t, good)
			for _, pair := range [][2]spanrecord.Span{{badView, badView}, {badView, goodView}, {goodView, badView}} {
				if equal, err := compareSpanMetadata(pair[0], pair[1]); equal || !errors.Is(err, want) {
					t.Fatal("malformed/unsupported link became a comparison result")
				}
			}
			if kind == "unknown" {
				if _, usable, err := spanrecord.TargetOfLink(p.Links[0]); err != nil || usable {
					t.Fatal("ID-only extraction claimed to validate all metadata")
				}
				if equal, err := compareSpanMetadata(comparisonView(t, in), comparisonView(t, in)); equal || !errors.Is(err, want) {
					t.Fatal("missing target bypassed unknown-field comparison guard")
				}
			}
		})
	}
	in := linkEvidenceFixture(2, 2)
	p := in.ResourceSpans[0].ScopeSpans[0].Spans[0]
	p.TraceId = nil
	if equal, err := compareSpanMetadata(comparisonView(t, in), comparisonView(t, in)); equal || err == nil {
		t.Fatal("valid link target replaced invalid own identity")
	}
}

func FuzzOTLPLinkEvidence(f *testing.F) {
	for trace := uint8(0); trace < 3; trace++ {
		for span := uint8(0); span < 3; span++ {
			f.Add(trace, span, uint32(1<<31), int64(42))
		}
	}
	f.Fuzz(func(t *testing.T, trace, span uint8, flags uint32, value int64) {
		in := linkEvidenceFixture(trace, span)
		link := in.ResourceSpans[0].ScopeSpans[0].Spans[0].Links[0]
		link.Flags = flags
		link.Attributes[0].Value = otlpInt(value)
		checkLinkEvidence(t, in, trace%3 == 2 && span%3 == 2)
		other := proto.Clone(in).(*tracepb.TracesData)
		other.ResourceSpans[0].ScopeSpans[0].Spans[0].Links[0].Flags ^= 1 << 31
		if checkDirectComparison(t, in, other) {
			t.Fatal("link flags stopped being retry content")
		}
	})
}
