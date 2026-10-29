package capturelab

import (
	"errors"
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"
)

// These are representation and scoped comparison checks, not a runtime flags
// interpreter, OTLP receiver, semantic admission policy or full retry comparator.
func TestOTLPCodecFlags(t *testing.T) {
	values := []uint32{0, 1, 255, 256, 512, 768, 1024, 1 << 31, 1<<31 | 1, ^uint32(0)}
	for _, span := range values {
		for _, link := range values {
			t.Run(fmt.Sprintf("%08x-%08x", span, link), func(t *testing.T) {
				checkOTLPFlagsRoundTrip(t, span, link)
			})
		}
	}
	t.Log("100 span/link flag pairs preserve all 32 bits through ordering and JSON")
}

func checkOTLPFlagsRoundTrip(t testing.TB, spanFlags, linkFlags uint32) {
	t.Helper()
	in := otlpFixture()
	p := in.ResourceSpans[0].ScopeSpans[0].Spans[0]
	p.Flags, p.Links[0].Flags = spanFlags, linkFlags
	if err := normalizeOwnedOTLP(in); err != nil {
		t.Fatal(err)
	}
	if p.Flags != spanFlags || p.Links[0].Flags != linkFlags {
		t.Fatal("attribute ordering changed flags")
	}
	before := proto.Clone(in)
	b, err := encodeOTLPJSON(in)
	if err != nil || !proto.Equal(in, before) {
		t.Fatalf("encoding refused or modified flags: %v", err)
	}
	// Flip every bit after encoding. Only the independent bytes survive.
	p.Flags, p.Links[0].Flags = ^spanFlags, ^linkFlags
	out, err := decodeOTLPJSON(b)
	if err != nil || !proto.Equal(out, before) {
		t.Fatalf("decoding changed flags or other metadata: %v", err)
	}
}

func TestOTLPComparisonFlags(t *testing.T) {
	in := otlpFixture()
	p := in.ResourceSpans[0].ScopeSpans[0].Spans[0]
	p.Flags, p.Links[0].Flags = 1, 1
	want := projectPresence(t, in)
	for _, field := range []*uint32{&p.Flags, &p.Links[0].Flags} {
		for bit := uint(0); bit < 32; bit++ {
			*field = 1 ^ (uint32(1) << bit)
			before := proto.Clone(in)
			if proto.Equal(want, projectPresence(t, in)) || !proto.Equal(in, before) {
				t.Fatal("changed flag bit was ignored or source was modified")
			}
		}
		*field = 1
	}
	// Same sampled bit is not the same recorded flags. A reader's mask must
	// never replace full-value comparison, whether on a span or on a link.
	p.Flags = 1<<31 | 1
	if p.Flags&1 != 1 || proto.Equal(want, projectPresence(t, in)) {
		t.Fatal("unknown bit became retry-equivalent to the sampled bit alone")
	}
	t.Log("64 independent flag-bit changes remain distinct in comparison projections")
}

func TestOTLPCodecFlagsInputRefusal(t *testing.T) {
	for _, field := range []string{`"flags":%s`, `"links":[{"flags":%s}]`} {
		packet := func(value string) []byte {
			return []byte(`{"resourceSpans":[{"scopeSpans":[{"spans":[{` + fmt.Sprintf(field, value) + `}]}]}]}`)
		}
		// Isolate scalar parsing: these packets do not prove valid identities.
		// Bit 31 and the maximum uint32 must pass before testing range refusal.
		for _, value := range []string{"2147483648", "4294967295"} {
			if _, err := decodeOTLPJSON(packet(value)); err != nil {
				t.Fatal(err)
			}
		}
		for _, value := range []string{"-1", "4294967296", "1e100", "0.5", `"FUTURE_FLAG"`, "true"} {
			if out, err := decodeOTLPJSON(packet(value)); !errors.Is(err, errInvalid) || out != nil {
				t.Fatalf("field=%s value=%s: invalid flags returned a usable message", field, value)
			}
		}
	}
}

func TestOTLPCodecFlagsDoNotPermitUnknownFields(t *testing.T) {
	for _, link := range []bool{false, true} {
		in := otlpFixture()
		p := in.ResourceSpans[0].ScopeSpans[0].Spans[0]
		p.Flags, p.Links[0].Flags = ^uint32(0), ^uint32(0)
		m := p.ProtoReflect()
		if link {
			m = p.Links[0].ProtoReflect()
		}
		m.SetUnknown(unknownWire([]byte("future field")))
		before := proto.Clone(in)
		if err := normalizeOwnedOTLP(in); !errors.Is(err, errUnsupportedMetadata) || !proto.Equal(in, before) {
			t.Fatal("flags bypassed unsupported-field refusal or source was modified")
		}
		if b, err := encodeOTLPJSON(in); !errors.Is(err, errUnsupportedMetadata) || b != nil || !proto.Equal(in, before) {
			t.Fatal("flags allowed an unsupported wire field to be encoded")
		}
	}
}

func FuzzOTLPFlags(f *testing.F) {
	for _, pair := range [][2]uint32{{0, 0}, {1, 768}, {1024, 1 << 31}, {1<<31 | 1, ^uint32(0)}} {
		f.Add(pair[0], pair[1])
	}
	f.Fuzz(func(t *testing.T, span, link uint32) {
		checkOTLPFlagsRoundTrip(t, span, link)
	})
}
