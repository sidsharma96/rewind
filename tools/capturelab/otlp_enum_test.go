package capturelab

import (
	"errors"
	"fmt"
	"testing"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// Numeric preservation does not establish the meaning of a future or erroneous
// enum value. Runtime acceptance and explicit unknown handling in consumers are
// still separate work; no viewer, aggregate or replay adapter is tested here.
func TestOTLPCodecEnumNumbers(t *testing.T) {
	for _, tc := range []struct {
		name       string
		kind, code int32
	}{
		{"known", 3, 2}, {"known-zero", 0, 0},
		{"kind99", 99, 2}, {"status99", 3, 99},
		{"kind-negative", -1, 2}, {"status-negative", 3, -1},
		{"kind-min", -2147483648, 2}, {"status-min", 3, -2147483648},
		{"kind-max", 2147483647, 2}, {"status-max", 3, 2147483647},
		{"both-unknown", 99, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			checkOTLPEnumRoundTrip(t, tc.kind, tc.code)
		})
	}
}

func checkOTLPEnumRoundTrip(t *testing.T, kind, code int32) {
	t.Helper()
	in := otlpFixture()
	span := in.ResourceSpans[0].ScopeSpans[0].Spans[0]
	span.Kind, span.Status.Code = tracepb.Span_SpanKind(kind), tracepb.Status_StatusCode(code)
	if err := normalizeOwnedOTLP(in); err != nil {
		t.Fatal(err)
	}
	if int32(span.Kind) != kind || int32(span.Status.Code) != code {
		t.Fatal("normalization reinterpreted an enum number")
	}
	before := proto.Clone(in)
	b, err := encodeOTLPJSON(in)
	if err != nil || !proto.Equal(in, before) {
		t.Fatalf("encoding refused or changed enum metadata: %v", err)
	}
	// Only independently encoded bytes survive the caller's later mutation.
	span.Kind, span.Status.Code = tracepb.Span_SPAN_KIND_INTERNAL, tracepb.Status_STATUS_CODE_OK
	out, err := decodeOTLPJSON(b)
	if err != nil || !proto.Equal(out, before) {
		t.Fatalf("enum number or other metadata changed after decoding: %v", err)
	}
}

func TestOTLPCodecEnumInputRefusal(t *testing.T) {
	for _, field := range []string{`"kind":%s`, `"status":{"code":%s}`} {
		packet := func(value string) []byte {
			return []byte(`{"resourceSpans":[{"scopeSpans":[{"spans":[{` + fmt.Sprintf(field, value) + `}]}]}]}`)
		}
		// These fixtures isolate parsing: the lab decoder does not require IDs.
		// A valid numeric control must pass before refusal can be attributed to
		// an out-of-range/non-integral number or unsupported enum name.
		if _, err := decodeOTLPJSON(packet("99")); err != nil {
			t.Fatal(err)
		}
		for _, value := range []string{"2147483648", "-2147483649", "1e100", "1.5", `"FUTURE_ENUM"`, `"99"`, "true"} {
			if out, err := decodeOTLPJSON(packet(value)); !errors.Is(err, errInvalid) || out != nil {
				t.Fatalf("field=%s value=%s: invalid enum returned a usable message", field, value)
			}
		}
	}
}

func TestOTLPCodecUnknownEnumDoesNotPermitUnknownFields(t *testing.T) {
	in := otlpFixture()
	span := in.ResourceSpans[0].ScopeSpans[0].Spans[0]
	span.Kind, span.Status.Code = 99, -1
	span.Status.ProtoReflect().SetUnknown(unknownWire([]byte("future field")))
	before := proto.Clone(in)
	if err := normalizeOwnedOTLP(in); !errors.Is(err, errUnsupportedMetadata) || !proto.Equal(in, before) {
		t.Fatal("unknown enum bypassed unsupported-field refusal or allowed mutation")
	}
	if b, err := encodeOTLPJSON(in); !errors.Is(err, errUnsupportedMetadata) || b != nil || !proto.Equal(in, before) {
		t.Fatal("unknown enum allowed an unsupported field to be encoded")
	}
}

func FuzzOTLPEnumNumbers(f *testing.F) {
	for _, pair := range [][2]int32{{0, 0}, {3, 2}, {99, -1}, {-2147483648, 2147483647}, {2147483647, -2147483648}} {
		f.Add(pair[0], pair[1])
	}
	f.Fuzz(func(t *testing.T, kind, code int32) {
		checkOTLPEnumRoundTrip(t, kind, code)
	})
}
