package capturelab

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

func floatMetadata(bits uint64) *tracepb.TracesData {
	in := otlpFixture()
	in.ResourceSpans[0].ScopeSpans[0].Spans[0].Attributes = []*commonpb.KeyValue{{
		Key: "value", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{
			DoubleValue: math.Float64frombits(bits),
		}},
	}}
	return in
}

func floatMetadataValue(in *tracepb.TracesData) *commonpb.AnyValue {
	return in.ResourceSpans[0].ScopeSpans[0].Spans[0].Attributes[0].Value
}

// Independent bit-class oracle: finite nonzero values and infinities have one
// relevant encoding, both zeros form one class, and all NaN encodings form one.
// This does not use proto.Equal or the production library's float comparison.
func sameFloatValueBits(a, b uint64) bool {
	const magnitude = uint64(0x7fffffffffffffff)
	const exponent = uint64(0x7ff0000000000000)
	const fraction = uint64(0x000fffffffffffff)
	nan := func(v uint64) bool { return v&exponent == exponent && v&fraction != 0 }
	return a == b || (a&magnitude == 0 && b&magnitude == 0) || (nan(a) && nan(b))
}

// checkFloatMetadata checks the existing lab codec and comparison projection,
// not a runtime comparator, complete validator or bit-exact NaN saved format.
func checkFloatMetadata(t testing.TB, bits uint64) ([]byte, *tracepb.TracesData) {
	t.Helper()
	in := floatMetadata(bits)
	if err := normalizeOwnedOTLP(in); err != nil {
		t.Fatal(err)
	}
	before := proto.Clone(in)
	b, err := encodeOTLPJSON(in)
	if err != nil || !proto.Equal(in, before) || math.Float64bits(floatMetadataValue(in).GetDoubleValue()) != bits {
		t.Fatalf("ordering or encoding refused/modified floating metadata: %v", err)
	}
	// Mutate the source after encoding; decoded output must own its contents.
	floatMetadataValue(in).Value = &commonpb.AnyValue_StringValue{StringValue: "changed"}
	out, err := decodeOTLPJSON(b)
	if err != nil || !proto.Equal(out, before) {
		t.Fatalf("decoded floating value or other metadata changed: %v", err)
	}
	v, ok := floatMetadataValue(out).Value.(*commonpb.AnyValue_DoubleValue)
	if !ok {
		t.Fatal("floating value lost its type")
	}
	got := math.Float64bits(v.DoubleValue)
	if !sameFloatValueBits(bits, got) {
		t.Fatal("decoded float changed value class")
	}
	// Saving preserves finite values, both zero signs and signed infinities.
	// NaN diagnostic bits are deliberately outside the selected JSON contract.
	if math.IsNaN(math.Float64frombits(bits)) {
		if !bytes.Contains(b, []byte(`"doubleValue":"NaN"`)) {
			t.Fatal("NaN lost its explicit saved category")
		}
	} else if got != bits {
		t.Fatal("non-NaN double changed bits in storage")
	}
	return b, out
}

func TestOTLPFloatingMetadata(t *testing.T) {
	values := []uint64{
		0, 0x8000000000000000, // Both zero signs.
		1, 0x000fffffffffffff, // Smallest subnormal, largest subnormal.
		0x3ff0000000000000, 0x3ff0000000000001, 0xbff0000000000000,
		0x7fefffffffffffff, 0xffefffffffffffff, // Largest finite magnitudes.
		0x7ff0000000000000, 0xfff0000000000000, // Signed infinities.
		0x7ff8000000000001, 0x7ff8000000000002, 0xfff8000000000001, 0x7ff0000000000001,
	}
	for _, bits := range values {
		t.Run(fmt.Sprintf("%016x", bits), func(t *testing.T) { checkFloatMetadata(t, bits) })
	}
	for _, a := range values {
		for _, b := range values {
			left, right := projectPresence(t, floatMetadata(a)), projectPresence(t, floatMetadata(b))
			if proto.Equal(left, right) != sameFloatValueBits(a, b) {
				t.Fatalf("pair %016x/%016x disagrees with bit-class oracle", a, b)
			}
		}
	}
	t.Log("15 float encodings round-trip by value; 225 comparisons match the bit-class oracle")
}

func TestOTLPFloatingRepresentation(t *testing.T) {
	positiveZero, _ := checkFloatMetadata(t, 0)
	negativeZero, _ := checkFloatMetadata(t, 0x8000000000000000)
	if bytes.Equal(positiveZero, negativeZero) {
		t.Fatal("encoder erased the original zero sign")
	}
	nan1, _ := checkFloatMetadata(t, 0x7ff8000000000001)
	nan2, _ := checkFloatMetadata(t, 0x7ff8000000000002)
	if !bytes.Equal(nan1, nan2) {
		t.Fatal("JSON introduced a bit-exact NaN representation")
	}
	t.Log("zero signs keep different saved forms; NaN diagnostic patterns use one saved spelling")
}

func TestOTLPFloatingTypesRemainDistinct(t *testing.T) {
	want := projectPresence(t, floatMetadata(math.Float64bits(12)))
	for _, other := range []*commonpb.AnyValue{otlpInt(12), otlpString("12"), {}, nil} {
		in := floatMetadata(math.Float64bits(12))
		in.ResourceSpans[0].ScopeSpans[0].Spans[0].Attributes[0].Value = other
		if proto.Equal(want, projectPresence(t, in)) {
			t.Fatal("float became equal to another type or absent value")
		}
	}
}

func TestOTLPFloatingNestedValues(t *testing.T) {
	_, positions := otlpUnknownFixture()
	for i, position := range positions {
		if _, ok := position.message.(*commonpb.AnyValue); !ok {
			continue
		}
		t.Run(position.name, func(t *testing.T) {
			build := func(bits uint64) (*tracepb.TracesData, *commonpb.AnyValue) {
				in, targets := otlpUnknownFixture()
				value := targets[i].message.(*commonpb.AnyValue)
				value.Value = &commonpb.AnyValue_DoubleValue{DoubleValue: math.Float64frombits(bits)}
				return in, value
			}
			for _, pair := range [][2]uint64{
				{0, 0x8000000000000000}, {0x7ff8000000000001, 0xfff8000000000002},
				{0x3ff0000000000000, 0x3ff0000000000001},
			} {
				left, value := build(pair[0])
				right, _ := build(pair[1])
				if proto.Equal(projectPresence(t, left), projectPresence(t, right)) != sameFloatValueBits(pair[0], pair[1]) {
					t.Fatal("nested floating comparison changed semantics")
				}
				encoded, err := encodeOTLPJSON(left)
				if err != nil || math.Float64bits(value.GetDoubleValue()) != pair[0] {
					t.Fatal("nested encoding or projection changed borrowed bits")
				}
				decoded, err := decodeOTLPJSON(encoded)
				if err != nil || !proto.Equal(decoded, left) {
					t.Fatal("nested floating round trip changed typed metadata")
				}
			}
		})
	}
}

func TestOTLPFloatingInputRefusal(t *testing.T) {
	b, _ := checkFloatMetadata(t, math.Float64bits(1))
	const field = `"doubleValue":1`
	if bytes.Count(b, []byte(field)) != 1 {
		t.Fatal("fixture does not isolate the float field")
	}
	for _, raw := range []string{"1e1000", "-1e1000", "NaN", `"not-a-number"`, "true", "[]"} {
		in := bytes.Replace(b, []byte(field), []byte(`"doubleValue":`+raw), 1)
		if out, err := decodeOTLPJSON(in); !errors.Is(err, errInvalid) || out != nil {
			t.Fatalf("%s returned a usable malformed float", raw)
		}
	}
}

func FuzzOTLPFloatingMetadata(f *testing.F) {
	for _, pair := range [][2]uint64{
		{0, 0x8000000000000000}, {0x7ff8000000000001, 0xfff8000000000002},
		{0x7ff0000000000000, 0xfff0000000000000}, {0x3ff0000000000000, 0x3ff0000000000001},
	} {
		f.Add(pair[0], pair[1])
	}
	f.Fuzz(func(t *testing.T, a, b uint64) {
		_, left := checkFloatMetadata(t, a)
		_, right := checkFloatMetadata(t, b)
		if proto.Equal(projectPresence(t, left), projectPresence(t, right)) != sameFloatValueBits(a, b) {
			t.Fatal("saved comparison disagrees with input bit-class oracle")
		}
	})
}
