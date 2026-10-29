package capturelab

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"unicode/utf8"

	spanrecord "github.com/sidsharma96/rewind/internal/record"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func comparisonView(t testing.TB, in *tracepb.TracesData) spanrecord.Span {
	t.Helper()
	view, err := spanrecord.SpanAt(in, 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	return view
}

// Normalize owned fixtures before the operation, then compare against the copying
// oracle in both directions. Binary snapshots also catch changes to NaN payloads
// and zero signs that proto.Equal would intentionally ignore.
func checkDirectComparison(t testing.TB, a, b *tracepb.TracesData) bool {
	t.Helper()
	for _, in := range []*tracepb.TracesData{a, b} {
		if err := normalizeOwnedOTLP(in); err != nil {
			t.Fatal(err)
		}
	}
	snapshot := func(in *tracepb.TracesData) []byte {
		wire, err := (proto.MarshalOptions{Deterministic: true}).Marshal(in)
		if err != nil {
			t.Fatal(err)
		}
		return wire
	}
	beforeA, beforeB := snapshot(a), snapshot(b)
	av, bv := comparisonView(t, a), comparisonView(t, b)
	pa, err := spanComparisonProjection(av)
	if err != nil {
		t.Fatal(err)
	}
	pb, err := spanComparisonProjection(bv)
	if err != nil {
		t.Fatal(err)
	}
	want := proto.Equal(pa, pb)
	for _, pair := range [][2]spanrecord.Span{{av, bv}, {bv, av}, {av, av}, {bv, bv}} {
		expected := want
		if pair[0].Metadata == pair[1].Metadata {
			expected = true
		}
		got, err := compareSpanMetadata(pair[0], pair[1])
		if err != nil || got != expected {
			t.Fatalf("direct=%v, oracle=%v, err=%v", got, expected, err)
		}
	}
	if !bytes.Equal(beforeA, snapshot(a)) || !bytes.Equal(beforeB, snapshot(b)) {
		t.Fatal("comparison mutated metadata, numeric bits, presence or sequence order")
	}
	return want
}

// Freeze the special-position assumption and every current top-level field.
// A dependency upgrade must revisit this fence, not silently change retry rules.
func TestOTLPDirectComparisonSchema(t *testing.T) {
	fields := (&tracepb.Span{}).ProtoReflect().Descriptor().Fields()
	var got []string
	for i := 0; i < fields.Len(); i++ {
		f := fields.Get(i)
		got = append(got, fmt.Sprintf("%s:%d:%s:%s", f.Name(), f.Number(), f.Kind(), f.Cardinality()))
	}
	want := strings.Fields(`trace_id:1:bytes:optional span_id:2:bytes:optional
 trace_state:3:string:optional parent_span_id:4:bytes:optional
 flags:16:fixed32:optional name:5:string:optional kind:6:enum:optional
 start_time_unix_nano:7:fixed64:optional end_time_unix_nano:8:fixed64:optional
 attributes:9:message:repeated dropped_attributes_count:10:uint32:optional
 events:11:message:repeated dropped_events_count:12:uint32:optional
 links:13:message:repeated dropped_links_count:14:uint32:optional status:15:message:optional`)
	if !slices.Equal(got, want) {
		t.Fatalf("Span schema changed; revisit comparison coverage:\n%v", got)
	}
}

func TestOTLPDirectComparisonFields(t *testing.T) {
	fields := (&tracepb.Span{}).ProtoReflect().Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		t.Run(string(field.Name()), func(t *testing.T) {
			a, b := otlpFixture(), otlpFixture()
			m := b.ResourceSpans[0].ScopeSpans[0].Spans[0].ProtoReflect()
			switch {
			case field.IsList():
				m.Mutable(field).List().Truncate(0)
			case field.Kind() == protoreflect.MessageKind:
				m.Clear(field)
			case field.Kind() == protoreflect.BytesKind:
				v := slices.Clone(m.Get(field).Bytes())
				v[0] ^= 1
				m.Set(field, protoreflect.ValueOfBytes(v))
			case field.Kind() == protoreflect.StringKind:
				m.Set(field, protoreflect.ValueOfString(m.Get(field).String()+"changed"))
			case field.Kind() == protoreflect.EnumKind:
				m.Set(field, protoreflect.ValueOfEnum(m.Get(field).Enum()+1))
			case field.Kind() == protoreflect.Fixed64Kind:
				m.Set(field, protoreflect.ValueOfUint64(m.Get(field).Uint()+1))
			case field.Kind() == protoreflect.Fixed32Kind || field.Kind() == protoreflect.Uint32Kind:
				m.Set(field, protoreflect.ValueOfUint32(uint32(m.Get(field).Uint()+1)))
			default:
				t.Fatalf("add a mutation for %s", field.Kind())
			}
			if checkDirectComparison(t, a, b) {
				t.Fatal("defined Span field was omitted")
			}
		})
	}
	for _, name := range []string{"resource-url", "scope-url", "resource-count", "resource-entity", "scope-version", "scope-count", "unknown-kind", "unknown-status", "high-link-flag"} {
		t.Run(name, func(t *testing.T) {
			a, b := otlpFixture(), otlpFixture()
			r := b.ResourceSpans[0]
			s := r.ScopeSpans[0]
			p := s.Spans[0]
			switch name {
			case "resource-url":
				r.SchemaUrl += "changed"
			case "scope-url":
				s.SchemaUrl += "changed"
			case "resource-count":
				r.Resource.DroppedAttributesCount++
			case "resource-entity":
				r.Resource.EntityRefs = []*commonpb.EntityRef{{Type: "service"}}
			case "scope-version":
				s.Scope.Version += "changed"
			case "scope-count":
				s.Scope.DroppedAttributesCount++
			case "unknown-kind":
				p.Kind = -2147483648
			case "unknown-status":
				p.Status.Code = 2147483647
			case "high-link-flag":
				p.Links[0].Flags ^= 1 << 31
			}
			if checkDirectComparison(t, a, b) {
				t.Fatal("context or uninterpreted numeric evidence was omitted")
			}
		})
	}
}

func TestOTLPDirectComparisonPolicies(t *testing.T) {
	for left := uint8(0); left < 8; left++ {
		for right := uint8(0); right < 8; right++ {
			if !checkDirectComparison(t, presenceFixture(left), presenceFixture(right)) {
				t.Fatal("empty-shell rule changed")
			}
		}
	}
	bits := []uint64{0, 1 << 63, 1, 0x3ff0000000000000, 0x3ff0000000000001, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff0000000000001, 0xfff8000000000042}
	for _, a := range bits {
		for _, b := range bits {
			if checkDirectComparison(t, floatMetadata(a), floatMetadata(b)) != sameFloatValueBits(a, b) {
				t.Fatal("numeric equality changed")
			}
		}
	}
	for _, name := range []string{"any-presence", "typed-empty", "numeric-type", "array-order", "event-order", "link-order"} {
		t.Run(name, func(t *testing.T) {
			a := presenceFixture(7)
			p := a.ResourceSpans[0].ScopeSpans[0].Spans[0]
			p.Attributes = []*commonpb.KeyValue{{Key: "value"}}
			p.Events = append(p.Events, &tracepb.Span_Event{Name: "second"})
			p.Links = append(p.Links, &tracepb.Span_Link{TraceState: "second"})
			switch name {
			case "typed-empty":
				p.Attributes[0].Value = &commonpb.AnyValue{}
			case "numeric-type":
				p.Attributes[0].Value = otlpInt(0)
			case "array-order":
				p.Attributes[0].Value = &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{Values: []*commonpb.AnyValue{otlpInt(1), otlpInt(2)}}}}
			}
			b := proto.Clone(a).(*tracepb.TracesData)
			q := b.ResourceSpans[0].ScopeSpans[0].Spans[0]
			switch name {
			case "any-presence":
				q.Attributes[0].Value = &commonpb.AnyValue{}
			case "typed-empty":
				q.Attributes[0].Value = &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{}}}
			case "numeric-type":
				q.Attributes[0].Value = &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: 0}}
			case "array-order":
				slices.Reverse(q.Attributes[0].Value.GetArrayValue().Values)
			case "event-order":
				slices.Reverse(q.Events)
			case "link-order":
				slices.Reverse(q.Links)
			}
			if checkDirectComparison(t, a, b) {
				t.Fatal("narrow equivalence was broadened")
			}
		})
	}
	t.Log("direct comparison agrees with copying oracle; inputs retain their original bytes")
}

func TestOTLPDirectComparisonRequiresOrdering(t *testing.T) {
	_, names := normalizationFixture()
	for i := range names {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			in, lists := normalizationFixture()
			oracleAttributeOrder(lists)
			slices.Reverse(*lists[i]) // Exactly one keyed collection is unordered.
			view, err := spanrecord.SpanAt(in, 1, 1, 1)
			if err != nil {
				t.Fatal(err)
			}
			before, err := proto.Marshal(in)
			if err != nil {
				t.Fatal(err)
			}
			if equal, err := compareSpanMetadata(view, view); equal || !errors.Is(err, errUnorderedMetadata) {
				t.Fatalf("unordered comparison: %v %v", equal, err)
			}
			after, err := proto.Marshal(in)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("comparison sorted borrowed data")
			}
		})
	}
}

func TestOTLPDirectComparisonNestedMetadata(t *testing.T) {
	in, lists := normalizationFixture()
	oracleAttributeOrder(lists)
	view, err := spanrecord.SpanAt(in, 1, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	packet, err := view.OTLP()
	if err != nil {
		t.Fatal(err)
	}
	if !checkDirectComparison(t, packet, proto.Clone(packet).(*tracepb.TracesData)) {
		t.Fatal("equal nested arrays and keyed lists differed")
	}
}

func TestOTLPDirectComparisonRefusesUnknown(t *testing.T) {
	_, names := otlpUnknownFixture()
	for i, target := range names {
		t.Run(target.name, func(t *testing.T) {
			in, targets := otlpUnknownFixture()
			if err := normalizeOwnedOTLP(in); err != nil {
				t.Fatal(err)
			}
			// Wrapper fields must fail selection; a Span view cannot represent them.
			targets[i].message.ProtoReflect().SetUnknown(unknownWire([]byte("future")))
			if i == 0 || i == 1 || i == 5 {
				if _, err := spanrecord.SpanAt(in, 0, 0, 0); !errors.Is(err, spanrecord.ErrUnsupported) {
					t.Fatal("wrapper evidence escaped selection")
				}
				return
			}
			bad := comparisonView(t, in)
			good := otlpFixture()
			if err := normalizeOwnedOTLP(good); err != nil {
				t.Fatal(err)
			}
			good.ResourceSpans[0].SchemaUrl = "obvious mismatch before nested comparison"
			other := comparisonView(t, good)
			for _, pair := range [][2]spanrecord.Span{{bad, bad}, {bad, other}, {other, bad}} {
				if equal, err := compareSpanMetadata(pair[0], pair[1]); equal || !errors.Is(err, errUnsupportedMetadata) {
					t.Fatalf("unsupported data became a comparison: %v %v", equal, err)
				}
			}
			if len(targets[i].message.ProtoReflect().GetUnknown()) == 0 {
				t.Fatal("comparison erased unknown evidence")
			}
		})
	}
}

func TestOTLPDirectComparisonErrorsAreNotConflicts(t *testing.T) {
	for _, name := range []string{"missing", "bad-id", "oversize", "duplicate", "cycle", "nil-oneof", "nil-list", "unordered"} {
		t.Run(name, func(t *testing.T) {
			in := otlpFixture()
			if err := normalizeOwnedOTLP(in); err != nil {
				t.Fatal(err)
			}
			bad := comparisonView(t, in)
			p := bad.Metadata
			switch name {
			case "missing":
				bad = spanrecord.Span{}
			case "bad-id":
				p.TraceId = []byte{1}
			case "oversize":
				bad.Context.ScopeSchemaURL = strings.Repeat("x", maxMetadata+1)
			case "duplicate":
				p.Attributes = []*commonpb.KeyValue{{Key: "same"}, {Key: "same"}}
			case "cycle":
				v := &commonpb.AnyValue{}
				v.Value = &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{Values: []*commonpb.AnyValue{v}}}
				p.Attributes = []*commonpb.KeyValue{{Key: "cycle", Value: v}}
			case "nil-oneof":
				p.Attributes = []*commonpb.KeyValue{{Key: "nil", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{}}}}
			case "nil-list":
				p.Events = []*tracepb.Span_Event{nil}
			case "unordered":
				slices.Reverse(p.Attributes)
			}
			good := otlpFixture()
			if err := normalizeOwnedOTLP(good); err != nil {
				t.Fatal(err)
			}
			good.ResourceSpans[0].SchemaUrl = "obvious mismatch"
			other := comparisonView(t, good)
			for _, pair := range [][2]spanrecord.Span{{bad, bad}, {bad, other}, {other, bad}} {
				if equal, err := compareSpanMetadata(pair[0], pair[1]); equal || err == nil {
					t.Fatalf("invalid data became equal/conflict: %v %v", equal, err)
				}
			}
		})
	}
}

func FuzzOTLPDirectComparison(f *testing.F) {
	for choice := uint8(0); choice < 60; choice++ {
		f.Add(choice, uint8(0), uint8(7), uint64(0), uint64(1<<63), uint32(1<<31), int32(-2147483648), "synthetic")
	}
	f.Add(uint8(0), uint8(7), uint8(0), uint64(0x7ff0000000000001), uint64(0xfff8000000000042), uint32(0), int32(99), "")
	f.Fuzz(func(t *testing.T, choice, left, right uint8, abits, bbits uint64, flags uint32, number int32, text string) {
		if len(text) > 1024 || !utf8.ValidString(text) {
			return
		}
		a, b := presenceFixture(left), presenceFixture(right)
		p, q := a.ResourceSpans[0].ScopeSpans[0].Spans[0], b.ResourceSpans[0].ScopeSpans[0].Spans[0]
		p.Attributes = []*commonpb.KeyValue{{Key: "value", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: math.Float64frombits(abits)}}}}
		q.Attributes = []*commonpb.KeyValue{{Key: "value", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: math.Float64frombits(bbits)}}}}
		switch choice % 12 {
		case 1:
			q.Flags = flags
		case 2:
			p.Links[0].Flags = flags
			q.Links[0].Flags = flags ^ 1
		case 3:
			q.Kind = tracepb.Span_SpanKind(number)
		case 4:
			q.Status = &tracepb.Status{Code: tracepb.Status_StatusCode(number)}
		case 5:
			b.ResourceSpans[0].SchemaUrl = text
		case 6:
			b.ResourceSpans[0].ScopeSpans[0].SchemaUrl = text
		case 7:
			q.Name = text
		case 8:
			q.Attributes[0].Value = otlpString(text)
		case 9:
			q.Attributes[0].Value = &commonpb.AnyValue{Value: &commonpb.AnyValue_BytesValue{BytesValue: []byte(text)}}
		case 10:
			q.Events = append(q.Events, &tracepb.Span_Event{Name: text})
		case 11:
			q.Links[0].TraceState = text
		}
		// Repeat the generated values at nested and embedded positions, so the
		// oracle checks the recursive equality path as well as root attributes.
		for _, span := range []*tracepb.Span{p, q} {
			value := span.Attributes[0].Value
			switch (choice / 12) % 5 {
			case 1:
				span.Attributes[0].Value = &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{Values: []*commonpb.AnyValue{value}}}}
			case 2:
				span.Attributes[0].Value = &commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{KvlistValue: &commonpb.KeyValueList{Values: []*commonpb.KeyValue{{Key: "nested", Value: value}}}}}
			case 3:
				span.Events[0].Attributes[0].Value = value
			case 4:
				span.Links[0].Attributes[0].Value = value
			}
		}
		checkDirectComparison(t, a, b)
	})
}
