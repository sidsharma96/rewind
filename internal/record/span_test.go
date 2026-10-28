package record_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/sidsharma96/rewind/internal/record"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func packetFixture() *tracepb.TracesData {
	in := &tracepb.TracesData{}
	for r := 0; r < 2; r++ {
		rs := &tracepb.ResourceSpans{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{{
				Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: fmt.Sprintf("service-%d", r)}},
			}}},
			SchemaUrl: fmt.Sprintf("https://resource.example/%d", r),
		}
		for s := 0; s < 2; s++ {
			ss := &tracepb.ScopeSpans{
				Scope:     &commonpb.InstrumentationScope{Name: fmt.Sprintf("library-%d-%d", r, s)},
				SchemaUrl: fmt.Sprintf("https://scope.example/%d/%d", r, s),
			}
			for p := 0; p < 2; p++ {
				ss.Spans = append(ss.Spans, &tracepb.Span{Name: fmt.Sprintf("call-%d-%d-%d", r, s, p)})
			}
			rs.ScopeSpans = append(rs.ScopeSpans, ss)
		}
		in.ResourceSpans = append(in.ResourceSpans, rs)
	}
	return in
}

func TestSpanAtContextAssociation(t *testing.T) {
	in := packetFixture()
	before := proto.Clone(in)
	for r := 0; r < 2; r++ {
		for s := 0; s < 2; s++ {
			for p := 0; p < 2; p++ {
				got, err := record.SpanAt(in, r, s, p)
				if err != nil {
					t.Fatal(err)
				}
				if got.Metadata.Name != fmt.Sprintf("call-%d-%d-%d", r, s, p) ||
					got.Context.Resource.Attributes[0].Value.GetStringValue() != fmt.Sprintf("service-%d", r) ||
					got.Context.Scope.Name != fmt.Sprintf("library-%d-%d", r, s) ||
					got.Context.ResourceSchemaURL != fmt.Sprintf("https://resource.example/%d", r) ||
					got.Context.ScopeSchemaURL != fmt.Sprintf("https://scope.example/%d/%d", r, s) {
					t.Fatal("selected span associated with wrong context")
				}
				out, err := got.OTLP()
				if err != nil || len(out.ResourceSpans) != 1 || len(out.ResourceSpans[0].ScopeSpans) != 1 || len(out.ResourceSpans[0].ScopeSpans[0].Spans) != 1 {
					t.Fatal("reconstruction includes missing or sibling records")
				}
				// Clone-and-prune is the deliberately simple independent baseline.
				want := proto.Clone(in).(*tracepb.TracesData)
				want.ResourceSpans = want.ResourceSpans[r : r+1]
				want.ResourceSpans[0].ScopeSpans = want.ResourceSpans[0].ScopeSpans[s : s+1]
				want.ResourceSpans[0].ScopeSpans[0].Spans = want.ResourceSpans[0].ScopeSpans[0].Spans[p : p+1]
				if !proto.Equal(out, want) {
					t.Fatal("reconstructed packet differs from baseline")
				}
			}
		}
	}
	if !proto.Equal(in, before) {
		t.Fatal("selection/reconstruction changed source")
	}
}

func TestSpanAtInvalidPath(t *testing.T) {
	for _, indexes := range [][3]int{{-1, 0, 0}, {2, 0, 0}, {0, -1, 0}, {0, 2, 0}, {0, 0, -1}, {0, 0, 2}} {
		got, err := record.SpanAt(packetFixture(), indexes[0], indexes[1], indexes[2])
		if !errors.Is(err, record.ErrInvalid) || got != (record.Span{}) {
			t.Fatal("invalid path returned a partial view or wrong error")
		}
	}
	for _, location := range []string{"nil", "empty", "resource", "scope", "span"} {
		in := packetFixture()
		switch location {
		case "nil":
			in = nil
		case "empty":
			in.ResourceSpans = nil
		case "resource":
			in.ResourceSpans[0] = nil
		case "scope":
			in.ResourceSpans[0].ScopeSpans[0] = nil
		case "span":
			in.ResourceSpans[0].ScopeSpans[0].Spans[0] = nil
		}
		got, err := record.SpanAt(in, 0, 0, 0)
		if !errors.Is(err, record.ErrInvalid) || got != (record.Span{}) {
			t.Fatalf("%s: missing path returned a partial view", location)
		}
	}
	if out, err := (record.Span{}).OTLP(); !errors.Is(err, record.ErrInvalid) || out != nil {
		t.Fatal("zero span reconstructed")
	}
}

func TestSpanContextPresence(t *testing.T) {
	for _, resource := range []bool{false, true} {
		for _, scope := range []bool{false, true} {
			in := packetFixture()
			rs := in.ResourceSpans[0]
			ss := rs.ScopeSpans[0]
			rs.Resource = nil
			ss.Scope = nil
			if resource {
				rs.Resource = &resourcepb.Resource{}
			}
			if scope {
				ss.Scope = &commonpb.InstrumentationScope{}
			}
			got, err := record.SpanAt(in, 0, 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			out, err := got.OTLP()
			if err != nil || (out.ResourceSpans[0].Resource != nil) != resource || (out.ResourceSpans[0].ScopeSpans[0].Scope != nil) != scope || out.ResourceSpans[0].SchemaUrl != rs.SchemaUrl || out.ResourceSpans[0].ScopeSpans[0].SchemaUrl != ss.SchemaUrl {
				t.Fatal("optional context presence or schema URLs changed")
			}
		}
	}
}

func unknownWire() []byte {
	return protowire.AppendBytes(protowire.AppendTag(nil, 1001, protowire.BytesType), []byte("future"))
}

func TestSpanWrapperUnknownRefusal(t *testing.T) {
	for _, position := range []string{"packet", "resource", "scope"} {
		in := packetFixture()
		var target proto.Message = in
		if position == "resource" {
			target = in.ResourceSpans[0]
		} else if position == "scope" {
			target = in.ResourceSpans[0].ScopeSpans[0]
		}
		target.ProtoReflect().SetUnknown(unknownWire())
		before := proto.Clone(in)
		got, err := record.SpanAt(in, 0, 0, 0)
		if !errors.Is(err, record.ErrUnsupported) || got != (record.Span{}) || !proto.Equal(in, before) {
			t.Fatalf("%s: wrapper unknowns lost or input changed", position)
		}
	}
	in := packetFixture()
	in.ResourceSpans[1].ProtoReflect().SetUnknown(unknownWire())
	in.ResourceSpans[1].ScopeSpans[0] = nil
	if _, err := record.SpanAt(in, 0, 0, 0); err != nil {
		t.Fatal("single-path selection unexpectedly validates a sibling batch")
	}
}

func TestSpanBorrowedLeavesAndIndependentWrappers(t *testing.T) {
	in := packetFixture()
	got, err := record.SpanAt(in, 1, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	rs := in.ResourceSpans[1]
	ss := rs.ScopeSpans[1]
	if got.Metadata != ss.Spans[1] || got.Context.Resource != rs.Resource || got.Context.Scope != ss.Scope {
		t.Fatal("selection cloned borrowed messages")
	}
	out, err := got.OTLP()
	if err != nil {
		t.Fatal(err)
	}
	if out.ResourceSpans[0].Resource != rs.Resource || out.ResourceSpans[0].ScopeSpans[0].Scope != ss.Scope || out.ResourceSpans[0].ScopeSpans[0].Spans[0] != ss.Spans[1] {
		t.Fatal("reconstruction cloned borrowed messages")
	}
	out.ResourceSpans[0].ScopeSpans[0].Spans[0] = &tracepb.Span{Name: "replacement"}
	out.ResourceSpans[0].ScopeSpans[0].SchemaUrl = "replacement"
	out.ResourceSpans[0].ScopeSpans = nil
	if len(rs.ScopeSpans) != 2 || len(ss.Spans) != 2 || ss.Spans[1].Name != "call-1-1-1" || ss.SchemaUrl != "https://scope.example/1/1" {
		t.Fatal("new wrapper mutation changed source grouping")
	}
}

// Projection reconstructs these wrappers explicitly. A new known schema field
// must fail this coverage test so dependency upgrades cannot silently drop it.
func TestSpanWrapperSchemaCoverage(t *testing.T) {
	for _, item := range []struct {
		message proto.Message
		fields  map[protoreflect.FieldNumber]protoreflect.Name
	}{
		{&tracepb.TracesData{}, map[protoreflect.FieldNumber]protoreflect.Name{1: "resource_spans"}},
		{&tracepb.ResourceSpans{}, map[protoreflect.FieldNumber]protoreflect.Name{1: "resource", 2: "scope_spans", 3: "schema_url"}},
		{&tracepb.ScopeSpans{}, map[protoreflect.FieldNumber]protoreflect.Name{1: "scope", 2: "spans", 3: "schema_url"}},
	} {
		fields := item.message.ProtoReflect().Descriptor().Fields()
		if fields.Len() != len(item.fields) {
			t.Fatal("wrapper schema changed; review selection and reconstruction")
		}
		for number, name := range item.fields {
			field := fields.ByNumber(number)
			if field == nil || field.Name() != name {
				t.Fatal("wrapper field changed; review projection")
			}
		}
	}
}
