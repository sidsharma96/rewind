package capturelab

import (
	"bytes"
	"encoding/json"
	"math"
	"strings"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

func TestOTLPCodecValues(t *testing.T) {
	cases := otlpValues()
	for _, value := range []float64{math.Inf(1), math.Inf(-1), math.NaN()} {
		cases = append(cases, struct {
			name string
			in   *commonpb.AnyValue
		}{"special-double", &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: value}}})
	}
	cases = append(cases, struct {
		name string
		in   *commonpb.AnyValue
	}{"ordinary-id-names", &commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{KvlistValue: &commonpb.KeyValueList{
		Values: []*commonpb.KeyValue{{Key: "traceId", Value: otlpString("leave-this-text")}, {Key: "spanId", Value: otlpInt(12)}},
	}}}})
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := otlpFixture()
			span := in.ResourceSpans[0].ScopeSpans[0].Spans[0]
			span.Attributes = append(span.Attributes, &commonpb.KeyValue{Key: "case", Value: tc.in})
			b, err := encodeOTLPJSON(in)
			if err != nil {
				t.Fatal(err)
			}
			out, err := decodeOTLPJSON(b)
			if err != nil || !proto.Equal(in, out) {
				t.Fatalf("typed metadata/context changed: %v", err)
			}
		})
	}
}

func TestOTLPCodecContexts(t *testing.T) {
	emptyStatus := otlpFixture()
	emptyStatus.ResourceSpans[0].ScopeSpans[0].Spans[0].Status = &tracepb.Status{}
	multiple := otlpFixture()
	multiple.ResourceSpans = append(multiple.ResourceSpans, otlpFixture().ResourceSpans[0])
	cases := []*tracepb.TracesData{
		{},
		{ResourceSpans: []*tracepb.ResourceSpans{{}}},
		emptyStatus,
		multiple,
	}
	for _, in := range cases {
		b, err := encodeOTLPJSON(in)
		if err != nil {
			t.Fatal(err)
		}
		out, err := decodeOTLPJSON(b)
		if err != nil || !proto.Equal(in, out) {
			t.Fatalf("context or presence changed: %v", err)
		}
	}
}

func TestOTLPCodecRepresentation(t *testing.T) {
	in := otlpFixture()
	in.ResourceSpans[0].ScopeSpans[0].Spans[0].Name = "<planner>&"
	b, err := encodeOTLPJSON(in)
	if err != nil {
		t.Fatal(err)
	}
	var fields struct {
		Resources []struct {
			Scopes []struct {
				Spans []struct {
					TraceID string             `json:"traceId"`
					SpanID  string             `json:"spanId"`
					Parent  string             `json:"parentSpanId"`
					Kind    int                `json:"kind"`
					StartNS string             `json:"startTimeUnixNano"`
					Status  struct{ Code int } `json:"status"`
					Links   []struct {
						TraceID string `json:"traceId"`
						SpanID  string `json:"spanId"`
					} `json:"links"`
					Attributes []struct {
						Key   string
						Value map[string]json.RawMessage
					} `json:"attributes"`
				} `json:"spans"`
			} `json:"scopeSpans"`
		} `json:"resourceSpans"`
	}
	if err := json.Unmarshal(b, &fields); err != nil {
		t.Fatal(err)
	}
	s := fields.Resources[0].Scopes[0].Spans[0]
	if s.TraceID != "000102030405060708090a0b0c0d0e0f" || s.SpanID != "08090a0b0c0d0e0f" || s.Parent != "0102030405060708" ||
		s.Kind != 3 || s.Status.Code != 2 || s.StartNS != "1700000000000000001" ||
		s.Links[0].TraceID != "101112131415161718191a1b1c1d1e1f" || s.Links[0].SpanID != "0102030405060708" {
		t.Fatal("ID, enum or timestamp representation changed")
	}
	if string(s.Attributes[0].Value["intValue"]) != `"12"` || string(s.Attributes[1].Value["stringValue"]) != `"12"` ||
		string(s.Attributes[2].Value["bytesValue"]) != `"AP8H"` {
		t.Fatal("typed values changed; metadata bytes must remain Base64")
	}
	if !bytes.Contains(b, []byte("<planner>&")) {
		t.Fatal("HTML escaping was added to a JSON record")
	}
}

func TestOTLPCodecOwnership(t *testing.T) {
	in := otlpFixture()
	want := proto.Clone(in)
	b, err := encodeOTLPJSON(in)
	if err != nil {
		t.Fatal(err)
	}
	saved := bytes.Clone(b)
	out, err := decodeOTLPJSON(b)
	if err != nil {
		t.Fatal(err)
	}
	in.ResourceSpans[0].ScopeSpans[0].Spans[0].Name = "changed"
	in.ResourceSpans[0].ScopeSpans[0].Spans[0].Attributes[2].Value.GetBytesValue()[0] = 1
	if !bytes.Equal(b, saved) {
		t.Fatal("encoded bytes share input state")
	}
	clear(b)
	if !proto.Equal(want, out) {
		t.Fatal("decoded fields share input bytes or original message")
	}
	before := proto.Clone(in)
	out.ResourceSpans[0].ScopeSpans[0].Spans[0].Attributes[2].Value.GetBytesValue()[0] = 2
	if !proto.Equal(before, in) {
		t.Fatal("decoded metadata shares original message state")
	}
}

func TestOTLPCodecMalformed(t *testing.T) {
	valid, err := encodeOTLPJSON(otlpFixture())
	if err != nil {
		t.Fatal(err)
	}
	cases := [][]byte{
		nil, []byte(`null`), []byte(`[]`), []byte(`true`), []byte(`{`), []byte(`{} {}`),
		[]byte(`{"resourceSpans":[],"resourceSpans":[]}`),
		[]byte(`{"resourceSpans":[],"\u0072esourceSpans":[]}`),
		[]byte(`{"resourceSpans":[{"scopeSpans":[{"spans":[{"attributes":[{"key":"x","value":{"intValue":"1","intValue":"2"}}]}]}]}]}`),
		[]byte(`{"resourceSpans":{}}`), []byte(`{"resourceSpans":[null]}`),
		append([]byte(`{"resourceSpans":[],"bad":"`), 0xff, '"', '}'),
	}
	for _, id := range []string{`"xyz"`, `"00"`, `"000102030405060708090a0b0c0d0e0f0"`, `null`, `12`} {
		cases = append(cases, bytes.Replace(valid, []byte(`"traceId":"000102030405060708090a0b0c0d0e0f"`), []byte(`"traceId":`+id), 1))
	}
	for i, b := range cases {
		if out, err := decodeOTLPJSON(b); err == nil || out != nil {
			t.Fatalf("case %d returned a usable malformed record", i)
		}
	}
}

func TestOTLPCodecRejectsIDPathAliases(t *testing.T) {
	valid, err := encodeOTLPJSON(otlpFixture())
	if err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2]string{
		{"resourceSpans", "resource_spans"}, {"scopeSpans", "scope_spans"},
		{"traceId", "trace_id"}, {"spanId", "span_id"}, {"parentSpanId", "parent_span_id"},
	} {
		old, replacement := []byte(`"`+pair[0]+`"`), []byte(`"`+pair[1]+`"`)
		last := bytes.LastIndex(valid, old)
		lastOnly := append(bytes.Clone(valid[:last]), bytes.Replace(valid[last:], old, replacement, 1)...)
		for _, b := range [][]byte{
			bytes.Replace(valid, old, replacement, 1),
			bytes.ReplaceAll(valid, old, replacement),
			lastOnly, // Exercise link aliases without an earlier span alias.
		} {
			if out, err := decodeOTLPJSON(b); err == nil || out != nil {
				t.Fatalf("%s bypassed ID conversion", pair[1])
			}
		}
	}
}

func TestOTLPCodecLimits(t *testing.T) {
	padded := append([]byte(`{}`), bytes.Repeat([]byte{' '}, maxMetadata-2)...)
	if _, err := decodeOTLPJSON(padded); err != nil {
		t.Fatal("exact byte bound refused")
	}
	if out, err := decodeOTLPJSON(append(padded, ' ')); err == nil || out != nil {
		t.Fatal("excess input accepted")
	}
	atDepth := strings.Repeat("[", otlpJSONDepth) + "0" + strings.Repeat("]", otlpJSONDepth)
	if checkOTLPJSON([]byte(atDepth)) != nil || checkOTLPJSON([]byte("["+atDepth+"]")) == nil {
		t.Fatal("JSON depth boundary changed")
	}
	for _, size := range []int{maxMetadata + 1, maxMetadata / 2} {
		in := otlpFixture()
		// The second case fits the input budget but expands past the output cap.
		in.ResourceSpans[0].ScopeSpans[0].Spans[0].Name = strings.Repeat("\x00", size)
		if b, err := encodeOTLPJSON(in); err == nil || b != nil {
			t.Fatal("excess input/output returned")
		}
	}
	in := otlpFixture()
	span := in.ResourceSpans[0].ScopeSpans[0].Spans[0]
	span.TraceId = []byte{1}
	if b, err := encodeOTLPJSON(in); err == nil || b != nil {
		t.Fatal("malformed generated ID returned")
	}
	if b, err := encodeOTLPJSON(nil); err == nil || b != nil {
		t.Fatal("nil message returned")
	}
	cycle := &commonpb.ArrayValue{}
	value := &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: cycle}}
	cycle.Values = []*commonpb.AnyValue{value}
	span.TraceId = make([]byte, 16)
	span.Attributes = []*commonpb.KeyValue{{Key: "cycle", Value: value}}
	if b, err := encodeOTLPJSON(in); err == nil || b != nil {
		t.Fatal("cyclic Go message returned")
	}
}

func TestOTLPCodecIsNotReceiverValidation(t *testing.T) {
	// Parsing does not certify a valid execution or a complete OTLP receiver.
	// Empty metadata and all-zero execution IDs still need semantic validation.
	for _, b := range []string{`{}`, `{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"00000000000000000000000000000000","spanId":"0000000000000000"}]}]}]}`} {
		if _, err := decodeOTLPJSON([]byte(b)); err != nil {
			t.Fatal(err)
		}
	}
}

func FuzzOTLPCodec(f *testing.F) {
	b, err := encodeOTLPJSON(otlpFixture())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(b)
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"resourceSpans":[],"resourceSpans":[]}`))
	f.Fuzz(func(t *testing.T, b []byte) {
		in, err := decodeOTLPJSON(b)
		if err != nil {
			if in != nil {
				t.Fatal("partial message returned with error")
			}
			return
		}
		encoded, err := encodeOTLPJSON(in)
		if err != nil {
			// A permitted JSON spelling can expand on re-encoding or exceed
			// the separate Go traversal budget. Success is not admission.
			return
		}
		out, err := decodeOTLPJSON(encoded)
		if err != nil || !proto.Equal(in, out) {
			t.Fatalf("successful paired roundtrip changed metadata: %v", err)
		}
	})
}
