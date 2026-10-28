package capturelab

import (
	"bytes"
	"google.golang.org/protobuf/proto"
	"testing"
)

// Synthetic otlpFixture encoded independently by Collector pdata v1.70.0
// ptrace.JSONMarshaler, with canonical OTLP v1.9.0 -> protobuf -> pdata bridge.
// Source: https://github.com/open-telemetry/opentelemetry-collector/blob/pdata/v1.70.0/pdata/ptrace/json.go
// This fixed fixture avoids a Collector dependency in normal repository tests.
const collectorTraceJSON = `{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"agent-runner"}}],"droppedAttributesCount":2},"scopeSpans":[{"scope":{"name":"rewind-test","version":"0.0.synthetic","attributes":[{"key":"scope.attr","value":{"intValue":"7"}}],"droppedAttributesCount":1},"spans":[{"traceId":"000102030405060708090a0b0c0d0e0f","spanId":"08090a0b0c0d0e0f","traceState":"synthetic=1","parentSpanId":"0102030405060708","flags":1,"name":"planner","kind":3,"startTimeUnixNano":"1700000000000000001","endTimeUnixNano":"1700000000000000100","attributes":[{"key":"tokens-int","value":{"intValue":"12"}},{"key":"tokens-text","value":{"stringValue":"12"}},{"key":"metadata-bytes","value":{"bytesValue":"AP8H"}}],"droppedAttributesCount":1,"events":[{"timeUnixNano":"1700000000000000010","name":"tool.completed","attributes":[{"key":"event.attr","value":{"stringValue":"event-value"}}],"droppedAttributesCount":1}],"droppedEventsCount":2,"links":[{"traceId":"101112131415161718191a1b1c1d1e1f","spanId":"0102030405060708","traceState":"other=2","attributes":[{"key":"link.attr","value":{"intValue":"9"}}],"droppedAttributesCount":1,"flags":1}],"droppedLinksCount":3,"status":{"message":"synthetic failure","code":2}}],"schemaUrl":"https://example.invalid/scope/v1"}],"schemaUrl":"https://example.invalid/resource/v1"}]}`

func TestOTLPCodecCollectorFixture(t *testing.T) {
	in := otlpFixture()
	for _, b := range [][]byte{
		[]byte(collectorTraceJSON),
		bytes.ReplaceAll([]byte(collectorTraceJSON), []byte("000102030405060708090a0b0c0d0e0f"), []byte("000102030405060708090A0B0C0D0E0F")),
	} {
		out, err := decodeOTLPJSON(b)
		if err != nil || !proto.Equal(in, out) {
			t.Fatalf("Collector fixture changed known fields: %v", err)
		}
	}
	// Compare decoded structure, not whole JSON spelling or field order.
	b, err := encodeOTLPJSON(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := decodeOTLPJSON(b)
	if err != nil {
		t.Fatal(err)
	}
	peer, err := decodeOTLPJSON([]byte(collectorTraceJSON))
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(peer, out) {
		t.Fatal("candidate differs from Collector known metadata")
	}
}
