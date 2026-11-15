package httpapi

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/sidsharma96/rewind/internal/record"
)

const diagTrace = "0af7651916cd43dd8448eb211c80319c"

func diagCodec(t *testing.T) record.MetadataCodec {
	t.Helper()
	c, err := record.NewMetadataCodec(record.MetadataLimits{MaxBytes: 1 << 16, MaxWork: 1 << 18, MaxDepth: 64})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func spanID(n byte) string { return hex.EncodeToString([]byte{0, 0, 0, 0, 0, 0, 0, n}) }

type diagSpan struct {
	id      byte
	name    string
	owner   string
	start   uint64
	status  int32
	kind    int32
	parent  []byte
	links   []*tracepb.Span_Link
	message string
}

func diagItem(t *testing.T, s diagSpan) Item {
	t.Helper()
	trace, _ := hex.DecodeString(diagTrace)
	id, _ := hex.DecodeString(spanID(s.id))
	m := &tracepb.Span{TraceId: trace, SpanId: id, Name: s.name, Kind: tracepb.Span_SpanKind(s.kind), ParentSpanId: s.parent, Links: s.links,
		StartTimeUnixNano: s.start, EndTimeUnixNano: s.start + 900_000_000, Status: &tracepb.Status{Code: tracepb.Status_StatusCode(s.status), Message: s.message},
		Attributes: []*commonpb.KeyValue{{Key: "gen_ai.tool.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "search"}}}}}
	packet := &tracepb.TracesData{ResourceSpans: []*tracepb.ResourceSpans{{ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{m}}}}}}
	data, err := diagCodec(t).Encode(packet)
	if err != nil {
		t.Fatal(err)
	}
	owner := hex.EncodeToString([]byte(s.owner))
	return Item{Execution: Execution{Project: Opaque("p"), Trace: diagTrace, Span: spanID(s.id)}, State: "available", Metadata: data,
		Receipt: &Receipt{Instance: owner, Submitter: owner}}
}

func diagBody(state record.BodyState, data string) Body {
	encoded := base64.StdEncoding.EncodeToString([]byte(data))
	if state == record.BodyMissing || state == record.BodyRedacted {
		encoded = ""
	}
	return Body{State: &state, Data: &encoded}
}

func ptr[T any](v T) *T { return &v }

func render(t *testing.T, r DiagnosisRead, limit int) string {
	t.Helper()
	var out bytes.Buffer
	if err := WriteDiagnosis(&out, r, diagCodec(t), limit); err != nil {
		t.Fatal(err)
	}
	return out.String()
}

func baseRead(t *testing.T) DiagnosisRead {
	absent, _ := hex.DecodeString(spanID(99))
	trace, _ := hex.DecodeString(diagTrace)
	other, _ := hex.DecodeString("1af7651916cd43dd8448eb211c80319c")
	saved := func(req, res string) DiagnosisCapture {
		return ReduceCapture(Capture{Request: diagBody(record.BodyPresent, req), Response: diagBody(record.BodyPresent, res)}, true)
	}
	// Span-ID order differs from start order on purpose.
	return DiagnosisRead{Project: Opaque("p"), Trace: diagTrace, Started: time.Unix(10, 0), Finished: time.Unix(11, 0), Spans: []DiagnosisSpan{
		{diagItem(t, diagSpan{id: 1, name: "late", owner: "alice", start: 9e9, status: 1, parent: absent}), saved(`{"q":"same"}`, "late output")},
		{diagItem(t, diagSpan{id: 2, name: "", owner: "bob", start: 2e9, status: 0, kind: 9}), DiagnosisCapture{Outcome: CaptureNone}},
		{diagItem(t, diagSpan{id: 3, name: "tool", owner: "bob", start: 1e9, status: 2, message: "boom",
			links: []*tracepb.Span_Link{{TraceId: trace, SpanId: make([]byte, 8)}, {TraceId: other, SpanId: absent}}}), saved(`{"q":"same"}`, "\x1b[31mred\nCOVERAGE forged‮")},
		{diagItem(t, diagSpan{id: 4, name: "tool", owner: "bob", start: 3e9, status: 7}), saved(`{"q":"same"}`, "x")},
		{diagItem(t, diagSpan{id: 5, name: "redacted", owner: "carol", start: 4e9, status: 1}),
			ReduceCapture(Capture{Request: diagBody(record.BodyRedacted, ""), Response: diagBody(record.BodyMissing, "")}, true)},
		{diagItem(t, diagSpan{id: 6, name: "failed read", owner: "carol", start: 5e9, status: 1}), FailedCapture(&Failure{Code: "busy", Outcome: "refused"})},
		{Item{Execution: Execution{Project: Opaque("p"), Trace: diagTrace, Span: spanID(7)}, State: "no_span"}, DiagnosisCapture{Outcome: CaptureNone}},
	}}
}

func TestDiagnosisViewMakesGapsAndFailuresExplicit(t *testing.T) {
	out := render(t, baseRead(t), 1<<20)
	for _, want := range []string{
		"Read status: the last page reported no continuation.",
		"not proof that the run is complete",
		"items: 7 (available 6, no final span 1, expired 0)",
		"captures: saved 4, no capture saved 2, capture expired 0, capture read failed 1, not read 0",
		"request bodies: present 3, redacted 1",
		"response bodies: present 3, missing 1",
		"capture read failures: #005 busy",
		"parent " + spanID(99) + " (not found in this read) of #006",
		"links to other traces: #001 -> 1af7651916cd43dd8448eb211c80319c/" + spanID(99) + " (not read)",
		"parents or links without a usable target: link of #001",
		"error (code 2): tool x1 -> #001",
		"unset (code 0, not success): (blank name) x1",
		"#003 status 7",
		"#002 span kind 9",
		"same owner bob, tool x2: #001 ERROR, #003 status(7)",
		`attributes: {"key":"gen_ai.tool.name","value":{"stringValue":"search"}}`,
		"across owners: #001 bob, #003 bob, #006 alice",
		"status message: boom",
		`"gen_ai.tool.name"`,
		"request: redacted",
		"response: missing",
		"capture: none saved at read time",
		"capture: read_failed:busy",
		"item state no_span",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in\n%s", want, out)
		}
	}
	// Start order, not span-ID order: span 3 (1s) comes first, span 1 (9s) last.
	if strings.Index(out, "span="+spanID(3)) > strings.Index(out, "span="+spanID(1)) {
		t.Fatal("not in reported start order")
	}
	if strings.Contains(out, "\x1b") || strings.Contains(out, "‮") {
		t.Fatal("terminal control or bidi character passed through")
	}
	if !strings.Contains(out, `\u{1b}[31mred`) || !strings.Contains(out, "    | COVERAGE forged") {
		t.Fatal("captured text not escaped and prefixed")
	}
	if out != render(t, baseRead(t), 1<<20) {
		t.Fatal("output not deterministic")
	}
}

func TestDiagnosisViewStopsHonestly(t *testing.T) {
	r := baseRead(t)
	r.Stop = "span limit 7 reached; more items exist"
	out := render(t, r, 1<<20)
	if !strings.Contains(out, "STOPPED (span limit 7 reached; more items exist)") || !strings.Contains(out, "subset selected by span-ID page order") ||
		!strings.Contains(out, spanID(99)+" (not read: outside this read)") {
		t.Fatalf("early stop not explained:\n%s", out)
	}
	invalid := "!"
	bad := ReduceCapture(Capture{Request: Body{State: ptr(record.BodyPresent), Data: &invalid}, Response: diagBody(record.BodyPresent, "x")}, false)
	r = baseRead(t)
	r.Spans = append(r.Spans[:0:0], DiagnosisSpan{diagItem(t, diagSpan{id: 8, name: "a", owner: "dan", start: 1e9, status: 1}), bad},
		DiagnosisSpan{diagItem(t, diagSpan{id: 9, name: "a", owner: "dan", start: 2e9, status: 1}), bad})
	if out := render(t, r, 1<<20); !strings.Contains(out, "IDENTICAL REQUEST BYTES (same digest; not necessarily retries)\nnone\n") {
		t.Fatalf("undecodable requests grouped as identical:\n%s", out)
	}
	short := render(t, baseRead(t), 600)
	if len(short) > 700 || !strings.HasSuffix(short, "OUTPUT LIMIT REACHED: the remaining sections are not shown.\n") {
		t.Fatalf("output bound not explicit: %d bytes", len(short))
	}
}

func TestDiagnosisCaptureReductionIsBounded(t *testing.T) {
	big := strings.Repeat("y", DiagnosisBodyLimit+10)
	c := ReduceCapture(Capture{Request: diagBody(record.BodyPresent, "q"), Response: diagBody(record.BodyTruncated, big)}, false)
	if len(c.Response.Text) != DiagnosisBodyLimit || c.Response.Note != "+10 bytes not shown; use rewind capture for exact bytes" || c.Response.State != "truncated" {
		t.Fatalf("body not bounded with explicit cut: %d %q", len(c.Response.Text), c.Response.Note)
	}
	if c.Retained > DiagnosisBodyLimit+16 {
		t.Fatal("retained more than the display bound")
	}
	binary := ReduceCapture(Capture{Request: diagBody(record.BodyPresent, "\xff\xfe"), Response: diagBody(record.BodyPresent, "")}, false)
	if binary.Request.Text != "base64://4=" || binary.Response.Size != 0 {
		t.Fatalf("binary or empty body misrendered: %q", binary.Request.Text)
	}
	a := ReduceCapture(Capture{Request: diagBody(record.BodyPresent, "same"), Response: diagBody(record.BodyPresent, "1")}, false)
	b := ReduceCapture(Capture{Request: diagBody(record.BodyPresent, "same"), Response: diagBody(record.BodyPresent, "2")}, false)
	m := ReduceCapture(Capture{Request: diagBody(record.BodyMissing, ""), Response: diagBody(record.BodyPresent, "2")}, false)
	if a.Digest != b.Digest || a.Digest == m.Digest {
		t.Fatal("request digest must depend on request state and bytes only")
	}
	if terminalSafe("a\u200bb\u061cc\ufeff") != `a\u{200b}b\u{61c}c\u{feff}` {
		t.Fatal("invisible format characters not escaped")
	}
	if FailedCapture(&Failure{Code: "not_found"}).Outcome != CaptureNone || FailedCapture(&Failure{Code: "expired"}).Outcome != CaptureExpired ||
		FailedCapture(errors.New("x")).Outcome != "read_failed:transport" {
		t.Fatal("capture failures not distinguished")
	}
}

func TestDiagnosisInterpretsCommandObservationsOnlyWhenSelected(t *testing.T) {
	stdout := base64.StdEncoding.EncodeToString([]byte("value=1.37\n"))
	req, _ := json.Marshal(map[string]any{"version": 1, "tool": "metricsctl query", "args": []string{"metricsctl", "query", "--service", "cart-sync-canary"}})
	res := `{"version":1,"outcome":"exited","exit_code":0,"signal":"","stdout":"` + stdout + `","stderr":""}`
	capture := Capture{Request: diagBody(record.BodyPresent, string(req)), Response: diagBody(record.BodyPresent, res)}
	r := DiagnosisRead{Project: Opaque("p"), Trace: diagTrace, Started: time.Unix(1, 0), Finished: time.Unix(2, 0),
		Spans: []DiagnosisSpan{{diagItem(t, diagSpan{id: 1, name: "metricsctl.query", owner: "bob", start: 1e9, status: 1}), ReduceCapture(capture, true)}}}
	out := render(t, r, 1<<20)
	if !strings.Contains(out, "cart-sync-canary") || !strings.Contains(out, "    | value=1.37") || !strings.Contains(out, "Tool label (reported)") && !strings.Contains(out, "Command arguments") {
		t.Fatalf("command observation not interpreted:\n%s", out)
	}
	// Producer-controlled outcome and signal fields are escaped like captured text,
	// including where identical requests are grouped.
	hostile := `{"version":1,"outcome":"signaled\u001b[2J\nCOVERAGE forged","exit_code":null,"signal":"KILL\u001b\nSTATUS forged","stdout":"","stderr":""}`
	bad := ReduceCapture(Capture{Request: diagBody(record.BodyPresent, string(req)), Response: diagBody(record.BodyPresent, hostile)}, true)
	evil := r
	evil.Spans = []DiagnosisSpan{{diagItem(t, diagSpan{id: 1, name: "q", owner: "bob", start: 1e9, status: 2}), bad},
		{diagItem(t, diagSpan{id: 2, name: "q", owner: "bob", start: 2e9, status: 2}), bad}}
	escaped := render(t, evil, 1<<20)
	for _, l := range strings.Split(escaped, "\n") {
		if strings.HasPrefix(l, "COVERAGE forged") || strings.HasPrefix(l, "STATUS forged") || strings.Contains(l, "\x1b") {
			t.Fatalf("producer field reached the terminal unescaped:\n%s", escaped)
		}
	}
	if !strings.Contains(escaped, `signal=KILL\u{1b}\nSTATUS forged`) {
		t.Fatalf("escaped signal not shown:\n%s", escaped)
	}
	r.Spans[0].Capture = ReduceCapture(capture, false)
	raw := render(t, r, 1<<20)
	if strings.Contains(raw, "    | value=1.37") || !strings.Contains(raw, "response (present") {
		t.Fatalf("interpretation applied without selection:\n%s", raw)
	}
}
