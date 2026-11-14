package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/sidsharma96/rewind/internal/record"
	"github.com/sidsharma96/rewind/internal/traceid"
)

// The diagnosis view is a reader-side rendering of separately read trace pages and
// captures. It orders, groups and counts recorded evidence; it infers no fault, cause
// or completeness. Captures are reduced on arrival so complete bodies are not retained.

// DiagnosisBodyLimit bounds each uninterpreted body shown; interpreted fields keep
// the browser's per-field preview bound.
const DiagnosisBodyLimit = 64 << 10

// Capture outcomes. Read failures are "read_failed:" plus the service failure code.
const (
	CaptureSaved   = "saved"
	CaptureNone    = "not_found"
	CaptureExpired = "expired"
	CaptureNotRead = "not_read"
)

type diagnosisBody struct {
	State string
	Size  int
	Text  string
	Note  string
}

// DiagnosisCapture is the bounded display record kept for one capture read.
type DiagnosisCapture struct {
	Outcome   string
	Request   diagnosisBody
	Response  diagnosisBody
	Interpret *commandPresentation
	Process   string   // command outcome when a command observation was read, else ""
	Digest    [32]byte // SHA-256 of request state and bytes; groups identical requests only
	Retained  int      // display bytes this record keeps
}

// DiagnosisSpan pairs one trace item with the reduced outcome of its capture read.
type DiagnosisSpan struct {
	Item    Item
	Capture DiagnosisCapture
}

// DiagnosisRead is everything one bounded diagnosis read gathered.
type DiagnosisRead struct {
	Project, Trace    string
	Started, Finished time.Time
	Spans             []DiagnosisSpan
	Stop              string // "" when the last page reported no continuation
}

// ReduceCapture keeps only bounded display text and a request digest. interpret
// selects the reader-side command observation v1 and Claude CLI exchange v1 views.
func ReduceCapture(c Capture, interpret bool) DiagnosisCapture {
	out := DiagnosisCapture{Outcome: CaptureSaved, Request: reduceBody(c.Request), Response: reduceBody(c.Response)}
	h := sha256.New()
	if c.Request.State != nil && c.Request.Data != nil {
		h.Write([]byte(*c.Request.State))
		h.Write([]byte{0})
		if raw, err := base64.StdEncoding.Strict().DecodeString(*c.Request.Data); err == nil {
			h.Write(raw)
		}
	}
	copy(out.Digest[:], h.Sum(nil))
	if interpret {
		var response commandResponse
		if commandBody(c.Response, &response) == nil && response.Version == 1 {
			switch {
			case response.ExitCode != nil:
				out.Process = "exit=" + strconv.Itoa(*response.ExitCode)
			case response.Signal != "":
				out.Process = "signal=" + response.Signal
			}
			if response.Outcome != "exited" {
				out.Process = strings.TrimSpace(out.Process + " outcome=" + response.Outcome)
			}
		}
		var request commandRequest
		if commandBody(c.Request, &request) == nil && request.Version == 1 && request.Tool != "" {
			v := commandView(c)
			out.Interpret = &v
		} else if _, err := sessionBody(c.Request); err == nil {
			v := sessionView(c)
			out.Interpret = &v
		}
	}
	if out.Interpret != nil {
		out.Request.Text, out.Request.Note = "", ""
		if out.parsedResponse(c) {
			out.Response.Text, out.Response.Note = "", ""
		}
	}
	out.Retained = len(out.Request.Text) + len(out.Response.Text)
	if out.Interpret != nil {
		for _, n := range out.Interpret.Notices {
			out.Retained += len(n)
		}
		for _, f := range out.Interpret.Fields {
			out.Retained += len(f.Value.Text) + len(f.Note) + len(f.Title)
		}
	}
	return out
}

func (d DiagnosisCapture) parsedResponse(c Capture) bool {
	if d.Process != "" {
		return true
	}
	_, err := sessionBody(c.Response)
	return err == nil
}

// FailedCapture records a capture read that returned no capture.
func FailedCapture(err error) DiagnosisCapture {
	var f *Failure
	switch {
	case errors.As(err, &f) && f.Code == "not_found":
		return DiagnosisCapture{Outcome: CaptureNone}
	case errors.As(err, &f) && f.Code == "expired":
		return DiagnosisCapture{Outcome: CaptureExpired}
	case errors.As(err, &f):
		return DiagnosisCapture{Outcome: "read_failed:" + f.Code}
	}
	return DiagnosisCapture{Outcome: "read_failed:" + FailureLabel(err)}
}

// FailureLabel names an error for display without exposing response bodies.
func FailureLabel(err error) string {
	var f *Failure
	switch {
	case err == nil:
		return ""
	case errors.As(err, &f):
		return f.Code
	case errors.Is(err, ErrProtocol):
		return "protocol"
	case errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled):
		return "deadline"
	}
	return "transport"
}

func reduceBody(b Body) diagnosisBody {
	if b.State == nil || b.Data == nil {
		return diagnosisBody{State: "invalid"}
	}
	out := diagnosisBody{State: string(*b.State)}
	if *b.State == record.BodyMissing || *b.State == record.BodyRedacted {
		return out
	}
	raw, err := base64.StdEncoding.Strict().DecodeString(*b.Data)
	if err != nil {
		out.State = "invalid"
		return out
	}
	out.Size = len(raw)
	shown := raw
	if len(shown) > DiagnosisBodyLimit {
		shown = shown[:DiagnosisBodyLimit]
		out.Note = fmt.Sprintf("+%d bytes not shown; use rewind capture for exact bytes", len(raw)-DiagnosisBodyLimit)
	}
	if utf8.Valid(shown) || len(shown) < len(raw) && utf8.Valid(trimPartialRune(shown)) {
		out.Text = string(trimPartialRune(shown))
	} else {
		out.Text = "base64:" + base64.StdEncoding.EncodeToString(shown)
	}
	return out
}

func trimPartialRune(b []byte) []byte {
	for i := 0; i < utf8.UTFMax && len(b) > 0 && !utf8.Valid(b); i++ {
		b = b[:len(b)-1]
	}
	return b
}

// terminalSafe escapes control and format characters (Unicode category Cf, which
// includes bidirectional controls and zero-width characters) so recorded text
// cannot drive the terminal, visually reorder lines or hide text. Emoji joined by
// U+200D are therefore shown escaped. Newlines are kept for
// the caller's line prefixing; tabs are kept.
func terminalSafe(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r == '\n' || r == '\t':
			b.WriteRune(r)
		case r < 0x20 || r == 0x7f || (r >= 0x80 && r < 0xa0) || r == 0x2028 || r == 0x2029 || r == utf8.RuneError || unicode.Is(unicode.Cf, r):
			fmt.Fprintf(&b, "\\u{%x}", r)
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// oneLine is for labels: every newline is escaped too.
func oneLine(s string) string { return strings.ReplaceAll(terminalSafe(s), "\n", `\n`) }

type diagnosisSpan struct {
	in        DiagnosisSpan
	ref       int
	span      record.Span
	parsed    bool
	parseNote string
	start     uint64
}

type limitedWriter struct {
	w       io.Writer
	left    int
	stopped bool
	err     error
}

// line writes one output line. Every line is escaped here, at the only sink, so
// producer-controlled text in any field cannot add lines or control the terminal.
// Escaping is idempotent: already escaped text contains no characters it changes.
func (l *limitedWriter) line(format string, args ...any) {
	if l.stopped || l.err != nil {
		return
	}
	s := oneLine(fmt.Sprintf(format, args...)) + "\n"
	if len(s) > l.left {
		l.stopped = true
		_, l.err = io.WriteString(l.w, "OUTPUT LIMIT REACHED: the remaining sections are not shown.\n")
		return
	}
	l.left -= len(s)
	_, l.err = io.WriteString(l.w, s)
}

// diagnosisView holds one rendering: the spans in display order and their
// view-local references.
type diagnosisView struct {
	r     DiagnosisRead
	out   *limitedWriter
	spans []*diagnosisSpan
	byID  map[string]*diagnosisSpan
	first uint64 // earliest known reported start, for offsets
	shown map[[32]byte]string
}

// WriteDiagnosis renders r as text, writing at most maxOutput bytes.
func WriteDiagnosis(w io.Writer, r DiagnosisRead, metadata record.MetadataCodec, maxOutput int) error {
	v := &diagnosisView{r: r, out: &limitedWriter{w: w, left: maxOutput}, byID: map[string]*diagnosisSpan{}, shown: map[[32]byte]string{}}
	for _, in := range r.Spans {
		d := &diagnosisSpan{in: in}
		if in.Item.State == "available" {
			packet, err := metadata.Decode(in.Item.Metadata)
			if err == nil {
				d.span, err = record.SpanAt(packet, 0, 0, 0)
			}
			if err != nil {
				d.parseNote = "span metadata could not be interpreted"
			} else {
				d.parsed = true
				d.start = d.span.Metadata.StartTimeUnixNano
			}
		}
		v.spans = append(v.spans, d)
		v.byID[in.Item.Span] = d
	}
	// Reported start order; unknown starts and unavailable items follow, by span ID.
	sort.SliceStable(v.spans, func(i, j int) bool {
		a, b := v.spans[i], v.spans[j]
		ka, kb := a.parsed && a.start != 0, b.parsed && b.start != 0
		if ka != kb {
			return ka
		}
		if ka && a.start != b.start {
			return a.start < b.start
		}
		return a.in.Item.Span < b.in.Item.Span
	})
	for i, d := range v.spans {
		d.ref = i + 1
		if v.first == 0 && d.parsed && d.start != 0 {
			v.first = d.start
		}
	}
	v.header()
	v.coverage()
	v.relationships()
	v.status()
	v.repeats()
	v.out.line("SPANS (reported start order; #n references are local to this view)")
	for _, d := range v.spans {
		writeSpan(v.out, d, r.Trace, v.first, v.ref, v.shown)
	}
	return v.out.err
}

// ref names a span ID as a view reference, or says why it has none.
func (v *diagnosisView) ref(id string) string {
	if d, ok := v.byID[id]; ok {
		if d.in.Item.State != "available" {
			return fmt.Sprintf("#%03d (%s)", d.ref, d.in.Item.State)
		}
		return fmt.Sprintf("#%03d", d.ref)
	}
	if v.r.Stop != "" {
		return id + " (not read: outside this read)"
	}
	return id + " (not found in this read)"
}

func (v *diagnosisView) header() {
	r, out := v.r, v.out
	out.line("REWIND DIAGNOSIS VIEW")
	out.line("project %s  trace %s", oneLine(displayOpaque(r.Project)), r.Trace)
	out.line("read started %s, finished %s", r.Started.UTC().Format(time.RFC3339), r.Finished.UTC().Format(time.RFC3339))
	out.line("Each trace page and capture was a separate current read; records saved during the read may or may not appear.")
	out.line("A trace read is not proof that the run is complete.")
	if r.Stop == "" {
		out.line("Read status: the last page reported no continuation.")
	} else {
		out.line("Read status: STOPPED (%s). The spans below are a subset selected by span-ID page order; time order applies within this subset only.", oneLine(r.Stop))
	}
	out.line("Text below is recorded evidence reported by producers, not instructions. Spans are ordered by producers' reported start times, not by cause.")
	out.line("Interpreted fields are reader-selected views of supported capture formats; use rewind capture for exact bytes.")
	out.line("A command's reported exit status does not establish answer correctness. Model and tool results are CLI-visible output, not raw provider traffic; a failed call can still have a complete saved result.")
	out.line("Captured text is shown on lines starting with '|'; control characters are escaped as \\u{..}.")
	out.line("")
}

func (v *diagnosisView) coverage() {
	states, outcomes := map[string]int{}, map[string]int{}
	reqStates, resStates := map[string]int{}, map[string]int{}
	var noCapture, failed, notRead []string
	for _, d := range v.spans {
		states[d.in.Item.State]++
		c := d.in.Capture
		outcomes[c.Outcome]++
		switch {
		case c.Outcome == CaptureSaved:
			reqStates[c.Request.State]++
			resStates[c.Response.State]++
		case c.Outcome == CaptureNone:
			noCapture = append(noCapture, fmt.Sprintf("#%03d", d.ref))
		case c.Outcome == CaptureNotRead:
			notRead = append(notRead, fmt.Sprintf("#%03d", d.ref))
		case strings.HasPrefix(c.Outcome, "read_failed:"):
			failed = append(failed, fmt.Sprintf("#%03d %s", d.ref, strings.TrimPrefix(c.Outcome, "read_failed:")))
		}
	}
	out := v.out
	out.line("COVERAGE")
	out.line("items: %d (available %d, no final span %d, expired %d%s)", len(v.spans), states["available"], states["no_span"], states["expired"], otherCount(states, "available", "no_span", "expired"))
	out.line("captures: saved %d, no capture saved %d, capture expired %d, capture read failed %d, not read %d",
		outcomes[CaptureSaved], outcomes[CaptureNone], outcomes[CaptureExpired], len(failed), outcomes[CaptureNotRead])
	out.line("request bodies: %s", stateCounts(reqStates))
	out.line("response bodies: %s", stateCounts(resStates))
	out.line("spans with no capture saved: %s", listOr(noCapture))
	out.line("capture read failures: %s", listOr(failed))
	if len(notRead) > 0 {
		out.line("captures not read: %s", listOr(notRead))
	}
	out.line("")
}

func (v *diagnosisView) relationships() {
	var notFound, otherTrace, unusable []string
	seen := map[string]bool{}
	note := func(list *[]string, s string) {
		if !seen[s] {
			seen[s] = true
			*list = append(*list, s)
		}
	}
	for _, d := range v.spans {
		if !d.parsed {
			continue
		}
		if p, err := traceid.SpanFromBytes(d.span.Metadata.ParentSpanId); err == nil {
			if _, ok := v.byID[p.String()]; !ok {
				note(&notFound, fmt.Sprintf("parent %s of #%03d", v.ref(p.String()), d.ref))
			}
		} else if len(d.span.Metadata.ParentSpanId) > 0 {
			note(&unusable, fmt.Sprintf("parent of #%03d", d.ref))
		}
		for _, l := range d.span.Metadata.Links {
			t, usable, err := record.TargetOfLink(l)
			switch {
			case err != nil || !usable:
				note(&unusable, fmt.Sprintf("link of #%03d", d.ref))
			case t.Trace.String() != v.r.Trace:
				note(&otherTrace, fmt.Sprintf("#%03d -> %s/%s (not read)", d.ref, t.Trace, t.Span))
			default:
				if _, ok := v.byID[t.Span.String()]; !ok {
					note(&notFound, fmt.Sprintf("link %s of #%03d", v.ref(t.Span.String()), d.ref))
				}
			}
		}
	}
	v.out.line("RELATIONSHIPS (as reported; no tree is constructed)")
	v.out.line("parent/link targets absent from this read: %s", listOr(notFound))
	v.out.line("links to other traces: %s", listOr(otherTrace))
	v.out.line("parents or links without a usable target: %s", listOr(unusable))
	v.out.line("")
}

func (v *diagnosisView) status() {
	errorsBy := map[string][]string{}
	var errorKeys, unknown, interpretFail []string
	unset := map[string]int{}
	for _, d := range v.spans {
		if !d.parsed {
			if d.parseNote != "" {
				interpretFail = append(interpretFail, fmt.Sprintf("#%03d", d.ref))
			}
			continue
		}
		code := int32(0)
		if d.span.Metadata.Status != nil {
			code = int32(d.span.Metadata.Status.Code)
		}
		name := spanName(d.span.Metadata.Name)
		switch code {
		case 0:
			unset[name]++
		case 1:
		case 2:
			k := name
			if d.in.Capture.Process != "" {
				k += " " + d.in.Capture.Process
			}
			if errorsBy[k] == nil {
				errorKeys = append(errorKeys, k)
			}
			errorsBy[k] = append(errorsBy[k], fmt.Sprintf("#%03d", d.ref))
		default:
			unknown = append(unknown, fmt.Sprintf("#%03d status %d", d.ref, code))
		}
		if k := int32(d.span.Metadata.Kind); k < 0 || k > 5 {
			unknown = append(unknown, fmt.Sprintf("#%03d span kind %d", d.ref, k))
		}
	}
	sort.Strings(errorKeys)
	out := v.out
	out.line("STATUS")
	if len(errorKeys) == 0 {
		out.line("error (code 2): none")
	}
	for _, k := range errorKeys {
		out.line("error (code 2): %s x%d -> %s", oneLine(k), len(errorsBy[k]), strings.Join(errorsBy[k], ", "))
	}
	out.line("unset (code 0, not success): %s", nameCounts(unset))
	out.line("unknown status or kind values: %s", listOr(unknown))
	if len(interpretFail) > 0 {
		out.line("span metadata not interpreted: %s", listOr(interpretFail))
	}
	out.line("")
}

// repeats groups saved captures whose request digest matches, first per verified
// owner and then across owners. Matching bytes do not prove a retry.
func (v *diagnosisView) repeats() {
	byDigest := map[[32]byte][]*diagnosisSpan{}
	var digests [][32]byte
	for _, d := range v.spans {
		// An undecodable request has no bytes to compare.
		if d.in.Capture.Outcome != CaptureSaved || d.in.Capture.Request.State == "invalid" {
			continue
		}
		k := d.in.Capture.Digest
		if byDigest[k] == nil {
			digests = append(digests, k)
		}
		byDigest[k] = append(byDigest[k], d)
	}
	describe := func(d *diagnosisSpan) string {
		s := fmt.Sprintf("#%03d", d.ref)
		if d.in.Capture.Process != "" {
			s += " " + d.in.Capture.Process
		}
		if d.parsed {
			s += " " + statusWord(d.span.Metadata)
		}
		return s
	}
	out := v.out
	out.line("IDENTICAL REQUEST BYTES (same digest; not necessarily retries)")
	found := false
	for _, k := range digests {
		group := byDigest[k]
		if len(group) < 2 {
			continue
		}
		owners := map[string][]*diagnosisSpan{}
		var order []string
		for _, d := range group {
			o := ownerOf(d.in.Item)
			if owners[o] == nil {
				order = append(order, o)
			}
			owners[o] = append(owners[o], d)
		}
		for _, o := range order {
			if len(owners[o]) < 2 {
				continue
			}
			found = true
			var parts []string
			for _, d := range owners[o] {
				parts = append(parts, describe(d))
			}
			out.line("same owner %s, %s x%d: %s", oneLine(o), oneLine(nameOf(owners[o][0])), len(owners[o]), strings.Join(parts, ", "))
		}
		if len(order) > 1 {
			found = true
			var parts []string
			for _, d := range group {
				parts = append(parts, fmt.Sprintf("#%03d %s", d.ref, oneLine(ownerOf(d.in.Item))))
			}
			out.line("across owners: %s", strings.Join(parts, ", "))
		}
	}
	if !found {
		out.line("none")
	}
	out.line("")
}

func writeSpan(out *limitedWriter, d *diagnosisSpan, trace string, first uint64, ref func(string) string, shown map[[32]byte]string) {
	it := d.in.Item
	head := fmt.Sprintf("#%03d", d.ref)
	if d.parsed {
		m := d.span.Metadata
		timing := "start unknown"
		if m.StartTimeUnixNano != 0 && first != 0 {
			timing = fmt.Sprintf("+%.3fs", float64(m.StartTimeUnixNano-first)/1e9)
		}
		if n, ok := d.span.DurationNanos(); ok {
			timing += fmt.Sprintf(" (%.3fs)", float64(n)/1e9)
		} else {
			timing += " (duration unknown: check reported times)"
		}
		head += fmt.Sprintf(" %s %s %s %s span=%s", timing, oneLine(ownerOf(it)), oneLine(spanName(m.Name)), statusWord(m), it.Span)
		if p, err := traceid.SpanFromBytes(m.ParentSpanId); err == nil {
			head += " parent=" + ref(p.String())
		} else if len(m.ParentSpanId) > 0 {
			head += " parent=unusable(" + hex.EncodeToString(m.ParentSpanId) + ")"
		} else {
			head += " parent=none"
		}
		var links []string
		for _, l := range m.Links {
			t, usable, err := record.TargetOfLink(l)
			switch {
			case err != nil || !usable:
				links = append(links, "no-usable-target")
			case t.Trace.String() != trace:
				links = append(links, t.Trace.String()+"/"+t.Span.String()+" (other trace, not read)")
			default:
				links = append(links, ref(t.Span.String()))
			}
		}
		if len(links) > 0 {
			head += " links=" + strings.Join(links, ",")
		}
	} else {
		head += fmt.Sprintf(" item state %s span=%s", it.State, it.Span)
		if d.parseNote != "" {
			head += " (" + d.parseNote + ")"
		}
	}
	out.line("%s", head)
	if it.Receipt != nil && it.Receipt.Submitter != it.Receipt.Instance {
		out.line("  submitted by %s for owner %s", oneLine(displayOpaque(it.Receipt.Submitter)), oneLine(displayOpaque(it.Receipt.Instance)))
	}
	if d.parsed {
		writeDetails(out, d.span)
	}
	c := d.in.Capture
	switch {
	case c.Outcome == CaptureSaved:
	case c.Outcome == CaptureNone:
		out.line("  capture: none saved at read time")
		return
	case c.Outcome == CaptureExpired:
		out.line("  capture: expired")
		return
	case c.Outcome == CaptureNotRead:
		out.line("  capture: not read")
		return
	default:
		out.line("  capture: %s", oneLine(c.Outcome))
		return
	}
	if c.Interpret != nil {
		for _, n := range c.Interpret.Notices {
			if strings.HasPrefix(n, "Command observation v1 is") || strings.HasPrefix(n, "Claude CLI exchange v1 is") {
				continue // stated once in the header
			}
			if strings.HasPrefix(n, "Command outcome (reported): ") && c.Process != "" {
				out.line("  process (reported): %s", c.Process)
				continue
			}
			out.line("  note: %s", oneLine(n))
		}
		for _, f := range c.Interpret.Fields {
			label, text := oneLine(f.Title), f.Value.Text
			if f.Value.Encoding != "UTF-8 preview" {
				label += " [" + oneLine(f.Value.Encoding) + "]"
			} else if f.Title == "Command arguments" {
				var b bytes.Buffer
				if json.Compact(&b, []byte(text)) == nil {
					text = b.String() // same JSON array, one line
				}
			}
			if f.Note != "" && !constantNotes[f.Note] {
				data, _, _ := strings.Cut(f.Note, ". ") // later sentences are disclaimers stated in the header
				label += " (" + oneLine(data) + ")"
			}
			writeText(out, label, text, shown, d.ref)
		}
		if c.Response.Text != "" {
			label := fmt.Sprintf("response (%s, %d bytes, not interpreted)", c.Response.State, c.Response.Size)
			if c.Response.Note != "" {
				label += " [" + c.Response.Note + "]"
			}
			writeText(out, label, c.Response.Text, shown, d.ref)
		}
		out.line("  request %s %d bytes; response %s %d bytes", c.Request.State, c.Request.Size, c.Response.State, c.Response.Size)
		return
	}
	for _, side := range []struct {
		name string
		b    diagnosisBody
	}{{"request", c.Request}, {"response", c.Response}} {
		switch side.b.State {
		case string(record.BodyMissing), string(record.BodyRedacted), "invalid":
			out.line("  %s: %s", side.name, side.b.State)
			continue
		}
		label := fmt.Sprintf("%s (%s, %d bytes)", side.name, side.b.State, side.b.Size)
		if side.b.Note != "" {
			label += " [" + side.b.Note + "]"
		}
		writeText(out, label, side.b.Text, shown, d.ref)
	}
}

// writeText prints captured text with a line prefix. Long text already printed in
// full is referenced instead of repeated.
func writeText(out *limitedWriter, label, text string, shown map[[32]byte]string, ref int) {
	if len(text) >= 128 {
		k := sha256.Sum256([]byte(text))
		if at, ok := shown[k]; ok {
			out.line("  %s: identical to %s", label, at)
			return
		}
		shown[k] = fmt.Sprintf("#%03d %s", ref, label)
	}
	if text == "" {
		out.line("  %s: (zero bytes)", label)
		return
	}
	out.line("  %s:", label)
	for _, l := range strings.Split(strings.TrimSuffix(terminalSafe(text), "\n"), "\n") {
		out.line("    | %s", l)
	}
}

// stableJSON compacts protojson output, whose whitespace intentionally varies.
type stableJSON struct{}

func (stableJSON) Marshal(m interface{ ProtoReflect() protoreflect.Message }) ([]byte, error) {
	raw, err := protojson.Marshal(m)
	if err != nil {
		return nil, err
	}
	var b bytes.Buffer
	if err := json.Compact(&b, raw); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// constantNotes are interpretation notes that carry no per-capture data; the header
// covers them. Notes with data, such as is_error or a tool label, are printed.
var constantNotes = map[string]bool{
	"Description supplied by the producer":                                             true,
	"Bytes supplied to the command":                                                    true,
	"Exact stream shown as a bounded display preview":                                  true,
	"A complete failed command can still have a Present saved response":                true,
	"Operator-supplied context; the stream does not prove this was its original input": true,
}

func writeDetails(out *limitedWriter, s record.Span) {
	m := s.Metadata
	opts := stableJSON{}
	if s.Context.Scope != nil && (s.Context.Scope.Name != "" || s.Context.Scope.Version != "") {
		out.line("  scope: %s", strings.TrimSpace(oneLine(s.Context.Scope.Name)+" "+oneLine(s.Context.Scope.Version)))
	}
	if s.Context.Resource != nil && len(s.Context.Resource.Attributes) > 0 {
		if b, err := opts.Marshal(s.Context.Resource); err == nil {
			out.line("  resource: %s", oneLine(string(b)))
		}
	}
	if len(m.Attributes) > 0 {
		var parts []string
		for _, a := range m.Attributes {
			if b, err := opts.Marshal(a); err == nil {
				parts = append(parts, string(b))
			}
		}
		out.line("  attributes: %s", oneLine(strings.Join(parts, ", ")))
	}
	if m.Status != nil && m.Status.Message != "" {
		out.line("  status message: %s", oneLine(m.Status.Message))
	}
	for _, e := range m.Events {
		if b, err := opts.Marshal(e); err == nil {
			out.line("  event: %s", oneLine(string(b)))
		}
	}
	if m.DroppedAttributesCount+m.DroppedEventsCount+m.DroppedLinksCount > 0 {
		out.line("  dropped by producer: attributes %d, events %d, links %d", m.DroppedAttributesCount, m.DroppedEventsCount, m.DroppedLinksCount)
	}
}

func statusWord(m *tracepb.Span) string {
	if m.Status == nil {
		return "unset"
	}
	switch int32(m.Status.Code) {
	case 0:
		return "unset"
	case 1:
		return "ok"
	case 2:
		return "ERROR"
	}
	return "status(" + strconv.Itoa(int(m.Status.Code)) + ")"
}

func spanName(n string) string {
	if n == "" {
		return "(blank name)"
	}
	return n
}

func nameOf(d *diagnosisSpan) string {
	if d.parsed {
		return spanName(d.span.Metadata.Name)
	}
	return "(no final span)"
}

func ownerOf(it Item) string {
	if it.Receipt == nil {
		return "(owner unavailable)"
	}
	return displayOpaque(it.Receipt.Instance)
}

func listOr(xs []string) string {
	if len(xs) == 0 {
		return "none"
	}
	return strings.Join(xs, ", ")
}

func stateCounts(m map[string]int) string {
	order := []string{string(record.BodyPresent), string(record.BodyMissing), string(record.BodyRedacted), string(record.BodyTruncated), "invalid"}
	var parts []string
	for _, k := range order {
		if m[k] > 0 {
			parts = append(parts, fmt.Sprintf("%s %d", k, m[k]))
		}
	}
	return listOr(parts)
}

func nameCounts(m map[string]int) string {
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s x%d", oneLine(k), m[k]))
	}
	return listOr(parts)
}

func otherCount(m map[string]int, known ...string) string {
	n := 0
	for k, v := range m {
		skip := false
		for _, s := range known {
			skip = skip || k == s
		}
		if !skip {
			n += v
		}
	}
	if n == 0 {
		return ""
	}
	return fmt.Sprintf(", other %d", n)
}
