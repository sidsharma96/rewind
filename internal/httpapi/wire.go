// Package httpapi translates the explicit Rewind HTTP protocol into existing
// coordinators. It defines no saved-object format or authority from wire labels.
package httpapi

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/sidsharma96/rewind/internal/ingest"
	"github.com/sidsharma96/rewind/internal/record"
	"github.com/sidsharma96/rewind/internal/traceid"
)

var ErrProtocol = errors.New("rewind: invalid service message")

type Execution struct {
	Project string `json:"project_hex"`
	Trace   string `json:"trace_id"`
	Span    string `json:"span_id"`
}

func execution(e ingest.Execution) Execution {
	return Execution{Opaque(e.Tenant), e.Trace.String(), e.Span.String()}
}

// Opaque encodes identifiers without assuming that their bytes are UTF-8.
func Opaque(s string) string { return hex.EncodeToString([]byte(s)) }
func parseOpaque(s string) (string, error) {
	if len(s) < 2 || len(s) > 512 {
		return "", ErrProtocol
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return "", ErrProtocol
	}
	return string(b), nil
}

type Receipt struct {
	Execution
	Kind      string `json:"kind"`
	Digest    string `json:"sha256"`
	Size      int    `json:"stored_bytes"`
	Run       string `json:"owner_run_hex"`
	Instance  string `json:"owner_instance_hex"`
	Submitter string `json:"submitted_by_hex"`
	Accepted  string `json:"accepted_at"`
	Expires   string `json:"expires_at"`
	Duplicate bool   `json:"duplicate"`
}

func receipt(r ingest.Receipt) (Receipt, error) {
	var kind string
	switch r.Reference.Identity.Kind {
	case ingest.SpanRecord:
		kind = "span"
	case ingest.CaptureRecord:
		kind = "capture"
	default:
		return Receipt{}, ingest.ErrIntegrity
	}
	id := r.Reference.Identity
	return Receipt{execution(ingest.Execution{Tenant: id.Tenant, Trace: id.Trace, Span: id.Span}), kind,
		hex.EncodeToString(r.Reference.Digest[:]), r.Reference.Size, Opaque(r.Attribution.Owner.Run),
		Opaque(r.Attribution.Owner.Instance), Opaque(r.Attribution.SubmittedBy),
		r.AcceptedAt.UTC().Format(time.RFC3339Nano), r.ExpiresAt.UTC().Format(time.RFC3339Nano), r.Duplicate}, nil
}

type RegisterInput struct {
	Run      string `json:"run_hex"`
	Instance string `json:"instance_hex"`
}
type Registration struct {
	Execution
	Run       string `json:"owner_run_hex"`
	Instance  string `json:"owner_instance_hex"`
	Registrar string `json:"registered_by_hex"`
	Duplicate bool   `json:"duplicate"`
}
type SpanInput struct {
	Ended    *bool           `json:"ended"`
	Metadata json.RawMessage `json:"metadata"`
}
type Body struct {
	State *record.BodyState `json:"state"`
	Data  *string           `json:"data_base64"`
}

func body(b record.Body) Body {
	state, data := b.State, base64.StdEncoding.EncodeToString(b.Data)
	return Body{&state, &data}
}
func (b Body) value() (record.Body, error) {
	if b.State == nil || b.Data == nil {
		return record.Body{}, ErrProtocol
	}
	data, err := base64.StdEncoding.Strict().DecodeString(*b.Data)
	if err != nil || base64.StdEncoding.EncodeToString(data) != *b.Data {
		return record.Body{}, ErrProtocol
	}
	switch *b.State {
	case record.BodyMissing, record.BodyRedacted:
		if len(data) != 0 {
			return record.Body{}, ErrProtocol
		}
	case record.BodyPresent, record.BodyTruncated:
	default:
		return record.Body{}, ErrProtocol
	}
	return record.Body{State: *b.State, Data: data}, nil
}

type CaptureInput struct {
	Request  *Body `json:"request"`
	Response *Body `json:"response"`
}

func (v CaptureInput) value(e ingest.Execution) (record.Capture, error) {
	if v.Request == nil || v.Response == nil {
		return record.Capture{}, ErrProtocol
	}
	request, err := v.Request.value()
	if err != nil {
		return record.Capture{}, err
	}
	response, err := v.Response.value()
	if err != nil {
		return record.Capture{}, err
	}
	return record.Capture{Trace: e.Trace, Span: e.Span, Request: request, Response: response}, nil
}

type Capture struct {
	Execution
	Receipt  Receipt `json:"receipt"`
	Request  Body    `json:"request"`
	Response Body    `json:"response"`
}
type Result struct {
	Execution
	Receipt      Receipt          `json:"receipt"`
	RequestState record.BodyState `json:"request_state"`
	Data         string           `json:"data_base64"`
}
type Item struct {
	Execution
	State    string          `json:"state"`
	Receipt  *Receipt        `json:"receipt,omitempty"`
	Metadata json.RawMessage `json:"metadata,omitempty"`
}
type Page struct {
	Items []Item `json:"items"`
	Next  string `json:"next"`
}
type Failure struct {
	Code    string `json:"code"`
	Outcome string `json:"outcome"`
}

func (e *Failure) Error() string { return "rewind: " + e.Code + " (" + e.Outcome + ")" }

func cursor(e ingest.Execution, c ingest.TraceCursor) string {
	if c.IsZero() {
		return ""
	}
	return "1." + Opaque(e.Tenant) + "." + e.Trace.String() + "." + c.After().String()
}
func parseCursor(e ingest.Execution, s string) (ingest.TraceCursor, error) {
	if s == "" {
		return ingest.TraceCursor{}, nil
	}
	if len(s) > 565 {
		return ingest.TraceCursor{}, ErrProtocol
	}
	parts := strings.Split(s, ".")
	if len(parts) != 4 || parts[0] != "1" || parts[1] != Opaque(e.Tenant) || parts[2] != e.Trace.String() {
		return ingest.TraceCursor{}, ErrProtocol
	}
	after, err := traceid.ParseSpan(parts[3])
	if err != nil {
		return ingest.TraceCursor{}, ErrProtocol
	}
	return ingest.ResumeTrace(e.Tenant, e.Trace, after)
}

// strictJSON bounds token nesting, refuses duplicate keys before decoding, and
// preserves typed numeric parsing. The caller must bound input bytes first.
// Per-object key maps and tokens consume O(input bytes); depth is at most 64.
func strictJSON(data []byte, dst any) error {
	if !utf8.Valid(data) || !unicodeEscapes(data) {
		return ErrProtocol
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	target := reflect.TypeOf(dst)
	if target == nil || target.Kind() != reflect.Pointer {
		return ErrProtocol
	}
	if err := jsonValue(d, 0, target.Elem()); err != nil {
		return ErrProtocol
	}
	if _, err := d.Token(); err != io.EOF {
		return ErrProtocol
	}
	d = json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if d.Decode(dst) != nil || d.Decode(new(any)) != io.EOF {
		return ErrProtocol
	}
	return nil
}
func jsonValue(d *json.Decoder, depth int, target reflect.Type) error {
	nullable := target == nil
	if target != nil {
		switch target.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Map, reflect.Interface:
			nullable = true
		}
	}

	for target != nil && target.Kind() == reflect.Pointer {
		target = target.Elem()
	}
	if depth > 64 {
		return ErrProtocol
	}
	t, err := d.Token()
	if err != nil {
		return err
	}
	if t == nil && !nullable {
		return ErrProtocol
	}
	delim, ok := t.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		fields := jsonFields(target)
		keys := make(map[string]bool)
		for d.More() {
			k, err := d.Token()
			if err != nil {
				return err
			}
			key, ok := k.(string)
			if !ok || keys[key] {
				return ErrProtocol
			}
			keys[key] = true
			var child reflect.Type
			if fields != nil {
				var known bool
				field, exists := fields[key]
				known = exists
				child = field.kind
				if !known {
					return ErrProtocol
				}
			} else if target != nil && target.Kind() == reflect.Map {
				child = target.Elem()
			}
			if err := jsonValue(d, depth+1, child); err != nil {
				return err
			}
		}
		for name, field := range fields {
			if field.required && !keys[name] {
				return ErrProtocol
			}
		}
		t, err = d.Token()
		if err != nil || t != json.Delim('}') {
			return ErrProtocol
		}
	case '[':
		var child reflect.Type
		if target != nil && (target.Kind() == reflect.Slice || target.Kind() == reflect.Array) {
			child = target.Elem()
		}
		for d.More() {
			if err := jsonValue(d, depth+1, child); err != nil {
				return err
			}
		}
		t, err = d.Token()
		if err != nil || t != json.Delim(']') {
			return ErrProtocol
		}
	default:
		return ErrProtocol
	}
	return nil
}

// DecodeDocument parses one strict bounded-by-caller protocol/config document.
// It never reads a file or selects the caller's byte limit.
func DecodeDocument(data []byte, dst any) error { return strictJSON(data, dst) }

// encoding/json replaces unpaired UTF-16 escapes with U+FFFD. Reject them
// before tokenization so a malformed metadata string cannot become new evidence.
// This pass uses constant auxiliary memory and O(input bytes) work.
func unicodeEscapes(data []byte) bool {
	inString := false
	for i := 0; i < len(data); i++ {
		if data[i] == '"' {
			inString = !inString
			continue
		}
		if !inString || data[i] != 92 {
			continue
		}
		i++
		if i >= len(data) {
			return false
		}
		if data[i] != 'u' {
			continue
		}
		if i+4 >= len(data) {
			return false
		}
		value, err := strconv.ParseUint(string(data[i+1:i+5]), 16, 16)
		if err != nil {
			return false
		}
		i += 4
		if value >= 0xdc00 && value <= 0xdfff {
			return false
		}
		if value < 0xd800 || value > 0xdbff {
			continue
		}
		if i+6 >= len(data) || data[i+1] != 92 || data[i+2] != 'u' {
			return false
		}
		low, err := strconv.ParseUint(string(data[i+3:i+7]), 16, 16)
		if err != nil || low < 0xdc00 || low > 0xdfff {
			return false
		}
		i += 6
	}
	return true
}

// encoding/json also matches field names case-insensitively. Protocol keys
// instead use their exact declared names, including promoted execution fields.
// RawMessage descendants remain lexical-only here; the metadata codec owns its
// generated schema. No mutable reflection cache or second parsed graph is kept.
type jsonField struct {
	kind     reflect.Type
	required bool
}

func jsonFields(t reflect.Type) map[string]jsonField {
	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == nil || t.Kind() != reflect.Struct {
		return nil
	}
	fields := make(map[string]jsonField)
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.PkgPath != "" {
			continue
		}
		options := strings.Split(field.Tag.Get("json"), ",")
		name := options[0]
		required := true
		for _, option := range options[1:] {
			if option == "omitempty" || option == "omitzero" {
				required = false
			}
		}
		if name == "-" {
			continue
		}
		if name == "" && field.Anonymous {
			for key, value := range jsonFields(field.Type) {
				fields[key] = value
			}
			continue
		}
		if name == "" {
			name = field.Name
		}
		fields[name] = jsonField{kind: field.Type, required: required}
	}
	return fields
}
