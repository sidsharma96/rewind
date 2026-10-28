package capturelab

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"unicode/utf8"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// This is the selected serializer direction exercised in the test lab, not a
// production saved format or OTLP receiver. Admission, unknown-field policy,
// normalization, versions and logical retry equality remain separate contracts.
// Callers own messages exclusively during encoding. All results are owned copies.
const otlpJSONDepth = 64 // Experimental bound, not a production default.

func encodeOTLPJSON(in *tracepb.TracesData) ([]byte, error) {
	if in == nil {
		return nil, errInvalid
	}
	// Bound traversal before protojson sees a cyclic or oversized Go message.
	// This work budget counts input bytes and values; it is not a peak-memory cap.
	budget := maxMetadata
	if !boundedOTLPMessage(in.ProtoReflect(), 0, &budget) {
		return nil, errInvalid
	}
	b, err := (protojson.MarshalOptions{UseEnumNumbers: true}).Marshal(in)
	if err != nil || checkOTLPJSON(b) != nil {
		return nil, errInvalid
	}
	return rewriteOTLPJSON(b, tracePacket, true)
}

func decodeOTLPJSON(b []byte) (*tracepb.TracesData, error) {
	if checkOTLPJSON(b) != nil {
		return nil, errInvalid
	}
	b, err := rewriteOTLPJSON(b, tracePacket, false)
	if err != nil {
		return nil, errInvalid
	}
	var out tracepb.TracesData
	// Default unknown-field/alias behavior belongs to this lab, not a chosen
	// public decoder policy. OTLP reception still needs its own exact contract.
	err = (protojson.UnmarshalOptions{RecursionLimit: otlpJSONDepth}).Unmarshal(b, &out)
	if err != nil {
		return nil, errInvalid // Never return a partially populated message.
	}
	return &out, nil
}

func boundedOTLPMessage(m protoreflect.Message, depth int, budget *int) bool {
	if depth > otlpJSONDepth || *budget <= 0 {
		return false
	}
	*budget -= 1
	ok := true
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		// The pinned trace schema has no map fields. Refuse an unsupported
		// representation rather than assuming it behaves like a message.
		if fd.IsMap() {
			ok = false
			return false
		}
		visit := func(v protoreflect.Value) bool {
			*budget -= 1
			switch fd.Kind() {
			case protoreflect.MessageKind:
				return boundedOTLPMessage(v.Message(), depth+1, budget)
			case protoreflect.StringKind:
				*budget -= len(v.String())
			case protoreflect.BytesKind:
				*budget -= len(v.Bytes())
			}
			return *budget >= 0
		}
		if fd.IsList() {
			list := v.List()
			for i := 0; i < list.Len(); i++ {
				if !visit(list.Get(i)) {
					ok = false
					return false
				}
			}
			return true
		}
		ok = visit(v)
		return ok
	})
	return ok
}

// Check duplicate keys before decoding objects into maps, which would otherwise
// silently keep only the last value. Number tokens are never converted to float64.
func checkOTLPJSON(b []byte) error {
	if len(b) == 0 || len(b) > maxMetadata || !utf8.Valid(b) {
		return errInvalid
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	if scanOTLPJSON(d, 0) != nil {
		return errInvalid
	}
	if _, err := d.Token(); err != io.EOF {
		return errInvalid
	}
	return nil
}

func scanOTLPJSON(d *json.Decoder, depth int) error {
	if depth > otlpJSONDepth {
		return errInvalid
	}
	token, err := d.Token()
	if err != nil {
		return errInvalid
	}
	switch token {
	case json.Delim('{'):
		seen := make(map[string]struct{})
		for d.More() {
			keyToken, err := d.Token()
			key, ok := keyToken.(string)
			if err != nil || !ok {
				return errInvalid
			}
			if _, duplicate := seen[key]; duplicate {
				return errInvalid
			}
			seen[key] = struct{}{}
			if scanOTLPJSON(d, depth+1) != nil {
				return errInvalid
			}
		}
		end, err := d.Token()
		if err != nil || end != json.Delim('}') {
			return errInvalid
		}
	case json.Delim('['):
		for d.More() {
			if scanOTLPJSON(d, depth+1) != nil {
				return errInvalid
			}
		}
		end, err := d.Token()
		if err != nil || end != json.Delim(']') {
			return errInvalid
		}
	case json.Delim('}'), json.Delim(']'):
		return errInvalid
	}
	return nil
}

type traceJSONLevel uint8

const (
	tracePacket traceJSONLevel = iota
	traceResource
	traceScope
	traceSpan
	traceLink
)

// Walk only the pinned trace schema's nesting, never arbitrary attribute objects.
// RawMessage keeps all unrelated values as JSON bytes rather than float64 values.
func rewriteOTLPJSON(b []byte, level traceJSONLevel, toHex bool) ([]byte, error) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(b, &fields) != nil || fields == nil {
		return nil, errInvalid
	}
	if level == traceSpan || level == traceLink {
		// ProtoJSON also accepts protobuf's snake_case names. They must not
		// bypass hexadecimal conversion and be interpreted as Base64 instead.
		for _, alias := range []string{"trace_id", "span_id", "parent_span_id"} {
			if _, exists := fields[alias]; exists {
				return nil, errInvalid
			}
		}
		for _, field := range []struct {
			name string
			size int
		}{{"traceId", 16}, {"spanId", 8}, {"parentSpanId", 8}} {
			if level == traceLink && field.name == "parentSpanId" {
				continue
			}
			if raw, exists := fields[field.name]; exists {
				var text string
				if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) || json.Unmarshal(raw, &text) != nil {
					return nil, errInvalid
				}
				var id []byte
				var err error
				if toHex {
					id, err = base64.StdEncoding.DecodeString(text)
				} else {
					id, err = hex.DecodeString(text)
				}
				if err != nil || (len(id) != 0 && len(id) != field.size) {
					return nil, errInvalid
				}
				if toHex {
					text = hex.EncodeToString(id)
				} else {
					text = base64.StdEncoding.EncodeToString(id)
				}
				fields[field.name], err = json.Marshal(text)
				if err != nil {
					return nil, errInvalid
				}
			}
		}
	}
	var child, alias string
	switch level {
	case tracePacket:
		child, alias = "resourceSpans", "resource_spans"
	case traceResource:
		child, alias = "scopeSpans", "scope_spans"
	case traceScope:
		child = "spans"
	case traceSpan:
		child = "links"
	}
	if _, exists := fields[alias]; alias != "" && exists {
		return nil, errInvalid
	}
	if raw, exists := fields[child]; child != "" && exists {
		var values []json.RawMessage
		if json.Unmarshal(raw, &values) != nil {
			return nil, errInvalid
		}
		for i := range values {
			var err error
			values[i], err = rewriteOTLPJSON(values[i], level+1, toHex)
			if err != nil {
				return nil, errInvalid
			}
		}
		encoded, err := marshalOTLPFragment(values)
		if err != nil {
			return nil, errInvalid
		}
		fields[child] = encoded
	}
	return marshalOTLPFragment(fields)
}

func marshalOTLPFragment(value any) ([]byte, error) {
	var b bytes.Buffer
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	if e.Encode(value) != nil || b.Len() > maxMetadata+1 {
		return nil, errInvalid
	}
	// Encoder adds one newline after the complete JSON value.
	return bytes.TrimSuffix(b.Bytes(), []byte{'\n'}), nil
}
