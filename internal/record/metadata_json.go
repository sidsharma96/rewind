package record

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

// Encode returns independently owned JSON with numeric enums and hexadecimal
// span/link IDs. The caller keeps the entire graph stable. Unsupported fields,
// repeated attribute keys and exceeded bounds return no bytes. This is metadata
// serialization, not full semantic validation, a saved envelope or OTLP receiver.
func (c MetadataCodec) Encode(in *tracepb.TracesData) ([]byte, error) {
	// Bound traversal before protojson sees a cyclic or oversized Go message.
	// This work budget counts input bytes and values; it is not a peak-memory cap.
	if err := c.Check(in); err != nil {
		return nil, err
	}
	b, err := (protojson.MarshalOptions{UseEnumNumbers: true}).Marshal(in)
	if err != nil || c.checkJSON(b) != nil {
		return nil, ErrMetadataInvalid
	}
	return c.rewriteJSON(b, tracePacket, true)
}

// Decode strictly parses bounded JSON into an independently owned graph. It
// refuses unknown JSON fields and returns no partially populated result on error.
// Parsing alone does not check semantic validity, IDs, duplicate attribute keys
// or ordering; run Check/NormalizeOwned before subsequent encoding/comparison.
func (c MetadataCodec) Decode(b []byte) (*tracepb.TracesData, error) {
	if !c.valid() || c.checkJSON(b) != nil {
		return nil, ErrMetadataInvalid
	}
	b, err := c.rewriteJSON(b, tracePacket, false)
	if err != nil {
		return nil, ErrMetadataInvalid
	}
	var out tracepb.TracesData
	// Saved metadata must not silently discard unknown JSON fields. Keep the
	// default strict decoder; a future OTLP receiver has a different transport
	// rule. Public error mapping and complete receiver validation remain open.
	err = (protojson.UnmarshalOptions{RecursionLimit: c.limits.MaxDepth}).Unmarshal(b, &out)
	if err != nil {
		return nil, ErrMetadataInvalid // Never return a partially populated message.
	}
	return &out, nil
}

func (c MetadataCodec) validateMessage(m protoreflect.Message, depth int, budget *int) error {
	// Nil repeated messages and selected oneof arms with nil message pointers
	// are malformed Go graphs. Clone/ProtoJSON can turn them into empty shells;
	// refuse before either operation can erase that distinction.
	if !m.IsValid() || depth > c.limits.MaxDepth || *budget <= 0 {
		return ErrMetadataInvalid
	}
	// ProtoJSON drops these bytes. Check every message, including nested
	// attribute values, without clearing, copying or interpreting the unknowns.
	if len(m.GetUnknown()) != 0 {
		return ErrMetadataUnsupported
	}
	// Missing/zero target IDs are preserved evidence, but a wrong nonempty
	// width is malformed. Check here as well as in the JSON adapter so direct
	// comparison cannot turn malformed link IDs into equal/conflicting content.
	if link, ok := m.Interface().(*tracepb.Span_Link); ok {
		if _, _, err := TargetOfLink(link); err != nil {
			return ErrMetadataInvalid
		}
	}
	*budget -= 1
	var err error
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		// The pinned trace schema has no map fields. Refuse an unsupported
		// representation rather than assuming it behaves like a message.
		if fd.IsMap() {
			err = ErrMetadataUnsupported
			return false
		}
		visit := func(v protoreflect.Value) error {
			*budget -= 1
			switch fd.Kind() {
			case protoreflect.MessageKind:
				return c.validateMessage(v.Message(), depth+1, budget)
			case protoreflect.StringKind:
				*budget -= len(v.String())
			case protoreflect.BytesKind:
				*budget -= len(v.Bytes())
			}
			if *budget < 0 {
				return ErrMetadataInvalid
			}
			return nil
		}
		if fd.IsList() {
			list := v.List()
			for i := 0; i < list.Len(); i++ {
				if err = visit(list.Get(i)); err != nil {
					return false
				}
			}
			return true
		}
		err = visit(v)
		return err == nil
	})
	if err != nil {
		return err
	}
	// Bound the complete subtree before allocating its temporary key set.
	// Both encoder and normalizer use this read-only guard.
	return validateUniqueAttributeKeys(otlpAttributeList(m))
}

// Check duplicate keys before decoding objects into maps, which would otherwise
// silently keep only the last value. Number tokens are never converted to float64.
func (c MetadataCodec) checkJSON(b []byte) error {
	if len(b) == 0 || len(b) > c.limits.MaxBytes || !utf8.Valid(b) {
		return ErrMetadataInvalid
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	if c.scanJSON(d, 0) != nil {
		return ErrMetadataInvalid
	}
	if _, err := d.Token(); err != io.EOF {
		return ErrMetadataInvalid
	}
	return nil
}

func (c MetadataCodec) scanJSON(d *json.Decoder, depth int) error {
	if depth > c.limits.MaxDepth {
		return ErrMetadataInvalid
	}
	token, err := d.Token()
	if err != nil {
		return ErrMetadataInvalid
	}
	switch token {
	case json.Delim('{'):
		seen := make(map[string]struct{})
		for d.More() {
			keyToken, err := d.Token()
			key, ok := keyToken.(string)
			if err != nil || !ok {
				return ErrMetadataInvalid
			}
			if _, duplicate := seen[key]; duplicate {
				return ErrMetadataInvalid
			}
			seen[key] = struct{}{}
			if c.scanJSON(d, depth+1) != nil {
				return ErrMetadataInvalid
			}
		}
		end, err := d.Token()
		if err != nil || end != json.Delim('}') {
			return ErrMetadataInvalid
		}
	case json.Delim('['):
		for d.More() {
			if c.scanJSON(d, depth+1) != nil {
				return ErrMetadataInvalid
			}
		}
		end, err := d.Token()
		if err != nil || end != json.Delim(']') {
			return ErrMetadataInvalid
		}
	case json.Delim('}'), json.Delim(']'):
		return ErrMetadataInvalid
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
func (c MetadataCodec) rewriteJSON(b []byte, level traceJSONLevel, toHex bool) ([]byte, error) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(b, &fields) != nil || fields == nil {
		return nil, ErrMetadataInvalid
	}
	if level == traceSpan || level == traceLink {
		// ProtoJSON also accepts protobuf's snake_case names. They must not
		// bypass hexadecimal conversion and be interpreted as Base64 instead.
		for _, alias := range []string{"trace_id", "span_id", "parent_span_id"} {
			if _, exists := fields[alias]; exists {
				return nil, ErrMetadataInvalid
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
					return nil, ErrMetadataInvalid
				}
				var id []byte
				var err error
				if toHex {
					id, err = base64.StdEncoding.DecodeString(text)
				} else {
					id, err = hex.DecodeString(text)
				}
				if err != nil || (len(id) != 0 && len(id) != field.size) {
					return nil, ErrMetadataInvalid
				}
				if toHex {
					text = hex.EncodeToString(id)
				} else {
					text = base64.StdEncoding.EncodeToString(id)
				}
				fields[field.name], err = json.Marshal(text)
				if err != nil {
					return nil, ErrMetadataInvalid
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
		return nil, ErrMetadataInvalid
	}
	if raw, exists := fields[child]; child != "" && exists {
		var values []json.RawMessage
		if json.Unmarshal(raw, &values) != nil {
			return nil, ErrMetadataInvalid
		}
		for i := range values {
			var err error
			values[i], err = c.rewriteJSON(values[i], level+1, toHex)
			if err != nil {
				return nil, ErrMetadataInvalid
			}
		}
		encoded, err := c.marshalFragment(values)
		if err != nil {
			return nil, ErrMetadataInvalid
		}
		fields[child] = encoded
	}
	return c.marshalFragment(fields)
}

func (c MetadataCodec) marshalFragment(value any) ([]byte, error) {
	var b bytes.Buffer
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	if e.Encode(value) != nil {
		return nil, ErrMetadataInvalid
	}
	// Encoder adds one newline after the complete JSON value.
	out := bytes.TrimSuffix(b.Bytes(), []byte{'\n'})
	if len(out) > c.limits.MaxBytes {
		return nil, ErrMetadataInvalid
	}
	return out, nil
}
