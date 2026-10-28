package capturelab

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"unicode/utf8"
)

const (
	headerSize  = 30
	maxMetadata = 256 << 10
	maxBody     = 8 << 20
	maxEncoded  = 24 << 20
)

var errInvalid = errors.New("invalid experimental record")

// These are synthetic workload fields, not a schema for OpenTelemetry or Rewind.
// Bounded strings and attribute count keep their JSON encoding below maxMetadata
// even when each input byte expands into a six-byte JSON escape.
type metadata struct {
	TraceID    string
	SpanID     string
	Name       string
	Resource   string
	Scope      string
	StartNS    uint64
	EndNS      uint64
	Attributes []attribute
}

type attribute struct{ Key, Value string }

type record struct {
	Metadata metadata
	Request  []byte
	Response []byte
}

func valid(r record) bool {
	m := r.Metadata
	if len(r.Request) > maxBody || len(r.Response) > maxBody ||
		len(m.TraceID) > 128 || len(m.SpanID) > 128 || len(m.Name) > 1024 ||
		len(m.Resource) > 1024 || len(m.Scope) > 1024 || len(m.Attributes) > 64 {
		return false
	}
	for _, a := range m.Attributes {
		if len(a.Key) > 64 || len(a.Value) > 256 || !utf8.ValidString(a.Key) || !utf8.ValidString(a.Value) {
			return false
		}
	}
	for _, s := range []string{m.TraceID, m.SpanID, m.Name, m.Resource, m.Scope} {
		if !utf8.ValidString(s) {
			return false
		}
	}
	return true
}

type codec struct {
	name   string
	encode func(record) ([]byte, error)
	decode func([]byte) (record, error)
}

func plainCodecs() []codec {
	return []codec{{"JSON", encodeJSON, decodeJSON}, {"Hybrid", encodeHybrid, decodeHybrid}}
}

func codecs() []codec {
	result := plainCodecs()
	for _, setting := range []struct {
		name  string
		level int
	}{{"Gzip", gzip.BestSpeed}, {"GzipDefault", gzip.DefaultCompression}} {
		for _, base := range plainCodecs() {
			result = append(result, codec{base.name + setting.name, func(r record) ([]byte, error) {
				b, err := base.encode(r)
				if err != nil {
					return nil, err
				}
				return zipLevel(b, setting.level)
			}, func(b []byte) (record, error) {
				return decodeGzip(b, base.decode)
			}})
		}
	}
	return result
}

func decodeGzip(b []byte, decode func([]byte) (record, error)) (record, error) {
	if len(b) > maxEncoded {
		return record{}, errInvalid
	}
	r, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return record{}, err
	}
	// +1 distinguishes exactly-at-limit from oversized expanded output.
	decoded, readErr := io.ReadAll(io.LimitReader(r, maxEncoded+1))
	closeErr := r.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return record{}, err
	}
	if len(decoded) > maxEncoded {
		return record{}, errInvalid
	}
	return decode(decoded)
}

func zip(b []byte) ([]byte, error) {
	return zipLevel(b, gzip.BestSpeed)
}

func zipLevel(b []byte, level int) ([]byte, error) {
	var out bytes.Buffer
	w, err := gzip.NewWriterLevel(&out, level)
	if err != nil {
		return nil, err
	}
	_, writeErr := w.Write(b)
	closeErr := w.Close()
	if err := errors.Join(writeErr, closeErr); err != nil {
		return nil, err
	}
	if out.Len() > maxEncoded {
		return nil, errInvalid
	}
	return out.Bytes(), nil
}

func encodeJSON(r record) ([]byte, error) {
	if !valid(r) {
		return nil, errInvalid
	}
	b, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	if len(b) > maxEncoded {
		return nil, errInvalid
	}
	return b, nil
}

func decodeJSON(b []byte) (record, error) {
	if len(b) > maxEncoded {
		return record{}, errInvalid
	}
	var r record
	if err := json.Unmarshal(b, &r); err != nil {
		return record{}, err
	}
	if !valid(r) {
		return record{}, errInvalid
	}
	return r, nil
}

func encodeHybrid(r record) ([]byte, error) {
	if !valid(r) {
		return nil, errInvalid
	}
	m, err := json.Marshal(r.Metadata)
	if err != nil {
		return nil, err
	}
	if len(m) > maxMetadata {
		return nil, errInvalid
	}
	n := headerSize + len(m) + len(r.Request) + len(r.Response)
	if n > maxEncoded {
		return nil, errInvalid
	}
	b := make([]byte, 0, n)
	b = append(b, 'R', 'W', 'L', '1', present(r.Request), present(r.Response))
	for _, n := range []int{len(m), len(r.Request), len(r.Response)} {
		b = binary.LittleEndian.AppendUint64(b, uint64(n))
	}
	b = append(b, m...)
	b = append(b, r.Request...)
	return append(b, r.Response...), nil
}

func present(b []byte) byte {
	if b == nil {
		return 0
	}
	return 1
}

func decodeHybrid(b []byte) (record, error) {
	if len(b) < headerSize || len(b) > maxEncoded || string(b[:4]) != "RWL1" || b[4] > 1 || b[5] > 1 {
		return record{}, errInvalid
	}
	remaining := uint64(len(b) - headerSize)
	var lengths [3]int
	for i, limit := range []uint64{maxMetadata, maxBody, maxBody} {
		n := binary.LittleEndian.Uint64(b[6+i*8 : 14+i*8])
		if n > limit || n > remaining {
			return record{}, errInvalid
		}
		remaining -= n
		lengths[i] = int(n) // n is now <= maxMetadata or maxBody, also on 32-bit Go.
	}
	if remaining != 0 || b[4] == 0 && lengths[1] != 0 || b[5] == 0 && lengths[2] != 0 {
		return record{}, errInvalid
	}
	var r record
	end := headerSize + lengths[0]
	if err := json.Unmarshal(b[headerSize:end], &r.Metadata); err != nil {
		return record{}, err
	}
	if b[4] == 1 {
		r.Request = bytes.Clone(b[end : end+lengths[1]])
	}
	end += lengths[1]
	if b[5] == 1 {
		r.Response = bytes.Clone(b[end:])
	}
	if !valid(r) {
		return record{}, errInvalid
	}
	return r, nil
}
