// Package traceid holds the binary execution identifiers used by the catalog.
// These values identify executions; they confer no writing or reading authority.
package traceid

import (
	"encoding/hex"
	"errors"
)

var ErrInvalid = errors.New("invalid trace or span ID")

// Trace is an OpenTelemetry trace ID. Its zero value is invalid.
type Trace [16]byte

// Span is an OpenTelemetry span ID. Its zero value is invalid.
type Span [8]byte

func (id Trace) Valid() bool { return id != (Trace{}) }
func (id Span) Valid() bool  { return id != (Span{}) }

// String formats the bytes as lowercase hex; it does not validate the ID.
func (id Trace) String() string { return hex.EncodeToString(id[:]) }
func (id Span) String() string  { return hex.EncodeToString(id[:]) }

// TraceFromBytes checks a generated message's ID and copies it into an owned value.
// Subsequent changes to b cannot change the returned ID.
func TraceFromBytes(b []byte) (Trace, error) {
	var id Trace
	if len(b) != len(id) {
		return Trace{}, ErrInvalid
	}
	copy(id[:], b)
	if !id.Valid() {
		return Trace{}, ErrInvalid
	}
	return id, nil
}

// SpanFromBytes checks and copies an ID without retaining the input buffer.
func SpanFromBytes(b []byte) (Span, error) {
	var id Span
	if len(b) != len(id) {
		return Span{}, ErrInvalid
	}
	copy(id[:], b)
	if !id.Valid() {
		return Span{}, ErrInvalid
	}
	return id, nil
}

// ParseTrace accepts exactly 32 hex characters, in either case, and a nonzero ID.
// It returns a zero value on every error, including a partially decoded input.
func ParseTrace(s string) (Trace, error) {
	var id Trace
	if len(s) != 2*len(id) {
		return Trace{}, ErrInvalid
	}
	if _, err := hex.Decode(id[:], []byte(s)); err != nil || !id.Valid() {
		return Trace{}, ErrInvalid
	}
	return id, nil
}

// ParseSpan accepts exactly 16 hex characters, in either case, and a nonzero ID.
func ParseSpan(s string) (Span, error) {
	var id Span
	if len(s) != 2*len(id) {
		return Span{}, ErrInvalid
	}
	if _, err := hex.Decode(id[:], []byte(s)); err != nil || !id.Valid() {
		return Span{}, ErrInvalid
	}
	return id, nil
}
