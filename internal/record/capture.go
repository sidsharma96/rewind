package record

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"unicode/utf8"

	"github.com/sidsharma96/rewind/internal/traceid"
)

var ErrSavedCapture = errors.New("invalid saved capture")

// BodyState describes the producer's capture, not independently observed tool
// behavior or permission. Its zero and unknown values are invalid. Missing and
// Redacted carry no bytes; Present includes a complete zero-byte body. Truncated
// retains partial bytes, with no promised offset, original length or prefix.
type BodyState string

const (
	BodyMissing   BodyState = "missing"
	BodyRedacted  BodyState = "redacted"
	BodyTruncated BodyState = "truncated"
	BodyPresent   BodyState = "present"
)

type Body struct {
	State BodyState
	Data  []byte
}

// Capture is one immutable request/response observation for an execution. Its
// owner/project authority comes from the catalog, not this payload. It neither
// requires a saved final span nor declares trace or application completeness.
type Capture struct {
	Trace    traceid.Trace
	Span     traceid.Span
	Request  Body
	Response Body
}

// CaptureLimits bound actual metadata and each body. EnvelopeLimits additionally
// cap the actual combined expanded payload and stored object. Their hypothetical
// maxima need not all fit simultaneously. No production defaults are selected.
type CaptureLimits struct {
	MaxMetadataBytes int
	MaxBodyBytes     int
}

// CaptureCodec implements the first capture payload under RWND version 1/tag 2:
// three little-endian uint64 lengths (JSON header, request, response), then those
// sections. The four required header strings are IDs and body states. Unknown or
// duplicate fields are refused. This is internal, not a public receiver protocol.
// Config is immutable; no I/O, workers, authorization or digest checking occurs.
type CaptureCodec struct {
	limits   CaptureLimits
	envelope EnvelopeCodec
}

const capturePrefix = 24

type captureHeader struct {
	TraceID       string    `json:"trace_id"`
	SpanID        string    `json:"span_id"`
	RequestState  BodyState `json:"request_state"`
	ResponseState BodyState `json:"response_state"`
}

func NewCaptureCodec(limits CaptureLimits, envelope EnvelopeCodec) (CaptureCodec, error) {
	c := CaptureCodec{limits: limits, envelope: envelope}
	if !c.Configured() {
		return CaptureCodec{}, ErrSavedCapture
	}
	return c, nil
}

func (c CaptureCodec) Configured() bool {
	return c.limits.MaxMetadataBytes > 0 && c.limits.MaxBodyBytes > 0 && c.envelope.valid()
}

func validBody(b Body) bool {
	switch b.State {
	case BodyMissing, BodyRedacted:
		return len(b.Data) == 0
	case BodyPresent, BodyTruncated:
		return true
	default:
		return false
	}
}

// header checks all actual component/combined sizes before any body allocation.
// JSON encoding is bounded after validating the fixed-size IDs and known states.
func (c CaptureCodec) header(v Capture) ([]byte, int, error) {
	if !c.Configured() || !v.Trace.Valid() || !v.Span.Valid() || !validBody(v.Request) || !validBody(v.Response) {
		return nil, 0, ErrSavedCapture
	}
	if len(v.Request.Data) > c.limits.MaxBodyBytes || len(v.Response.Data) > c.limits.MaxBodyBytes {
		return nil, 0, ErrEnvelopeTooLarge
	}
	header, err := json.Marshal(captureHeader{v.Trace.String(), v.Span.String(), v.Request.State, v.Response.State})
	if err != nil {
		return nil, 0, ErrSavedCapture
	}
	if len(header) > c.limits.MaxMetadataBytes || capturePrefix > c.envelope.limits.MaxExpandedBytes {
		return nil, 0, ErrEnvelopeTooLarge
	}
	total := capturePrefix
	for _, size := range []int{len(header), len(v.Request.Data), len(v.Response.Data)} {
		if size > c.envelope.limits.MaxExpandedBytes-total {
			return nil, 0, ErrEnvelopeTooLarge
		}
		total += size
	}
	return header, total, nil
}

// Prepare borrows stable input, checks before copying, and returns independent
// prepared bodies and encoded bytes. Every error returns a zero capture/nil bytes.
func (c CaptureCodec) Prepare(v Capture) (Capture, []byte, error) {
	header, total, err := c.header(v)
	if err != nil {
		return Capture{}, nil, err
	}
	v.Request.Data, v.Response.Data = bytes.Clone(v.Request.Data), bytes.Clone(v.Response.Data)
	payload := make([]byte, capturePrefix, total)
	for i, size := range []int{len(header), len(v.Request.Data), len(v.Response.Data)} {
		binary.LittleEndian.PutUint64(payload[i*8:(i+1)*8], uint64(size))
	}
	payload = append(payload, header...)
	payload = append(payload, v.Request.Data...)
	payload = append(payload, v.Response.Data...)
	stored, err := c.envelope.Encode(CaptureObject, payload)
	if err != nil {
		return Capture{}, nil, err
	}
	return v, stored, nil
}

// Decode requires a caller-verified exact object size/digest and authorized
// expected capture kind. The caller cross-checks IDs with that same catalog key.
// Each body is separately owned; retained responses do not pin the whole payload.
func (c CaptureCodec) Decode(stored []byte) (Capture, error) {
	if !c.Configured() {
		return Capture{}, ErrSavedCapture
	}
	payload, err := c.envelope.Decode(CaptureObject, stored)
	if err != nil {
		return Capture{}, err
	}
	if len(payload) < capturePrefix {
		return Capture{}, ErrSavedCapture
	}
	remaining := uint64(len(payload) - capturePrefix)
	var lengths [3]int
	for i, limit := range []int{c.limits.MaxMetadataBytes, c.limits.MaxBodyBytes, c.limits.MaxBodyBytes} {
		n := binary.LittleEndian.Uint64(payload[i*8 : (i+1)*8])
		if n > uint64(limit) {
			return Capture{}, ErrEnvelopeTooLarge
		}
		if n > remaining {
			return Capture{}, ErrSavedCapture
		}
		remaining -= n
		lengths[i] = int(n) // n <= a positive int cap and actual remaining input.
	}
	if remaining != 0 {
		return Capture{}, ErrSavedCapture
	}
	end := capturePrefix + lengths[0]
	v, err := parseCaptureHeader(payload[capturePrefix:end])
	if err != nil {
		return Capture{}, err
	}
	v.Request.Data = payload[end : end+lengths[1]]
	end += lengths[1]
	v.Response.Data = payload[end:]
	if !validBody(v.Request) || !validBody(v.Response) {
		return Capture{}, ErrSavedCapture
	}
	v.Request.Data, v.Response.Data = bytes.Clone(v.Request.Data), bytes.Clone(v.Response.Data)
	return v, nil
}

// Compare validates both complete flat records before equality/difference. No
// byte normalization occurs; a changed state is different even with zero bytes.
func (c CaptureCodec) Compare(a, b Capture) (bool, error) {
	if _, _, err := c.header(a); err != nil {
		return false, err
	}
	if _, _, err := c.header(b); err != nil {
		return false, err
	}
	return a.Trace == b.Trace && a.Span == b.Span && a.Request.State == b.Request.State &&
		a.Response.State == b.Response.State && bytes.Equal(a.Request.Data, b.Request.Data) && bytes.Equal(a.Response.Data, b.Response.Data), nil
}

// Token decoding avoids encoding/json's struct case-folding and duplicate-key
// replacement. Four required strings keep this a fixed schema, not a JSON tree.
func parseCaptureHeader(raw []byte) (Capture, error) {
	if !utf8.Valid(raw) {
		return Capture{}, ErrSavedCapture
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	start, err := d.Token()
	if err != nil || start != json.Delim('{') {
		return Capture{}, ErrSavedCapture
	}
	fields := make(map[string]string, 4)
	for d.More() {
		token, err := d.Token()
		key, ok := token.(string)
		if err != nil || !ok {
			return Capture{}, ErrSavedCapture
		}
		switch key {
		case "trace_id", "span_id", "request_state", "response_state":
		default:
			return Capture{}, ErrSavedCapture
		}
		if _, duplicate := fields[key]; duplicate {
			return Capture{}, ErrSavedCapture
		}
		valueToken, err := d.Token()
		value, ok := valueToken.(string)
		if err != nil || !ok {
			return Capture{}, ErrSavedCapture
		}
		fields[key] = value
	}
	end, err := d.Token()
	if err != nil || end != json.Delim('}') || len(fields) != 4 {
		return Capture{}, ErrSavedCapture
	}
	if _, err := d.Token(); err != io.EOF {
		return Capture{}, ErrSavedCapture
	}
	trace, err := traceid.ParseTrace(fields["trace_id"])
	if err != nil {
		return Capture{}, ErrSavedCapture
	}
	span, err := traceid.ParseSpan(fields["span_id"])
	if err != nil {
		return Capture{}, ErrSavedCapture
	}
	return Capture{Trace: trace, Span: span, Request: Body{State: BodyState(fields["request_state"])}, Response: Body{State: BodyState(fields["response_state"])}}, nil
}
