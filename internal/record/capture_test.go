package record

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/sidsharma96/rewind/internal/traceid"
)

func captureCodec(t testing.TB, threshold int) CaptureCodec {
	t.Helper()
	e, err := NewEnvelopeCodec(EnvelopeLimits{8192, 8192}, CompressionPolicy{threshold, 1})
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewCaptureCodec(CaptureLimits{512, 2048}, e)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func captureFixture(request, response BodyState) Capture {
	v := Capture{Trace: traceid.Trace{1}, Span: traceid.Span{42}, Request: Body{State: request}, Response: Body{State: response}}
	if request == BodyPresent || request == BodyTruncated {
		v.Request.Data = []byte{0, 255, 'q'}
	}
	if response == BodyPresent || response == BodyTruncated {
		v.Response.Data = []byte{'r', 128, 0}
	}
	return v
}

func TestCaptureStatesBytesAndOwnership(t *testing.T) {
	states := []BodyState{BodyMissing, BodyRedacted, BodyTruncated, BodyPresent}
	for _, request := range states {
		for _, response := range states {
			v := captureFixture(request, response)
			for _, threshold := range []int{1, 32768} {
				c := captureCodec(t, threshold)
				owned, stored, err := c.Prepare(v)
				if err != nil {
					t.Fatal(err)
				}
				read, err := c.Decode(stored)
				if err != nil {
					t.Fatal(err)
				}
				if equal, err := c.Compare(v, read); err != nil || !equal {
					t.Fatal("state/bytes changed", err)
				}
				// Independent fixed-schema oracle for the actual writer's header/raw bodies.
				payload, err := c.envelope.Decode(CaptureObject, stored)
				if err != nil {
					t.Fatal(err)
				}
				n := int(binary.LittleEndian.Uint64(payload[:8]))
				var header map[string]string
				if err := json.Unmarshal(payload[24:24+n], &header); err != nil {
					t.Fatal(err)
				}
				if len(header) != 4 || header["request_state"] != string(request) || header["response_state"] != string(response) || header["trace_id"] != v.Trace.String() || header["span_id"] != v.Span.String() {
					t.Fatal("header oracle")
				}
				if !bytes.Equal(payload[24+n:], append(bytes.Clone(v.Request.Data), v.Response.Data...)) {
					t.Fatal("raw bodies changed")
				}
				before := bytes.Clone(read.Request.Data)
				clear(v.Request.Data)
				clear(owned.Request.Data)
				clear(stored)
				if !bytes.Equal(read.Request.Data, before) {
					t.Fatal("read borrows input/prepared/encoded data")
				}
				v = captureFixture(request, response)
			}
		}
	}
	// Explicit state, not slice nilness, carries presence and preserves empty output.
	c := captureCodec(t, 32768)
	v := captureFixture(BodyMissing, BodyPresent)
	v.Response.Data = nil
	_, stored, err := c.Prepare(v)
	if err != nil {
		t.Fatal(err)
	}
	r, err := c.Decode(stored)
	if err != nil || r.Response.State != BodyPresent || len(r.Response.Data) != 0 {
		t.Fatal("present-empty disappeared")
	}
	v.Response.State = BodyMissing
	if same, err := c.Compare(v, r); err != nil || same {
		t.Fatal("missing equals present-empty", err)
	}
	// Each body has a separate buffer: reslicing cannot expose/change its peer.
	v = captureFixture(BodyPresent, BodyPresent)
	_, stored, err = c.Prepare(v)
	if err != nil {
		t.Fatal(err)
	}
	r, err = c.Decode(stored)
	if err != nil {
		t.Fatal(err)
	}
	before := bytes.Clone(r.Response.Data)
	r.Request.Data = append(r.Request.Data, bytes.Repeat([]byte{1}, 64)...)
	if !bytes.Equal(r.Response.Data, before) {
		t.Fatal("append corrupted other body")
	}
	t.Log("all 16 body-state pairs preserve arbitrary bytes; present-empty differs from missing; returned bodies are owned")
}

func captureFrame(t testing.TB, c CaptureCodec, header []byte, request, response []byte) []byte {
	t.Helper()
	p := make([]byte, 24)
	for i, n := range []int{len(header), len(request), len(response)} {
		binary.LittleEndian.PutUint64(p[i*8:(i+1)*8], uint64(n))
	}
	p = append(p, header...)
	p = append(p, request...)
	p = append(p, response...)
	b, err := c.envelope.Encode(CaptureObject, p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestCaptureHeaderRefusesAmbiguousAndUnsupportedRecords(t *testing.T) {
	c := captureCodec(t, 32768)
	header := `{"trace_id":"01000000000000000000000000000000","span_id":"2a00000000000000","request_state":"missing","response_state":"present"}`
	for _, bad := range []string{
		strings.Replace(header, `"trace_id"`, `"Trace_ID"`, 1),
		strings.Replace(header, `"trace_id":`, `"extra":"x","trace_id":`, 1),
		strings.Replace(header, `"trace_id":`, `"trace_id":"02000000000000000000000000000000","trace_id":`, 1),
		strings.Replace(header, `"trace_id":`, `"\u0074race_id":"02000000000000000000000000000000","trace_id":`, 1),
		strings.Replace(header, `,"response_state":"present"`, "", 1),
		strings.Replace(header, `"present"`, `null`, 1),
		strings.Replace(header, `"present"`, `1`, 1),
		strings.Replace(header, `"present"`, `[]`, 1),
		strings.Replace(header, `"present"`, `"future"`, 1),
		strings.Replace(header, `"present"`, `"\ud800"`, 1),
		strings.Replace(header, `"present"`, "\"\xff\"", 1),
		strings.Replace(header, `01000000000000000000000000000000`, `00000000000000000000000000000000`, 1),
		strings.Replace(header, `2a00000000000000`, `2a`, 1),
		header + ` {}`, `[]`, `{`,
	} {
		read, err := c.Decode(captureFrame(t, c, []byte(bad), nil, []byte("result")))
		if !errors.Is(err, ErrSavedCapture) || !reflect.DeepEqual(read, Capture{}) {
			t.Fatalf("accepted ambiguous header %q: %v", bad, err)
		}
	}
	// Reordered, whitespace-formatted metadata and upper-case IDs remain equivalent.
	other := ` { "response_state":"present", "request_state":"missing", "span_id":"2A00000000000000", "trace_id":"01000000000000000000000000000000" } `
	a, err := c.Decode(captureFrame(t, c, []byte(header), nil, []byte("result")))
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.Decode(captureFrame(t, c, []byte(other), nil, []byte("result")))
	if err != nil {
		t.Fatal(err)
	}
	if equal, err := c.Compare(a, b); err != nil || !equal {
		t.Fatal("formatting changed content identity", err)
	}
	for _, state := range []BodyState{BodyMissing, BodyRedacted} {
		bad := strings.Replace(header, `"request_state":"missing"`, `"request_state":"`+string(state)+`"`, 1)
		if _, err := c.Decode(captureFrame(t, c, []byte(bad), []byte("not absent"), nil)); !errors.Is(err, ErrSavedCapture) {
			t.Fatal("unavailable state retained body", err)
		}
	}
}

func TestCaptureLengthAndCombinedLimits(t *testing.T) {
	c := captureCodec(t, 32768)
	v := captureFixture(BodyPresent, BodyPresent)
	_, stored, err := c.Prepare(v)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := c.envelope.Decode(CaptureObject, stored)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func([]byte) []byte{
		func(b []byte) []byte { return b[:23] },
		func(b []byte) []byte { return b[:len(b)-1] },
		func(b []byte) []byte { return append(b, 0) },
		func(b []byte) []byte { binary.LittleEndian.PutUint64(b[:8], ^uint64(0)); return b },
		func(b []byte) []byte { binary.LittleEndian.PutUint64(b[8:16], ^uint64(0)); return b },
		func(b []byte) []byte { binary.LittleEndian.PutUint64(b[16:24], ^uint64(0)); return b },
	} {
		b, err := c.envelope.Encode(CaptureObject, mutate(bytes.Clone(payload)))
		if err != nil {
			t.Fatal(err)
		}
		read, err := c.Decode(b)
		if err == nil || !reflect.DeepEqual(read, Capture{}) {
			t.Fatal("accepted inconsistent lengths")
		}
	}
	e, err := NewEnvelopeCodec(EnvelopeLimits{1024, 400}, CompressionPolicy{32768, 1})
	if err != nil {
		t.Fatal(err)
	}
	limited, err := NewCaptureCodec(CaptureLimits{512, 300}, e)
	if err != nil {
		t.Fatal(err)
	}
	v.Request.Data = bytes.Repeat([]byte{'a'}, 100)
	v.Response.Data = nil
	if _, _, err := limited.Prepare(v); err != nil {
		t.Fatal("independent maxima wrongly required to fit together", err)
	}
	v.Request.Data = bytes.Repeat([]byte{'a'}, 300)
	if owned, b, err := limited.Prepare(v); !errors.Is(err, ErrEnvelopeTooLarge) || b != nil || !reflect.DeepEqual(owned, Capture{}) {
		t.Fatal("actual total above cap copied/encoded", err)
	}
	if _, err := limited.Compare(captureFixture(BodyPresent, BodyPresent), v); !errors.Is(err, ErrEnvelopeTooLarge) {
		t.Fatal("second complete record not bounded before comparison", err)
	}
	bad := captureFixture(BodyPresent, BodyPresent)
	bad.Request.State = "future"
	bad.Trace = traceid.Trace{2}
	if _, err := c.Compare(v, bad); !errors.Is(err, ErrSavedCapture) {
		t.Fatal("invalid second record became different", err)
	}
	if _, _, err := (CaptureCodec{}).Prepare(v); !errors.Is(err, ErrSavedCapture) {
		t.Fatal("unconfigured codec")
	}
}
