package record

import (
	"bytes"
	"compress/gzip"
	"io"
	"testing"
)

func FuzzEnvelopeDecode(f *testing.F) {
	f.Add(envelopeFixture(1, 0, []byte{0, 255}), false)
	f.Add(envelopeFixture(2, 1, envelopeGzip(f, []byte("synthetic"))), true)
	f.Add(envelopeFixture(1, 1, append(envelopeGzip(f, nil), envelopeGzip(f, []byte("joined"))...)), false)
	f.Add([]byte{}, false)
	f.Fuzz(func(t *testing.T, stored []byte, capture bool) {
		if len(stored) > 8192 {
			t.Skip()
		}
		kind := SpanObject
		if capture {
			kind = CaptureObject
		}
		before := bytes.Clone(stored)
		got, err := envelopeCodec(t, 4096, 4096, 32).Decode(kind, stored)
		if !bytes.Equal(stored, before) {
			t.Fatal("decoder changed borrowed bytes")
		}
		if err != nil {
			if got != nil {
				t.Fatal("refusal leaked usable bytes")
			}
			return
		}
		if len(stored) < 7 || len(stored) > 4096 || len(got) > 4096 ||
			!bytes.Equal(stored[:6], []byte{'R', 'W', 'N', 'D', 1, byte(kind)}) {
			t.Fatal("accepted malformed or excessive frame")
		}
		want := stored[7:]
		switch stored[6] {
		case 0:
		case 1:
			r, err := gzip.NewReader(bytes.NewReader(want))
			if err != nil {
				t.Fatal("accepted invalid gzip header")
			}
			want, err = io.ReadAll(io.LimitReader(r, 4097))
			closeErr := r.Close()
			if err != nil || closeErr != nil || len(want) > 4096 {
				t.Fatal("accepted incomplete or excessive gzip")
			}
		default:
			t.Fatal("accepted unsupported encoding")
		}
		if !bytes.Equal(got, want) {
			t.Fatal("decoder changed logical payload")
		}
		owned := bytes.Clone(got)
		clear(stored)
		if !bytes.Equal(got, owned) {
			t.Fatal("result borrowed input")
		}
	})
}

func FuzzEnvelopeRoundTrip(f *testing.F) {
	f.Add([]byte{}, uint16(1), false, false)
	f.Add(bytes.Repeat([]byte{'x'}, 8192), uint16(8190), true, false)
	f.Add([]byte{0, 255, 128}, uint16(32), false, true)
	f.Fuzz(func(t *testing.T, payload []byte, threshold uint16, capture, defaultLevel bool) {
		if len(payload) > 16384 {
			t.Skip()
		}
		level := gzip.BestSpeed
		if defaultLevel {
			level = gzip.DefaultCompression
		}
		kind := SpanObject
		if capture {
			kind = CaptureObject
		}
		cutoff := 1 + int(threshold)
		c, err := NewEnvelopeCodec(EnvelopeLimits{16391, 16384}, CompressionPolicy{cutoff, level})
		if err != nil {
			t.Fatal(err)
		}
		before := bytes.Clone(payload)
		stored, err := c.Encode(kind, payload)
		if err != nil || len(stored) > len(payload)+7 || !bytes.Equal(payload, before) {
			t.Fatal("encoder changed input or expanded stored object", err)
		}
		got, err := c.Decode(kind, stored)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatal("round trip changed payload", err)
		}
		// Use direct standard-library compression as the size-selection oracle.
		var candidate bytes.Buffer
		w, err := gzip.NewWriterLevel(&candidate, level)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = w.Write(payload); err != nil {
			t.Fatal(err)
		}
		if err = w.Close(); err != nil {
			t.Fatal(err)
		}
		compressed := len(payload)+6 >= cutoff && candidate.Len() < len(payload)
		if (stored[6] == 1) != compressed {
			t.Fatal("selection differs from independent full gzip candidate")
		}
		clear(payload)
		if !bytes.Equal(got, before) {
			t.Fatal("result borrowed original payload")
		}
	})
}
