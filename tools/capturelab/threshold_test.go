package capturelab

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"reflect"
	"testing"
)

// selection is a lab result, not a wire envelope. Count one marker byte when
// reporting size, identically for both encodings. attempted is observation only.
type selection struct {
	data      []byte
	gzip      bool
	attempted bool
}

// chooseCompression borrows raw on either raw outcome; it neither mutates nor
// retains it. compress must not mutate raw and must return an owned buffer.
// encodeThreshold owns raw first, so its caller receives an owned result.
func chooseCompression(raw []byte, threshold int, compress func([]byte) ([]byte, error)) (selection, error) {
	if threshold <= 0 || threshold > maxEncoded+1 || len(raw) > maxEncoded || compress == nil {
		return selection{}, errInvalid
	}
	if len(raw) < threshold {
		return selection{data: raw}, nil
	}
	zipped, err := compress(raw)
	if err != nil {
		return selection{}, err
	}
	if len(zipped) < len(raw) {
		return selection{data: zipped, gzip: true, attempted: true}, nil
	}
	return selection{data: raw, attempted: true}, nil
}

func encodeThreshold(r record, threshold, level int) (selection, error) {
	if level != gzip.BestSpeed && level != gzip.DefaultCompression {
		return selection{}, errInvalid
	}
	raw, err := encodeHybrid(r)
	if err != nil {
		return selection{}, err
	}
	return chooseCompression(raw, threshold, func(b []byte) ([]byte, error) { return zipLevel(b, level) })
}

func decodeSelection(s selection) (record, error) {
	if s.gzip {
		return decodeGzip(s.data, decodeHybrid)
	}
	return decodeHybrid(s.data)
}

type thresholdPolicy struct {
	name             string
	threshold, level int
}

func thresholdPolicies() []thresholdPolicy {
	return []thresholdPolicy{
		{"Never", maxEncoded + 1, gzip.BestSpeed},
		{"AlwaysFast", 1, gzip.BestSpeed}, {"8KiBFast", 8 << 10, gzip.BestSpeed},
		{"32KiBFast", 32 << 10, gzip.BestSpeed}, {"64KiBFast", 64 << 10, gzip.BestSpeed},
		{"AlwaysDefault", 1, gzip.DefaultCompression}, {"8KiBDefault", 8 << 10, gzip.DefaultCompression},
		{"32KiBDefault", 32 << 10, gzip.DefaultCompression}, {"64KiBDefault", 64 << 10, gzip.DefaultCompression},
	}
}

func TestCompressionGates(t *testing.T) {
	for _, size := range []int{7, 8, 9} {
		for _, outputSize := range []int{3, size, size + 1} {
			raw := bytes.Repeat([]byte{'r'}, size)
			original := bytes.Clone(raw)
			candidate := bytes.Repeat([]byte{'z'}, outputSize)
			calls := 0
			got, err := chooseCompression(raw, 8, func(b []byte) ([]byte, error) {
				calls++
				if !bytes.Equal(b, original) {
					t.Fatal("wrong compression input")
				}
				return candidate, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			attempt := size >= 8
			want := raw
			compressed := attempt && outputSize < size
			if compressed {
				want = candidate
			}
			if got.attempted != attempt || got.gzip != compressed || !bytes.Equal(got.data, want) ||
				(calls == 1) != attempt || !bytes.Equal(raw, original) {
				t.Fatal("wrong gate result", size, outputSize)
			}
		}
	}
	failed := errors.New("synthetic compression failure")
	for _, size := range []int{7, 8} {
		got, err := chooseCompression(make([]byte, size), 8, func([]byte) ([]byte, error) { return nil, failed })
		if size == 7 {
			if err != nil || got.attempted {
				t.Fatal("below-threshold compressor called")
			}
		} else if !errors.Is(err, failed) || got.data != nil {
			t.Fatal("compression failure hidden", err)
		}
	}
	for _, threshold := range []int{0, -1, maxEncoded + 2} {
		if _, err := encodeThreshold(record{}, threshold, gzip.BestSpeed); err == nil {
			t.Fatal("invalid threshold accepted")
		}
	}
	if _, err := encodeThreshold(record{}, 8, 100); err == nil {
		t.Fatal("invalid gzip level accepted")
	}
	if _, err := chooseCompression(nil, 8, nil); err == nil {
		t.Fatal("nil compressor accepted")
	}
}

func TestThresholdRoundTrip(t *testing.T) {
	for _, f := range thresholdFixtures() {
		for _, p := range thresholdPolicies() {
			got, err := encodeThreshold(f.record, p.threshold, p.level)
			if err != nil {
				t.Fatal(f.name, p.name, err)
			}
			decoded, err := decodeSelection(got)
			if err != nil || !reflect.DeepEqual(decoded, f.record) {
				t.Fatal("changed values", f.name, p.name, err)
			}
			raw, err := encodeHybrid(f.record)
			if err != nil {
				t.Fatal(err)
			}
			// Independent oracle: derive the selected size and encoding from a
			// direct unconditional compression, not from the selector's result.
			zipped, err := zipLevel(raw, p.level)
			if err != nil {
				t.Fatal(err)
			}
			want := raw
			compressed := len(raw) >= p.threshold && len(zipped) < len(raw)
			if compressed {
				want = zipped
			}
			if got.attempted != (len(raw) >= p.threshold) || got.gzip != compressed || !bytes.Equal(got.data, want) {
				t.Fatal("selector differs from oracle", f.name, p.name)
			}
			if len(got.data) > len(raw) {
				t.Fatal("selection expanded stored data")
			}
		}
	}
}

func TestThresholdOwnership(t *testing.T) {
	for _, p := range thresholdPolicies() {
		r := record{Request: bytes.Repeat([]byte{'a'}, 80<<10), Response: []byte{0, 255, 128}}
		s, err := encodeThreshold(r, p.threshold, p.level)
		if err != nil {
			t.Fatal(err)
		}
		original := bytes.Clone(s.data)
		clear(r.Request)
		clear(r.Response)
		if !bytes.Equal(s.data, original) {
			t.Fatal("encoder borrowed caller bodies")
		}
		decoded, err := decodeSelection(s)
		if err != nil {
			t.Fatal(err)
		}
		clear(s.data)
		if !bytes.Equal(decoded.Request, bytes.Repeat([]byte{'a'}, 80<<10)) || !bytes.Equal(decoded.Response, []byte{0, 255, 128}) {
			t.Fatal("decoded output borrowed stored bytes")
		}
	}
}

func FuzzThresholdSelection(f *testing.F) {
	f.Add([]byte{}, uint32(0), false)
	f.Add(bytes.Repeat([]byte{'a'}, 8192), uint32(8191), false)
	f.Add([]byte{0, 255, 128}, uint32(2), true)
	f.Fuzz(func(t *testing.T, raw []byte, threshold uint32, useDefault bool) {
		// Bound this fuzz workload independently of the larger lab object cap.
		if len(raw) > 64<<10 {
			t.Skip()
		}
		cutoff := int(threshold%(64<<10+1)) + 1
		level := gzip.BestSpeed
		if useDefault {
			level = gzip.DefaultCompression
		}
		original := bytes.Clone(raw)
		got, err := chooseCompression(raw, cutoff, func(b []byte) ([]byte, error) { return zipLevel(b, level) })
		if err != nil {
			t.Fatal(err)
		}
		if got.attempted != (len(raw) >= cutoff) || !bytes.Equal(raw, original) || len(got.data) > len(raw) {
			t.Fatal("wrong boundary, mutation or expansion")
		}
		if !got.gzip {
			if !bytes.Equal(got.data, raw) {
				t.Fatal("raw outcome changed bytes")
			}
			return
		}
		if !got.attempted || len(got.data) >= len(raw) {
			t.Fatal("compressed without savings")
		}
		r, err := gzip.NewReader(bytes.NewReader(got.data))
		if err != nil {
			t.Fatal(err)
		}
		decoded, readErr := io.ReadAll(io.LimitReader(r, int64(len(raw))+1))
		closeErr := r.Close()
		if readErr != nil || closeErr != nil || !bytes.Equal(decoded, raw) {
			t.Fatal("compression changed bytes", readErr, closeErr)
		}
	})
}
