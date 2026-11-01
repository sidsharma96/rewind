package record

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"math/rand/v2"
	"sync"
	"testing"
)

func envelopeCodec(t testing.TB, stored, expanded, threshold int) EnvelopeCodec {
	t.Helper()
	c, err := NewEnvelopeCodec(EnvelopeLimits{stored, expanded}, CompressionPolicy{threshold, gzip.BestSpeed})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func envelopeGzip(t testing.TB, payload []byte) []byte {
	t.Helper()
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	if _, err := w.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

// Independent fixture bytes deliberately do not use Encode or production tags.
func envelopeFixture(kind, encoding byte, payload []byte) []byte {
	return append([]byte{'R', 'W', 'N', 'D', 1, kind, encoding}, payload...)
}

func TestEnvelopeHeaderAndOwnership(t *testing.T) {
	c := envelopeCodec(t, 1024, 1024, 32768)
	for _, kind := range []ObjectKind{SpanObject, CaptureObject} {
		payload := []byte{0, 255, 128, 'x'}
		stored, err := c.Encode(kind, payload)
		if err != nil || !bytes.Equal(stored, envelopeFixture(byte(kind), 0, payload)) {
			t.Fatal("wrong fixed envelope", err)
		}
		payload[0] = 42
		if stored[7] != 0 {
			t.Fatal("raw result borrowed payload")
		}
		decoded, err := c.Decode(kind, stored)
		if err != nil || !bytes.Equal(decoded, []byte{0, 255, 128, 'x'}) {
			t.Fatal("changed payload", err)
		}
		clear(stored)
		if decoded[1] != 255 {
			t.Fatal("decoded raw result borrowed stored buffer")
		}
	}
	if got, err := c.Encode(SpanObject, nil); err != nil || !bytes.Equal(got, envelopeFixture(1, 0, nil)) {
		t.Fatal("framing boundary refused empty payload", err)
	}
}

func TestEnvelopeConfigurationAndZeroValue(t *testing.T) {
	defaults := DefaultCompressionPolicy()
	if defaults.Threshold != 32768 || defaults.Level != gzip.BestSpeed {
		t.Fatal("wrong default compression policy")
	}
	for _, limits := range []EnvelopeLimits{{0, 1}, {6, 1}, {-1, 1}, {7, 0}, {7, -1}, {7, int(^uint(0) >> 1)}} {
		if _, err := NewEnvelopeCodec(limits, defaults); !errors.Is(err, ErrEnvelopeInvalid) {
			t.Fatal("invalid limits accepted", limits)
		}
	}
	for _, policy := range []CompressionPolicy{{0, 1}, {-1, 1}, {1, -3}, {1, 10}} {
		if _, err := NewEnvelopeCodec(EnvelopeLimits{64, 64}, policy); !errors.Is(err, ErrEnvelopeInvalid) {
			t.Fatal("invalid compression policy accepted", policy)
		}
	}
	for level := gzip.HuffmanOnly; level <= gzip.BestCompression; level++ {
		if _, err := NewEnvelopeCodec(EnvelopeLimits{64, 64}, CompressionPolicy{1, level}); err != nil {
			t.Fatal("standard gzip level refused", level, err)
		}
	}
	var zero EnvelopeCodec
	if b, err := zero.Encode(SpanObject, nil); b != nil || !errors.Is(err, ErrEnvelopeInvalid) {
		t.Fatal("zero encoder accepted")
	}
	if b, err := zero.Decode(SpanObject, envelopeFixture(1, 0, nil)); b != nil || !errors.Is(err, ErrEnvelopeInvalid) {
		t.Fatal("zero decoder accepted")
	}
	c := envelopeCodec(t, 64, 64, 32)
	if b, err := c.Encode(0, nil); b != nil || !errors.Is(err, ErrEnvelopeInvalid) {
		t.Fatal("invalid kind encoded")
	}
	if b, err := c.Decode(0, envelopeFixture(1, 0, nil)); b != nil || !errors.Is(err, ErrEnvelopeInvalid) {
		t.Fatal("invalid expected kind accepted")
	}
}

func TestEnvelopeLabelsBeforeExpansion(t *testing.T) {
	c := envelopeCodec(t, 1024, 1024, 32)
	good := envelopeFixture(1, 1, []byte("not gzip"))
	for n := 0; n < 7; n++ {
		if b, err := c.Decode(SpanObject, good[:n]); b != nil || !errors.Is(err, ErrEnvelopeInvalid) {
			t.Fatal("short prefix accepted", n)
		}
	}
	for offset := 0; offset < 7; offset++ {
		for value := 0; value < 256; value++ {
			if byte(value) == good[offset] || (offset == 5 && value == 2) || (offset == 6 && value == 0) {
				continue
			}
			b := bytes.Clone(good)
			b[offset] = byte(value)
			want := ErrEnvelopeUnsupported
			if offset < 4 {
				want = ErrEnvelopeInvalid
			}
			if got, err := c.Decode(SpanObject, b); got != nil || !errors.Is(err, want) {
				t.Fatal("invalid label reached gzip", offset, value, err)
			}
		}
	}
	if b, err := c.Decode(CaptureObject, good); b != nil || !errors.Is(err, ErrEnvelopeKindMismatch) {
		t.Fatal("kind mismatch reached corrupt gzip", err)
	}
}

func TestEnvelopeIndependentBounds(t *testing.T) {
	payload := bytes.Repeat([]byte{'x'}, 65536)
	stored := envelopeFixture(1, 1, envelopeGzip(t, payload))
	for _, tc := range []struct {
		name             string
		stored, expanded int
		wantErr          bool
	}{
		{"exact", len(stored), len(payload), false},
		{"stored", len(stored) - 1, len(payload), true},
		{"expanded", len(stored), len(payload) - 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := envelopeCodec(t, tc.stored, tc.expanded, 32)
			before := bytes.Clone(stored)
			got, err := c.Decode(SpanObject, stored)
			if tc.wantErr {
				if got != nil || !errors.Is(err, ErrEnvelopeTooLarge) {
					t.Fatal("wrong bound result", err)
				}
			} else if err != nil || !bytes.Equal(got, payload) {
				t.Fatal("exact bound changed payload", err)
			}
			if !bytes.Equal(stored, before) {
				t.Fatal("decode changed input")
			}
		})
	}
	plain := envelopeFixture(1, 0, []byte("abcd"))
	if b, err := envelopeCodec(t, 11, 3, 32).Decode(SpanObject, plain); b != nil || !errors.Is(err, ErrEnvelopeTooLarge) {
		t.Fatal("raw payload bypassed expanded bound")
	}
	if b, err := envelopeCodec(t, 7, 1, 1).Decode(SpanObject, envelopeFixture(1, 0, nil)); b == nil || err != nil {
		t.Fatal("empty payload did not fit exact header cap", err)
	}
	// A small stored budget must not reject compressible expanded input early.
	c := envelopeCodec(t, 512, len(payload), 32)
	if b, err := c.Encode(SpanObject, payload); err != nil || len(b) > 512 || b[6] != 1 {
		t.Fatal("input was checked against stored instead of expanded cap", err)
	}
	if b, err := c.Encode(SpanObject, append(bytes.Clone(payload), 'x')); b != nil || !errors.Is(err, ErrEnvelopeTooLarge) {
		t.Fatal("encoder exceeded expanded bound", err)
	}
}

func TestEnvelopeCompressionSelection(t *testing.T) {
	const threshold = 32768
	c := envelopeCodec(t, 1<<17, 1<<17, threshold)
	for _, size := range []int{threshold - 7, threshold - 6, threshold - 5} {
		payload := bytes.Repeat([]byte{'x'}, size)
		b, err := c.Encode(SpanObject, payload)
		wantGzip := size+6 >= threshold
		if err != nil || (b[6] == 1) != wantGzip {
			t.Fatal("threshold must include six non-encoding header bytes", size, err)
		}
		if wantGzip {
			// Independent decoder verifies that our writer emits one member.
			src := bytes.NewReader(b[7:])
			r, err := gzip.NewReader(src)
			if err != nil {
				t.Fatal(err)
			}
			r.Multistream(false)
			got, readErr := io.ReadAll(r)
			closeErr := r.Close()
			if readErr != nil || closeErr != nil || src.Len() != 0 || !bytes.Equal(got, payload) {
				t.Fatal("writer emitted multiple or incomplete members")
			}
		}
		got, err := c.Decode(SpanObject, b)
		clear(b)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatal("gzip result borrowed stored bytes", err)
		}
	}
	// Deterministic synthetic incompressible input exercises bounded-candidate
	// abort and raw fallback; tiny inputs cannot save the gzip header/trailer.
	rng := rand.New(rand.NewPCG(1, 2))
	payload := make([]byte, 4096)
	for i := range payload {
		payload[i] = byte(rng.Uint64())
	}
	for _, p := range [][]byte{nil, {1}, {1, 2, 3}, payload} {
		c := envelopeCodec(t, len(p)+7, max(1, len(p)), 1)
		b, err := c.Encode(CaptureObject, p)
		if err != nil || !bytes.Equal(b, envelopeFixture(2, 0, p)) {
			t.Fatal("unhelpful gzip failed to fall back to raw", err)
		}
	}
	if b, err := envelopeCodec(t, 8, 2, 1).Encode(SpanObject, []byte{1, 2}); b != nil || !errors.Is(err, ErrEnvelopeTooLarge) {
		t.Fatal("neither encoding fits but encoder succeeded", err)
	}
}

func TestEnvelopeGzipWholeSequence(t *testing.T) {
	first := envelopeGzip(t, []byte("one "))
	second := envelopeGzip(t, []byte("payload"))
	badCRC := bytes.Clone(second)
	badCRC[len(badCRC)-8] ^= 1
	badSize := bytes.Clone(second)
	badSize[len(badSize)-4] ^= 1
	for _, tc := range []struct {
		name string
		tail []byte
		want error
	}{
		{"second", second, nil},
		{"empty", envelopeGzip(t, nil), nil},
		{"checksum", badCRC, gzip.ErrChecksum},
		{"size", badSize, gzip.ErrChecksum},
		{"short_header", second[:5], io.ErrUnexpectedEOF},
		{"short_trailer", second[:len(second)-1], io.ErrUnexpectedEOF},
		{"junk", []byte("trailing junk"), gzip.ErrHeader},
		{"zero_padding", make([]byte, 10), gzip.ErrHeader},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := append(bytes.Clone(first), tc.tail...)
			stored := envelopeFixture(1, 1, body)
			got, err := envelopeCodec(t, len(stored), 11, 32).Decode(SpanObject, stored)
			if !errors.Is(err, tc.want) || (err != nil && got != nil) {
				t.Fatal("member failure returned data or wrong error", err)
			}
			if err == nil {
				want := []byte("one payload")
				if tc.name == "empty" {
					want = []byte("one ")
				}
				if !bytes.Equal(got, want) {
					t.Fatal("wrong combined output")
				}
			}
		})
	}
	joined := envelopeFixture(1, 1, append(first, second...))
	if b, err := envelopeCodec(t, len(joined), 10, 32).Decode(SpanObject, joined); b != nil || !errors.Is(err, ErrEnvelopeTooLarge) {
		t.Fatal("combined expanded bound was reset per member")
	}
	empty := envelopeFixture(2, 1, envelopeGzip(t, nil))
	if b, err := envelopeCodec(t, len(empty), 1, 32).Decode(CaptureObject, empty); err != nil || len(b) != 0 {
		t.Fatal("valid empty gzip refused", err)
	}
}

func TestEnvelopeConcurrentCalls(t *testing.T) {
	c := envelopeCodec(t, 4096, 4096, 1)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			payload := bytes.Repeat([]byte{byte(i)}, 1024)
			for range 8 {
				b, err := c.Encode(SpanObject, payload)
				if err != nil {
					t.Error(err)
					return
				}
				got, err := c.Decode(SpanObject, b)
				if err != nil || !bytes.Equal(got, payload) {
					t.Error("shared codec retained another request", err)
				}
			}
		})
	}
	wg.Wait()
}
