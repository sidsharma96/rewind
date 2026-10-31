package capturelab

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"reflect"
	"testing"
)

// expandGzip is the encoding lab's byte boundary, not a saved-record decoder.
// Limits are caller-selected experimental values, not production defaults. The
// caller keeps b stable until return; success returns owned bytes. Every error
// returns no output, even when the decompressor delivered a valid-looking prefix.
//
// The stored and expanded caps are independent. One probe byte distinguishes
// exact fit from excess; neither cap is a peak-memory or elapsed-time guarantee.
// The selected gzip policy joins all valid members into one payload. Every member
// must finish successfully; the caps cover the whole input and combined output.
// This exercises the policy in the lab; runtime saved-object decoding is separate.
func expandGzip(b []byte, maxStored, maxExpanded int) ([]byte, error) {
	if maxStored <= 0 || maxExpanded <= 0 || maxExpanded == int(^uint(0)>>1) || len(b) > maxStored {
		return nil, errInvalid
	}
	r, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	decoded, readErr := io.ReadAll(io.LimitReader(r, int64(maxExpanded)+1))
	closeErr := r.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return nil, err
	}
	if len(decoded) > maxExpanded {
		return nil, errInvalid
	}
	return decoded, nil
}

func gzipFixture(t testing.TB, plain []byte) []byte {
	t.Helper()
	var out bytes.Buffer
	w, err := gzip.NewWriterLevel(&out, gzip.BestSpeed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(plain); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func TestGzipIndependentLimits(t *testing.T) {
	plain := bytes.Repeat([]byte{'x'}, 64<<10)
	encoded := gzipFixture(t, plain)
	for _, tc := range []struct {
		name             string
		stored, expanded int
		wantErr          bool
	}{
		{"exact_fit", len(encoded), len(plain), false},
		{"stored_too_large", len(encoded) - 1, len(plain), true},
		{"expanded_too_large", len(encoded), len(plain) - 1, true},
		{"small_stored_large_output", len(encoded), 1024, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := bytes.Clone(encoded)
			got, err := expandGzip(encoded, tc.stored, tc.expanded)
			if tc.wantErr {
				if !errors.Is(err, errInvalid) || got != nil {
					t.Fatal("limit failure returned a partial payload or wrong error")
				}
			} else if err != nil || !bytes.Equal(got, plain) {
				t.Fatal("exact limit changed the payload", err)
			}
			if !bytes.Equal(encoded, before) {
				t.Fatal("expansion changed caller-owned encoded bytes")
			}
		})
	}
	t.Logf("gzip limits: stored=%d expanded=%d; exact fit accepted; either smaller cap refused", len(encoded), len(plain))
	got, err := expandGzip(encoded, len(encoded), len(plain))
	if err != nil {
		t.Fatal(err)
	}
	clear(encoded)
	if !bytes.Equal(got, plain) {
		t.Fatal("expanded result retained the caller's encoded buffer")
	}
	// Empty plaintext is allowed under positive limits; a zero cap is not an
	// implicit configuration default.
	empty := gzipFixture(t, nil)
	if got, err := expandGzip(empty, len(empty), 1); err != nil || len(got) != 0 {
		t.Fatal("empty gzip plaintext refused", err)
	}
}

func TestGzipInvalidLimits(t *testing.T) {
	encoded := gzipFixture(t, []byte("synthetic"))
	for _, limits := range [][2]int{{0, 1}, {-1, 1}, {1, 0}, {1, -1}, {len(encoded), int(^uint(0) >> 1)}} {
		if got, err := expandGzip(encoded, limits[0], limits[1]); !errors.Is(err, errInvalid) || got != nil {
			t.Fatal("invalid limits returned output or wrong error")
		}
	}
}

func TestGzipCompletionBeforePayloadDecode(t *testing.T) {
	encoded := gzipFixture(t, []byte("synthetic complete body"))
	for _, tc := range []struct {
		name string
		body []byte
		want error
	}{
		{"truncated_trailer", encoded[:len(encoded)-1], io.ErrUnexpectedEOF},
		{"bad_checksum", bytes.Clone(encoded), gzip.ErrChecksum},
		{"bad_size", bytes.Clone(encoded), gzip.ErrChecksum},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "bad_checksum" {
				tc.body[len(tc.body)-8] ^= 1
			}
			if tc.name == "bad_size" {
				tc.body[len(tc.body)-4] ^= 1
			}
			called := false
			got, err := decodeGzip(tc.body, func([]byte) (record, error) {
				called = true
				return record{}, nil
			})
			if !errors.Is(err, tc.want) || called || !reflect.DeepEqual(got, record{}) {
				t.Fatal("unfinished gzip reached payload decoder or returned data", err)
			}
		})
	}
}

func TestGzipMembers(t *testing.T) {
	plain := []byte("one logical payload")
	first := gzipFixture(t, plain[:7])
	second := gzipFixture(t, plain[7:])
	joined := append(bytes.Clone(first), second...)
	got, err := expandGzip(joined, len(joined), len(plain))
	if err != nil || !bytes.Equal(got, plain) {
		t.Fatal("gzip members did not produce their combined payload", err)
	}
	if got, err := expandGzip(joined, len(joined), len(plain)-1); !errors.Is(err, errInvalid) || got != nil {
		t.Fatal("second member bypassed the total expanded cap")
	}
	if got, err := expandGzip(joined, len(first), len(plain)); !errors.Is(err, errInvalid) || got != nil {
		t.Fatal("second member bypassed the total stored cap")
	}
	empty := gzipFixture(t, nil)
	withEmpty := append(gzipFixture(t, plain), empty...)
	if got, err := expandGzip(withEmpty, len(withEmpty), len(plain)); err != nil || !bytes.Equal(got, plain) {
		t.Fatal("empty extra gzip member changed the lab payload", err)
	}
	t.Log("gzip members: split payload and extra empty member accepted; whole-input and combined-output caps enforced")
}

func TestGzipMemberCompletionBeforePayloadDecode(t *testing.T) {
	plain := []byte("one logical payload")
	first := gzipFixture(t, plain[:7])
	second := gzipFixture(t, plain[7:])
	badChecksum := bytes.Clone(second)
	badChecksum[len(badChecksum)-8] ^= 1
	badSize := bytes.Clone(second)
	badSize[len(badSize)-4] ^= 1
	for _, tc := range []struct {
		name string
		tail []byte
		want error
	}{
		{"valid_second", second, nil},
		{"bad_second_checksum", badChecksum, gzip.ErrChecksum},
		{"bad_second_size", badSize, gzip.ErrChecksum},
		{"truncated_second_trailer", second[:len(second)-1], io.ErrUnexpectedEOF},
		{"truncated_second_header", second[:5], io.ErrUnexpectedEOF},
		{"trailing_junk", []byte("not a gzip member"), gzip.ErrHeader},
		{"trailing_zero_padding", make([]byte, 10), gzip.ErrHeader},
	} {
		t.Run(tc.name, func(t *testing.T) {
			encoded := append(bytes.Clone(first), tc.tail...)
			decoded, err := expandGzip(encoded, len(encoded), len(plain))
			if !errors.Is(err, tc.want) || (err != nil && decoded != nil) {
				t.Fatal("member failure returned partial bytes or wrong error", err)
			}
			calls := 0
			got, err := decodeGzip(encoded, func(b []byte) (record, error) {
				calls++
				if !bytes.Equal(b, plain) {
					t.Fatal("payload parser saw an incomplete joined payload")
				}
				return record{}, nil
			})
			wantCalls := 0
			if tc.want == nil {
				wantCalls = 1
			}
			if !errors.Is(err, tc.want) || calls != wantCalls || !reflect.DeepEqual(got, record{}) {
				t.Fatal("member completion did not guard the payload parser", err, calls)
			}
		})
	}
	t.Log("gzip completion: invalid later members, trailing junk and zero padding refused before payload parsing")
}

func FuzzGzipExpandedLimit(f *testing.F) {
	f.Add(gzipFixture(f, []byte("synthetic seed")), uint16(64))
	joined := append(gzipFixture(f, []byte("first")), gzipFixture(f, []byte("second"))...)
	f.Add(joined, uint16(64))
	f.Add([]byte{}, uint16(1))
	f.Fuzz(func(t *testing.T, encoded []byte, size uint16) {
		maxExpanded := 1 + int(size%4096)
		got, err := expandGzip(encoded, 4096, maxExpanded)
		if err != nil {
			if got != nil {
				t.Fatal("failed expansion leaked a partial payload")
			}
			return
		}
		if len(got) > maxExpanded || len(encoded) > 4096 {
			t.Fatal("successful expansion exceeded an independent limit")
		}
	})
}
