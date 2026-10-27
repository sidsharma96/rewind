package capturelab

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"reflect"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	for _, f := range fixtures() {
		for _, c := range codecs() {
			t.Run(f.name+"/"+c.name, func(t *testing.T) {
				b, err := c.encode(f.record)
				if err != nil {
					t.Fatal(err)
				}
				got, err := c.decode(b)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, f.record) {
					t.Fatal("round trip changed supported values")
				}
			})
		}
	}
}

func TestPresenceAndOwnership(t *testing.T) {
	for _, request := range [][]byte{nil, {}, {0, 255, 128, '\n'}} {
		for _, response := range [][]byte{nil, {}, {255, 0, '"', '\\'}} {
			for _, c := range codecs() {
				r := record{Metadata: fixtures()[0].record.Metadata, Request: bytes.Clone(request), Response: bytes.Clone(response)}
				b, err := c.encode(r)
				if err != nil {
					t.Fatal(err)
				}
				original := bytes.Clone(b)
				clear(r.Request)
				clear(r.Response)
				if !bytes.Equal(b, original) {
					t.Fatal(c.name, "encoded output aliases caller bodies")
				}
				got, err := c.decode(b)
				if err != nil {
					t.Fatal(err)
				}
				clear(b)
				if !bytes.Equal(got.Request, request) || (got.Request == nil) != (request == nil) ||
					!bytes.Equal(got.Response, response) || (got.Response == nil) != (response == nil) {
					t.Fatal(c.name, "body or presence changed / decoded output aliases input")
				}
				// Check the reverse direction too: mutation of decoded bodies must
				// leave the caller's encoded buffer intact.
				input := bytes.Clone(original)
				got, err = c.decode(input)
				if err != nil {
					t.Fatal(err)
				}
				clear(got.Request)
				clear(got.Response)
				if !bytes.Equal(input, original) {
					t.Fatal(c.name, "input aliases decoded output")
				}
			}
		}
	}
}

func TestMalformed(t *testing.T) {
	r := record{Request: []byte{0, 255}, Response: []byte{1, 2}}
	for _, c := range codecs() {
		good, err := c.encode(r)
		if err != nil {
			t.Fatal(err)
		}
		for n := 0; n < len(good); n++ {
			if _, err := c.decode(good[:n]); err == nil {
				t.Fatalf("%s accepted truncation %d/%d", c.name, n, len(good))
			}
		}
		if _, err := c.decode(append(bytes.Clone(good), '!')); err == nil {
			t.Fatal(c.name, "accepted trailing garbage")
		}
	}
	good, _ := encodeHybrid(r)
	for _, mutate := range []func([]byte){
		func(b []byte) { b[3] = '2' },
		func(b []byte) { b[4] = 2 },
		func(b []byte) { b[4] = 0 }, // missing with nonempty request
		func(b []byte) { b[5] = 0 },
		func(b []byte) { binary.LittleEndian.PutUint64(b[6:14], ^uint64(0)) },
		func(b []byte) { binary.LittleEndian.PutUint64(b[14:22], maxBody+1) },
		func(b []byte) { binary.LittleEndian.PutUint64(b[22:30], ^uint64(0)) },
		func(b []byte) { b[headerSize] = '!' },
	} {
		b := bytes.Clone(good)
		mutate(b)
		if _, err := decodeHybrid(b); err == nil {
			t.Fatal("accepted malformed hybrid")
		}
	}
	for _, c := range codecs()[2:] {
		b, err := c.encode(r)
		if err != nil {
			t.Fatal(err)
		}
		b[len(b)-8] ^= 1 // gzip CRC: failure must survive a complete bounded read.
		if _, err := c.decode(b); err == nil {
			t.Fatal(c.name, "accepted broken checksum")
		}
	}
}

func TestLimits(t *testing.T) {
	oversized := make([]byte, maxEncoded+1)
	for _, c := range codecs() {
		if _, err := c.decode(oversized); err == nil {
			t.Fatal(c.name, "accepted oversized encoded object")
		}
		r := record{Request: make([]byte, maxBody+1)}
		if _, err := c.encode(r); err == nil {
			t.Fatal(c.name, "encoded oversized body")
		}
	}
	// Exercise expansion independently of body/metadata validation.
	z, err := zip(oversized)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range codecs()[2:] {
		if _, err := c.decode(z); err == nil {
			t.Fatal(c.name, "accepted excessive gzip output")
		}
	}
	for _, n := range []int{maxBody, maxBody + 1} {
		r := record{Request: make([]byte, n)}
		b, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		_, err = decodeJSON(b)
		if (err == nil) != (n == maxBody) {
			t.Fatal("JSON decoded body boundary", n, err)
		}
		if n == maxBody {
			b, err = encodeHybrid(r)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = decodeHybrid(b); err != nil {
				t.Fatal(err)
			}
		}
	}
	// The fixed header's metadata bound is checked before JSON parsing/copying.
	b, _ := encodeHybrid(record{})
	binary.LittleEndian.PutUint64(b[6:14], maxMetadata+1)
	if _, err := decodeHybrid(b); err == nil {
		t.Fatal("accepted oversized metadata")
	}
	r := record{Metadata: metadata{Attributes: make([]attribute, 65)}}
	for _, c := range codecs() {
		if _, err := c.encode(r); err == nil {
			t.Fatal(c.name, "accepted too many attributes")
		}
	}
}

func FuzzDecode(f *testing.F) {
	all := codecs()
	for i, c := range all {
		for _, r := range []record{{}, {Request: []byte{}, Response: []byte{0, 255}}} {
			b, err := c.encode(r)
			if err != nil {
				f.Fatal(err)
			}
			f.Add(byte(i), b)
		}
		f.Add(byte(i), []byte("RWL1\x01\x01"))
	}
	f.Fuzz(func(t *testing.T, which byte, b []byte) {
		c := all[int(which)%len(all)]
		r, err := c.decode(b)
		if err != nil {
			return
		}
		encoded, err := c.encode(r)
		if err != nil {
			t.Fatal("successful decode could not re-encode", err)
		}
		again, err := c.decode(encoded)
		if err != nil || !reflect.DeepEqual(r, again) {
			t.Fatal("supported values changed", err)
		}
	})
}
