package capturelab

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"
)

type fixture struct {
	name   string
	record record
}

func fixtures() []fixture {
	m := metadata{TraceID: "00112233445566778899aabbccddeeff", SpanID: "0123456789abcdef",
		Name: "synthetic model call", Resource: "capture-lab", Scope: "synthetic/1",
		StartNS: 1793059200000000001, EndNS: 1793059201000000002,
		Attributes: []attribute{{"provider", "synthetic"}, {"operation", "completion"}}}
	text := func(n int) []byte {
		return bytes.Repeat([]byte("agent asks about a trace and receives a recorded answer.\n"), n/55+1)[:n]
	}
	// PCG is deterministic fixture generation, not security-sensitive randomness.
	rng := rand.New(rand.NewPCG(42, 7))
	noise := func(n int) []byte {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(rng.Uint32())
		}
		return b
	}
	words := []string{"agent", "trace", "request", "response", "record", "owner", "span", "retry", "catalog", "capture", "project", "run", "lookup", "body", "time", "token"}
	varied := func(n int) []byte {
		b := make([]byte, 0, n+16)
		for len(b) < n {
			b = append(b, words[rng.IntN(len(words))]...)
			b = append(b, ' ')
		}
		return b[:n]
	}
	compressed := func(n int) []byte {
		b, err := zip(varied(n))
		if err != nil {
			panic(err)
		}
		return b
	}
	return []fixture{
		{"Span", record{Metadata: m}},
		{"SmallText", record{m, text(512), text(4 << 10)}},
		{"RepeatedText", record{m, text(64 << 10), text(256 << 10)}},
		{"VariedText", record{m, varied(64 << 10), varied(256 << 10)}},
		{"Binary", record{m, noise(64 << 10), noise(1 << 20)}},
		{"Precompressed", record{m, compressed(64 << 10), compressed(256 << 10)}},
	}
}

// Report bytes from the actual complete encoding, independently of timing.
func TestSizes(t *testing.T) {
	for _, f := range fixtures() {
		for _, c := range codecs() {
			b, err := c.encode(f.record)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("SIZE %s %s bodies=%d encoded=%d", f.name, c.name, len(f.record.Request)+len(f.record.Response), len(b))
		}
	}
}

func BenchmarkEncoding(b *testing.B) {
	for _, f := range fixtures() {
		for _, c := range codecs() {
			encoded, err := c.encode(f.record)
			if err != nil {
				b.Fatal(err)
			}
			for _, operation := range []string{"Encode", "Decode"} {
				b.Run(fmt.Sprintf("%s/%s/%s", f.name, c.name, operation), func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						if operation == "Encode" {
							if _, err := c.encode(f.record); err != nil {
								b.Fatal(err)
							}
						} else {
							if _, err := c.decode(encoded); err != nil {
								b.Fatal(err)
							}
						}
					}
				})
			}
		}
	}
}
