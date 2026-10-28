package capturelab

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"math/rand/v2"
	"strings"
	"testing"
)

type thresholdFixture struct {
	name   string
	size   int
	record record
}

func thresholdFixtures() []thresholdFixture {
	m := fixtures()[0].record.Metadata
	empty, err := encodeHybrid(record{Metadata: m})
	if err != nil {
		panic(err)
	}
	var result []thresholdFixture
	for _, kind := range []string{"Repeated", "Varied", "Binary", "Precompressed"} {
		for _, size := range []int{4 << 10, 8<<10 - 1, 8 << 10, 8<<10 + 1, 16 << 10, 32<<10 - 1, 32 << 10, 32<<10 + 1, 48 << 10, 64<<10 - 1, 64 << 10, 64<<10 + 1, 128 << 10, 256 << 10} {
			rng := rand.New(rand.NewPCG(42, 7))
			noise := func(n int) []byte {
				b := make([]byte, n)
				for i := range b {
					b[i] = byte(rng.Uint32())
				}
				return b
			}
			body := func(n int) []byte {
				switch kind {
				case "Repeated":
					word := []byte("agent asks about a trace and receives a recorded answer.\n")
					return bytes.Repeat(word, n/len(word)+1)[:n]
				case "Varied":
					words := []string{"agent", "trace", "request", "response", "record", "owner", "span", "retry", "catalog", "capture", "project", "run", "lookup", "body", "time", "token"}
					b := make([]byte, 0, n+16)
					for len(b) < n {
						b = append(b, words[rng.IntN(len(words))]...)
						b = append(b, ' ')
					}
					return b[:n]
				case "Binary":
					return noise(n)
				case "Precompressed":
					// Find an input whose complete gzip stream has exactly n bytes.
					// The loop is bounded fixture construction, outside timing.
					inputSize := n - 28
					for range 8 {
						if inputSize < 1 {
							panic("invalid compressed fixture size")
						}
						b, err := zip(noise(inputSize))
						if err != nil {
							panic(err)
						}
						if len(b) == n {
							return b
						}
						inputSize += n - len(b)
					}
				}
				panic("could not construct exact-size fixture")
			}
			bodyBytes := size - len(empty)
			requestBytes := bodyBytes / 5
			result = append(result, thresholdFixture{fmt.Sprintf("%s/%d", kind, size), size, record{m, body(requestBytes), body(bodyBytes - requestBytes)}})
		}
	}
	return result
}

func TestThresholdFixtures(t *testing.T) {
	for _, f := range thresholdFixtures() {
		b, err := encodeHybrid(f.record)
		if err != nil || len(b) != f.size {
			t.Fatal("fixture size mismatch", f.name, len(b), err)
		}
		if strings.HasPrefix(f.name, "Precompressed/") {
			for _, body := range [][]byte{f.record.Request, f.record.Response} {
				r, err := gzip.NewReader(bytes.NewReader(body))
				if err != nil {
					t.Fatal(err)
				}
				_, readErr := io.Copy(io.Discard, r)
				closeErr := r.Close()
				if readErr != nil || closeErr != nil {
					t.Fatal("invalid gzip fixture", readErr, closeErr)
				}
			}
		}
	}
}

func TestThresholdPolicyReport(t *testing.T) {
	for _, f := range thresholdFixtures() {
		for _, p := range thresholdPolicies() {
			s, err := encodeThreshold(f.record, p.threshold, p.level)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("POLICY %s %s stored=%d attempted=%t gzip=%t", f.name, p.name, len(s.data)+1, s.attempted, s.gzip)
		}
	}
}

func BenchmarkThreshold(b *testing.B) {
	for _, f := range thresholdFixtures() {
		if f.size&(f.size-1) != 0 {
			continue
		} // Only power-of-two sizes are timed.
		for _, p := range thresholdPolicies() {
			if p.threshold != 1 && p.threshold != maxEncoded+1 {
				continue
			}
			encoded, err := encodeThreshold(f.record, p.threshold, p.level)
			if err != nil {
				b.Fatal(err)
			}
			for _, operation := range []string{"Encode", "Decode"} {
				b.Run(f.name+"/"+p.name+"/"+operation, func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						if operation == "Encode" {
							if _, err := encodeThreshold(f.record, p.threshold, p.level); err != nil {
								b.Fatal(err)
							}
						} else if _, err := decodeSelection(encoded); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}
