package capturelab

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

func sortFixture(n int, shape string) []*commonpb.KeyValue {
	in := make([]*commonpb.KeyValue, n)
	for i := range in {
		prefix := ""
		if shape == "prefix" {
			prefix = strings.Repeat("shared-prefix-", 16)
		}
		in[i] = &commonpb.KeyValue{Key: fmt.Sprintf("%skey-%06d", prefix, i), Value: otlpInt(int64(i))}
	}
	switch shape {
	case "reverse":
		slices.Reverse(in)
	case "random", "prefix":
		r := rand.New(rand.NewPCG(17, 29))
		r.Shuffle(n, func(i, j int) { in[i], in[j] = in[j], in[i] })
	case "nearly":
		if n > 2 {
			in[n/2], in[n/2+1] = in[n/2+1], in[n/2]
		}
	}
	return in
}

// Copying the pointer slice restores the same input on every iteration. That
// common restoration cost is timed in all variants and reported alone as a
// baseline; fixture construction is outside timing. These are not ingest costs.
func BenchmarkOTLPAttributeSort(b *testing.B) {
	for _, n := range []int{0, 1, 8, 32, 128, 512} {
		for _, shape := range []string{"sorted", "reverse", "random", "nearly", "prefix"} {
			for _, strategy := range []string{"restore", "direct", "checked"} {
				b.Run(fmt.Sprintf("%d/%s/%s", n, shape, strategy), func(b *testing.B) {
					in := sortFixture(n, shape)
					work := make([]*commonpb.KeyValue, n)
					b.ReportAllocs()
					for b.Loop() {
						copy(work, in)
						switch strategy {
						case "direct":
							slices.SortFunc(work, compareAttributeKeys)
						case "checked":
							if !slices.IsSortedFunc(work, compareAttributeKeys) {
								slices.SortFunc(work, compareAttributeKeys)
							}
						}
					}
				})
			}
		}
	}
}

func TestOTLPAttributeSortComparisons(t *testing.T) {
	for _, n := range []int{8, 32, 128, 512} {
		for _, shape := range []string{"sorted", "reverse", "random", "nearly", "prefix"} {
			for _, checked := range []bool{false, true} {
				in := sortFixture(n, shape)
				comparisons := 0
				cmp := func(a, b *commonpb.KeyValue) int {
					comparisons++
					return compareAttributeKeys(a, b)
				}
				if !checked || !slices.IsSortedFunc(in, cmp) {
					slices.SortFunc(in, cmp)
				}
				if !slices.IsSortedFunc(in, compareAttributeKeys) {
					t.Fatal("unsorted result")
				}
				t.Logf("n=%d shape=%s checked=%v comparisons=%d", n, shape, checked, comparisons)
			}
		}
	}
}
