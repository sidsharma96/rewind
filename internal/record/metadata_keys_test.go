package record

import (
	"fmt"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
)

func metadataKeyFixture(n int, shape string) []*commonpb.KeyValue {
	in := make([]*commonpb.KeyValue, n)
	for i := range in {
		prefix := ""
		if shape == "prefix" {
			prefix = strings.Repeat("shared-prefix-", 16)
		}
		in[i] = &commonpb.KeyValue{Key: fmt.Sprintf("%skey-%06d", prefix, i), Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: int64(i)}}}
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

func duplicateKeyOracle(attrs []*commonpb.KeyValue) bool {
	for i, attr := range attrs {
		for _, previous := range attrs[:i] {
			if attr.GetKey() == previous.GetKey() {
				return true
			}
		}
	}
	return false
}

func BenchmarkOTLPRepeatedKeys(b *testing.B) {
	for _, n := range []int{8, 32, 128, 512} {
		for _, shape := range []string{"sorted", "random", "first-repeat", "last-repeat"} {
			for _, strategy := range []string{"set", "pairwise"} {
				b.Run(fmt.Sprintf("%d/%s/%s", n, shape, strategy), func(b *testing.B) {
					attrs := metadataKeyFixture(n, "random")
					if shape == "sorted" {
						attrs = metadataKeyFixture(n, "sorted")
					}
					if shape == "first-repeat" {
						attrs[1] = attrs[0]
					} else if shape == "last-repeat" {
						attrs[n-1] = attrs[0]
					}
					expect := duplicateKeyOracle(attrs)
					b.ReportAllocs()
					for b.Loop() {
						got := false
						if strategy == "set" {
							got = validateUniqueAttributeKeys(attrs) != nil
						} else {
							got = duplicateKeyOracle(attrs)
						}
						if got != expect {
							b.Fatal("incorrect duplicate result")
						}
					}
				})
			}
		}
	}
}
