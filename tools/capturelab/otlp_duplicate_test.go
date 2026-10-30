package capturelab

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/protobuf/proto"
)

// Pairwise key comparison is the deliberately simple exact oracle. Attribute
// values have no role in deciding whether a key repeats.
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

func TestOTLPRepeatedKeysRefused(t *testing.T) {
	_, positions := normalizationFixture()
	for position := range positions {
		for _, value := range []string{"identical", "different-int", "different-type", "absent", "empty"} {
			for _, adjacent := range []bool{false, true} {
				t.Run(fmt.Sprintf("collection-%d/%s/adjacent-%v", position, value, adjacent), func(t *testing.T) {
					in, lists := normalizationFixture()
					attrs := *lists[position]
					attrs[0].Key = "private-synthetic-key"
					pair := proto.Clone(attrs[0]).(*commonpb.KeyValue)
					switch value {
					case "different-int":
						pair.Value = otlpInt(999)
					case "different-type":
						pair.Value = otlpString("999")
					case "absent":
						pair.Value = nil
					case "empty":
						pair.Value = &commonpb.AnyValue{}
					}
					attrs = append(attrs, pair)
					if adjacent {
						attrs[1], attrs[len(attrs)-1] = attrs[len(attrs)-1], attrs[1]
					}
					*lists[position] = attrs
					before := proto.Clone(in)
					if !duplicateKeyOracle(attrs) {
						t.Fatal("invalid test fixture")
					}
					if err := normalizeOwnedOTLP(in); !errors.Is(err, errDuplicateAttribute) || !proto.Equal(in, before) {
						t.Fatal("normalizer did not refuse unchanged")
					}
					out, err := encodeOTLPJSON(in)
					if !errors.Is(err, errDuplicateAttribute) || out != nil || !proto.Equal(in, before) {
						t.Fatal("encoder bypassed duplicate guard or changed input")
					}
					if strings.Contains(err.Error(), attrs[0].Key) {
						t.Fatal("error contains attribute key")
					}
				})
			}
		}
	}
}

func TestOTLPRepeatedKeyScope(t *testing.T) {
	// Reusing keys across separate collections, including shared references,
	// remains valid. Case-distinct keys within one collection also remain valid.
	in, lists := normalizationFixture()
	for _, p := range lists {
		(*p)[0].Key = "tokens"
		(*p)[1].Key = "Tokens"
	}
	if err := normalizeOwnedOTLP(in); err != nil {
		t.Fatal(err)
	}
	if _, err := encodeOTLPJSON(in); err != nil {
		t.Fatal(err)
	}
}

func TestOTLPRepeatedKeyFromJSON(t *testing.T) {
	// Two array entries can repeat a key even though no JSON object repeats a
	// field name. Parsing is deliberately not the record-validity boundary.
	in, err := decodeOTLPJSON([]byte(`{"resourceSpans":[{"scopeSpans":[{"spans":[{"attributes":[{"key":"tokens","value":{"intValue":"12"}},{"key":"tokens","value":{"intValue":"12"}}]}]}]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	before := proto.Clone(in)
	if err := normalizeOwnedOTLP(in); !errors.Is(err, errDuplicateAttribute) || !proto.Equal(in, before) {
		t.Fatal("decoded repetitions did not refuse unchanged")
	}
	if out, err := encodeOTLPJSON(in); !errors.Is(err, errDuplicateAttribute) || out != nil {
		t.Fatal("decoded repetitions were encoded")
	}
}

func FuzzOTLPRepeatedKeys(f *testing.F) {
	f.Add([]byte{2, 1, 2})
	f.Add([]byte{0, 1, 2, 3})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, keys []byte) {
		if len(keys) > 128 {
			t.Skip()
		}
		in := otlpFixture()
		span := in.ResourceSpans[0].ScopeSpans[0].Spans[0]
		span.Attributes = nil
		for i, key := range keys {
			span.Attributes = append(span.Attributes, &commonpb.KeyValue{Key: fmt.Sprintf("key-%02x", key), Value: otlpInt(int64(i))})
		}
		before := proto.Clone(in)
		duplicate := duplicateKeyOracle(span.Attributes)
		check := metadataCodec.Check(in)
		if errors.Is(check, errDuplicateAttribute) != duplicate || (check != nil && !duplicate) {
			t.Fatal("key set disagrees with pairwise oracle")
		}
		out, err := encodeOTLPJSON(in)
		if errors.Is(err, errDuplicateAttribute) != duplicate || !proto.Equal(in, before) || (duplicate && out != nil) || (!duplicate && (err != nil || len(out) == 0)) {
			t.Fatal("encoder disagrees with oracle or changed input")
		}
		err = normalizeOwnedOTLP(in)
		if errors.Is(err, errDuplicateAttribute) != duplicate || (duplicate && !proto.Equal(in, before)) || (!duplicate && err != nil) {
			t.Fatal("normalizer disagrees with oracle or changed refused input")
		}
	})
}
