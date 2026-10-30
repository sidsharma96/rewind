package capturelab

import (
	"errors"
	spanrecord "github.com/sidsharma96/rewind/internal/record"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// Explicit experiment bounds, not production defaults or a saved format.
const otlpJSONDepth = 64

var metadataCodec = func() spanrecord.MetadataCodec {
	c, err := spanrecord.NewMetadataCodec(spanrecord.MetadataLimits{
		MaxBytes: maxMetadata, MaxWork: maxMetadata, MaxDepth: otlpJSONDepth,
	})
	if err != nil {
		panic(err)
	} // Programmer-owned constant configuration.
	return c
}()
var errUnsupportedMetadata = spanrecord.ErrMetadataUnsupported
var errDuplicateAttribute = spanrecord.ErrDuplicateAttribute
var errUnorderedMetadata = spanrecord.ErrUnorderedMetadata

func labMetadataError(err error) error {
	if errors.Is(err, spanrecord.ErrMetadataInvalid) {
		return errInvalid
	}
	return err
}
func encodeOTLPJSON(in *tracepb.TracesData) ([]byte, error) {
	out, err := metadataCodec.Encode(in)
	return out, labMetadataError(err)
}
func decodeOTLPJSON(in []byte) (*tracepb.TracesData, error) {
	out, err := metadataCodec.Decode(in)
	return out, labMetadataError(err)
}
