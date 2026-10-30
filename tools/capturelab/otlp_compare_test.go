package capturelab

import spanrecord "github.com/sidsharma96/rewind/internal/record"

// Keep the copying projection in the lab as an independent test oracle.
func compareSpanMetadata(a, b spanrecord.Span) (bool, error) {
	same, err := metadataCodec.Compare(a, b)
	return same, labMetadataError(err)
}
