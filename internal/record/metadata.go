package record

import (
	"errors"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

var (
	ErrMetadataInvalid     = errors.New("invalid or excessive metadata")
	ErrMetadataUnsupported = errors.New("unsupported metadata")
	ErrDuplicateAttribute  = errors.New("repeated attribute key")
	ErrUnorderedMetadata   = errors.New("unordered metadata attributes")
)

// MetadataLimits bound serialized bytes, graph work and nesting separately.
// MaxWork counts visited values and input string/byte lengths, including repeated
// visits to shared descendants. Neither MaxBytes nor MaxWork caps peak memory.
// These caller-selected bounds are not public or default production limits.
type MetadataLimits struct {
	MaxBytes int
	MaxWork  int
	MaxDepth int
}

// MetadataCodec is the concrete generated-span metadata boundary. Its private
// configuration is immutable; operations keep no request state. Independent
// requests may share it, but callers must keep borrowed input graphs stable.
// Its zero value refuses operations. It is not a complete span validator, saved
// record format, OTLP receiver, ended declaration or authorization capability.
type MetadataCodec struct{ limits MetadataLimits }

// NewMetadataCodec requires explicit positive bounds. Configuration does not
// select a saved version or promise byte-for-byte encoding across program builds.
func NewMetadataCodec(limits MetadataLimits) (MetadataCodec, error) {
	c := MetadataCodec{limits: limits}
	if !c.valid() {
		return MetadataCodec{}, ErrMetadataInvalid
	}
	return c, nil
}

func (c MetadataCodec) valid() bool {
	return c.limits.MaxBytes > 0 && c.limits.MaxWork > 0 && c.limits.MaxDepth > 0
}

// Check bounds the complete graph and refuses unsupported wire fields, malformed
// message pointers, map fields and repeated keyed attributes without mutation.
// It does not require IDs, infer completion, check every span field's semantics,
// or order attributes. Encoding checks its output size separately.
func (c MetadataCodec) Check(in *tracepb.TracesData) error {
	if !c.valid() || in == nil {
		return ErrMetadataInvalid
	}
	budget := c.limits.MaxWork
	return c.validateMessage(in.ProtoReflect(), 0, &budget)
}
