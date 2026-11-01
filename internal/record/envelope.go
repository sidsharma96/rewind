package record

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
)

var (
	ErrEnvelopeInvalid      = errors.New("invalid saved-object envelope")
	ErrEnvelopeUnsupported  = errors.New("unsupported saved-object envelope")
	ErrEnvelopeKindMismatch = errors.New("saved-object kind does not match catalog")
	ErrEnvelopeTooLarge     = errors.New("saved-object byte limit exceeded")
)

// ObjectKind is an envelope dispatch label, separate from OTLP span kind and
// ingest's internal catalog enum. Callers must map the catalog kind explicitly.
type ObjectKind byte

const (
	SpanObject    ObjectKind = 1
	CaptureObject ObjectKind = 2

	envelopeMagic   = "RWND"
	envelopeVersion = 1
	envelopeSize    = 7
	encodingPlain   = 0
	encodingGzip    = 1
)

// EnvelopeLimits bound the whole stored object and the expanded payload
// separately. The seven-byte prefix is not part of MaxExpandedBytes. Neither
// bound caps peak memory or elapsed CPU. There are no production byte defaults.
type EnvelopeLimits struct {
	MaxStoredBytes   int
	MaxExpandedBytes int
}

// CompressionPolicy counts all uncompressed framing and payload bytes except
// the one encoding byte toward Threshold. Equality attempts compression; gzip
// is kept only if the complete stored object is smaller. Level is a gzip level.
type CompressionPolicy struct {
	Threshold int
	Level     int
}

func DefaultCompressionPolicy() CompressionPolicy {
	return CompressionPolicy{Threshold: 32 << 10, Level: gzip.BestSpeed}
}

// EnvelopeCodec implements RWND | version | kind | encoding | payload. The
// version selects a complete format revision (prefix and kind-specific payload
// schemas together), not independently extensible header fields. This boundary
// only frames bytes: kind-specific schemas, typed acceptance and retry-content
// comparison remain separate. Synthetic payloads do not freeze a public format.
//
// Configuration is immutable, with no retained request state. Independent calls
// may share a codec; each caller keeps its borrowed byte slice stable until return.
// Every successful result owns its bytes; every error returns nil bytes. No I/O,
// authorization, digest verification, background work or payload parsing occurs.
type EnvelopeCodec struct {
	limits EnvelopeLimits
	policy CompressionPolicy
}

func NewEnvelopeCodec(limits EnvelopeLimits, policy CompressionPolicy) (EnvelopeCodec, error) {
	c := EnvelopeCodec{limits: limits, policy: policy}
	if !c.valid() {
		return EnvelopeCodec{}, ErrEnvelopeInvalid
	}
	return c, nil
}

func (c EnvelopeCodec) valid() bool {
	return c.limits.MaxStoredBytes >= envelopeSize && c.limits.MaxExpandedBytes > 0 &&
		c.limits.MaxExpandedBytes <= int(^uint(0)>>1)-envelopeSize &&
		c.policy.Threshold > 0 && c.policy.Level >= gzip.HuffmanOnly && c.policy.Level <= gzip.BestCompression
}

func validObjectKind(kind ObjectKind) bool { return kind == SpanObject || kind == CaptureObject }

// Encode wraps a supported kind's payload without interpreting it. The producer
// must prepare a valid kind-specific payload before acceptance; this call neither
// declares an ended span nor proves semantic validity. Oversized input is refused
// before compression. Input is borrowed; both raw and gzip results are independent.
func (c EnvelopeCodec) Encode(kind ObjectKind, payload []byte) ([]byte, error) {
	if !c.valid() || !validObjectKind(kind) {
		return nil, ErrEnvelopeInvalid
	}
	if len(payload) > c.limits.MaxExpandedBytes {
		return nil, ErrEnvelopeTooLarge
	}
	selected, encoding := payload, byte(encodingPlain)
	// Avoid adding framing to a potentially large configured threshold.
	if len(payload) >= c.policy.Threshold-(envelopeSize-1) {
		// Stop once gzip cannot fit the stored cap or cannot beat plain. A full
		// candidate is an intentional size outcome, not a compressor I/O failure.
		limit := min(c.limits.MaxStoredBytes-envelopeSize, max(0, len(payload)-1))
		candidate := envelopeCandidate{limit: limit}
		w, err := gzip.NewWriterLevel(&candidate, c.policy.Level)
		if err != nil {
			return nil, err
		}
		_, writeErr := w.Write(payload)
		closeErr := w.Close()
		if (writeErr != nil && writeErr != errCandidateFull) || (closeErr != nil && closeErr != errCandidateFull) {
			return nil, errors.Join(writeErr, closeErr)
		}
		if writeErr == nil && closeErr == nil && candidate.buf.Len() < len(payload) {
			selected, encoding = candidate.buf.Bytes(), encodingGzip
		}
	}
	if len(selected) > c.limits.MaxStoredBytes-envelopeSize {
		return nil, ErrEnvelopeTooLarge
	}
	out := make([]byte, envelopeSize+len(selected))
	copy(out, envelopeMagic)
	out[4], out[5], out[6] = envelopeVersion, byte(kind), encoding
	copy(out[envelopeSize:], selected)
	return out, nil
}

// Decode requires a catalog-authorized expected kind and bytes already verified
// against that catalog reference's size/digest. Labels are only cross-checks;
// this function cannot establish those prerequisites. It validates framing and
// supported labels before kind comparison, and kind before decompression. On
// success the caller must still parse and validate the kind-specific payload.
func (c EnvelopeCodec) Decode(expected ObjectKind, stored []byte) ([]byte, error) {
	if !c.valid() {
		return nil, ErrEnvelopeInvalid
	}
	if len(stored) > c.limits.MaxStoredBytes {
		return nil, ErrEnvelopeTooLarge
	}
	if len(stored) < envelopeSize || string(stored[:4]) != envelopeMagic {
		return nil, ErrEnvelopeInvalid
	}
	kind, encoding := ObjectKind(stored[5]), stored[6]
	if stored[4] != envelopeVersion || !validObjectKind(kind) || (encoding != encodingPlain && encoding != encodingGzip) {
		return nil, ErrEnvelopeUnsupported
	}
	if !validObjectKind(expected) {
		return nil, ErrEnvelopeInvalid
	}
	if kind != expected {
		return nil, ErrEnvelopeKindMismatch
	}
	payload := stored[envelopeSize:]
	if encoding == encodingPlain {
		if len(payload) > c.limits.MaxExpandedBytes {
			return nil, ErrEnvelopeTooLarge
		}
		return bytes.Clone(payload), nil
	}
	r, err := gzip.NewReader(bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	// Default Multistream joins every member. The extra byte detects excess;
	// exact fit still reads through all trailers, later members and final EOF.
	out, readErr := io.ReadAll(io.LimitReader(r, int64(c.limits.MaxExpandedBytes)+1))
	closeErr := r.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return nil, err
	}
	if len(out) > c.limits.MaxExpandedBytes {
		return nil, ErrEnvelopeTooLarge
	}
	return out, nil
}

var errCandidateFull = errors.New("gzip candidate cannot fit or save bytes")

// envelopeCandidate only buffers a potentially useful candidate. The gzip
// writer sees an explicit error at the cap, never a successful truncated write.
type envelopeCandidate struct {
	buf   bytes.Buffer
	limit int
}

func (b *envelopeCandidate) Write(p []byte) (int, error) {
	if len(p) > b.limit-b.buf.Len() {
		return 0, errCandidateFull
	}
	return b.buf.Write(p)
}
