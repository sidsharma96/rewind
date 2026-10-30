package record

import (
	"github.com/sidsharma96/rewind/internal/traceid"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// LinkTarget holds copied target IDs, not the containing span's identity. Valid
// IDs neither prove that a target exists nor grant permission to read or write it.
type LinkTarget struct {
	Trace traceid.Trace
	Span  traceid.Span
}

// TargetOfLink returns a target only when both IDs have their standard widths and
// are nonzero. Empty or correctly sized all-zero IDs return a zero target, false,
// nil; preserve the original link and show an explicit no-usable-target case.
// Nil links return ErrInvalid. Wrong nonempty widths return traceid.ErrInvalid,
// even when the other ID is empty. Every failure returns a zero target and false.
//
// This read-only operation copies at most 24 bytes and retains no source buffers.
// Keep the borrowed link stable during the call. It does not validate other link
// fields, the containing span, authorization or complete record admission, and
// performs no lookup. Unsupported fields still need the independent metadata
// guard. An unavailable target is distinct from a valid target not found by lookup.
func TargetOfLink(link *tracepb.Span_Link) (LinkTarget, bool, error) {
	if link == nil {
		return LinkTarget{}, false, ErrInvalid
	}
	var target LinkTarget
	if (len(link.TraceId) != 0 && len(link.TraceId) != len(target.Trace)) ||
		(len(link.SpanId) != 0 && len(link.SpanId) != len(target.Span)) {
		return LinkTarget{}, false, traceid.ErrInvalid
	}
	copy(target.Trace[:], link.TraceId)
	copy(target.Span[:], link.SpanId)
	if !target.Trace.Valid() || !target.Span.Valid() {
		return LinkTarget{}, false, nil
	}
	return target, true, nil
}
