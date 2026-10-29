package record

import "github.com/sidsharma96/rewind/internal/traceid"

// IDs copies this span's binary execution IDs into checked, comparable values.
// Nil metadata returns ErrInvalid; invalid IDs return traceid.ErrInvalid. Both
// returned IDs are zero on every error. Source ID buffers are not retained.
//
// The caller must keep the borrowed graph stable during this call. Extraction
// works before a call ends so its owner can be registered; it does not validate
// completion, metadata semantics, attribution or permission to accept any record.
// Parent/link IDs and context labels do not supply this execution's identity.
func (s Span) IDs() (traceid.Trace, traceid.Span, error) {
	if s.Metadata == nil {
		return traceid.Trace{}, traceid.Span{}, ErrInvalid
	}
	trace, err := traceid.TraceFromBytes(s.Metadata.TraceId)
	if err != nil {
		return traceid.Trace{}, traceid.Span{}, err
	}
	span, err := traceid.SpanFromBytes(s.Metadata.SpanId)
	if err != nil {
		return traceid.Trace{}, traceid.Span{}, err
	}
	return trace, span, nil
}
