package record

// EndedSpan carries an explicit declaration that the operation represented by a
// span has ended, including an ended failure. It is not independent evidence of
// completion, a validated saved record, or permission to write the execution.
// Its zero value has no declaration and View refuses it.
//
// Like Span, it borrows the generated messages read-only. It does not freeze or
// copy them; the caller must keep the reachable graph stable while it is in use.
type EndedSpan struct {
	span Span
}

// DeclareEnded declares that this span describes an ended operation. Calling it
// is the declaration: no timestamp, status, event, or context label can infer it.
// An ended failure or unavailable duration is allowed. It does not end a live SDK
// span, change metadata, or create a catalog completion transition.
//
// Only the selected execution IDs are checked here. Missing metadata returns
// ErrInvalid; invalid execution IDs return traceid.ErrInvalid. Every error returns
// a zero EndedSpan. Complete metadata validation, normalization, owned encoding,
// authorization and publication remain separate operations.
func (s Span) DeclareEnded() (EndedSpan, error) {
	if _, _, err := s.IDs(); err != nil {
		return EndedSpan{}, err
	}
	return EndedSpan{span: s}, nil
}

// View returns the declared span's borrowed read-only view, or ErrInvalid for a
// zero EndedSpan. It transfers no ownership and grants no additional validity or
// authority. Any encoded output must be independent of these borrowed messages.
func (s EndedSpan) View() (Span, error) {
	if s.span.Metadata == nil {
		return Span{}, ErrInvalid
	}
	return s.span, nil
}
