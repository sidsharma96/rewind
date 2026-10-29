package record

// DurationNanos returns the unsigned timestamp difference and whether that
// arithmetic is available. Nil metadata or end before start returns (0, false).
// Equal timestamps return (0, true); callers must check the boolean.
//
// This is an order check, not proof of clock accuracy, timestamp presence, ended
// status or complete validity. In particular, zero-valued timestamps receive no
// special interpretation here. The caller must keep borrowed metadata stable
// during the call. Reported times and all other metadata remain unchanged.
func (s Span) DurationNanos() (uint64, bool) {
	if s.Metadata == nil {
		return 0, false
	}
	start, end := s.Metadata.StartTimeUnixNano, s.Metadata.EndTimeUnixNano
	if end < start {
		return 0, false
	}
	return end - start, true
}
