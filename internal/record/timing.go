package record

// DurationNanos returns the exact unsigned timestamp difference and whether it is
// available under Rewind's timing policy. Nil metadata, either zero endpoint or
// end before start returns (0, false). Equal nonzero times return (0, true).
// Callers must check the boolean; it does not distinguish unavailability reasons.
//
// A zero endpoint means unknown for timing use: generated OTLP metadata cannot
// distinguish an omitted scalar from explicit zero. This does not prove a field
// was omitted, that clocks are accurate, that the operation ended or that the
// record is valid. The caller must keep borrowed metadata stable during the call.
// Reported times, including any nonzero endpoint, remain unchanged.
func (s Span) DurationNanos() (uint64, bool) {
	if s.Metadata == nil {
		return 0, false
	}
	start, end := s.Metadata.StartTimeUnixNano, s.Metadata.EndTimeUnixNano
	if start == 0 || end == 0 {
		return 0, false
	}
	if end < start {
		return 0, false
	}
	return end - start, true
}
