package ingest

import "time"

// DefaultPayloadRetention is seven elapsed days from a record's first acceptance.
// Span and capture each have their own acceptance time. Callers supply trusted
// policy explicitly; this constant does not schedule expiry or delete objects.
const DefaultPayloadRetention = 7 * 24 * time.Hour

// DefaultMarkerWindow is the selected initial window AFTER payload expiry.
// It is not a payload retention duration or an automatically running policy.
const DefaultMarkerWindow = 24 * time.Hour

// RetentionPhase describes time relative to original deadlines, not physical
// object presence, replay readiness or an instruction to delete anything.
type RetentionPhase uint8

const (
	InvalidPhase RetentionPhase = iota
	BeforeExpiry
	WithinMarkerWindow
	AfterMarkerWindow
)

// Lifetime holds fixed deadlines. Catalog implementations must preserve these
// original deadlines across retries rather than rebuild them from new policy.
// Its representation is not a selected persisted encoding. It supplies no clock.
type Lifetime struct {
	payloadExpiresAt time.Time
	markerUntil      time.Time
}

// NewLifetime takes an already chosen payload expiry and a positive marker window.
// The caller supplies trusted policy, not a producer-supplied span timestamp.
// UTC removes local monotonic readings so the values describe wall-clock instants;
// choosing and enforcing an authoritative catalog clock remains backend work.
func NewLifetime(payloadExpiresAt time.Time, markerWindow time.Duration) (Lifetime, error) {
	if payloadExpiresAt.IsZero() || markerWindow <= 0 {
		return Lifetime{}, ErrInvalid
	}
	expires := payloadExpiresAt.UTC()
	until := expires.Add(markerWindow)
	if !until.After(expires) || until.Sub(expires) != markerWindow {
		return Lifetime{}, ErrInvalid
	}
	return Lifetime{payloadExpiresAt: expires, markerUntil: until}, nil
}

func (l Lifetime) PayloadExpiresAt() time.Time { return l.payloadExpiresAt }
func (l Lifetime) MarkerUntil() time.Time      { return l.markerUntil }

// PhaseAt classifies a supplied instant without changing the deadlines. Payload
// expiry is inclusive; the marker window ends at markerUntil. AfterMarkerWindow
// does not require forgetting: an existing marker can still refuse an old write.
// Neither the zero Lifetime nor an unspecified time is a valid classification.
func (l Lifetime) PhaseAt(now time.Time) (RetentionPhase, error) {
	if l.payloadExpiresAt.IsZero() || l.markerUntil.IsZero() || now.IsZero() {
		return InvalidPhase, ErrInvalid
	}
	now = now.UTC()
	if now.Before(l.payloadExpiresAt) {
		return BeforeExpiry, nil
	}
	if now.Before(l.markerUntil) {
		return WithinMarkerWindow, nil
	}
	return AfterMarkerWindow, nil
}
