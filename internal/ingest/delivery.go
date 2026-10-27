package ingest

import "time"

// DefaultDeliveryWindow is the selected configurable duration after trusted normal
// completion. It is not a credential lifetime, retention period or running policy.
const DefaultDeliveryWindow = 10 * time.Minute

// DeliveryWindow holds one original completion instant and its fixed delivery
// deadline. It supplies no clock, authorization, timer or run lifecycle. Catalog
// adapters must preserve the original value across retries, rather than construct
// it again using a later time or changed configuration. This is not a wire format.
type DeliveryWindow struct {
	completedAt time.Time
	until       time.Time
}

// NewDeliveryWindow takes trusted completion time and an explicit positive policy
// duration. UTC removes process-local monotonic readings; an authoritative catalog
// still needs clock/isolation rules that make the deadline safe across hosts.
func NewDeliveryWindow(completedAt time.Time, duration time.Duration) (DeliveryWindow, error) {
	if completedAt.IsZero() || duration <= 0 {
		return DeliveryWindow{}, ErrInvalid
	}
	completed := completedAt.UTC()
	until := completed.Add(duration)
	if !until.After(completed) || until.Sub(completed) != duration {
		return DeliveryWindow{}, ErrInvalid
	}
	return DeliveryWindow{completedAt: completed, until: until}, nil
}

func (w DeliveryWindow) CompletedAt() time.Time { return w.completedAt }
func (w DeliveryWindow) Until() time.Time       { return w.until }

// AllowsAt includes completion and excludes the deadline. It does not prove an
// execution is registered or a submitter is authorized; those checks are separate.
// Unspecified values fail closed. A time before completion is outside this window.
// Classification is pure, not a latched closure: clock rollback needs backend rules.
func (w DeliveryWindow) AllowsAt(now time.Time) (bool, error) {
	if w.completedAt.IsZero() || !w.until.After(w.completedAt) || now.IsZero() {
		return false, ErrInvalid
	}
	now = now.UTC()
	return !now.Before(w.completedAt) && now.Before(w.until), nil
}
