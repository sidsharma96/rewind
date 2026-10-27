package ingest

import "time"

type runPhase uint8

const (
	runRunning runPhase = iota + 1 // Zero is an unknown state, never permission.
	runCompleted
	runRevoked
	runClosed
)

// RunAdmission is a catalog-owned value, not authentication or a concurrent run
// controller. A catalog must serialize its updates with registration/publication,
// persist original completion state, and supply an authoritative time. This value
// supplies no clock, timer, credential lifetime or wire encoding. A saved closed
// value refuses later delivery despite clock regression; an unsaved observation
// or an unobserved clock jump does not establish that protection.
type RunAdmission struct {
	phase  runPhase
	window DeliveryWindow
}

func NewRunAdmission() RunAdmission { return RunAdmission{phase: runRunning} }

// Complete establishes the first fixed window. A retry of an already completed
// value preserves the original, ignoring the new time/configuration. Revocation
// cannot be undone. The trusted caller must check errors before saving the result.
func (r RunAdmission) Complete(at time.Time, duration time.Duration) (RunAdmission, error) {
	if !r.valid() {
		return RunAdmission{}, ErrInvalid
	}
	switch r.phase {
	case runCompleted, runClosed:
		return r, nil
	case runRevoked:
		return RunAdmission{}, ErrDenied
	default:
		w, err := NewDeliveryWindow(at, duration)
		if err != nil {
			return RunAdmission{}, err
		}
		return RunAdmission{phase: runCompleted, window: w}, nil
	}
}

// Revoke closes registration and delivery without erasing original completion.
// Repeated revocation is unchanged. Read permission is outside this value.
func (r RunAdmission) Revoke() (RunAdmission, error) {
	if !r.valid() {
		return RunAdmission{}, ErrInvalid
	}
	r.phase = runRevoked
	return r, nil
}

// CloseElapsed records elapsed delivery admission without conflating it with
// emergency revocation. The caller must save the returned value atomically with
// catalog policy, preserving it across retries/restarts. Merely checking a time
// does not close anything. A wrong forward clock jump can close admission early.
// Running values and observations before the deadline cannot establish closure.
func (r RunAdmission) CloseElapsed(now time.Time) (RunAdmission, error) {
	if !r.valid() || now.IsZero() {
		return RunAdmission{}, ErrInvalid
	}
	switch r.phase {
	case runClosed, runRevoked:
		return r, nil
	case runCompleted:
		if !now.Before(r.window.until) {
			r.phase = runClosed
			return r, nil
		}
	}
	return RunAdmission{}, ErrDenied
}

// CheckRecord checks only run admission. A valid result never replaces a
// registered execution binding or a current submitter grant. Closed admission
// returns ErrDenied before any acceptance, duplicate or conflict outcome.
func (r RunAdmission) CheckRecord(now time.Time) error {
	if !r.valid() || now.IsZero() {
		return ErrInvalid
	}
	switch r.phase {
	case runRunning:
		return nil
	case runRevoked, runClosed:
		return ErrDenied
	default:
		allowed, err := r.window.AllowsAt(now)
		if err != nil {
			return err
		}
		if !allowed {
			return ErrDenied
		}
		return nil
	}
}

// CheckRegistration additionally refuses new/changed bindings after completion.
// identicalExisting must come from comparing the stored execution/owner under the
// catalog's atomic operation, never a caller-supplied "retry" label. The catalog
// must first check current creator permission and check both affected runs when a
// proposed owner differs from an existing binding. Original registrar is preserved.
func (r RunAdmission) CheckRegistration(now time.Time, identicalExisting bool) error {
	if err := r.CheckRecord(now); err != nil {
		return err
	}
	if r.phase == runCompleted && !identicalExisting {
		return ErrDenied
	}
	return nil
}

func (r RunAdmission) valid() bool {
	validWindow := !r.window.completedAt.IsZero() && r.window.until.After(r.window.completedAt)
	switch r.phase {
	case runRunning:
		return r.window == (DeliveryWindow{})
	case runCompleted, runClosed:
		return validWindow
	case runRevoked:
		return r.window == (DeliveryWindow{}) || validWindow
	default:
		return false
	}
}
