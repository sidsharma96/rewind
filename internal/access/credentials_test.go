package access

import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func credentialFixture(token, name string, expiry time.Time) Credential {
	return Credential{Digest: sha256.Sum256([]byte(token)), Principal: name, ExpiresAt: expiry}
}

func TestNewToken(t *testing.T) {
	first, firstDigest, err := NewToken()
	if err != nil || !canonicalToken(first) || firstDigest != sha256.Sum256([]byte(first)) {
		t.Fatal("token generation did not return canonical text and its digest")
	}
	second, secondDigest, err := NewToken()
	if err != nil || !canonicalToken(second) || secondDigest != sha256.Sum256([]byte(second)) || first == second {
		t.Fatal("token generation did not produce an independent credential")
	}
}

func TestCredentialConfiguration(t *testing.T) {
	now := time.Unix(100, 0)
	clock := func() time.Time { return now }
	valid := credentialFixture(strings.Repeat("a", 64), "alice", now.Add(time.Hour))
	for _, tc := range []struct {
		name    string
		maximum int
		entries []Credential
		clock   func() time.Time
	}{
		{"zero maximum", 0, []Credential{valid}, clock},
		{"negative maximum", -1, nil, clock},
		{"over bound", 1, []Credential{valid, valid}, clock},
		{"nil clock", 1, []Credential{valid}, nil},
		{"zero digest", 1, []Credential{{Principal: "alice", ExpiresAt: valid.ExpiresAt}}, clock},
		{"empty principal", 1, []Credential{{Digest: valid.Digest, ExpiresAt: valid.ExpiresAt}}, clock},
		{"long principal", 1, []Credential{{Digest: valid.Digest, Principal: strings.Repeat("a", 257), ExpiresAt: valid.ExpiresAt}}, clock},
		{"principal byte bound", 1, []Credential{{Digest: valid.Digest, Principal: strings.Repeat("界", 86), ExpiresAt: valid.ExpiresAt}}, clock},
		{"zero expiry", 1, []Credential{{Digest: valid.Digest, Principal: "alice"}}, clock},
		{"duplicate same principal", 2, []Credential{valid, valid}, clock},
		{"duplicate other principal", 2, []Credential{valid, {Digest: valid.Digest, Principal: "bob", ExpiresAt: valid.ExpiresAt}}, clock},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NewCredentials(tc.maximum, tc.entries, tc.clock)
			if got != nil || !errors.Is(err, ErrInvalidConfig) || err.Error() != ErrInvalidConfig.Error() {
				t.Fatal("invalid credential configuration was not generically refused")
			}
		})
	}
	valid.Principal = strings.Repeat("a", 256)
	if _, err := NewCredentials(1, []Credential{valid}, clock); err != nil {
		t.Fatal("exact principal byte bound was refused")
	}
	valid.ExpiresAt = now.Add(-time.Second)
	if _, err := NewCredentials(1, []Credential{valid}, clock); err != nil {
		t.Fatal("expired entry cannot be represented in an operator snapshot")
	}
	empty, err := NewCredentials(1, nil, clock)
	if err != nil {
		t.Fatal(err)
	}
	if ctx, err := empty.Authenticate(context.Background(), strings.Repeat("a", 64)); ctx != nil || !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("empty snapshot admitted a token")
	}
}

func TestCredentialRotationAndOwnedSnapshot(t *testing.T) {
	now := time.Unix(100, 0)
	old, rotated, bob := strings.Repeat("1", 64), strings.Repeat("2", 64), strings.Repeat("3", 64)
	entries := []Credential{
		credentialFixture(old, "alice", now.Add(time.Hour)),
		credentialFixture(bob, "bob", now.Add(time.Hour)),
		credentialFixture(rotated, "alice", now.Add(2*time.Hour)),
	}
	verifier, err := NewCredentials(3, entries, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	// Changing or reusing the caller's slice must not change authority already
	// configured. Strings and time values are immutable/value-owned Go data here.
	for i := range entries {
		entries[i] = credentialFixture(strings.Repeat("4", 64), "mallory", now.Add(-time.Second))
	}
	for token, want := range map[string]string{old: "alice", rotated: "alice", bob: "bob"} {
		ctx, err := verifier.Authenticate(context.Background(), token)
		if err != nil {
			t.Fatal("configured token failed after caller changed input")
		}
		if got, ok := principal(ctx); !ok || got != want {
			t.Fatal("credential identity or rotation changed principal")
		}
	}
	if ctx, err := verifier.Authenticate(context.Background(), strings.Repeat("4", 64)); ctx != nil || !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("caller mutation added authority")
	}
	now = now.Add(time.Hour)
	if ctx, err := verifier.Authenticate(context.Background(), old); ctx != nil || !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("old rotation credential survived its expiry")
	}
	ctx, err := verifier.Authenticate(context.Background(), rotated)
	if err != nil {
		t.Fatal("rotated credential expired with its predecessor")
	}
	if got, ok := principal(ctx); !ok || got != "alice" {
		t.Fatal("rotated credential lost its principal")
	}
}

func TestCredentialExpiryOnAuthenticationAndContextReuse(t *testing.T) {
	token := strings.Repeat("a", 64)
	expiry := time.Unix(200, 0)
	var current atomic.Int64
	current.Store(expiry.Add(-time.Nanosecond).UnixNano())
	verifier, err := NewCredentials(1, []Credential{credentialFixture(token, "alice", expiry)}, func() time.Time {
		return time.Unix(0, current.Load())
	})
	if err != nil {
		t.Fatal(err)
	}
	verified, err := verifier.Authenticate(context.Background(), token)
	if err != nil {
		t.Fatal("credential was refused before expiry")
	}
	if got, ok := principal(verified); !ok || got != "alice" {
		t.Fatal("fresh evidence missing")
	}
	for _, when := range []time.Time{expiry, expiry.Add(time.Nanosecond)} {
		current.Store(when.UnixNano())
		if got, ok := principal(verified); ok || got != "" {
			t.Fatal("reused context retained authority at or after expiry")
		}
		if got, err := verifier.Authenticate(context.Background(), token); got != nil || !errors.Is(err, ErrUnauthenticated) {
			t.Fatal("expired credential authenticated")
		}
	}
}

func TestCredentialCancellationAndUnverifiedContext(t *testing.T) {
	token := strings.Repeat("a", 64)
	now := time.Unix(100, 0)
	verifier, err := NewCredentials(1, []Credential{credentialFixture(token, "alice", now.Add(time.Hour))}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	parent, cancel := context.WithCancel(context.Background())
	verified, err := verifier.Authenticate(parent, token)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if got, ok := principal(verified); ok || got != "" {
		t.Fatal("canceled context retained principal")
	}
	if got, err := verifier.Authenticate(parent, token); got != nil || !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation was not preserved")
	}
	deadline, stop := context.WithDeadline(context.Background(), time.Unix(1, 0))
	defer stop()
	if got, err := verifier.Authenticate(deadline, token); got != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("deadline failure was not preserved")
	}
	type allegedPrincipalKey struct{}
	for _, ctx := range []context.Context{nil, context.Background(), context.WithValue(context.Background(), allegedPrincipalKey{}, "alice")} {
		if got, ok := principal(ctx); ok || got != "" {
			t.Fatal("unverified context supplied a principal")
		}
	}
	if got, err := verifier.Authenticate(nil, token); got != nil || !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("nil context did not fail closed")
	}
}

func TestMalformedUnknownAndUnconfiguredCredentials(t *testing.T) {
	token := strings.Repeat("a", 64)
	now := time.Unix(100, 0)
	verifier, err := NewCredentials(1, []Credential{credentialFixture(token, "private-principal", now.Add(time.Hour))}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	for _, input := range []string{"", token[:63], token + "0", strings.ToUpper(token), " " + token, token + "\n", "Bearer " + token, "g" + token[1:], "\x00" + token[1:], strings.Repeat("界", 21) + "a", strings.Repeat("b", 64)} {
		got, err := verifier.Authenticate(context.Background(), input)
		if got != nil || !errors.Is(err, ErrUnauthenticated) || err.Error() != ErrUnauthenticated.Error() {
			t.Fatal("malformed or unknown credential was not generically refused")
		}
	}
	for _, unconfigured := range []*Credentials{nil, {}} {
		if ctx, err := unconfigured.Authenticate(context.Background(), token); ctx != nil || !errors.Is(err, ErrUnauthenticated) {
			t.Fatal("unconfigured verifier admitted a token")
		}
	}
	zeroClock, err := NewCredentials(1, []Credential{credentialFixture(token, "alice", now.Add(time.Hour))}, func() time.Time { return time.Time{} })
	if err != nil {
		t.Fatal(err)
	}
	if ctx, err := zeroClock.Authenticate(context.Background(), token); ctx != nil || !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("invalid clock authenticated a token")
	}
}

func TestCredentialCancellationDuringClockCheck(t *testing.T) {
	token := strings.Repeat("a", 64)
	now := time.Unix(100, 0)
	for _, cancelOn := range []int{1, 2} {
		parent, cancel := context.WithCancel(context.Background())
		calls := 0
		verifier, err := NewCredentials(1, []Credential{credentialFixture(token, "alice", now.Add(time.Hour))}, func() time.Time {
			calls++
			if calls == cancelOn {
				cancel()
			}
			return now
		})
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		verified, err := verifier.Authenticate(parent, token)
		if cancelOn == 1 {
			if verified != nil || !errors.Is(err, context.Canceled) {
				t.Fatal("authentication ignored cancellation during its clock check")
			}
		} else {
			if err != nil {
				t.Fatal(err)
			}
			if got, ok := principal(verified); ok || got != "" {
				t.Fatal("context reuse ignored cancellation during its clock check")
			}
		}
		cancel()
	}
}

func TestCredentialSnapshotReplacementIsNotGlobalRevocation(t *testing.T) {
	token := strings.Repeat("a", 64)
	now := time.Unix(100, 0)
	clock := func() time.Time { return now }
	old, err := NewCredentials(1, []Credential{credentialFixture(token, "alice", now.Add(time.Hour))}, clock)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := old.Authenticate(context.Background(), token)
	if err != nil {
		t.Fatal(err)
	}
	replacement, err := NewCredentials(1, nil, clock)
	if err != nil {
		t.Fatal(err)
	}
	if ctx, err := replacement.Authenticate(context.Background(), token); ctx != nil || !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("replacement retained removed credential")
	}
	if got, ok := principal(verified); !ok || got != "alice" {
		t.Fatal("snapshot replacement unexpectedly changed old evidence")
	}
}

func TestCredentialConcurrentAuthentication(t *testing.T) {
	now := time.Unix(100, 0)
	tokens := []string{strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("c", 64)}
	entries := make([]Credential, len(tokens))
	for i, token := range tokens {
		entries[i] = credentialFixture(token, token[:1], now.Add(time.Hour))
	}
	verifier, err := NewCredentials(len(entries), entries, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	for i := range 24 {
		workers.Go(func() {
			for range 100 {
				token := tokens[i%len(tokens)]
				ctx, err := verifier.Authenticate(context.Background(), token)
				if err != nil {
					t.Error("concurrent credential verification failed")
					return
				}
				if got, ok := principal(ctx); !ok || got != token[:1] {
					t.Error("concurrent verification changed identity")
					return
				}
			}
		})
	}
	workers.Wait()
}
