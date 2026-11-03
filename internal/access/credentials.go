// Package access verifies trusted credentials before constructing ingest authority.
package access

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"time"
)

var (
	ErrUnauthenticated = errors.New("access: unauthenticated")
	ErrInvalidConfig   = errors.New("access: invalid configuration")
)

const maxCredentialPrincipalBytes = 256

// Credential binds the SHA-256 digest of a canonical token's text to one opaque
// principal. ExpiresAt is exclusive: the credential is invalid at that instant.
// Raw tokens belong to their holders and must not be retained in this value.
type Credential struct {
	Digest    [sha256.Size]byte
	Principal string
	ExpiresAt time.Time
}

// Credentials is an immutable, bounded credential snapshot. Replacing a snapshot
// does not revoke contexts made with an earlier one. The owner supplies a clock
// safe for concurrent calls; clock rollback can extend apparent validity.
type Credentials struct {
	entries []Credential
	now     func() time.Time
}

// NewCredentials copies entries after validating their explicit bound. An empty
// list denies everyone; expired entries are allowed but cannot authenticate.
// Several digests may name one principal to permit credential rotation.
func NewCredentials(maxCredentials int, entries []Credential, now func() time.Time) (*Credentials, error) {
	if maxCredentials <= 0 || len(entries) > maxCredentials || now == nil {
		return nil, ErrInvalidConfig
	}
	seen := make(map[[sha256.Size]byte]struct{}, len(entries))
	for _, entry := range entries {
		if entry.Digest == ([sha256.Size]byte{}) || entry.Principal == "" || len(entry.Principal) > maxCredentialPrincipalBytes || entry.ExpiresAt.IsZero() {
			return nil, ErrInvalidConfig
		}
		if _, duplicate := seen[entry.Digest]; duplicate {
			return nil, ErrInvalidConfig
		}
		seen[entry.Digest] = struct{}{}
	}
	return &Credentials{entries: append([]Credential(nil), entries...), now: now}, nil
}

// NewToken creates 32 random bytes and returns their canonical lowercase
// hexadecimal text plus the SHA-256 digest of that text. Only the returned digest
// belongs in server configuration. The caller owns delivery of the secret token.
func NewToken() (token string, digest [sha256.Size]byte, err error) {
	var random [32]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", [sha256.Size]byte{}, errors.New("access: token generation failed")
	}
	token = hex.EncodeToString(random[:])
	return token, sha256.Sum256([]byte(token)), nil
}

type credentialKey struct{}

type authenticatedPrincipal struct {
	name      string
	expiresAt time.Time
	now       func() time.Time
}

// Authenticate accepts a raw token, not an HTTP Authorization header. Well-formed
// tokens scan the entire bounded snapshot using constant-time digest comparison
// and selection, without a position-dependent early return. Syntax, expiry and
// cancellation checks are not a constant-time protocol. No object or catalog I/O
// occurs. A returned context contains the principal and expiry, never the token.
func (c *Credentials) Authenticate(ctx context.Context, token string) (context.Context, error) {
	if ctx == nil {
		return nil, ErrUnauthenticated
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c == nil || c.now == nil || !canonicalToken(token) {
		return nil, ErrUnauthenticated
	}
	digest := sha256.Sum256([]byte(token))
	index, matched := 0, 0
	for i, entry := range c.entries {
		equal := subtle.ConstantTimeCompare(digest[:], entry.Digest[:])
		index = subtle.ConstantTimeSelect(equal, i, index)
		matched |= equal
	}
	if matched != 1 {
		return nil, ErrUnauthenticated
	}
	entry := c.entries[index]
	verified := context.WithValue(ctx, credentialKey{}, authenticatedPrincipal{
		name: entry.Principal, expiresAt: entry.ExpiresAt, now: c.now,
	})
	if _, ok := principal(verified); !ok {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, ErrUnauthenticated
	}
	return verified, nil
}

func canonicalToken(token string) bool {
	if len(token) != 64 {
		return false
	}
	for i := range token {
		b := token[i]
		if !(b >= '0' && b <= '9' || b >= 'a' && b <= 'f') {
			return false
		}
	}
	return true
}

// principal rechecks the evidence on every new protected operation. An operation
// already authorized may finish after expiry; no mid-operation revocation is
// promised. The explicit clock remains trusted operator configuration.
func principal(ctx context.Context) (string, bool) {
	if ctx == nil || ctx.Err() != nil {
		return "", false
	}
	evidence, ok := ctx.Value(credentialKey{}).(authenticatedPrincipal)
	if !ok || evidence.name == "" || evidence.now == nil || evidence.expiresAt.IsZero() {
		return "", false
	}
	now := evidence.now()
	if ctx.Err() != nil || now.IsZero() || !now.Before(evidence.expiresAt) {
		return "", false
	}
	return evidence.name, true
}
