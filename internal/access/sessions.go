package access

import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"sync"
	"time"
)

var ErrSessionLimit = errors.New("access: session limit reached")

type browserSession struct {
	credential [sha256.Size]byte
	expires    time.Time
}

// Sessions owns disposable bounded login state for one immutable credential
// snapshot. It retains neither raw credentials nor raw session tokens. Logout
// stops new authentication, not requests already authenticated. Restart loses
// sessions, never accepted records. The trusted credential clock governs expiry.
type Sessions struct {
	credentials *Credentials
	mu          sync.Mutex
	entries     map[[sha256.Size]byte]browserSession
	max         int
	ttl         time.Duration
}

func NewSessions(c *Credentials, max int, ttl time.Duration) (*Sessions, error) {
	if c == nil || c.now == nil || max <= 0 || ttl <= 0 {
		return nil, ErrInvalidConfig
	}
	return &Sessions{credentials: c, entries: make(map[[sha256.Size]byte]browserSession), max: max, ttl: ttl}, nil
}

// Login verifies the original token and returns a fresh session token plus its
// exclusive deadline. Permission remains a per-operation catalog check.
func (s *Sessions) Login(ctx context.Context, token string) (string, time.Time, error) {
	verified, err := s.credentials.Authenticate(ctx, token)
	if err != nil {
		return "", time.Time{}, err
	}
	p := verified.Value(credentialKey{}).(authenticatedPrincipal)
	now := s.credentials.now()
	expires := now.Add(s.ttl)
	if p.expiresAt.Before(expires) {
		expires = p.expiresAt
	}
	if now.IsZero() || !now.Before(expires) {
		return "", time.Time{}, ErrUnauthenticated
	}
	session, _, err := NewToken()
	if err != nil {
		return "", time.Time{}, err
	}
	// Distinct syntax prevents a session token from ever being accepted as a
	// configured full-authority Bearer credential, even on random-byte collision.
	session = "s1." + session
	digest := sha256.Sum256([]byte(session))
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, entry := range s.entries {
		if !now.Before(entry.expires) {
			delete(s.entries, key)
		}
	}
	if err := ctx.Err(); err != nil {
		return "", time.Time{}, err
	}
	if len(s.entries) >= s.max {
		return "", time.Time{}, ErrSessionLimit
	}
	if _, exists := s.entries[digest]; exists {
		return "", time.Time{}, ErrSessionLimit
	}
	s.entries[digest] = browserSession{credential: sha256.Sum256([]byte(token)), expires: expires}
	return session, expires, nil
}

// Authenticate constructs a fresh read-only context, rechecking the credential
// entry and both deadlines. It never accepts an arbitrary principal from callers.
func (s *Sessions) Authenticate(ctx context.Context, token string) (context.Context, error) {
	if ctx == nil || !validSessionToken(token) {
		return nil, ErrUnauthenticated
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key := sha256.Sum256([]byte(token))
	s.mu.Lock()
	entry, ok := s.entries[key]
	now := s.credentials.now()
	if ok && (now.IsZero() || !now.Before(entry.expires)) {
		delete(s.entries, key)
		ok = false
	}
	s.mu.Unlock()
	if !ok {
		return nil, ErrUnauthenticated
	}
	// Credential digests are not public bearer credentials. Only access owns
	// this lookup, and the private context below is deliberately read-only.
	for _, credential := range s.credentials.entries {
		if credential.Digest != entry.credential {
			continue
		}
		if !now.Before(credential.ExpiresAt) {
			return nil, ErrUnauthenticated
		}
		expires := entry.expires
		if credential.ExpiresAt.Before(expires) {
			expires = credential.ExpiresAt
		}
		out := context.WithValue(ctx, credentialKey{}, authenticatedPrincipal{
			name: credential.Principal, expiresAt: expires, now: s.credentials.now, readOnly: true,
		})
		if _, ok := principal(out); !ok {
			return nil, ErrUnauthenticated
		}
		return out, nil
	}
	return nil, ErrUnauthenticated
}

func (s *Sessions) Logout(token string) {
	if !validSessionToken(token) {
		return
	}
	key := sha256.Sum256([]byte(token))
	s.mu.Lock()
	delete(s.entries, key)
	s.mu.Unlock()
}

func validSessionToken(token string) bool {
	return strings.HasPrefix(token, "s1.") && canonicalToken(token[3:])
}
