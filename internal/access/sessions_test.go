package access

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sidsharma96/rewind/internal/ingest"
)

func TestSessionExpiryLogoutAndReadOnlyAuthority(t *testing.T) {
	var clock atomic.Int64
	clock.Store(100)
	now := func() time.Time { return time.Unix(clock.Load(), 0) }
	token := strings.Repeat("a", 64)
	credentials, err := NewCredentials(1, []Credential{credentialFixture(token, "collector", time.Unix(200, 0))}, now)
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := NewSessions(credentials, 1, 50*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	id, expires, err := sessions.Login(t.Context(), token)
	if err != nil || expires != time.Unix(150, 0) || id == token {
		t.Fatal("wrong session lifetime", err)
	}
	if _, _, err := sessions.Login(t.Context(), token); !errors.Is(err, ErrSessionLimit) {
		t.Fatal("session table unbounded")
	}
	if _, err := credentials.Authenticate(t.Context(), id); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("session token became a Bearer credential")
	}
	ctx, err := sessions.Authenticate(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	f := newAuthorizerFixture(t)
	f.permissions.writer.SubmittedBy = "collector"
	if _, err := f.authorizer.AuthorizeRead(ctx, "project"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.authorizer.AuthorizeWrite(ctx, f.execution); !errors.Is(err, ingest.ErrDenied) {
		t.Fatal("session gained writes", err)
	}
	if _, err := f.authorizer.AuthorizeRegistration(ctx, f.execution, f.owner); !errors.Is(err, ingest.ErrDenied) {
		t.Fatal("session gained creation", err)
	}
	if len(f.permissions.calls) != 1 {
		t.Fatal("read-only refusal reached write permissions")
	}
	clock.Store(150)
	if _, err := sessions.Authenticate(t.Context(), id); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("deadline was not exclusive")
	}
	id, _, err = sessions.Login(t.Context(), token)
	if err != nil {
		t.Fatal("expired entry did not release capacity", err)
	}
	fresh, err := NewSessions(credentials, 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fresh.Authenticate(t.Context(), id); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("new service accepted an old session")
	}
	sessions.Logout(id)
	if _, err := sessions.Authenticate(t.Context(), id); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("logout retained authentication")
	}
	clock.Store(190)
	id, expires, err = sessions.Login(t.Context(), token)
	if err != nil || expires != time.Unix(200, 0) {
		t.Fatal("session outlived credential", err)
	}
	clock.Store(200)
	if _, err := sessions.Authenticate(t.Context(), id); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("expired credential remained valid")
	}
}

func TestSessionConcurrentLoginAuthenticateAndLogout(t *testing.T) {
	token := strings.Repeat("b", 64)
	c, err := NewCredentials(1, []Credential{credentialFixture(token, "reader", time.Now().Add(time.Hour))}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewSessions(c, 16, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			id, _, err := s.Login(context.Background(), token)
			if err != nil {
				t.Error(err)
				return
			}
			if _, err := s.Authenticate(context.Background(), id); err != nil {
				t.Error(err)
			}
			s.Logout(id)
		})
	}
	wg.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.entries) != 0 {
		t.Fatal("logged-out sessions leaked")
	}
}
