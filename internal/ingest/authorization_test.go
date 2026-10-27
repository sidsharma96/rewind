package ingest

import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"sync"
	"testing"
)

// Controlled request identity, not a token verifier. Nil operations are denied.
type recordAuth struct {
	write func(context.Context, Execution) (Attribution, error)
	read  func(context.Context, string) (string, error)
}

func (a recordAuth) AuthorizeWrite(ctx context.Context, e Execution) (Attribution, error) {
	if a.write == nil {
		return Attribution{}, ErrDenied
	}
	return a.write(ctx, e)
}

func (a recordAuth) AuthorizeRead(ctx context.Context, tenant string) (string, error) {
	if a.read == nil {
		return "", ErrDenied
	}
	return a.read(ctx, tenant)
}

func writerAuth(a Attribution) recordAuth {
	return recordAuth{write: func(ctx context.Context, _ Execution) (Attribution, error) { return a, ctx.Err() }}
}

func readerAuth(principal string) recordAuth {
	return recordAuth{read: func(ctx context.Context, _ string) (string, error) { return principal, ctx.Err() }}
}

func setWriter(c *modelCatalog, tenant string, a Attribution, allowed bool) {
	c.mu.Lock()
	c.writers[writerGrant{tenant, a}] = allowed
	c.mu.Unlock()
}

func TestRecordOwnershipRejectsOtherInstancesBeforeUpload(t *testing.T) {
	for _, id := range []Identity{sampleID, captureID()} {
		for _, accepted := range []bool{false, true} {
			s, o, c := fixture(t)
			original := []byte("Alice's evidence")
			if accepted {
				if _, err := s.Write(t.Context(), id, original); err != nil {
					t.Fatal(err)
				}
			}
			for _, owner := range []Owner{
				{Run: sampleOwner.Run, Instance: "bob-1"},
				{Run: "another-run", Instance: sampleOwner.Instance},
			} {
				bob := Attribution{Owner: owner, SubmittedBy: "principal-b"}
				setWriter(c, id.Tenant, bob, true) // Valid authority for Bob's own scope.
				other := newStore(t, o, c, writerAuth(bob), Limits{1024, 1})
				for _, contents := range [][]byte{original, []byte("different evidence")} {
					creates, pubs := o.creates.Load(), c.publications.Load()
					if r, err := other.Write(t.Context(), id, contents); !errors.Is(err, ErrDenied) || errors.Is(err, ErrConflict) || errors.Is(err, ErrUncertain) || r != (Receipt{}) {
						t.Fatalf("other instance got record outcome: %+v, %v", r, err)
					}
					if o.creates.Load() != creates || c.publications.Load() != pubs {
						t.Fatal("unauthorized instance uploaded or published")
					}
				}
			}
			if accepted {
				requireRead(t, s, id, original)
			}
		}
	}
}

func TestRecordRequiresRegistrationAndMatchingProject(t *testing.T) {
	s, o, c := fixture(t)
	for _, id := range []Identity{
		{Tenant: sampleID.Tenant, Trace: sampleID.Trace, Span: "unregistered"},
		{Tenant: "other-project", Trace: sampleID.Trace, Span: sampleID.Span},
	} {
		setWriter(c, id.Tenant, Attribution{sampleOwner, "principal-a"}, true)
		if _, err := s.Write(t.Context(), id, nil); !errors.Is(err, ErrDenied) {
			t.Fatalf("unregistered execution: %v", err)
		}
	}
	if o.creates.Load() != 0 || c.publications.Load() != 0 {
		t.Fatal("unregistered write accessed objects or publication")
	}
}

func TestProjectReaderCanInspectAgentsButWriterCannotRead(t *testing.T) {
	_, o, c := fixture(t)
	alice := Attribution{sampleOwner, "principal-a"}
	bob := Attribution{Owner{Run: sampleOwner.Run, Instance: "bob-1"}, "principal-b"}
	bobID := sampleID
	bobID.Span = "bobs-call"
	provision(t, c, bobID, bob)
	for _, tc := range []struct {
		id Identity
		a  Attribution
	}{{captureID(), alice}, {bobID, bob}} {
		writer := newStore(t, o, c, writerAuth(tc.a), Limits{1024, 2})
		if _, err := writer.Write(t.Context(), tc.id, []byte(tc.a.SubmittedBy)); err != nil {
			t.Fatal(err)
		}
		lookups := c.lookups.Load()
		if _, err := writer.Read(t.Context(), tc.id); !errors.Is(err, ErrDenied) || c.lookups.Load() != lookups {
			t.Fatalf("writer acquired automatic reads: %v", err)
		}
	}
	debugger := newStore(t, o, c, readerAuth("debugger"), Limits{1024, 2})
	// Even a successful preliminary authorizer cannot bypass the catalog grant.
	for _, id := range []Identity{sampleID, captureID(), bobID} {
		if data, err := debugger.Read(t.Context(), id); !errors.Is(err, ErrDenied) || errors.Is(err, ErrNotFound) || data != nil {
			t.Fatalf("ungranted reader learned record state: %q, %v", data, err)
		}
	}
	if o.reads.Load() != 0 {
		t.Fatal("ungranted reader fetched bytes")
	}
	c.mu.Lock()
	c.readers[readerGrant{sampleID.Tenant, "debugger"}] = true
	c.mu.Unlock()
	requireRead(t, debugger, captureID(), []byte(alice.SubmittedBy))
	requireRead(t, debugger, bobID, []byte(bob.SubmittedBy))
	pubs, creates := c.publications.Load(), o.creates.Load()
	if _, err := debugger.Write(t.Context(), bobID, nil); !errors.Is(err, ErrDenied) || c.publications.Load() != pubs || o.creates.Load() != creates {
		t.Fatalf("reader acquired writes: %v", err)
	}
}

func TestRecordCollectorRetryPreservesOriginalAttribution(t *testing.T) {
	_, o, c := fixture(t)
	first := Attribution{sampleOwner, "collector-one"}
	second := Attribution{sampleOwner, "collector-two"}
	setWriter(c, sampleID.Tenant, first, true)
	setWriter(c, sampleID.Tenant, second, true)
	for _, id := range []Identity{captureID(), sampleID} {
		original := newStore(t, o, c, writerAuth(first), Limits{1024, 1})
		c.afterPublish = func(context.Context) error { return errors.New("lost commit reply") }
		if _, err := original.Write(t.Context(), id, []byte("same bytes")); !errors.Is(err, ErrUncertain) {
			t.Fatal(err)
		}
		c.afterPublish = nil
		fresh := newStore(t, o, c, writerAuth(second), Limits{1024, 1})
		r, err := fresh.Write(t.Context(), id, []byte("same bytes"))
		if err != nil || !r.Duplicate || r.Attribution != first {
			t.Fatalf("retry changed original attribution: %+v, %v", r, err)
		}
		stored, err := c.Lookup(t.Context(), id, "reader-a")
		if err != nil || stored.Attribution != first || stored.Duplicate {
			t.Fatalf("stored attribution: %+v, %v", stored, err)
		}
		setWriter(c, id.Tenant, second, false)
		if _, err := fresh.Write(t.Context(), id, []byte("same bytes")); !errors.Is(err, ErrDenied) {
			t.Fatalf("collector credential without grant accepted retry: %v", err)
		}
		setWriter(c, id.Tenant, second, true)
	}
}

func TestRecordRevokedDuringUploadRefusesEveryPublicationOutcome(t *testing.T) {
	for _, state := range []string{"new", "duplicate", "conflict"} {
		t.Run(state, func(t *testing.T) {
			s, o, c := fixture(t)
			data := []byte("original")
			if state != "new" {
				if _, err := s.Write(t.Context(), sampleID, data); err != nil {
					t.Fatal(err)
				}
			}
			if state == "conflict" {
				data = []byte("changed")
			}
			before := c.count()
			entered, release := make(chan struct{}), make(chan struct{})
			o.afterCreate = func(ctx context.Context) error {
				close(entered)
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			done := make(chan error, 1)
			go func() { _, err := s.Write(t.Context(), sampleID, data); done <- err }()
			<-entered
			setWriter(c, sampleID.Tenant, Attribution{sampleOwner, "principal-a"}, false)
			close(release)
			if err := <-done; !errors.Is(err, ErrDenied) || errors.Is(err, ErrConflict) || errors.Is(err, ErrUncertain) {
				t.Fatalf("revoked publisher disclosed an outcome: %v", err)
			}
			if c.count() != before || o.count() == 0 {
				t.Fatal("revoked publication changed acceptance or removed uploaded bytes")
			}
			if state != "new" {
				requireRead(t, s, sampleID, []byte("original"))
			}
		})
	}
}

func TestRecordPolicyUnavailableFailsClosed(t *testing.T) {
	broken := errors.New("grant state unavailable")
	for _, duringUpload := range []bool{false, true} {
		s, o, c := fixture(t)
		if duringUpload {
			o.afterCreate = func(context.Context) error { c.policyErr = broken; return nil }
		} else {
			c.policyErr = broken
		}
		if _, err := s.Write(t.Context(), sampleID, nil); !errors.Is(err, broken) || errors.Is(err, ErrUncertain) != duringUpload {
			t.Fatalf("unavailable policy duringUpload=%v: %v", duringUpload, err)
		}
		if data, err := s.Read(t.Context(), sampleID); !errors.Is(err, broken) || data != nil || o.reads.Load() != 0 || c.count() != 0 {
			t.Fatalf("unavailable policy read: %q, %v", data, err)
		}
	}
}

func TestRecordLookupRechecksReaderBeforeAbsenceOrExpiry(t *testing.T) {
	for _, expired := range []bool{false, true} {
		_, o, c := fixture(t)
		if expired {
			c.expiry = func(Identity) error { return ErrExpired }
		}
		auth := recordAuth{read: func(ctx context.Context, tenant string) (string, error) {
			// Model authority changing after the preliminary authorizer approves.
			c.mu.Lock()
			c.readers[readerGrant{tenant, "reader-a"}] = false
			c.mu.Unlock()
			return "reader-a", ctx.Err()
		}}
		s := newStore(t, o, c, auth, Limits{1024, 1})
		if data, err := s.Read(t.Context(), sampleID); !errors.Is(err, ErrDenied) || errors.Is(err, ErrExpired) || errors.Is(err, ErrNotFound) || data != nil || o.reads.Load() != 0 {
			t.Fatalf("revoked reader learned state: %q, %v", data, err)
		}
	}
}

func TestRecordAuthorizationCancellationReleasesSlot(t *testing.T) {
	for _, write := range []bool{false, true} {
		_, o, c := fixture(t)
		ctx, cancel := context.WithCancel(t.Context())
		auth := recordAuth{
			write: func(context.Context, Execution) (Attribution, error) {
				cancel()
				return Attribution{sampleOwner, "principal-a"}, nil
			},
			read: func(context.Context, string) (string, error) { cancel(); return "reader-a", nil },
		}
		s := newStore(t, o, c, auth, Limits{1024, 1})
		var err error
		if write {
			_, err = s.Write(ctx, sampleID, nil)
		} else {
			_, err = s.Read(ctx, sampleID)
		}
		if !errors.Is(err, context.Canceled) || c.lookups.Load()+c.publications.Load()+o.creates.Load()+o.reads.Load() != 0 {
			t.Fatalf("post-authorization cancellation reached storage: %v", err)
		}
		s.authorizer = authorizeFunc(allow)
		if _, err := s.Write(t.Context(), sampleID, nil); err != nil {
			t.Fatalf("slot was not released: %v", err)
		}
	}
}

func TestRecordRejectsLegacyUnattributedRows(t *testing.T) {
	for _, registered := range []bool{false, true} {
		s, o, c := fixture(t)
		if !registered {
			delete(c.bindings, sampleExecution)
		}
		c.rows[sampleID] = Receipt{Reference: Reference{sampleID, sha256.Sum256(nil), 0}}
		registrar := newRegistrar(t, trustedRegistrar("trusted-host-1"), c, 1)
		_, err := registrar.Register(t.Context(), sampleExecution, sampleOwner)
		if !registered && (!errors.Is(err, ErrIntegrity) || !errors.Is(err, ErrRegistrationUncertain)) {
			t.Fatalf("legacy row retroactively registered: %v", err)
		}
		if _, err := s.Write(t.Context(), sampleID, nil); err == nil {
			t.Fatal("legacy row acquired attribution")
		}
		if data, err := s.Read(t.Context(), sampleID); !errors.Is(err, ErrIntegrity) || data != nil {
			t.Fatalf("legacy row read: %q, %v", data, err)
		}
		if o.creates.Load()+o.reads.Load()+c.publications.Load() != 0 {
			t.Fatal("legacy row reached object storage or publication")
		}
	}
}

func TestRecordMalformedReceiptsFailClosed(t *testing.T) {
	for _, mutate := range []func(Receipt) Receipt{
		func(r Receipt) Receipt { r.Reference.Identity.Kind = CaptureRecord; return r },
		func(r Receipt) Receipt { r.Attribution.Owner.Run = "wrong"; return r },
		func(r Receipt) Receipt { r.Attribution.SubmittedBy = ""; return r },
		func(r Receipt) Receipt { r.Attribution.SubmittedBy = "impostor"; return r },
	} {
		s, _, c := fixture(t)
		c.changeReceipt = mutate
		if r, err := s.Write(t.Context(), sampleID, nil); !errors.Is(err, ErrIntegrity) || !errors.Is(err, ErrUncertain) || r != (Receipt{}) {
			t.Fatalf("malformed publish: %+v, %v", r, err)
		}
	}
	for _, mutate := range []func(Receipt) Receipt{
		func(r Receipt) Receipt { r.Attribution.Owner.Instance = ""; return r },
		func(r Receipt) Receipt { r.Attribution.SubmittedBy = ""; return r },
		func(r Receipt) Receipt { r.Duplicate = true; return r },
	} {
		s, o, c := fixture(t)
		if _, err := s.Write(t.Context(), sampleID, nil); err != nil {
			t.Fatal(err)
		}
		c.changeReceipt = mutate
		if data, err := s.Read(t.Context(), sampleID); !errors.Is(err, ErrIntegrity) || data != nil || o.reads.Load() != 0 {
			t.Fatalf("malformed lookup: %q, %v", data, err)
		}
	}
}

func TestRecordValidatesTrustedIdentityAndExactExecution(t *testing.T) {
	_, o, c := fixture(t)
	for _, bad := range []string{"", strings.Repeat("x", maxIdentityBytes+1)} {
		for field := range 3 {
			a := Attribution{sampleOwner, "principal-a"}
			parts := []*string{&a.Owner.Run, &a.Owner.Instance, &a.SubmittedBy}
			*parts[field] = bad
			s := newStore(t, o, c, writerAuth(a), Limits{1024, 1})
			if _, err := s.Write(t.Context(), sampleID, nil); !errors.Is(err, ErrInvalid) {
				t.Fatal(err)
			}
		}
		s := newStore(t, o, c, readerAuth(bad), Limits{1024, 1})
		if _, err := s.Read(t.Context(), sampleID); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	if c.lookups.Load()+c.publications.Load()+o.creates.Load()+o.reads.Load() != 0 {
		t.Fatal("invalid trusted identity reached storage")
	}
	auth := recordAuth{write: func(ctx context.Context, e Execution) (Attribution, error) {
		if e != sampleExecution {
			t.Fatalf("authorizer execution: %+v", e)
		}
		return Attribution{sampleOwner, "principal-a"}, ctx.Err()
	}}
	s := newStore(t, o, c, auth, Limits{1024, 1})
	for _, id := range []Identity{captureID(), sampleID} {
		if _, err := s.Write(t.Context(), id, nil); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRecordConcurrentCollectorsKeepOneOriginalSubmitter(t *testing.T) {
	_, o, c := fixture(t)
	const count = 12
	var wg sync.WaitGroup
	results := make(chan Receipt, count)
	for i := range count {
		a := Attribution{sampleOwner, strings.Repeat("c", i+1)}
		setWriter(c, sampleID.Tenant, a, true)
		s := newStore(t, o, c, writerAuth(a), Limits{1024, 1})
		wg.Go(func() {
			r, err := s.Write(t.Context(), captureID(), []byte("same capture"))
			if err != nil {
				t.Error(err)
			}
			results <- r
		})
	}
	wg.Wait()
	close(results)
	var original Attribution
	created := 0
	for r := range results {
		if original == (Attribution{}) {
			original = r.Attribution
		}
		if r.Attribution != original {
			t.Fatal("concurrent retry changed original submitter")
		}
		if !r.AcceptedAt.Equal(c.now) || !r.ExpiresAt.Equal(c.now.Add(DefaultPayloadRetention)) {
			t.Fatal("concurrent retry changed original times")
		}
		if !r.Duplicate {
			created++
		}
	}
	if created != 1 || c.count() != 1 {
		t.Fatalf("created=%d, accepted=%d", created, c.count())
	}
}
