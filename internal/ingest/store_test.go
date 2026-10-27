package ingest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var sampleID = Identity{Tenant: "tenant-a", Trace: "trace-1", Span: "span-42"}

type authorizeFunc func(context.Context, string) error

// These adapters keep the older storage-failure tests focused on their original
// boundaries. The fixture separately provisions binding and grant state.
func (f authorizeFunc) AuthorizeWrite(ctx context.Context, e Execution) (Attribution, error) {
	return Attribution{Owner: sampleOwner, SubmittedBy: "principal-a"}, f(ctx, e.Tenant)
}
func (f authorizeFunc) AuthorizeRead(ctx context.Context, tenant string) (string, error) {
	return "reader-a", f(ctx, tenant)
}

// These backends are a controlled model, not durable storage. Their state belongs
// to the fixture so replacing Store discards all coordinator-local state only.
type modelObjects struct {
	mu           sync.Mutex
	data         map[ObjectID][]byte
	creates      atomic.Int64
	reads        atomic.Int64
	beforeCreate func(context.Context) error
	afterCreate  func(context.Context) error
	readErr      error
	changeRead   func([]byte) []byte
}

func (m *modelObjects) Create(ctx context.Context, id ObjectID, data []byte) error {
	m.creates.Add(1)
	if m.beforeCreate != nil {
		if err := m.beforeCreate(ctx); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if sha256.Sum256(data) != id.Digest {
		return ErrIntegrity
	}
	m.mu.Lock()
	if old, exists := m.data[id]; exists && !bytes.Equal(old, data) {
		m.mu.Unlock()
		return ErrIntegrity
	}
	m.data[id] = bytes.Clone(data)
	m.mu.Unlock()
	if m.afterCreate != nil {
		return m.afterCreate(ctx)
	}
	return nil
}

func (m *modelObjects) Read(ctx context.Context, id ObjectID, maxBytes int) ([]byte, error) {
	m.reads.Add(1)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if m.readErr != nil {
		return nil, m.readErr
	}
	m.mu.Lock()
	data, exists := m.data[id]
	if !exists {
		m.mu.Unlock()
		return nil, ErrObjectMissing
	}
	if len(data) > maxBytes {
		m.mu.Unlock()
		return nil, ErrTooLarge
	}
	owned := bytes.Clone(data)
	m.mu.Unlock()
	if m.changeRead != nil {
		owned = m.changeRead(owned)
	}
	return owned, nil
}

func (m *modelObjects) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.data)
}

type writerGrant struct {
	Tenant      string
	Attribution Attribution
}

type readerGrant struct{ Tenant, Principal string }
type runKey struct{ Tenant, Run string }
type creatorGrant struct {
	Execution Execution
	Owner     Owner
	Principal string
}

type modelCatalog struct {
	mu                 sync.Mutex
	rows               map[Identity]Receipt
	retention          time.Duration
	bindings           map[Execution]Registration
	writers            map[writerGrant]bool
	readers            map[readerGrant]bool
	runs               map[runKey]RunAdmission
	creators           map[creatorGrant]bool
	now                time.Time
	registrations      atomic.Int64
	beforeRegistration func(context.Context) error
	afterRegistration  func(context.Context) error
	publications       atomic.Int64
	lookups            atomic.Int64
	beforePublish      func(context.Context) error
	afterPublish       func(context.Context) error
	lookupErr          error
	changeLookup       func(Reference) Reference
	changeReceipt      func(Receipt) Receipt
	afterLookup        func()
	expiry             func(Identity) error // sequential clock fixture only; called under mu
	policyErr          error
}

func (m *modelCatalog) writerAllowed(id Identity, a Attribution) error {
	if m.policyErr != nil {
		return m.policyErr
	}
	reg, exists := m.bindings[executionOf(id)]
	if !exists || reg.Owner != a.Owner || !m.writers[writerGrant{id.Tenant, a}] {
		return ErrDenied
	}
	run, exists := m.runs[runKey{id.Tenant, a.Owner.Run}]
	if !exists {
		return ErrDenied
	}
	return run.CheckRecord(m.now)
}

func (m *modelCatalog) recordValid(id Identity) error {
	if _, exists := m.rows[id]; !exists {
		return nil
	}
	record := m.rows[id]
	a := record.Attribution
	reg, exists := m.bindings[executionOf(id)]
	if !validAttribution(a) || !exists || reg.Owner != a.Owner || !validReceiptTimes(record) {
		return ErrIntegrity
	}
	return nil
}

func (m *modelCatalog) checkExpiry(id Identity) error {
	if m.expiry != nil {
		return m.expiry(id)
	}
	return nil
}

func (m *modelCatalog) CheckWrite(ctx context.Context, id Identity, a Attribution) error {
	m.lookups.Add(1)
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	err := m.writerAllowed(id, a)
	if err == nil {
		err = m.lookupErr
	}
	if err == nil {
		err = m.recordValid(id)
	}
	if err == nil {
		err = m.checkExpiry(id)
	}
	m.mu.Unlock()
	if m.afterLookup != nil {
		m.afterLookup()
	}
	return err
}

func (m *modelCatalog) Publish(ctx context.Context, ref Reference, a Attribution) (Receipt, error) {
	m.publications.Add(1)
	if m.beforePublish != nil {
		if err := m.beforePublish(ctx); err != nil {
			return Receipt{}, err
		}
	}
	m.mu.Lock()
	if err := ctx.Err(); err != nil {
		m.mu.Unlock()
		return Receipt{}, err
	}
	if err := m.writerAllowed(ref.Identity, a); err != nil {
		m.mu.Unlock()
		return Receipt{}, err
	}
	if err := m.recordValid(ref.Identity); err != nil {
		m.mu.Unlock()
		return Receipt{}, err
	}
	if err := m.checkExpiry(ref.Identity); err != nil {
		m.mu.Unlock()
		return Receipt{}, err
	}
	old, duplicate := m.rows[ref.Identity]
	if duplicate && old.Reference != ref {
		m.mu.Unlock()
		return Receipt{}, ErrConflict
	}
	if !duplicate {
		accepted := m.now.UTC()
		m.rows[ref.Identity] = Receipt{Reference: ref, Attribution: a,
			AcceptedAt: accepted, ExpiresAt: accepted.Add(m.retention)}
	}
	receipt := m.rows[ref.Identity]
	receipt.Duplicate = duplicate
	m.mu.Unlock()
	if m.afterPublish != nil {
		if err := m.afterPublish(ctx); err != nil {
			return Receipt{}, err
		}
	}
	if m.changeReceipt != nil {
		receipt = m.changeReceipt(receipt)
	}
	return receipt, nil
}

func (m *modelCatalog) Lookup(ctx context.Context, id Identity, principal string) (Receipt, error) {
	m.lookups.Add(1)
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	m.mu.Lock()
	if m.policyErr != nil {
		err := m.policyErr
		m.mu.Unlock()
		return Receipt{}, err
	}
	if !m.readers[readerGrant{id.Tenant, principal}] {
		m.mu.Unlock()
		return Receipt{}, ErrDenied
	}
	if m.lookupErr != nil {
		err := m.lookupErr
		m.mu.Unlock()
		return Receipt{}, err
	}
	if err := m.recordValid(id); err != nil {
		m.mu.Unlock()
		return Receipt{}, err
	}
	if err := m.checkExpiry(id); err != nil {
		m.mu.Unlock()
		return Receipt{}, err
	}
	receipt, exists := m.rows[id]
	m.mu.Unlock()
	if m.afterLookup != nil {
		m.afterLookup()
	}
	if !exists {
		return Receipt{}, ErrNotFound
	}
	if m.changeLookup != nil {
		receipt.Reference = m.changeLookup(receipt.Reference)
	}
	if m.changeReceipt != nil {
		receipt = m.changeReceipt(receipt)
	}
	return receipt, nil
}

// Registration uses the same mutex and binding state as publication. The
// independent Registrar tests also have a smaller registration-only fixture.
func (m *modelCatalog) RegisterOwnership(ctx context.Context, proposed Registration) (Registration, bool, error) {
	m.registrations.Add(1)
	if m.beforeRegistration != nil {
		if err := m.beforeRegistration(ctx); err != nil {
			return Registration{}, false, err
		}
	}
	// The locked helper makes the lifecycle check and binding outcome one operation.
	m.mu.Lock()
	original, duplicate, err := m.registerLocked(ctx, proposed)
	m.mu.Unlock()
	if err != nil {
		return Registration{}, false, err
	}
	if m.afterRegistration != nil {
		if err := m.afterRegistration(ctx); err != nil {
			return Registration{}, false, err
		}
	}
	return original, duplicate, nil
}

func (m *modelCatalog) registerLocked(ctx context.Context, proposed Registration) (Registration, bool, error) {
	if err := ctx.Err(); err != nil {
		return Registration{}, false, err
	}
	if m.policyErr != nil {
		return Registration{}, false, m.policyErr
	}
	if !m.creators[creatorGrant{proposed.Execution, proposed.Owner, proposed.RegisteredBy}] {
		return Registration{}, false, ErrDenied
	}
	old, exists := m.bindings[proposed.Execution]
	identical := exists && old.Owner == proposed.Owner
	keys := []runKey{{proposed.Execution.Tenant, proposed.Owner.Run}}
	if exists && old.Owner.Run != proposed.Owner.Run {
		keys = append(keys, runKey{old.Execution.Tenant, old.Owner.Run})
	}
	for _, key := range keys {
		run, known := m.runs[key]
		if !known {
			return Registration{}, false, ErrDenied
		}
		if err := run.CheckRegistration(m.now, identical); err != nil {
			return Registration{}, false, err
		}
	}
	if exists {
		if !identical {
			return Registration{}, false, ErrOwnershipConflict
		}
		return old, true, nil
	}
	for _, kind := range []Kind{SpanRecord, CaptureRecord} {
		id := Identity{Tenant: proposed.Execution.Tenant, Trace: proposed.Execution.Trace, Span: proposed.Execution.Span, Kind: kind}
		if _, exists := m.rows[id]; exists {
			return Registration{}, false, ErrIntegrity
		}
	}
	m.bindings[proposed.Execution] = proposed
	return proposed, false, nil
}

func (m *modelCatalog) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.rows)
}

func newModelCatalog() *modelCatalog {
	return &modelCatalog{rows: make(map[Identity]Receipt), retention: DefaultPayloadRetention,
		bindings: make(map[Execution]Registration), writers: make(map[writerGrant]bool), readers: make(map[readerGrant]bool),
		runs: make(map[runKey]RunAdmission), creators: make(map[creatorGrant]bool),
		now: time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)}
}

func provision(t *testing.T, c *modelCatalog, id Identity, a Attribution) {
	t.Helper()
	c.mu.Lock()
	key := runKey{id.Tenant, a.Owner.Run}
	if _, exists := c.runs[key]; !exists {
		c.runs[key] = NewRunAdmission()
	}
	c.creators[creatorGrant{executionOf(id), a.Owner, "trusted-host-1"}] = true
	c.mu.Unlock()
	registrar := newRegistrar(t, trustedRegistrar("trusted-host-1"), c, 1)
	if _, err := registrar.Register(t.Context(), executionOf(id), a.Owner); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	c.writers[writerGrant{id.Tenant, a}] = true
	c.readers[readerGrant{id.Tenant, "reader-a"}] = true
	c.mu.Unlock()
}

func allow(ctx context.Context, _ string) error { return ctx.Err() }

func newStore(t *testing.T, objects *modelObjects, catalog *modelCatalog, auth Authorizer, limits Limits) *Store {
	t.Helper()
	s, err := New(auth, objects, catalog, limits)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func fixture(t *testing.T) (*Store, *modelObjects, *modelCatalog) {
	t.Helper()
	o := &modelObjects{data: make(map[ObjectID][]byte)}
	c := newModelCatalog()
	provision(t, c, sampleID, Attribution{sampleOwner, "principal-a"})
	return newStore(t, o, c, authorizeFunc(allow), Limits{1024, 32}), o, c
}

func TestWriteFailureBoundaries(t *testing.T) {
	broken := errors.New("connection interrupted")
	for _, tc := range []struct {
		name              string
		configure         func(*modelObjects, *modelCatalog)
		wantErr           error
		objects, accepted int
		published         int64
	}{
		{"success", func(*modelObjects, *modelCatalog) {}, nil, 1, 1, 1},
		{"upload refused", func(o *modelObjects, _ *modelCatalog) {
			o.beforeCreate = func(context.Context) error { return broken }
		}, broken, 0, 0, 0},
		{"upload stored but reply lost", func(o *modelObjects, _ *modelCatalog) {
			o.afterCreate = func(context.Context) error { return broken }
		}, broken, 1, 0, 0},
		{"catalog refused before commit", func(_ *modelObjects, c *modelCatalog) {
			c.beforePublish = func(context.Context) error { return broken }
		}, ErrUncertain, 1, 0, 1},
		{"catalog committed but reply lost", func(_ *modelObjects, c *modelCatalog) {
			c.afterPublish = func(context.Context) error { return broken }
		}, ErrUncertain, 1, 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, o, c := fixture(t)
			tc.configure(o, c)
			receipt, err := s.Write(t.Context(), sampleID, []byte("captured call"))
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v, want %v", err, tc.wantErr)
			}
			if tc.wantErr != nil && receipt != (Receipt{}) {
				t.Fatalf("failure returned success receipt: %+v", receipt)
			}
			if o.count() != tc.objects || c.count() != tc.accepted || c.publications.Load() != tc.published {
				t.Fatalf("state objects=%d accepted=%d publish calls=%d", o.count(), c.count(), c.publications.Load())
			}
			if errors.Is(err, ErrUncertain) && !errors.Is(err, broken) {
				t.Fatal("lost publication cause")
			}
		})
	}
}

func TestFreshCoordinatorResolvesLostCommitReply(t *testing.T) {
	first, o, c := fixture(t)
	c.afterPublish = func(context.Context) error { return errors.New("lost commit reply") }
	data := []byte("recorded answer")
	if _, err := first.Write(t.Context(), sampleID, data); !errors.Is(err, ErrUncertain) {
		t.Fatalf("write = %v", err)
	}
	c.afterPublish = nil
	fresh := newStore(t, o, c, authorizeFunc(allow), Limits{1024, 1})
	got, err := fresh.Read(t.Context(), sampleID)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("fresh read = %q, %v", got, err)
	}
	r, err := fresh.Write(t.Context(), sampleID, data)
	if err != nil || !r.Duplicate || c.count() != 1 || o.count() != 1 {
		t.Fatalf("retry receipt=%+v err=%v accepted=%d objects=%d", r, err, c.count(), o.count())
	}
	if _, err := fresh.Write(t.Context(), sampleID, []byte("different answer")); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed retry = %v", err)
	}
	got, err = fresh.Read(t.Context(), sampleID)
	if err != nil || !bytes.Equal(got, data) || c.count() != 1 || o.count() != 2 {
		t.Fatalf("original was not preserved: %q, %v", got, err)
	}
}

func TestNewExecutionAndTenantHaveSeparateIdentities(t *testing.T) {
	s, o, c := fixture(t)
	for _, id := range []Identity{sampleID,
		{Tenant: sampleID.Tenant, Trace: sampleID.Trace, Span: "span-43"},
		{Tenant: "tenant-b", Trace: sampleID.Trace, Span: sampleID.Span},
	} {
		provision(t, c, id, Attribution{sampleOwner, "principal-a"})
		r, err := s.Write(t.Context(), id, []byte("identical contents"))
		if err != nil || r.Duplicate {
			t.Fatalf("new identity = %+v, %v", r, err)
		}
	}
	if c.count() != 3 || o.count() != 2 {
		t.Fatalf("accepted=%d objects=%d", c.count(), o.count())
	}
}

func TestRetryAfterUnpublishedObject(t *testing.T) {
	for _, afterUpload := range []bool{false, true} {
		t.Run(map[bool]string{false: "catalog refused", true: "upload reply lost"}[afterUpload], func(t *testing.T) {
			first, o, c := fixture(t)
			broken := func(context.Context) error { return errors.New("connection interrupted") }
			if afterUpload {
				o.afterCreate = broken
			} else {
				c.beforePublish = broken
			}
			if _, err := first.Write(t.Context(), sampleID, []byte("same captured call")); err == nil {
				t.Fatal("expected interrupted first write")
			}
			if o.count() != 1 || c.count() != 0 {
				t.Fatal("expected retained unpublished object")
			}
			o.afterCreate, c.beforePublish = nil, nil
			fresh := newStore(t, o, c, authorizeFunc(allow), Limits{1024, 1})
			r, err := fresh.Write(t.Context(), sampleID, []byte("same captured call"))
			if err != nil || r.Duplicate || o.count() != 1 || c.count() != 1 {
				t.Fatalf("retry=%+v err=%v objects=%d accepted=%d", r, err, o.count(), c.count())
			}
		})
	}
}

func TestAuthorizationUsesRequestedTenant(t *testing.T) {
	_, o, c := fixture(t)
	denied := errors.New("not authorized for requested tenant")
	auth := authorizeFunc(func(ctx context.Context, tenant string) error {
		if tenant != sampleID.Tenant {
			return denied
		}
		return ctx.Err()
	})
	s := newStore(t, o, c, auth, Limits{1024, 1})
	if _, err := s.Write(t.Context(), sampleID, []byte("owned by tenant-a")); err != nil {
		t.Fatal(err)
	}
	other := sampleID
	other.Tenant = "tenant-b"
	if _, err := s.Write(t.Context(), other, []byte("attempt")); !errors.Is(err, denied) {
		t.Fatalf("unauthorized write = %v", err)
	}
	if _, err := s.Read(t.Context(), other); !errors.Is(err, denied) {
		t.Fatalf("unauthorized read = %v", err)
	}
	if o.creates.Load() != 1 || o.reads.Load() != 0 || c.lookups.Load() != 1 || c.publications.Load() != 1 {
		t.Fatal("unauthorized tenant reached stores")
	}
}

func TestConcurrentIdenticalRetriesAcceptOneSpan(t *testing.T) {
	s, o, c := fixture(t)
	const writers = 16
	var wg sync.WaitGroup
	var created, duplicates, failed atomic.Int64
	for range writers {
		wg.Go(func() {
			r, err := s.Write(t.Context(), sampleID, []byte("same call"))
			if err != nil {
				failed.Add(1)
			} else if r.Duplicate {
				duplicates.Add(1)
			} else {
				created.Add(1)
			}
		})
	}
	wg.Wait()
	if failed.Load() != 0 || created.Load() != 1 || duplicates.Load() != writers-1 || c.count() != 1 || o.count() != 1 {
		t.Fatalf("created=%d duplicates=%d failures=%d accepted=%d objects=%d", created.Load(), duplicates.Load(), failed.Load(), c.count(), o.count())
	}
}

func TestConcurrentConflictsPreserveWinningContents(t *testing.T) {
	s, _, c := fixture(t)
	type result struct {
		data []byte
		err  error
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	for _, text := range []string{"first possibility", "other possibility"} {
		go func() {
			<-start
			data := []byte(text)
			_, err := s.Write(t.Context(), sampleID, data)
			results <- result{data, err}
		}()
	}
	close(start)
	var winner []byte
	conflicts := 0
	for range 2 {
		r := <-results
		if r.err == nil {
			winner = r.data
		} else if errors.Is(r.err, ErrConflict) {
			conflicts++
		} else {
			t.Fatal(r.err)
		}
	}
	got, err := s.Read(t.Context(), sampleID)
	if err != nil || conflicts != 1 || c.count() != 1 || !bytes.Equal(got, winner) {
		t.Fatalf("winner=%q read=%q conflicts=%d err=%v", winner, got, conflicts, err)
	}
}

func TestAuthorizationPrecedesAllStorageAccess(t *testing.T) {
	_, o, c := fixture(t)
	denied := errors.New("tenant denied")
	s := newStore(t, o, c, authorizeFunc(func(context.Context, string) error { return denied }), Limits{1024, 1})
	for range 2 { // A failed authorization must also release the admission slot.
		if _, err := s.Write(t.Context(), sampleID, []byte("private")); !errors.Is(err, denied) {
			t.Fatalf("write = %v", err)
		}
		if _, err := s.Read(t.Context(), sampleID); !errors.Is(err, denied) {
			t.Fatalf("read = %v", err)
		}
	}
	if o.creates.Load()+o.reads.Load()+c.lookups.Load()+c.publications.Load() != 0 {
		t.Fatal("denied request reached storage")
	}
}

func TestAdmissionAndCancellationReleaseSlot(t *testing.T) {
	_, o, c := fixture(t)
	s := newStore(t, o, c, authorizeFunc(allow), Limits{1024, 1})
	entered := make(chan struct{})
	var once sync.Once
	o.beforeCreate = func(ctx context.Context) error {
		blocked := false
		once.Do(func() { close(entered); blocked = true })
		if blocked {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := s.Write(ctx, sampleID, []byte("blocked")); done <- err }()
	<-entered
	if _, err := s.Read(t.Context(), sampleID); !errors.Is(err, ErrBusy) {
		t.Fatalf("read while write occupies slot = %v", err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) || errors.Is(err, ErrUncertain) {
		t.Fatalf("canceled upload = %v", err)
	}
	if c.publications.Load() != 0 {
		t.Fatal("canceled upload published")
	}
	if _, err := s.Write(t.Context(), sampleID, []byte("after cancellation")); err != nil {
		t.Fatalf("slot not released: %v", err)
	}
}

func TestCancellationAroundPublication(t *testing.T) {
	t.Run("before publication call", func(t *testing.T) {
		s, o, c := fixture(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		o.afterCreate = func(context.Context) error { cancel(); return nil }
		if _, err := s.Write(ctx, sampleID, nil); !errors.Is(err, context.Canceled) || errors.Is(err, ErrUncertain) {
			t.Fatalf("write = %v", err)
		}
		if c.publications.Load() != 0 || o.count() != 1 {
			t.Fatal("unexpected publication after cancellation")
		}
	})
	t.Run("publication returns cancellation", func(t *testing.T) {
		s, _, c := fixture(t)
		c.beforePublish = func(context.Context) error { return context.Canceled }
		if _, err := s.Write(t.Context(), sampleID, nil); !errors.Is(err, ErrUncertain) || !errors.Is(err, context.Canceled) {
			t.Fatalf("write = %v", err)
		}
	})
	t.Run("after confirmed commit", func(t *testing.T) {
		s, _, c := fixture(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		c.afterPublish = func(context.Context) error { cancel(); return nil }
		r, err := s.Write(ctx, sampleID, nil)
		if err != nil || r.Reference.Identity != sampleID || ctx.Err() == nil {
			t.Fatalf("confirmed publication receipt=%+v err=%v", r, err)
		}
	})
}

func TestReadFailuresAreNotAbsence(t *testing.T) {
	broken := errors.New("backend unavailable")
	for _, tc := range []struct {
		name      string
		configure func(*modelObjects, *modelCatalog)
		wantErr   error
	}{
		{"catalog unavailable", func(_ *modelObjects, c *modelCatalog) { c.lookupErr = broken }, broken},
		{"object unavailable", func(o *modelObjects, _ *modelCatalog) { o.readErr = broken }, broken},
		{"referenced object missing", func(o *modelObjects, _ *modelCatalog) { o.data = make(map[ObjectID][]byte) }, ErrObjectMissing},
		{"corrupt bytes", func(o *modelObjects, _ *modelCatalog) {
			o.changeRead = func(data []byte) []byte { data[0] ^= 1; return data }
		}, ErrIntegrity},
		{"truncated bytes", func(o *modelObjects, _ *modelCatalog) {
			o.changeRead = func(data []byte) []byte { return data[:len(data)-1] }
		}, ErrIntegrity},
		{"oversized backend response", func(o *modelObjects, _ *modelCatalog) {
			o.changeRead = func(data []byte) []byte { return append(data, 0) }
		}, ErrIntegrity},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, o, c := fixture(t)
			if _, err := s.Write(t.Context(), sampleID, []byte("original")); err != nil {
				t.Fatal(err)
			}
			tc.configure(o, c)
			data, err := s.Read(t.Context(), sampleID)
			if !errors.Is(err, tc.wantErr) || errors.Is(err, ErrNotFound) || data != nil {
				t.Fatalf("read = %q, %v, want %v without absence", data, err, tc.wantErr)
			}
		})
	}
	t.Run("actual catalog absence", func(t *testing.T) {
		s, o, _ := fixture(t)
		if _, err := s.Read(t.Context(), sampleID); !errors.Is(err, ErrNotFound) || o.reads.Load() != 0 {
			t.Fatalf("missing span = %v, object reads=%d", err, o.reads.Load())
		}
	})
}

func TestInvalidCatalogReferenceCannotReachObjects(t *testing.T) {
	for _, change := range []func(Reference) Reference{
		func(r Reference) Reference { r.Identity.Tenant = "tenant-b"; return r },
		func(r Reference) Reference { r.Identity.Span = "other-span"; return r },
		func(r Reference) Reference { r.Identity.Kind = CaptureRecord; return r },
		func(r Reference) Reference { r.Size = -1; return r },
	} {
		s, o, c := fixture(t)
		if _, err := s.Write(t.Context(), sampleID, []byte("valid")); err != nil {
			t.Fatal(err)
		}
		c.changeLookup = change
		if _, err := s.Read(t.Context(), sampleID); !errors.Is(err, ErrIntegrity) || o.reads.Load() != 0 {
			t.Fatalf("invalid reference = %v; object reads=%d", err, o.reads.Load())
		}
	}
}

func TestReducedReadLimitDoesNotLabelAcceptedSpanCorrupt(t *testing.T) {
	original, o, c := fixture(t)
	data := bytes.Repeat([]byte("x"), 64)
	if _, err := original.Write(t.Context(), sampleID, data); err != nil {
		t.Fatal(err)
	}
	smaller := newStore(t, o, c, authorizeFunc(allow), Limits{32, 1})
	if _, err := smaller.Read(t.Context(), sampleID); !errors.Is(err, ErrTooLarge) || errors.Is(err, ErrIntegrity) {
		t.Fatalf("read under reduced limit = %v", err)
	}
	if o.reads.Load() != 0 {
		t.Fatal("over-limit record reached object read")
	}
	got, err := original.Read(t.Context(), sampleID)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("valid accepted data = %q, %v", got, err)
	}
}

func TestInputBoundsBeforeStorage(t *testing.T) {
	s, o, c := fixture(t)
	if _, err := s.Write(t.Context(), sampleID, make([]byte, 1025)); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversized record = %v", err)
	}
	for _, id := range []Identity{
		{Tenant: "", Trace: "trace", Span: "span"},
		{Tenant: "tenant", Trace: "", Span: "span"},
		{Tenant: "tenant", Trace: "trace", Span: ""},
		{Tenant: strings.Repeat("x", maxIdentityBytes+1), Trace: "trace", Span: "span"},
		{Tenant: "tenant", Trace: "trace", Span: "span", Kind: Kind(255)},
	} {
		if _, err := s.Write(t.Context(), id, nil); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid write = %v", err)
		}
		if _, err := s.Read(t.Context(), id); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid read = %v", err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.Write(ctx, sampleID, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled write = %v", err)
	}
	if o.creates.Load()+o.reads.Load()+c.publications.Load()+c.lookups.Load() != 0 {
		t.Fatal("rejected input reached storage")
	}
	if _, err := s.Write(t.Context(), sampleID, make([]byte, 1024)); err != nil {
		t.Fatalf("record at byte limit = %v", err)
	}
}

func TestWriteChecksCatalogBeforeUpload(t *testing.T) {
	broken := errors.New("catalog unavailable")
	for _, accepted := range []bool{false, true} {
		t.Run(map[bool]string{false: "absent", true: "accepted"}[accepted], func(t *testing.T) {
			s, o, c := fixture(t)
			data := []byte("original")
			if accepted {
				if _, err := s.Write(t.Context(), sampleID, data); err != nil {
					t.Fatal(err)
				}
			}
			creates, publications, lookups := o.creates.Load(), c.publications.Load(), c.lookups.Load()
			c.lookupErr = broken
			if r, err := s.Write(t.Context(), sampleID, data); !errors.Is(err, broken) || errors.Is(err, ErrUncertain) || errors.Is(err, ErrNotFound) || r != (Receipt{}) {
				t.Fatalf("lookup failure=%+v,%v", r, err)
			}
			if o.creates.Load() != creates || c.publications.Load() != publications || c.lookups.Load() != lookups+1 {
				t.Fatal("lookup failure reached upload or publication")
			}
			c.lookupErr = nil
			// Neither absence nor an accepted result is a reservation or success.
			// Both must still upload and ask Publish for the authoritative result.
			r, err := s.Write(t.Context(), sampleID, data)
			if err != nil || r.Duplicate != accepted || o.creates.Load() != creates+1 || c.publications.Load() != publications+1 || c.lookups.Load() != lookups+2 {
				t.Fatalf("after lookup recovery=%+v,%v", r, err)
			}
			requireRead(t, s, sampleID, data)
		})
	}
}

func TestCancellationAfterPreliminaryLookupReleasesSlot(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		t.Run(map[bool]string{false: "absent", true: "accepted"}[accepted], func(t *testing.T) {
			_, o, c := fixture(t)
			s := newStore(t, o, c, authorizeFunc(allow), Limits{1024, 1})
			data := []byte("original")
			if accepted {
				if _, err := s.Write(t.Context(), sampleID, data); err != nil {
					t.Fatal(err)
				}
			}
			creates, publications := o.creates.Load(), c.publications.Load()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			c.afterLookup = cancel
			if r, err := s.Write(ctx, sampleID, data); !errors.Is(err, context.Canceled) || errors.Is(err, ErrUncertain) || r != (Receipt{}) {
				t.Fatalf("canceled after lookup=%+v,%v", r, err)
			}
			if o.creates.Load() != creates || c.publications.Load() != publications {
				t.Fatal("cancellation after lookup reached upload or publication")
			}
			c.afterLookup = nil
			if r, err := s.Write(t.Context(), sampleID, data); err != nil || r.Duplicate != accepted {
				t.Fatalf("slot not released=%+v,%v", r, err)
			}
		})
	}
}
