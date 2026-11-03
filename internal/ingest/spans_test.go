package ingest

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"

	"github.com/sidsharma96/rewind/internal/record"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// Independent in-memory authority model; native SQL tests exercise real locks.
type spanModel struct {
	*modelCatalog
	beforeResolve func()
	afterInspect  func()
	changeInspect func(Receipt, bool) (Receipt, bool)
	changePublish func(Receipt, bool) (Receipt, bool)
	changeResolve func(Receipt) Receipt
}

func (m *spanModel) checked(ctx context.Context, id Identity, a Attribution) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := m.writerAllowed(id, a); err != nil {
		return err
	}
	if err := m.recordValid(id); err != nil {
		return err
	}
	return m.checkExpiry(id)
}
func (m *spanModel) InspectSpan(ctx context.Context, id Identity, a Attribution) (Receipt, bool, error) {
	m.mu.Lock()
	err := m.checked(ctx, id, a)
	r, found := m.rows[id]
	m.mu.Unlock()
	if err != nil {
		return Receipt{}, false, err
	}
	if m.afterInspect != nil {
		m.afterInspect()
	}
	if m.changeInspect != nil {
		r, found = m.changeInspect(r, found)
	}
	return r, found, nil
}
func (m *spanModel) PublishSpan(ctx context.Context, ref Reference, a Attribution) (Receipt, bool, error) {
	r, err := m.Publish(ctx, ref, a)
	if errors.Is(err, ErrConflict) {
		m.mu.Lock()
		err = m.checked(ctx, ref.Identity, a)
		r = m.rows[ref.Identity]
		m.mu.Unlock()
		r.Duplicate = true
	}
	if err != nil {
		return Receipt{}, false, err
	}
	compare := r.Reference != ref
	if m.changePublish != nil {
		r, compare = m.changePublish(r, compare)
	}
	return r, compare, nil
}
func (m *spanModel) ResolveSpan(ctx context.Context, ref Reference, a Attribution, equal bool) (Receipt, error) {
	if m.beforeResolve != nil {
		m.beforeResolve()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checked(ctx, ref.Identity, a); err != nil {
		return Receipt{}, err
	}
	r, found := m.rows[ref.Identity]
	if !found || r.Reference != ref {
		return Receipt{}, ErrIntegrity
	}
	if !equal {
		return Receipt{}, ErrConflict
	}
	r.Duplicate = true
	if m.changeResolve != nil {
		r = m.changeResolve(r)
	}
	return r, nil
}

func spanCodec(t *testing.T, threshold int) record.SpanCodec {
	t.Helper()
	m, err := record.NewMetadataCodec(record.MetadataLimits{MaxBytes: 8192, MaxWork: 16384, MaxDepth: 64})
	if err != nil {
		t.Fatal(err)
	}
	p := record.DefaultCompressionPolicy()
	p.Threshold = threshold
	e, err := record.NewEnvelopeCodec(record.EnvelopeLimits{MaxStoredBytes: 8192, MaxExpandedBytes: 8192}, p)
	if err != nil {
		t.Fatal(err)
	}
	c, err := record.NewSpanCodec(m, e)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func endedFixture(t *testing.T, contextPresent bool, name string) record.EndedSpan {
	t.Helper()
	s := record.Span{Metadata: &tracepb.Span{TraceId: sampleID.Trace[:], SpanId: sampleID.Span[:], Name: name,
		StartTimeUnixNano: 20, EndTimeUnixNano: 10, Kind: -17, Flags: 0x80000000}}
	if contextPresent {
		s.Context.Resource = &resourcepb.Resource{}
		s.Context.Scope = &commonpb.InstrumentationScope{}
		s.Metadata.Status = &tracepb.Status{}
	}
	e, err := s.DeclareEnded()
	if err != nil {
		t.Fatal(err)
	}
	return e
}
func typedFixture(t *testing.T, threshold int) (*Spans, *modelObjects, *spanModel) {
	t.Helper()
	_, o, c := fixture(t)
	model := &spanModel{modelCatalog: c}
	s, err := NewSpans(authorizeFunc(allow), o, model, Limits{8192, 1}, spanCodec(t, threshold))
	if err != nil {
		t.Fatal(err)
	}
	return s, o, model
}

func TestTypedSpanRetryUsesValuesAndKeepsOriginal(t *testing.T) {
	s, o, c := typedFixture(t, 32768)
	execution := executionOf(sampleID)
	original, err := s.WriteSpan(t.Context(), execution, endedFixture(t, false, ""))
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := NewSpans(writerAuth(original.Attribution), o, c, Limits{8192, 1}, spanCodec(t, 1))
	if err != nil {
		t.Fatal(err)
	}
	for _, present := range []bool{false, true} {
		r, err := fresh.WriteSpan(t.Context(), execution, endedFixture(t, present, ""))
		want := original
		want.Duplicate = true
		if err != nil || r != want || o.creates.Load() != 1 {
			t.Fatal("equivalent encoding/presence retry changed original", err)
		}
	}
	if _, err := fresh.ReadSpan(t.Context(), execution); !errors.Is(err, ErrDenied) {
		t.Fatal("writer gained project read access")
	}
	if _, err := fresh.WriteSpan(t.Context(), execution, endedFixture(t, false, "now named")); !errors.Is(err, ErrConflict) {
		t.Fatal("changed name did not conflict")
	}
	read, err := s.ReadSpan(t.Context(), execution)
	if err != nil || read.Metadata.Name != "" || read.Metadata.Flags != 0x80000000 || read.Metadata.Kind != -17 || read.Metadata.EndTimeUnixNano != 10 {
		t.Fatal("saved evidence changed", err)
	}
	if o.creates.Load() != 1 {
		t.Fatal("known conflicting retry uploaded candidate")
	}
	if _, err := s.ReadSpan(t.Context(), Execution{Tenant: sampleID.Tenant, Trace: sampleID.Trace, Span: sampleID.Span}); err != nil {
		t.Fatal(err)
	}
	t.Log("blank name preserved; equivalent compression and empty-shell retries retain original acceptance; writer cannot read")
}

func TestTypedSpanComparisonRechecksStateAndTreatsErrorsAsErrors(t *testing.T) {
	broken := errors.New("controlled read or close failure")
	for _, fault := range []string{"revoked", "read-error", "digest", "missing", "unsupported", "canceled", "expired", "reference", "receipt"} {
		t.Run(fault, func(t *testing.T) {
			s, o, c := typedFixture(t, 32768)
			e := executionOf(sampleID)
			original, err := s.WriteSpan(t.Context(), e, endedFixture(t, false, ""))
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			want := ErrIntegrity
			s.codec = spanCodec(t, 1)
			switch fault {
			case "revoked":
				c.beforeResolve = func() { setWriter(c.modelCatalog, sampleID.Tenant, original.Attribution, false) }
				want = ErrDenied
			case "read-error":
				o.readErr = broken
				want = broken
			case "digest":
				o.changeRead = func(b []byte) []byte { b[0] ^= 1; return b }
			case "missing":
				o.mu.Lock()
				delete(o.data, ObjectID{sampleID.Tenant, original.Reference.Digest})
				o.mu.Unlock()
				want = ErrObjectMissing
			case "unsupported":
				o.mu.Lock()
				for key, b := range o.data {
					b[4] = 255
					delete(o.data, key)
					key.Digest = sha256.Sum256(b)
					o.data[key] = b
					c.rows[sampleID] = Receipt{Reference: Reference{sampleID, key.Digest, len(b)}, Attribution: original.Attribution, AcceptedAt: original.AcceptedAt, ExpiresAt: original.ExpiresAt}
				}
				o.mu.Unlock()
				want = record.ErrEnvelopeUnsupported
			case "canceled":
				o.changeRead = func(b []byte) []byte { cancel(); return b }
				want = context.Canceled
			case "expired":
				c.beforeResolve = func() { c.expiry = func(Identity) error { return ErrExpired } }
				want = ErrExpired
			case "reference":
				c.beforeResolve = func() { c.mu.Lock(); delete(c.rows, sampleID); c.mu.Unlock() }
			case "receipt":
				c.changeResolve = func(r Receipt) Receipt { r.Attribution.SubmittedBy = "altered"; return r }
			}
			r, err := s.WriteSpan(ctx, e, endedFixture(t, true, ""))
			if !errors.Is(err, want) || errors.Is(err, ErrConflict) || r != (Receipt{}) {
				t.Fatal("comparison failure became retry outcome", err)
			}
			if o.creates.Load() != 1 {
				t.Fatal("failed comparison uploaded a candidate")
			}
		})
	}
}

func TestTypedSpanAdmissionAndIdentityPrecedePreparation(t *testing.T) {
	s, o, c := typedFixture(t, 32768)
	e := executionOf(sampleID)
	entered, release := make(chan struct{}), make(chan struct{})
	c.afterInspect = func() { close(entered); <-release }
	var wg sync.WaitGroup
	wg.Add(1)
	done := make(chan error, 1)
	go func() {
		defer wg.Done()
		_, err := s.WriteSpan(t.Context(), e, endedFixture(t, false, ""))
		done <- err
	}()
	<-entered
	malformed := endedFixture(t, false, "")
	v, _ := malformed.View()
	v.Metadata.Attributes = []*commonpb.KeyValue{nil}
	if _, err := s.WriteSpan(t.Context(), e, malformed); !errors.Is(err, ErrBusy) {
		t.Fatal("graph work bypassed admission", err)
	}
	if got, err := s.ReadSpan(t.Context(), e); !errors.Is(err, ErrBusy) || got != (record.Span{}) {
		t.Fatal("read used a separate gate", err)
	}
	close(release)
	wg.Wait()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	c.afterInspect = nil
	before := o.creates.Load()
	wrong := e
	wrong.Span[0]++
	if _, err := s.WriteSpan(t.Context(), wrong, endedFixture(t, false, "")); !errors.Is(err, ErrInvalid) {
		t.Fatal("execution mismatch accepted")
	}
	if _, err := s.WriteSpan(t.Context(), e, malformed); err == nil {
		t.Fatal("malformed graph accepted")
	}
	if o.creates.Load() != before {
		t.Fatal("invalid input accessed object create")
	}
	if _, err := s.ReadSpan(t.Context(), e); err != nil {
		t.Fatal("admission slot leaked", err)
	}
}

func TestTypedSpanRejectsMalformedCatalogResults(t *testing.T) {
	for _, stage := range []string{"inspect", "publication", "resolution"} {
		t.Run(stage, func(t *testing.T) {
			s, o, c := typedFixture(t, 32768)
			e := executionOf(sampleID)
			switch stage {
			case "inspect":
				c.changeInspect = func(r Receipt, found bool) (Receipt, bool) { return r, true }
			case "publication":
				c.changePublish = func(r Receipt, compare bool) (Receipt, bool) { r.Reference.Size++; return r, compare }
			case "resolution":
				if _, err := s.WriteSpan(t.Context(), e, endedFixture(t, false, "")); err != nil {
					t.Fatal(err)
				}
				c.changeResolve = func(r Receipt) Receipt { r.Duplicate = false; return r }
			}
			if r, err := s.WriteSpan(t.Context(), e, endedFixture(t, false, "")); !errors.Is(err, ErrIntegrity) || r != (Receipt{}) {
				t.Fatal("malformed catalog response became acceptance", err)
			}
			if stage == "inspect" && o.creates.Load() != 0 {
				t.Fatal("invalid preflight reached objects")
			}
		})
	}
}
