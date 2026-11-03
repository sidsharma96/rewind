package ingest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/sidsharma96/rewind/internal/record"
	"github.com/sidsharma96/rewind/internal/traceid"
)

// The independent authority model already handles both record kinds. The
// explicit capture capability checks its kind before using that same model.
type captureModel struct{ *spanModel }

func (m *captureModel) InspectCapture(ctx context.Context, id Identity, a Attribution) (Receipt, bool, error) {
	if id.Kind != CaptureRecord {
		return Receipt{}, false, ErrInvalid
	}
	return m.InspectSpan(ctx, id, a)
}
func (m *captureModel) PublishCapture(ctx context.Context, r Reference, a Attribution) (Receipt, bool, error) {
	if r.Identity.Kind != CaptureRecord {
		return Receipt{}, false, ErrInvalid
	}
	return m.PublishSpan(ctx, r, a)
}
func (m *captureModel) ResolveCapture(ctx context.Context, r Reference, a Attribution, equal bool) (Receipt, error) {
	if r.Identity.Kind != CaptureRecord {
		return Receipt{}, ErrInvalid
	}
	return m.ResolveSpan(ctx, r, a, equal)
}

func captureCodec(t *testing.T, threshold int) record.CaptureCodec {
	t.Helper()
	policy := record.DefaultCompressionPolicy()
	policy.Threshold = threshold
	e, err := record.NewEnvelopeCodec(record.EnvelopeLimits{MaxStoredBytes: 8192, MaxExpandedBytes: 8192}, policy)
	if err != nil {
		t.Fatal(err)
	}
	c, err := record.NewCaptureCodec(record.CaptureLimits{MaxMetadataBytes: 512, MaxBodyBytes: 2048}, e)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func captureFixture() record.Capture {
	return record.Capture{Trace: sampleID.Trace, Span: sampleID.Span, Request: record.Body{State: record.BodyRedacted}, Response: record.Body{State: record.BodyPresent, Data: []byte{0, 255, 'r'}}}
}
func capturesFixture(t *testing.T, threshold int) (*Captures, *modelObjects, *captureModel) {
	t.Helper()
	spans, o, m := typedFixture(t, 32768)
	model := &captureModel{m}
	spans.catalog = model
	spans.store.catalog = model
	c, err := NewCaptures(spans, captureCodec(t, threshold))
	if err != nil {
		t.Fatal(err)
	}
	return c, o, model
}

func TestCapturePlaybackKeepsWithheldRequestAndOriginalAcceptance(t *testing.T) {
	c, o, m := capturesFixture(t, 32768)
	execution := executionOf(sampleID)
	input := captureFixture()
	original, err := c.WriteCapture(t.Context(), execution, input)
	if err != nil {
		t.Fatal(err)
	}
	beforeRead := o.reads.Load()
	beforeCreate := o.creates.Load()
	want := original
	want.Duplicate = true
	if retry, err := c.WriteCapture(t.Context(), execution, input); err != nil || retry != want || o.reads.Load() != beforeRead || o.creates.Load() != beforeCreate {
		t.Fatal("exact retry changed acceptance or used objects", err)
	}
	c.codec = captureCodec(t, 1)
	if retry, err := c.WriteCapture(t.Context(), execution, input); err != nil || retry != want || o.reads.Load() != beforeRead+1 || o.creates.Load() != beforeCreate {
		t.Fatal("encoding retry changed acceptance", err)
	}
	result, err := c.Playback(t.Context(), execution)
	if err != nil || result.Execution != execution || result.Receipt != original || result.RequestState != record.BodyRedacted || !bytes.Equal(result.Bytes, input.Response.Data) {
		t.Fatal("redacted request blocked or disappeared from saved result", err)
	}
	clear(result.Bytes)
	clear(input.Response.Data)
	read, err := c.ReadCapture(t.Context(), execution)
	if err != nil || read.Receipt != original || !bytes.Equal(read.Capture.Response.Data, []byte{0, 255, 'r'}) || len(read.Capture.Request.Data) != 0 {
		t.Fatal("result did not own bytes", err)
	}
	// No span was accepted to make this capture readable.
	if _, err := m.Lookup(t.Context(), sampleID, "reader-a"); !errors.Is(err, ErrNotFound) {
		t.Fatal("capture acceptance invented a span")
	}
	for _, change := range []func(*record.Capture){
		func(v *record.Capture) { v.Request.State = record.BodyMissing },
		func(v *record.Capture) { v.Response.Data[1] = 1 },
		func(v *record.Capture) { v.Response.State = record.BodyTruncated },
	} {
		v := captureFixture()
		change(&v)
		if r, err := c.WriteCapture(t.Context(), execution, v); !errors.Is(err, ErrConflict) || r != (Receipt{}) {
			t.Fatal("changed capture did not conflict", err)
		}
	}
	if o.creates.Load() != 1 {
		t.Fatal("known conflicts uploaded candidates")
	}
	t.Log("capture-only acceptance; exact retry has no object calls; equivalent gzip retry reads one original; playback retains Redacted request and original receipt")
}

func TestPlaybackBodyRequirementsAndErrors(t *testing.T) {
	for _, request := range []record.BodyState{record.BodyMissing, record.BodyRedacted, record.BodyTruncated, record.BodyPresent} {
		for _, response := range []record.BodyState{record.BodyMissing, record.BodyRedacted, record.BodyTruncated, record.BodyPresent} {
			c, _, _ := capturesFixture(t, 32768)
			v := captureFixture()
			v.Request = record.Body{State: request}
			v.Response = record.Body{State: response}
			// Present nil/zero bytes is a complete empty result, not absence.
			if _, err := c.WriteCapture(t.Context(), executionOf(sampleID), v); err != nil {
				t.Fatal(err)
			}
			read, err := c.ReadCapture(t.Context(), executionOf(sampleID))
			if err != nil || read.Capture.Request.State != request || read.Capture.Response.State != response {
				t.Fatal("unavailable evidence not inspectable", err)
			}
			r, err := c.Playback(t.Context(), executionOf(sampleID))
			if response == record.BodyPresent {
				if err != nil || len(r.Bytes) != 0 || r.RequestState != request {
					t.Fatal("present-empty refusal", err)
				}
			} else if !errors.Is(err, ErrResultUnavailable) || !reflect.DeepEqual(r, SavedResult{}) {
				t.Fatal("unavailable result became empty success", err)
			}
		}
	}
	c, _, m := capturesFixture(t, 32768)
	if _, err := c.Playback(t.Context(), executionOf(sampleID)); !errors.Is(err, ErrNotFound) || errors.Is(err, ErrResultUnavailable) {
		t.Fatal("absent catalog record became body absence", err)
	}
	m.lookupErr = errors.New("controlled catalog outage")
	if _, err := c.Playback(t.Context(), executionOf(sampleID)); !errors.Is(err, m.lookupErr) || errors.Is(err, ErrResultUnavailable) {
		t.Fatal("outage became unavailable body", err)
	}
}

func TestCaptureFailuresNeverBecomeConflictsOrPartialSuccess(t *testing.T) {
	broken := errors.New("controlled read/close failure")
	for _, fault := range []string{"read", "digest", "missing", "unsupported", "revoke", "expiry", "reference", "receipt", "cancel"} {
		t.Run(fault, func(t *testing.T) {
			c, o, m := capturesFixture(t, 32768)
			e := executionOf(sampleID)
			v := captureFixture()
			original, err := c.WriteCapture(t.Context(), e, v)
			if err != nil {
				t.Fatal(err)
			}
			c.codec = captureCodec(t, 1)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			want := ErrIntegrity
			switch fault {
			case "read":
				o.readErr = broken
				want = broken
			case "digest":
				o.changeRead = func(b []byte) []byte { b[0] ^= 1; return b }
			case "missing":
				delete(o.data, ObjectID{sampleID.Tenant, original.Reference.Digest})
				want = ErrObjectMissing
			case "unsupported":
				key := ObjectID{sampleID.Tenant, original.Reference.Digest}
				b := o.data[key]
				delete(o.data, key)
				b[4] = 255
				key.Digest = sha256.Sum256(b)
				o.data[key] = b
				r := m.rows[captureIdentity(e)]
				r.Reference.Digest = key.Digest
				m.rows[captureIdentity(e)] = r
				want = record.ErrEnvelopeUnsupported
			case "revoke":
				m.beforeResolve = func() { setWriter(m.modelCatalog, e.Tenant, original.Attribution, false) }
				want = ErrDenied
			case "expiry":
				m.beforeResolve = func() { m.expiry = func(Identity) error { return broken } }
				want = broken
			case "reference":
				m.beforeResolve = func() { m.mu.Lock(); delete(m.rows, captureIdentity(e)); m.mu.Unlock() }
			case "receipt":
				m.changeResolve = func(r Receipt) Receipt { r.Attribution.SubmittedBy = "changed"; return r }
			case "cancel":
				o.changeRead = func(b []byte) []byte { cancel(); return b }
				want = context.Canceled
			}
			r, err := c.WriteCapture(ctx, e, v)
			if !errors.Is(err, want) || errors.Is(err, ErrConflict) || errors.Is(err, ErrUncertain) || r != (Receipt{}) {
				t.Fatal("failure became write outcome", err)
			}
			if o.creates.Load() != 1 || len(c.store.active) != 0 {
				t.Fatal("failure uploaded candidate or leaked gate")
			}
		})
	}
}

func TestOpaqueCaptureFailsClosedWithoutMigration(t *testing.T) {
	c, o, m := capturesFixture(t, 32768)
	e := executionOf(sampleID)
	original, err := c.store.Write(t.Context(), captureIdentity(e), []byte("opaque legacy record"))
	if err != nil {
		t.Fatal(err)
	}
	before := o.creates.Load()
	r, err := c.WriteCapture(t.Context(), e, captureFixture())
	if !errors.Is(err, record.ErrEnvelopeInvalid) || errors.Is(err, ErrUncertain) || errors.Is(err, ErrConflict) || r != (Receipt{}) {
		t.Fatal("opaque original reclassified as typed acceptance", err)
	}
	if r, err := c.Playback(t.Context(), e); !errors.Is(err, record.ErrEnvelopeInvalid) || !reflect.DeepEqual(r, SavedResult{}) {
		t.Fatal("opaque bytes played back", err)
	}
	if m.rows[captureIdentity(e)] != original || o.creates.Load() != before {
		t.Fatal("legacy original rewritten")
	}
}

func TestCaptureAuthorityGateAndActualStoredLimit(t *testing.T) {
	c, o, m := capturesFixture(t, 32768)
	e := executionOf(sampleID)
	c.store.active <- struct{}{}
	if _, err := c.WriteCapture(t.Context(), e, captureFixture()); !errors.Is(err, ErrBusy) {
		t.Fatal("write bypassed shared gate")
	}
	if _, err := c.Playback(t.Context(), e); !errors.Is(err, ErrBusy) {
		t.Fatal("playback bypassed shared gate")
	}
	c.store.end()
	c.store.limits.MaxRecordBytes = 8
	if _, err := c.WriteCapture(t.Context(), e, captureFixture()); !errors.Is(err, ErrTooLarge) || o.creates.Load() != 0 {
		t.Fatal("stored cap not checked before upload", err)
	}
	c.store.limits.MaxRecordBytes = 8192
	if _, err := c.WriteCapture(t.Context(), e, captureFixture()); err != nil {
		t.Fatal(err)
	}
	c.store.authorizer = writerAuth(Attribution{sampleOwner, "principal-a"})
	if _, err := c.Playback(t.Context(), e); !errors.Is(err, ErrDenied) {
		t.Fatal("writer gained implicit playback access", err)
	}
	before := o.reads.Load()
	c.store.authorizer = authorizeFunc(allow)
	m.readers = make(map[readerGrant]bool)
	if _, err := c.ReadCapture(t.Context(), e); !errors.Is(err, ErrDenied) || o.reads.Load() != before {
		t.Fatal("revoked reader touched objects", err)
	}
	bad := captureFixture()
	bad.Span = traceid.Span{55}
	if _, err := c.WriteCapture(t.Context(), e, bad); !errors.Is(err, ErrInvalid) {
		t.Fatal("payload identity ignored")
	}
}

func TestConcurrentCaptureEncodingsResolveOneOriginal(t *testing.T) {
	for _, same := range []bool{true, false} {
		_, o, m := capturesFixture(t, 32768)
		spans, err := NewSpans(authorizeFunc(allow), o, m, Limits{8192, 2}, spanCodec(t, 32768))
		if err != nil {
			t.Fatal(err)
		}
		first, err := NewCaptures(spans, captureCodec(t, 32768))
		if err != nil {
			t.Fatal(err)
		}
		second, err := NewCaptures(spans, captureCodec(t, 1))
		if err != nil {
			t.Fatal(err)
		}
		arrived := make(chan struct{}, 2)
		release := make(chan struct{})
		m.afterInspect = func() { arrived <- struct{}{}; <-release }
		var wg sync.WaitGroup
		receipts := make(chan Receipt, 2)
		errs := make(chan error, 2)
		for i, writer := range []*Captures{first, second} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				v := captureFixture()
				if i == 1 && !same {
					v.Response.Data = []byte("different")
				}
				r, e := writer.WriteCapture(t.Context(), executionOf(sampleID), v)
				receipts <- r
				errs <- e
			}()
		}
		<-arrived
		<-arrived
		close(release)
		wg.Wait()
		close(errs)
		close(receipts)
		succeeded, conflicts := 0, 0
		for err := range errs {
			if err == nil {
				succeeded++
			} else if errors.Is(err, ErrConflict) {
				conflicts++
			} else {
				t.Fatal(err)
			}
		}
		if (same && (succeeded != 2 || conflicts != 0)) || (!same && (succeeded != 1 || conflicts != 1)) {
			t.Fatal("race acceptance", same, succeeded, conflicts)
		}
		if len(m.rows) != 1 {
			t.Fatal("race created two records")
		}
		original := m.rows[captureIdentity(executionOf(sampleID))]
		accepted, duplicates := 0, 0
		for receipt := range receipts {
			if receipt == (Receipt{}) {
				continue // A refused conflicting writer receives no acceptance.
			}
			want := original
			want.Duplicate = receipt.Duplicate
			if receipt != want {
				t.Fatal("race changed the original receipt")
			}
			if receipt.Duplicate {
				duplicates++
			} else {
				accepted++
			}
		}
		if accepted != 1 || (same && duplicates != 1) || (!same && duplicates != 0) {
			t.Fatal("race receipt outcomes", accepted, duplicates)
		}
	}
}
