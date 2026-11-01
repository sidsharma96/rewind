package catalog

import (
	"bytes"
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/sidsharma96/rewind/internal/ingest"
	"github.com/sidsharma96/rewind/internal/record"
)

// The existing composed peer/controlled permissions stay unchanged. These
// synthetic metadata/capture payloads exercise runtime envelope framing, not a
// frozen kind-specific schema, typed acceptance or ingest-host replacement.
func TestNativeEnvelopeStoragePath(t *testing.T) {
	for _, threshold := range []int{1, 32768} {
		name := "gzip"
		if threshold == 32768 {
			name = "plain"
		}
		t.Run(name, func(t *testing.T) {
			f := database(t)
			writer := f.writer(t)
			peer := &publicationObjects{data: make(map[string][]byte)}
			server := httptest.NewServer(peer)
			t.Cleanup(server.Close)
			auth := publicationAuthorizer{f.r.Execution.Tenant, writer}
			lost := &lostPublicationReply{Catalog: f.c}
			lost.lose.Store(true)
			store := composedStore(t, lost, server, auth)
			policy := record.DefaultCompressionPolicy()
			policy.Threshold = threshold
			envelopes, err := record.NewEnvelopeCodec(record.EnvelopeLimits{MaxStoredBytes: 4096, MaxExpandedBytes: 4096}, policy)
			if err != nil {
				t.Fatal(err)
			}
			id := f.reference(ingest.SpanRecord, "").Identity
			payload := spanPayload(t, f)
			// Explicit mapping: ingest.SpanRecord is NOT the wire tag 1.
			body, err := envelopes.Encode(record.SpanObject, payload)
			if err != nil || (body[6] == 1) != (threshold == 1) {
				t.Fatal("fixture did not exercise selected encoding", err)
			}
			if receipt, err := store.Write(t.Context(), id, body); !errors.Is(err, ingest.ErrUncertain) || receipt != (ingest.Receipt{}) {
				t.Fatal("discarded publication reply not uncertain", err)
			}
			catalog := freshCatalog(t, f)
			fresh := composedStore(t, catalog, server, auth)
			verified, err := fresh.Read(t.Context(), id)
			if err != nil || !bytes.Equal(verified, body) {
				t.Fatal("fresh authorized read changed envelope", err)
			}
			// Store.Read has checked current permission and catalog size/digest.
			decoded, err := envelopes.Decode(record.SpanObject, verified)
			if err != nil || !bytes.Equal(decoded, payload) {
				t.Fatal("fresh envelope decode changed metadata", err)
			}
			packet, err := nativeMetadataCodec(t).Decode(decoded)
			if err != nil {
				t.Fatal(err)
			}
			view, err := record.SpanAt(packet, 0, 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			trace, span, err := view.IDs()
			if err != nil || trace != id.Trace || span != id.Span || view.Metadata.Name != "batch-worker" {
				t.Fatal("metadata lost its registered execution")
			}
			if b, err := envelopes.Decode(record.CaptureObject, verified); b != nil || !errors.Is(err, record.ErrEnvelopeKindMismatch) {
				t.Fatal("span object decoded with capture context")
			}
			original, err := catalog.Lookup(t.Context(), id, "reader")
			if err != nil {
				t.Fatal(err)
			}
			retry, err := fresh.Write(t.Context(), id, body)
			want := original
			want.Duplicate = true
			if err != nil || retry != want || peer.count() != 1 {
				t.Fatal("identical encoded retry changed original", err)
			}
			capture := id
			capture.Kind = ingest.CaptureRecord
			// Even equal logical payload bytes have different envelope-kind tags.
			captureBody, err := envelopes.Encode(record.CaptureObject, payload)
			if err != nil || bytes.Equal(captureBody, body) {
				t.Fatal("kind tag did not separate stored bytes", err)
			}
			if _, err := fresh.Write(t.Context(), capture, captureBody); err != nil || peer.count() != 2 {
				t.Fatal("independent capture failed", err)
			}
			captureRead, err := fresh.Read(t.Context(), capture)
			if err != nil {
				t.Fatal(err)
			}
			if got, err := envelopes.Decode(record.CaptureObject, captureRead); err != nil || !bytes.Equal(got, payload) {
				t.Fatal("capture envelope changed exact fixture bytes", err)
			}
			changed, err := envelopes.Encode(record.SpanObject, append(bytes.Clone(payload), ' '))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := fresh.Write(t.Context(), id, changed); !errors.Is(err, ingest.ErrConflict) {
				t.Fatal("changed encoded contents overwrote original")
			}
			preserved, err := fresh.Read(t.Context(), id)
			if err != nil || !bytes.Equal(preserved, body) || peer.count() != 3 {
				t.Fatal("conflict lost original or discarded orphan", err)
			}
			t.Log("RWND: verified fresh read -> expected-kind check -> original payload; retry and independent capture preserved")
		})
	}
}
