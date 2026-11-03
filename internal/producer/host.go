// Package producer connects trusted application span creation to ownership
// registration. It is not an exporter, receiver or agent credential broker.
package producer

import (
	"context"

	"github.com/sidsharma96/rewind/internal/ingest"
	"github.com/sidsharma96/rewind/internal/traceid"
	"go.opentelemetry.io/otel/trace"
)

// Host registers actual SDK span IDs for one configured project/run/instance.
// Its configuration and creator credentials belong outside agent control.
// It borrows Registrar, owns no goroutines and grants no submission/read rights.
type Host struct {
	registrar *ingest.Registrar
	project   string
	owner     ingest.Owner
}

func NewHost(registrar *ingest.Registrar, project string, owner ingest.Owner) (*Host, error) {
	for _, s := range []string{project, owner.Run, owner.Instance} {
		if len(s) == 0 || len(s) > 256 {
			return nil, ingest.ErrInvalid
		}
	}
	if registrar == nil {
		return nil, ingest.ErrInvalid
	}
	return &Host{registrar: registrar, project: project, owner: owner}, nil
}

// Register reads the caller's actual SpanContext without changing its IDs. Wait
// for success before delivering records. On uncertainty, retry this same context
// and owner; starting another SDK span would name a different execution. Failure
// does not stop/end the SDK span or prevent an independently configured exporter
// from attempting delivery, which must still be refused while unregistered.
func (h *Host) Register(ctx context.Context, span trace.SpanContext) (ingest.RegistrationReceipt, error) {
	if h == nil || h.registrar == nil || !span.IsValid() {
		return ingest.RegistrationReceipt{}, ingest.ErrInvalid
	}
	execution := ingest.Execution{Tenant: h.project, Trace: traceid.Trace(span.TraceID()), Span: traceid.Span(span.SpanID())}
	return h.registrar.Register(ctx, execution, h.owner)
}
