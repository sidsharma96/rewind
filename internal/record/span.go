// Package record associates generated span metadata with its inline context.
// It does not authenticate, validate complete records, or define a saved codec.
package record

import (
	"errors"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

var (
	ErrInvalid     = errors.New("invalid span selection or missing span")
	ErrUnsupported = errors.New("unsupported span context wrapper metadata")
)

// Context carries descriptive metadata, not authenticated execution ownership.
// Its generated messages and nested contents are borrowed, not copied.
type Context struct {
	Resource          *resourcepb.Resource
	ResourceSchemaURL string
	Scope             *commonpb.InstrumentationScope
	ScopeSchemaURL    string
}

// Span associates one generated span with its inline context. This is a borrowed
// read-only view, not an immutable snapshot or a validated acceptance record.
// Callers must not mutate the reachable graph while a view is in use. Clone
// explicitly before in-place normalization if another caller retains the graph.
// Complete semantic validity, identity mapping and retry equality are separate.
type Span struct {
	Context  Context
	Metadata *tracepb.Span
}

// SpanAt selects an exact zero-based resource/scope/span path. It does not scan or
// validate siblings or accept an OTLP batch. Nil resource/scope messages are kept
// absent; a missing selected group/span is an error. Unknown fields on wrappers
// that OTLP reconstruction would replace are refused, rather than discarded.
// Descendant messages, including their unknown fields, remain borrowed intact.
// The caller must prevent concurrent mutation during selection and view use.
func SpanAt(in *tracepb.TracesData, resourceIndex, scopeIndex, spanIndex int) (Span, error) {
	if in == nil || resourceIndex < 0 || resourceIndex >= len(in.ResourceSpans) {
		return Span{}, ErrInvalid
	}
	rs := in.ResourceSpans[resourceIndex]
	if rs == nil || scopeIndex < 0 || scopeIndex >= len(rs.ScopeSpans) {
		return Span{}, ErrInvalid
	}
	ss := rs.ScopeSpans[scopeIndex]
	if ss == nil || spanIndex < 0 || spanIndex >= len(ss.Spans) || ss.Spans[spanIndex] == nil {
		return Span{}, ErrInvalid
	}
	if len(in.ProtoReflect().GetUnknown()) != 0 ||
		len(rs.ProtoReflect().GetUnknown()) != 0 ||
		len(ss.ProtoReflect().GetUnknown()) != 0 {
		return Span{}, ErrUnsupported
	}
	return Span{
		Context: Context{
			Resource:          rs.Resource,
			ResourceSchemaURL: rs.SchemaUrl,
			Scope:             ss.Scope,
			ScopeSchemaURL:    ss.SchemaUrl,
		},
		Metadata: ss.Spans[spanIndex],
	}, nil
}

// OTLP builds fresh wrapper messages/slices containing exactly this span and its
// inline context. It does not deep-copy or validate the borrowed descendant graph;
// new wrappers do not grant exclusive ownership. An encoder must still validate
// contents, including unsupported descendant fields, before producing saved bytes.
// Changing the new wrappers does not edit the source packet's grouping slices.
func (s Span) OTLP() (*tracepb.TracesData, error) {
	if s.Metadata == nil {
		return nil, ErrInvalid
	}
	return &tracepb.TracesData{ResourceSpans: []*tracepb.ResourceSpans{{
		Resource:  s.Context.Resource,
		SchemaUrl: s.Context.ResourceSchemaURL,
		ScopeSpans: []*tracepb.ScopeSpans{{
			Scope:     s.Context.Scope,
			SchemaUrl: s.Context.ScopeSchemaURL,
			Spans:     []*tracepb.Span{s.Metadata},
		}},
	}}}, nil
}
