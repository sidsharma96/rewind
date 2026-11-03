package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sidsharma96/rewind/internal/ingest"
	"github.com/sidsharma96/rewind/internal/objectstore"
	"github.com/sidsharma96/rewind/internal/record"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// Only test deployment settings and original producer inputs cross this pipe.
// No saved receipt, object reference, accepted bytes or writer-local file does.
type recoveryConfig struct {
	DSN, Schema, ObjectURL, ProxyAddress string
	Role, Fault                          string
	Registration                         ingest.Registration
	CaptureAccepted                      bool
}

type recoveryEvent struct {
	Phase    string
	Resume   bool
	Receipts [2]ingest.Receipt
}

func recoverySend(t *testing.T, out *json.Encoder, event recoveryEvent) {
	t.Helper()
	if err := out.Encode(event); err != nil {
		t.Fatal("worker control output failed")
	}
}

type recoveryPublication struct {
	*Catalog
	fault string
	in    *json.Decoder
	out   *json.Encoder
	t     *testing.T
}

func (c *recoveryPublication) Publish(ctx context.Context, ref ingest.Reference, a ingest.Attribution) (ingest.Receipt, error) {
	if ref.Identity.Kind == ingest.CaptureRecord && c.fault == "before-publish" {
		phase := "publishing"
		if c.fault == "before-publish" {
			phase = "uploaded"
		}
		recoverySend(c.t, c.out, recoveryEvent{Phase: phase})
		var command recoveryEvent
		if err := c.in.Decode(&command); err != nil || !command.Resume || c.fault == "before-publish" {
			return ingest.Receipt{}, errors.New("publication barrier interrupted")
		}
	}
	return c.Catalog.Publish(ctx, ref, a)
}

// Lose the typed span's actual COMMIT reply after capture has already committed.
func (c *recoveryPublication) PublishSpan(ctx context.Context, ref ingest.Reference, a ingest.Attribution) (ingest.Receipt, bool, error) {
	if c.fault == "commit-reply" {
		recoverySend(c.t, c.out, recoveryEvent{Phase: "publishing"})
		var command recoveryEvent
		if err := c.in.Decode(&command); err != nil || !command.Resume {
			return ingest.Receipt{}, false, errors.New("publication barrier interrupted")
		}
	}
	return c.Catalog.PublishSpan(ctx, ref, a)
}

func recoveryEnded(t *testing.T, r ingest.Registration) record.EndedSpan {
	t.Helper()
	view := record.Span{Metadata: &tracepb.Span{TraceId: r.Execution.Trace[:], SpanId: r.Execution.Span[:], Name: "batch-worker", StartTimeUnixNano: 1, EndTimeUnixNano: 2, Status: &tracepb.Status{Code: tracepb.Status_STATUS_CODE_ERROR, Message: "synthetic failed call"}, Links: []*tracepb.Span_Link{{}}}}
	ended, err := view.DeclareEnded()
	if err != nil {
		t.Fatal(err)
	}
	return ended
}

func sameRecoveryReceipt(a, b ingest.Receipt) bool {
	return a.Reference == b.Reference && a.Attribution == b.Attribution && a.Duplicate == b.Duplicate &&
		a.AcceptedAt.Equal(b.AcceptedAt) && a.ExpiresAt.Equal(b.ExpiresAt)
}

func recoveryBodies(t *testing.T, r ingest.Registration) ([2][]byte, record.EnvelopeCodec) {
	t.Helper()
	policy := record.DefaultCompressionPolicy()
	policy.Threshold = 1
	codec, err := record.NewEnvelopeCodec(record.EnvelopeLimits{MaxStoredBytes: 4096, MaxExpandedBytes: 4096}, policy)
	if err != nil {
		t.Fatal(err)
	}
	_, span, err := nativeSpanCodec(t, 1).Prepare(recoveryEnded(t, r))
	if err != nil || span[6] != 1 {
		t.Fatal("recovery span fixture must use gzip")
	}
	// Exact synthetic capture bytes, not a selected request/response schema.
	capture, err := codec.Encode(record.CaptureObject, []byte{0, 255, 'q', '\n', 'r'})
	if err != nil || capture[6] != 0 {
		t.Fatal("recovery capture fixture must use plain")
	}
	return [2][]byte{span, capture}, codec
}

// Run only in an explicitly launched child. Normal tests do not read stdin or
// create clients here. The executable is the tracked test artifact, not the CLI.
func TestRecoveryWorker(t *testing.T) {
	if os.Getenv("REWIND_RECOVERY_WORKER") != "1" {
		return
	}
	in := json.NewDecoder(io.LimitReader(os.Stdin, 64<<10))
	out := json.NewEncoder(os.Stdout)
	var config recoveryConfig
	if err := in.Decode(&config); err != nil {
		t.Fatal("worker configuration failed")
	}
	cfg, err := pgxpool.ParseConfig(config.DSN)
	if err != nil {
		t.Fatal("worker database configuration failed")
	}
	if config.ProxyAddress != "" {
		host, port, err := net.SplitHostPort(config.ProxyAddress)
		if err != nil || !net.ParseIP(host).IsLoopback() || cfg.ConnConfig.TLSConfig != nil || len(cfg.ConnConfig.Fallbacks) != 0 {
			t.Fatal("unsafe protocol-proxy configuration")
		}
		n, err := strconv.ParseUint(port, 10, 16)
		if err != nil {
			t.Fatal("invalid protocol-proxy port")
		}
		cfg.ConnConfig.Host, cfg.ConnConfig.Port = host, uint16(n)
	}
	cfg.MaxConns = 2
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "8000"
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal("worker pool failed")
	}
	defer pool.Close()
	c, err := New(pool, config.Schema, Limits{MaxActive: 2, OperationTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{DialContext: (&net.Dialer{Timeout: 2 * time.Second}).DialContext,
		MaxConnsPerHost: 2, MaxIdleConnsPerHost: 2, ResponseHeaderTimeout: 3 * time.Second, MaxResponseHeaderBytes: 16 << 10}
	defer transport.CloseIdleConnections()
	client := s3.New(s3.Options{Region: "auto", BaseEndpoint: aws.String(config.ObjectURL), UsePathStyle: true,
		HTTPClient: &http.Client{Transport: transport, Timeout: 5 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "synthetic", SecretAccessKey: "synthetic"}, nil
		})})
	objects, err := objectstore.New(client, "test-bucket", objectstore.Limits{MaxBytes: 4096, MaxActive: 2, OperationTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	var catalog ingest.SpanCatalog = c
	if config.Role == "writer" {
		catalog = &recoveryPublication{Catalog: c, fault: config.Fault, in: in, out: out, t: t}
	}
	authorizer := publicationAuthorizer{project: config.Registration.Execution.Tenant,
		writer: ingest.Attribution{Owner: config.Registration.Owner, SubmittedBy: "collector"}}
	store, err := ingest.New(authorizer, objects, catalog, ingest.Limits{MaxRecordBytes: 4096, MaxActive: 2})
	if err != nil {
		t.Fatal(err)
	}
	spans, err := ingest.NewSpans(authorizer, objects, catalog, ingest.Limits{MaxRecordBytes: 4096, MaxActive: 2}, nativeSpanCodec(t, 1))
	if err != nil {
		t.Fatal(err)
	}
	bodies, codec := recoveryBodies(t, config.Registration)
	ids := [2]ingest.Identity{}
	for i := range ids {
		ids[i] = ingest.Identity{Tenant: config.Registration.Execution.Tenant, Trace: config.Registration.Execution.Trace,
			Span: config.Registration.Execution.Span, Kind: ingest.Kind(i)}
	}
	switch config.Role {
	case "writer":
		order := []int{0, 1}
		if config.Fault == "commit-reply" {
			order = []int{1, 0}
		}
		for _, i := range order {
			id := ids[i]
			var receipt ingest.Receipt
			var err error
			if i == 0 {
				receipt, err = spans.WriteSpan(ctx, config.Registration.Execution, recoveryEnded(t, config.Registration))
			} else {
				receipt, err = store.Write(ctx, id, bodies[i])
			}
			if i == 0 && config.Fault == "commit-reply" {
				if !errors.Is(err, ingest.ErrUncertain) || receipt != (ingest.Receipt{}) {
					t.Fatal("lost wire COMMIT response did not return uncertainty and empty receipt")
				}
				recoverySend(t, out, recoveryEvent{Phase: "uncertain"})
			} else if err != nil || receipt.Duplicate {
				t.Fatal("worker publication failed")
			}
		}
		if config.Fault != "commit-reply" {
			recoverySend(t, out, recoveryEvent{Phase: "acknowledged"})
		}
		// The parent kills this process, then waits and removes its private cwd.
		var stop recoveryEvent
		_ = in.Decode(&stop)
		t.Fatal("writer barrier ended without forced process termination")
	case "replacement":
		var receipts [2]ingest.Receipt
		for i, id := range ids {
			var got []byte
			var err error
			if i == 0 {
				view, readErr := spans.ReadSpan(ctx, config.Registration.Execution)
				if readErr != nil {
					t.Fatal("replacement typed read failed")
				}
				trace, span, idErr := view.IDs()
				if idErr != nil || trace != id.Trace || span != id.Span || view.Metadata.Name != "batch-worker" || view.Metadata.GetStatus().GetCode() != tracepb.Status_STATUS_CODE_ERROR {
					t.Fatal("typed replacement changed registered failed-span evidence")
				}
			} else {
				got, err = store.Read(ctx, id)
			}
			accepted := i == 0 || config.CaptureAccepted
			var original ingest.Receipt
			if !accepted {
				if got != nil || !errors.Is(err, ingest.ErrNotFound) {
					t.Fatal("unpublished capture object became accepted state")
				}
			} else {
				if i == 1 && (err != nil || !bytes.Equal(got, bodies[i])) {
					t.Fatal("replacement did not recover original verified bytes")
				}
				if i == 1 {
					payload, decodeErr := codec.Decode(record.CaptureObject, got)
					if decodeErr != nil || !bytes.Equal(payload, []byte{0, 255, 'q', '\n', 'r'}) {
						t.Fatal("exact capture fixture changed")
					}
				}

				original, err = c.Lookup(ctx, id, "reader")
				if err != nil {
					t.Fatal("replacement lookup failed")
				}
			}
			var retry ingest.Receipt
			if i == 0 {
				retry, err = spans.WriteSpan(ctx, config.Registration.Execution, recoveryEnded(t, config.Registration))
			} else {
				retry, err = store.Write(ctx, id, bodies[i])
			}
			if err != nil || retry.Duplicate != accepted {
				t.Fatal("replacement retry returned the wrong acceptance outcome")
			}
			if accepted {
				want := original
				want.Duplicate = true
				if !sameRecoveryReceipt(retry, want) {
					t.Fatal("retry changed original reference, attribution or times")
				}
			}
			receipts[i], err = c.Lookup(ctx, id, "reader")
			if err != nil {
				t.Fatal("replacement receipt lookup failed")
			}
		}
		changed := recoveryEnded(t, config.Registration)
		changedView, _ := changed.View()
		changedView.Metadata.Name = "different"
		if _, err := spans.WriteSpan(ctx, config.Registration.Execution, changed); !errors.Is(err, ingest.ErrConflict) {
			t.Fatal("conflicting typed metadata overwrote original")
		}
		if got, err := spans.ReadSpan(ctx, config.Registration.Execution); err != nil || got.Metadata.Name != "batch-worker" {
			t.Fatal("conflict changed accepted typed span")
		}

		recoverySend(t, out, recoveryEvent{Phase: "recovered", Receipts: receipts})
	case "denied":
		if got, err := spans.ReadSpan(ctx, config.Registration.Execution); got != (record.Span{}) || !errors.Is(err, ingest.ErrDenied) {
			t.Fatal("revoked project reader accessed saved span")
		}
		if _, err := spans.WriteSpan(ctx, config.Registration.Execution, recoveryEnded(t, config.Registration)); !errors.Is(err, ingest.ErrDenied) {
			t.Fatal("revoked typed writer retried saved span")
		}
		if got, err := store.Read(ctx, ids[1]); got != nil || !errors.Is(err, ingest.ErrDenied) {
			t.Fatal("revoked reader accessed capture")
		}
		if _, err := store.Write(ctx, ids[1], bodies[1]); !errors.Is(err, ingest.ErrDenied) {
			t.Fatal("revoked writer retried capture")
		}
		recoverySend(t, out, recoveryEvent{Phase: "denied"})
	default:
		t.Fatal("unknown recovery worker role")
	}
}
