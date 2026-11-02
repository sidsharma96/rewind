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
	if ref.Identity.Kind == ingest.CaptureRecord && (c.fault == "before-publish" || c.fault == "commit-reply") {
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
	span, err := codec.Encode(record.SpanObject, spanPayload(t, fixture{r: r}))
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
	var catalog ingest.Catalog = c
	if config.Role == "writer" {
		catalog = &recoveryPublication{Catalog: c, fault: config.Fault, in: in, out: out, t: t}
	}
	authorizer := publicationAuthorizer{project: config.Registration.Execution.Tenant,
		writer: ingest.Attribution{Owner: config.Registration.Owner, SubmittedBy: "collector"}}
	store, err := ingest.New(authorizer, objects, catalog, ingest.Limits{MaxRecordBytes: 4096, MaxActive: 2})
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
		for i, id := range ids {
			receipt, err := store.Write(ctx, id, bodies[i])
			if i == 1 && config.Fault == "commit-reply" {
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
			got, err := store.Read(ctx, id)
			accepted := i == 0 || config.CaptureAccepted
			var original ingest.Receipt
			if !accepted {
				if got != nil || !errors.Is(err, ingest.ErrNotFound) {
					t.Fatal("unpublished capture object became accepted state")
				}
			} else {
				if err != nil || !bytes.Equal(got, bodies[i]) {
					t.Fatal("replacement did not recover original verified bytes")
				}
				kind := record.SpanObject
				if i == 1 {
					kind = record.CaptureObject
				}
				payload, err := codec.Decode(kind, got)
				if err != nil {
					t.Fatal("replacement envelope failed")
				}
				if i == 1 {
					if !bytes.Equal(payload, []byte{0, 255, 'q', '\n', 'r'}) {
						t.Fatal("exact capture fixture changed")
					}
				} else {
					packet, err := nativeMetadataCodec(t).Decode(payload)
					if err != nil {
						t.Fatal("replacement metadata failed")
					}
					view, err := record.SpanAt(packet, 0, 0, 0)
					if err != nil {
						t.Fatal(err)
					}
					trace, span, err := view.IDs()
					if err != nil || trace != id.Trace || span != id.Span || view.Metadata.Name != "batch-worker" || view.Metadata.GetStatus().GetCode() != tracepb.Status_STATUS_CODE_ERROR {
						t.Fatal("registered failed-span evidence changed")
					}
				}
				original, err = c.Lookup(ctx, id, "reader")
				if err != nil {
					t.Fatal("replacement lookup failed")
				}
			}
			retry, err := store.Write(ctx, id, bodies[i])
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
		changed := append(bytes.Clone(bodies[0]), 0)
		if _, err := store.Write(ctx, ids[0], changed); !errors.Is(err, ingest.ErrConflict) {
			t.Fatal("conflicting opaque bytes overwrote original span")
		}
		if got, err := store.Read(ctx, ids[0]); err != nil || !bytes.Equal(got, bodies[0]) {
			t.Fatal("conflict changed accepted span")
		}
		recoverySend(t, out, recoveryEvent{Phase: "recovered", Receipts: receipts})
	case "denied":
		for i, id := range ids {
			if got, err := store.Read(ctx, id); got != nil || !errors.Is(err, ingest.ErrDenied) {
				t.Fatal("revoked project reader accessed saved record")
			}
			if _, err := store.Write(ctx, id, bodies[i]); !errors.Is(err, ingest.ErrDenied) {
				t.Fatal("revoked writer retried accepted record")
			}
		}
		recoverySend(t, out, recoveryEvent{Phase: "denied"})
	default:
		t.Fatal("unknown recovery worker role")
	}
}
