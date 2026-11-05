package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sidsharma96/rewind/internal/access"
	"github.com/sidsharma96/rewind/internal/catalog"
	"github.com/sidsharma96/rewind/internal/httpapi"
	"github.com/sidsharma96/rewind/internal/ingest"
	"github.com/sidsharma96/rewind/internal/objectstore"
	"github.com/sidsharma96/rewind/internal/record"
)

type serverCredential struct {
	Principal string    `json:"principal_hex"`
	Digest    string    `json:"sha256"`
	Expires   time.Time `json:"expires_at"`
}
type serviceConfig struct {
	Listen           string             `json:"listen"`
	Origin           string             `json:"origin"`
	CertFile         string             `json:"tls_cert_file"`
	KeyFile          string             `json:"tls_key_file"`
	Database         string             `json:"database_url"`
	DatabaseTLS      *bool              `json:"database_tls"`
	DatabaseCA       string             `json:"database_ca_file,omitempty"`
	Schema           string             `json:"schema"`
	R2Endpoint       string             `json:"r2_endpoint"`
	R2Bucket         string             `json:"r2_bucket"`
	R2Access         string             `json:"r2_access_key"`
	R2Secret         string             `json:"r2_secret_key"`
	Credentials      []serverCredential `json:"credentials"`
	MaxConnections   int                `json:"max_connections"`
	MaxActive        int                `json:"max_active"`
	MaxRequest       int                `json:"max_request_bytes"`
	MaxResponse      int                `json:"max_response_bytes"`
	TimeoutSeconds   int                `json:"timeout_seconds"`
	SessionCount     int                `json:"session_count"`
	SessionSeconds   int                `json:"session_seconds"`
	TraceCandidates  int                `json:"trace_candidates"`
	TraceStoredBytes int                `json:"trace_stored_bytes"`
}

func (c serviceConfig) valid() bool {
	u, err := url.Parse(c.Database)
	if err != nil || u.Scheme != "postgres" && u.Scheme != "postgresql" || u.User == nil || u.User.Username() == "" || u.Host == "" || u.Path == "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || c.DatabaseTLS == nil {
		return false
	}
	if !*c.DatabaseTLS {
		ip := net.ParseIP(u.Hostname())
		if u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) {
			return false
		}
	}
	if c.MaxConnections <= 0 || c.MaxConnections > 4096 || c.MaxActive < 2 || c.MaxActive > 1024 || c.MaxRequest <= 0 || c.MaxRequest > 64<<20 || c.MaxResponse < 4096 || c.MaxResponse > 64<<20 ||
		c.TimeoutSeconds <= 0 || c.TimeoutSeconds > 300 || c.SessionCount <= 0 || c.SessionCount > 65536 || c.SessionSeconds <= 0 || c.SessionSeconds > 3600 ||
		c.TraceCandidates <= 0 || c.TraceCandidates > 1024 || c.TraceStoredBytes <= 0 || c.TraceStoredBytes > 64<<20 || len(c.Credentials) > 1024 {
		return false
	}
	return true
}
func serverCommand(ctx context.Context, name string, args []string, out io.Writer) error {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	path := flags.String("config", "", "private server configuration")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return errArguments
	}
	data, err := readPrivate(*path, 1<<20)
	if err != nil {
		return err
	}
	var cfg serviceConfig
	if decodeInput(data, &cfg) != nil || !cfg.valid() {
		return errArguments
	}
	startup, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	pool, backend, err := openServiceCatalog(startup, cfg)
	if err != nil {
		return err
	}
	defer pool.Close()
	if name == "install" {
		if err := catalog.Install(startup, pool, cfg.Schema); err != nil {
			return errors.New("catalog installation unconfirmed; do not assume rollback")
		}
		return nil
	}
	handler, cleanup, err := newService(cfg, backend)
	if err != nil {
		return err
	}
	defer cleanup()
	certPEM, err := readPublic(cfg.CertFile, 1<<20)
	if err != nil {
		return errArguments
	}
	keyPEM, err := readPrivate(cfg.KeyFile, 1<<20)
	if err != nil {
		return errArguments
	}
	certificate, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return errors.New("invalid service TLS certificate")
	}
	var protocols http.Protocols
	protocols.SetHTTP1(true)
	serverCtx, stop := context.WithCancel(ctx)
	defer stop()
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: time.Duration(cfg.TimeoutSeconds) * time.Second,
		WriteTimeout: time.Duration(cfg.TimeoutSeconds) * time.Second, IdleTimeout: 20 * time.Second, MaxHeaderBytes: 16 << 10,
		Protocols: &protocols, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate}},
		BaseContext: func(net.Listener) context.Context { return serverCtx },
		ErrorLog:    log.New(io.Discard, "", 0)}
	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return errors.New("service listener failed")
	}
	bounded, err := httpapi.LimitConnections(listener, cfg.MaxConnections)
	if err != nil {
		_ = listener.Close()
		return errArguments
	}
	defer bounded.Close()
	done := make(chan error, 1)
	go func() { done <- server.ServeTLS(bounded, "", "") }()
	if _, err := fmt.Fprintln(out, "Rewind service listening at", cfg.Origin); err != nil {
		stop()
		_ = server.Close()
		<-handler.Seal()
		<-done
		return errors.New("service status output failed")
	}
	select {
	case err = <-done:
		stop()
		drained := handler.Seal()
		_ = server.Close()
		<-drained
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return errors.New("TLS service stopped")
		}
		return nil
	case <-ctx.Done():
		stop()
		drained := handler.Seal()
		shutdown, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.TimeoutSeconds)*time.Second+3*time.Second)
		err = server.Shutdown(shutdown)
		cancel()
		if err != nil {
			_ = server.Close()
		}
		<-drained // Join operation ownership before closing borrowed backends.
		serveErr := <-done
		if err != nil || serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			return errors.New("service shutdown required forced connection close")
		}
		return nil
	}
}
func openServiceCatalog(ctx context.Context, c serviceConfig) (*pgxpool.Pool, *catalog.Catalog, error) {
	if os.Getenv("PGSERVICE") != "" || os.Getenv("PGSERVICEFILE") != "" {
		return nil, nil, errors.New("unset PGSERVICE and PGSERVICEFILE for explicit service configuration")
	}
	u, _ := url.Parse(c.Database)
	hostname := u.Hostname()
	port := u.Port()
	if port == "" {
		port = "5432"
	}
	u.Host = net.JoinHostPort(hostname, port)
	password, _ := u.User.Password()
	// Force every connection field and TLS mode explicitly during pgx parsing;
	// never discover .pgpass credentials or environment-provided TLS settings.
	u.User = url.UserPassword(u.User.Username(), "explicit-placeholder")
	u.RawQuery = "sslmode=disable"
	cfg, err := pgxpool.ParseConfig(u.String())
	if err != nil {
		return nil, nil, errArguments
	}
	cfg.ConnConfig.ValidateConnect = nil
	cfg.ConnConfig.Password = password
	cfg.ConnConfig.Fallbacks = nil
	cfg.MaxConns = int32(c.MaxActive)
	cfg.MinConns = 0
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	cfg.ConnConfig.RuntimeParams = map[string]string{"application_name": "rewind-service", "statement_timeout": "10000", "idle_in_transaction_session_timeout": "10000", "synchronous_commit": "on"}
	if *c.DatabaseTLS {
		tc := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: u.Hostname()}
		if c.DatabaseCA != "" {
			data, err := readPublic(c.DatabaseCA, 1<<20)
			if err != nil || len(data) > 1<<20 {
				return nil, nil, errArguments
			}
			roots := x509.NewCertPool()
			if !roots.AppendCertsFromPEM(data) {
				return nil, nil, errArguments
			}
			tc.RootCAs = roots
		}
		cfg.ConnConfig.TLSConfig = tc
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, nil, errors.New("catalog pool failed")
	}
	backend, err := catalog.New(pool, c.Schema, catalog.Limits{MaxActive: c.MaxActive, OperationTimeout: time.Duration(c.TimeoutSeconds) * time.Second})
	if err != nil {
		pool.Close()
		return nil, nil, errArguments
	}
	var durable bool
	err = pool.QueryRow(ctx, "SELECT current_setting('fsync')='on' AND NOT pg_is_in_recovery() AND current_setting('transaction_read_only')='off'").Scan(&durable)
	if err != nil || !durable {
		pool.Close()
		return nil, nil, errors.New("writable durable catalog unavailable")
	}
	// Schema existence is checked by normal operations; install creates only a
	// fresh schema and never migrates or drops somebody else's data.
	return pool, backend, nil
}
func newService(c serviceConfig, backend *catalog.Catalog) (*httpapi.Server, func(), error) {
	entries := make([]access.Credential, 0, len(c.Credentials))
	for _, entry := range c.Credentials {
		digest, err := hex.DecodeString(entry.Digest)
		principal, decodeErr := hex.DecodeString(entry.Principal)
		if err != nil || len(digest) != 32 || decodeErr != nil || len(principal) == 0 {
			return nil, nil, errArguments
		}
		e := access.Credential{Principal: string(principal), ExpiresAt: entry.Expires}
		copy(e.Digest[:], digest)
		entries = append(entries, e)
	}
	credentials, err := access.NewCredentials(1024, entries, time.Now)
	if err != nil {
		return nil, nil, errArguments
	}
	sessions, err := access.NewSessions(credentials, c.SessionCount, time.Duration(c.SessionSeconds)*time.Second)
	if err != nil {
		return nil, nil, errArguments
	}
	auth, err := access.NewAuthorizer(backend)
	if err != nil {
		return nil, nil, errArguments
	}
	registrar, err := ingest.NewRegistrar(auth, backend, c.MaxActive)
	if err != nil {
		return nil, nil, errArguments
	}
	endpoint, err := url.Parse(c.R2Endpoint)
	if err != nil || endpoint.Scheme != "https" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || endpoint.Path != "" ||
		!strings.HasSuffix(endpoint.Hostname(), ".r2.cloudflarestorage.com") || c.R2Access == "" || c.R2Secret == "" {
		return nil, nil, errArguments
	}
	timeout := time.Duration(c.TimeoutSeconds) * time.Second
	transport := &http.Transport{DisableCompression: true, DialContext: (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 20 * time.Second}).DialContext,
		TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: timeout, MaxResponseHeaderBytes: 16 << 10,
		MaxConnsPerHost: c.MaxActive, MaxIdleConns: c.MaxActive, MaxIdleConnsPerHost: c.MaxActive, IdleConnTimeout: 20 * time.Second}
	cleanup := transport.CloseIdleConnections
	client := &http.Client{Transport: transport, Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	sdk := s3.New(s3.Options{Region: "auto", BaseEndpoint: aws.String(c.R2Endpoint), UsePathStyle: true, HTTPClient: client,
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: c.R2Access, SecretAccessKey: c.R2Secret, Source: "explicit Rewind service configuration"}, nil
		})})
	objects, err := objectstore.New(sdk, c.R2Bucket, objectstore.Limits{MaxBytes: c.MaxRequest, MaxActive: c.MaxActive, OperationTimeout: timeout})
	if err != nil {
		cleanup()
		return nil, nil, errArguments
	}
	envelope, err := record.NewEnvelopeCodec(record.EnvelopeLimits{MaxStoredBytes: c.MaxRequest, MaxExpandedBytes: c.MaxRequest}, record.DefaultCompressionPolicy())
	if err != nil {
		cleanup()
		return nil, nil, errArguments
	}
	metadata, err := record.NewMetadataCodec(record.MetadataLimits{MaxBytes: c.MaxRequest, MaxWork: c.MaxRequest * 8, MaxDepth: 64})
	if err != nil {
		cleanup()
		return nil, nil, errArguments
	}
	spanCodec, err := record.NewSpanCodec(metadata, envelope)
	if err != nil {
		cleanup()
		return nil, nil, errArguments
	}
	spans, err := ingest.NewSpans(auth, objects, backend, ingest.Limits{MaxRecordBytes: c.MaxRequest, MaxActive: c.MaxActive}, spanCodec)
	if err != nil {
		cleanup()
		return nil, nil, errArguments
	}
	captureCodec, err := record.NewCaptureCodec(record.CaptureLimits{MaxMetadataBytes: 4096, MaxBodyBytes: c.MaxRequest}, envelope)
	if err != nil {
		cleanup()
		return nil, nil, errArguments
	}
	captures, err := ingest.NewCaptures(spans, captureCodec)
	if err != nil {
		cleanup()
		return nil, nil, errArguments
	}
	reader, err := ingest.NewTraceReader(spans, ingest.TraceLimits{MaxCandidates: c.TraceCandidates, MaxStoredBytes: c.TraceStoredBytes, OperationTimeout: timeout})
	if err != nil {
		cleanup()
		return nil, nil, errArguments
	}
	handler, err := httpapi.New(c.Origin, httpapi.Limits{MaxActive: c.MaxActive, MaxLoginActive: 1, MaxRequestBytes: c.MaxRequest, MaxResponseBytes: c.MaxResponse, Timeout: timeout}, credentials, sessions, registrar, spans, captures, reader, metadata)
	if err != nil {
		cleanup()
		return nil, nil, errArguments
	}
	return handler, cleanup, nil
}
