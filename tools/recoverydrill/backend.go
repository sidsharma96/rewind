package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sidsharma96/rewind/internal/access"
	"github.com/sidsharma96/rewind/internal/catalog"
	"github.com/sidsharma96/rewind/internal/ingest"
	"github.com/sidsharma96/rewind/internal/objectstore"
	"github.com/sidsharma96/rewind/internal/record"
)

func openCatalog(ctx context.Context, d deployment) (*pgxpool.Pool, *catalog.Catalog, error) {
	if !d.valid() {
		return nil, nil, errConfig
	}
	cfg, err := pgxpool.ParseConfig(d.DSN)
	if err != nil || cfg.ConnConfig.TLSConfig != nil || len(cfg.ConnConfig.Fallbacks) != 0 {
		return nil, nil, errConfig
	}
	// The URL supplies every identity/address/password field; replace inherited
	// runtime settings so PGOPTIONS cannot change this drill's operation policy.
	cfg.MaxConns, cfg.MinConns = 2, 0
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	cfg.ConnConfig.RuntimeParams = map[string]string{"application_name": "rewind-recovery-drill", "statement_timeout": "10000", "idle_in_transaction_session_timeout": "10000", "synchronous_commit": "on"}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, nil, errors.New("recoverydrill: database pool failed")
	}
	if err := guardDatabase(ctx, pool); err != nil {
		pool.Close()
		return nil, nil, err
	}
	c, err := catalog.New(pool, d.Schema, catalog.Limits{MaxActive: 2, OperationTimeout: 10 * time.Second})
	if err != nil {
		pool.Close()
		return nil, nil, errors.New("recoverydrill: catalog configuration failed")
	}
	return pool, c, nil
}

func guardDatabase(ctx context.Context, pool *pgxpool.Pool) error {
	var database, user string
	var durable bool
	err := pool.QueryRow(ctx, "SELECT current_database(),current_user,current_setting('fsync')='on' AND NOT pg_is_in_recovery() AND current_setting('transaction_read_only')='off'").Scan(&database, &user, &durable)
	if err != nil {
		return errors.New("recoverydrill: database identity check failed")
	}
	if database != "rewind" || user != "rewind_drill" || !durable {
		return errors.New("recoverydrill: dedicated writable durable Rewind database required")
	}
	return nil
}

func setup(ctx context.Context, cfg setupConfig, pool *pgxpool.Pool) error {
	if !cfg.valid() {
		return errConfig
	}
	if err := guardDatabase(ctx, pool); err != nil {
		return err
	}
	if err := catalog.Install(ctx, pool, cfg.Schema); err != nil {
		return errors.New("recoverydrill: fresh schema installation failed")
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return errors.New("recoverydrill: setup transaction failed")
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	table := func(name string) string { return pgx.Identifier{cfg.Schema, name}.Sanitize() }
	if _, err = tx.Exec(ctx, "INSERT INTO "+table("projects")+"(id) VALUES($1)", []byte(cfg.Project)); err == nil {
		_, err = tx.Exec(ctx, "SELECT id FROM "+table("projects")+" WHERE id=$1 FOR UPDATE", []byte(cfg.Project))
	}
	if err == nil {
		_, err = tx.Exec(ctx, "INSERT INTO "+table("runs")+"(project,run_id,phase) VALUES($1,$2,'running')", []byte(cfg.Project), []byte("run"))
	}
	for _, instance := range []string{"alice", "bob"} {
		if err == nil {
			_, err = tx.Exec(ctx, "INSERT INTO "+table("host_creator_grants")+" VALUES($1,$2,$3,$4)", []byte(cfg.Project), []byte("run"), []byte(instance), []byte("host"))
		}
		if err == nil {
			_, err = tx.Exec(ctx, "INSERT INTO "+table("writer_grants")+" VALUES($1,$2,$3,$4)", []byte(cfg.Project), []byte("run"), []byte(instance), []byte(instance))
		}
	}
	if err == nil {
		_, err = tx.Exec(ctx, "INSERT INTO "+table("writer_grants")+" VALUES($1,$2,$3,$4)", []byte(cfg.Project), []byte("run"), []byte("alice"), []byte("collector"))
	}
	if err == nil {
		_, err = tx.Exec(ctx, "INSERT INTO "+table("reader_grants")+" VALUES($1,$2)", []byte(cfg.Project), []byte("reader"))
	}
	if err != nil {
		return errors.New("recoverydrill: fixture grant setup failed")
	}
	if err = tx.Commit(ctx); err != nil {
		return errors.New("recoverydrill: setup confirmation unavailable; do not assume rollback")
	}
	return nil
}

type objectMethods struct {
	Creates uint64 `json:"creates"`
	Reads   uint64 `json:"reads"`
}

type countedObjects struct {
	objects     ingest.Objects
	creates     atomic.Uint64
	reads       atomic.Uint64
	blockCreate atomic.Bool
}

func (o *countedObjects) Create(ctx context.Context, id ingest.ObjectID, data []byte) error {
	o.creates.Add(1)
	if o.blockCreate.Load() {
		return errors.New("recoverydrill: retry attempted object creation")
	}
	return o.objects.Create(ctx, id, data)
}

func (o *countedObjects) ReadExact(ctx context.Context, id ingest.ObjectID, size int) ([]byte, error) {
	o.reads.Add(1)
	return o.objects.ReadExact(ctx, id, size)
}

func (o *countedObjects) snapshot() objectMethods {
	return objectMethods{Creates: o.creates.Load(), Reads: o.reads.Load()}
}

type session struct {
	credentials *access.Credentials
	registrar   *ingest.Registrar
	spans       *ingest.Spans
	captures    *ingest.Captures
	reader      *ingest.TraceReader
	objects     *countedObjects
}

func newSession(c *catalog.Catalog, objects ingest.Objects, entries []credential, threshold int) (*session, error) {
	credentials, err := verifier(entries)
	if err != nil {
		return nil, err
	}
	auth, err := access.NewAuthorizer(c)
	if err != nil {
		return nil, errConfig
	}
	registrar, err := ingest.NewRegistrar(auth, c, 2)
	if err != nil {
		return nil, errConfig
	}
	policy := record.DefaultCompressionPolicy()
	policy.Threshold = threshold
	envelope, err := record.NewEnvelopeCodec(record.EnvelopeLimits{MaxStoredBytes: 4096, MaxExpandedBytes: 4096}, policy)
	if err != nil {
		return nil, errConfig
	}
	metadata, err := record.NewMetadataCodec(record.MetadataLimits{MaxBytes: 4096, MaxWork: 8192, MaxDepth: 32})
	if err != nil {
		return nil, errConfig
	}
	spanCodec, err := record.NewSpanCodec(metadata, envelope)
	if err != nil {
		return nil, errConfig
	}
	counted := &countedObjects{objects: objects}
	spans, err := ingest.NewSpans(auth, counted, c, ingest.Limits{MaxRecordBytes: 4096, MaxActive: 2}, spanCodec)
	if err != nil {
		return nil, errConfig
	}
	captureCodec, err := record.NewCaptureCodec(record.CaptureLimits{MaxMetadataBytes: 512, MaxBodyBytes: 1024}, envelope)
	if err != nil {
		return nil, errConfig
	}
	captures, err := ingest.NewCaptures(spans, captureCodec)
	if err != nil {
		return nil, errConfig
	}
	reader, err := ingest.NewTraceReader(spans, ingest.TraceLimits{MaxCandidates: 1, MaxStoredBytes: 4096, OperationTimeout: 30 * time.Second})
	if err != nil {
		return nil, errConfig
	}
	return &session{credentials, registrar, spans, captures, reader, counted}, nil
}

func liveSession(c *catalog.Catalog, d deployment, entries []credential, threshold int) (*session, func(), error) {
	if !d.valid() {
		return nil, nil, errConfig
	}
	transport := &http.Transport{DialContext: (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 20 * time.Second}).DialContext,
		ForceAttemptHTTP2: true, MaxIdleConns: 2, MaxIdleConnsPerHost: 2, MaxConnsPerHost: 2,
		IdleConnTimeout: 20 * time.Second, TLSHandshakeTimeout: 5 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second, MaxResponseHeaderBytes: 16 << 10}
	client := &http.Client{Transport: transport, Timeout: 20 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	s3client := s3.New(s3.Options{Region: "auto", BaseEndpoint: aws.String(d.R2Endpoint), UsePathStyle: true, HTTPClient: client,
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: d.R2AccessKey, SecretAccessKey: d.R2SecretKey, Source: "explicit rewind-dev recovery drill"}, nil
		})})
	objects, err := objectstore.New(s3client, "rewind-dev", objectstore.Limits{MaxBytes: 4096, MaxActive: 2, OperationTimeout: 20 * time.Second})
	if err != nil {
		transport.CloseIdleConnections()
		return nil, nil, errConfig
	}
	s, err := newSession(c, objects, entries, threshold)
	if err != nil {
		transport.CloseIdleConnections()
		return nil, nil, err
	}
	return s, transport.CloseIdleConnections, nil
}
