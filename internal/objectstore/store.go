// Package objectstore implements ingest.Objects using immutable, tenant-scoped
// S3 objects. Authentication and catalog acceptance belong to the caller.
package objectstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/logging"
	"github.com/sidsharma96/rewind/internal/ingest"
)

var (
	ErrUncertain     = errors.New("object create outcome unresolved; retry the same identity and bytes")
	ErrResponseLimit = errors.New("object response exceeds control byte limit")
)

// Limits bounds concurrent operations without waiting, each object's bytes and
// each whole operation's time. Transport buffers and retained results are separate.
type Limits struct {
	MaxBytes         int
	MaxActive        int
	OperationTimeout time.Duration
}

// Store borrows a concurrency-safe SDK client. Its owner supplies a trusted R2
// endpoint, credentials, region and bounded HTTP transport, and owns connections.
// No credential discovery, worker, deletion or catalog publication occurs here.
type Store struct {
	client *s3.Client
	bucket string
	limits Limits
	active chan struct{}
}

var _ ingest.Objects = (*Store)(nil)

func New(client *s3.Client, bucket string, limits Limits) (*Store, error) {
	if client == nil || len(bucket) < 3 || len(bucket) > 63 || limits.MaxBytes <= 0 || limits.MaxBytes == int(^uint(0)>>1) || limits.MaxActive <= 0 || limits.OperationTimeout <= 0 {
		return nil, ingest.ErrInvalid
	}
	// Bucket is trusted configuration, but may not introduce path/endpoint syntax.
	for _, c := range bucket {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '.') {
			return nil, ingest.ErrInvalid
		}
	}
	if strings.HasPrefix(bucket, ".") || strings.HasPrefix(bucket, "-") || strings.HasSuffix(bucket, ".") || strings.HasSuffix(bucket, "-") {
		return nil, ingest.ErrInvalid
	}
	return &Store{client: client, bucket: bucket, limits: limits, active: make(chan struct{}, limits.MaxActive)}, nil
}

func key(id ingest.ObjectID) (string, error) {
	if len(id.Tenant) == 0 || len(id.Tenant) > 256 {
		return "", ingest.ErrInvalid
	}
	return "v1/tenants/" + hex.EncodeToString([]byte(id.Tenant)) + "/sha256/" + hex.EncodeToString(id.Digest[:]), nil
}

func (s *Store) admit(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	select {
	case s.active <- struct{}{}:
		bounded, cancel := context.WithTimeout(ctx, s.limits.OperationTimeout)
		return bounded, func() { cancel(); <-s.active }, nil
	default:
		return nil, nil, ingest.ErrBusy
	}
}

// Create uses at most GET/conditional PUT/GET with SDK retries disabled. Success
// requires exact bytes, not just a successful PUT response. An uncertain outcome
// must never cause deletion; repeating the same identity/bytes is safe. Input
// remains caller-owned and must not be mutated until return; it is not retained.
func (s *Store) Create(ctx context.Context, id ingest.ObjectID, contents []byte) error {
	k, err := key(id)
	if err != nil {
		return err
	}
	if len(contents) > s.limits.MaxBytes {
		return ingest.ErrTooLarge
	}
	ctx, release, err := s.admit(ctx)
	if err != nil {
		return err
	}
	defer release()
	if sha256.Sum256(contents) != id.Digest {
		return ingest.ErrIntegrity
	}
	stored, err := s.readExact(ctx, id, k, len(contents))
	if err == nil {
		if !bytes.Equal(stored, contents) {
			return ingest.ErrIntegrity
		}
		return nil
	}
	if !errors.Is(err, ingest.ErrObjectMissing) {
		return err
	}
	_, putErr := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(k), Body: bytes.NewReader(contents),
		ContentLength: aws.Int64(int64(len(contents))), ContentType: aws.String("application/octet-stream"), IfNoneMatch: aws.String("*"),
	}, attemptOptions(controlBodyLimit))
	stored, err = s.readExact(ctx, id, k, len(contents))
	if err != nil {
		if errors.Is(err, ingest.ErrIntegrity) {
			return errors.Join(err, provider("create", putErr))
		}
		// Do not leak Read's absence classification after attempting a write. A
		// concurrent/in-flight PUT can still complete after that negative observation.
		var cause *providerError
		if errors.Is(err, ingest.ErrObjectMissing) && errors.As(err, &cause) {
			err = cause
		}
		return errors.Join(ErrUncertain, provider("create", putErr), err)
	}
	if !bytes.Equal(stored, contents) {
		return ingest.ErrIntegrity
	}
	return nil
}

// ReadExact returns owned bytes only after checking their exact stored length and
// digest. expectedBytes is not a maximum or an expanded-payload length. The
// independent configured byte cap is checked before admission or object I/O.
func (s *Store) ReadExact(ctx context.Context, id ingest.ObjectID, expectedBytes int) ([]byte, error) {
	k, err := key(id)
	if err != nil {
		return nil, err
	}
	if expectedBytes < 0 {
		return nil, ingest.ErrInvalid
	}
	if expectedBytes > s.limits.MaxBytes {
		return nil, ingest.ErrTooLarge
	}
	ctx, release, err := s.admit(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return s.readExact(ctx, id, k, expectedBytes)
}

func (s *Store) readExact(ctx context.Context, id ingest.ObjectID, k string, expectedBytes int) (data []byte, resultErr error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(k)}, attemptOptions(int64(expectedBytes)+1))
	if err != nil {
		var api smithy.APIError
		var status interface{ HTTPStatusCode() int }
		if errors.As(err, &api) && api.ErrorCode() == "NoSuchKey" && errors.As(err, &status) && status.HTTPStatusCode() == 404 {
			return nil, errors.Join(ingest.ErrObjectMissing, provider("read", err))
		}
		return nil, provider("read", err)
	}
	if out == nil || out.Body == nil {
		return nil, ingest.ErrIntegrity
	}
	defer func() {
		if err := out.Body.Close(); err != nil {
			data = nil
			resultErr = errors.Join(resultErr, provider("close read", err))
		}
	}()
	if out.ContentLength != nil && *out.ContentLength != int64(expectedBytes) {
		return nil, ingest.ErrIntegrity
	}
	data, err = io.ReadAll(io.LimitReader(out.Body, int64(expectedBytes)+1))
	if err != nil {
		return nil, provider("read body", err)
	}
	if len(data) != expectedBytes || sha256.Sum256(data) != id.Digest {
		return nil, ingest.ErrIntegrity
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return data, nil
}

func attemptOptions(successLimit int64) func(*s3.Options) {
	return func(o *s3.Options) {
		o.Retryer = aws.NopRetryer{}
		o.RetryMaxAttempts = 1
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		o.ClientLogMode = 0
		o.Logger = logging.Nop{}
		o.HTTPClient = limitedClient{o.HTTPClient, successLimit}
	}
}

// Provider text may contain secrets or captured data; normal formatting omits it.
// Causes remain available for inspection and must not be logged indiscriminately.
type providerError struct {
	operation string
	cause     error
}

func (e *providerError) Error() string { return "object store: " + e.operation + " failed" }
func (e *providerError) Unwrap() error { return e.cause }
func provider(op string, err error) error {
	if err == nil {
		return nil
	}
	return &providerError{op, err}
}
