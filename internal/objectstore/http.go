package objectstore

import (
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"io"
	"net/http"
)

const controlBodyLimit int64 = 64 << 10

// Cap bodies before SDK error/control decoders can buffer them. The client's
// owner must separately bound headers, connections, TLS and transport buffers.
type limitedClient struct {
	next         s3.HTTPClient
	successLimit int64
}

func (c limitedClient) Do(r *http.Request) (*http.Response, error) {
	response, err := c.next.Do(r)
	if err != nil || response == nil || response.Body == nil {
		return response, err
	}
	limit := controlBodyLimit
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		limit = c.successLimit
	}
	response.Body = &limitedBody{next: response.Body, remaining: limit}
	return response, nil
}

type limitedBody struct {
	next      io.ReadCloser
	remaining int64
	exceeded  bool
}

func (b *limitedBody) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if b.exceeded {
		return 0, ErrResponseLimit
	}
	if b.remaining == 0 {
		var probe [1]byte
		n, err := b.next.Read(probe[:])
		if n > 0 {
			b.exceeded = true
			return 0, ErrResponseLimit
		}
		return 0, err
	}
	if int64(len(p)) > b.remaining {
		p = p[:int(b.remaining)]
	}
	n, err := b.next.Read(p)
	b.remaining -= int64(n)
	return n, err
}
func (b *limitedBody) Close() error { return b.next.Close() }
