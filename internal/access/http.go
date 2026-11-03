package access

import (
	"context"
	"net/http"
	"strings"
)

// AuthenticateRequest authenticates a direct TLS server request. It accepts one
// Bearer credential, never an owner/principal header or forwarded TLS assertion.
// The server owner must bound headers, connections and request time independently;
// this method is not a receiver or a trusted reverse-proxy integration.
func (c *Credentials) AuthenticateRequest(r *http.Request) (context.Context, error) {
	if r == nil || r.TLS == nil {
		return nil, ErrUnauthenticated
	}
	values := r.Header.Values("Authorization")
	if len(values) != 1 || len(values[0]) != len("Bearer ")+64 || !strings.EqualFold(values[0][:7], "Bearer ") {
		return nil, ErrUnauthenticated
	}
	return c.Authenticate(r.Context(), values[0][7:])
}
