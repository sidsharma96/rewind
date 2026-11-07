package httpapi

import (
	"github.com/sidsharma96/rewind/internal/ingest"
	"github.com/sidsharma96/rewind/internal/traceid"
	"net/url"
)

func inspectionURL(path string, e ingest.Execution, format string) string {
	result := viewURL(path, e)
	if format != "" {
		result += "&format=" + url.QueryEscape(format)
	}
	return result
}

// Only internal read pages may survive a sign-in. Never accept an arbitrary
// relative path, authority, fragment, download or authentication endpoint.
func loginDestination(raw string) (string, error) {
	if raw == "" || raw == "/" {
		return "/", nil
	}
	if len(raw) > 1536 {
		return "", ErrProtocol
	}
	u, err := url.Parse(raw)
	if err != nil || u.IsAbs() || u.Host != "" || u.User != nil || u.Fragment != "" || u.Opaque != "" || u.RawPath != "" || (u.Path != "/view" && u.Path != "/view/capture") {
		return "", ErrProtocol
	}
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil || len(q) > 4 {
		return "", ErrProtocol
	}
	for key, values := range q {
		if len(values) != 1 || (key != "project" && key != "trace" && key != "span" && key != "cursor" && key != "format") {
			return "", ErrProtocol
		}
	}
	if _, err = parseOpaque(q.Get("project")); err != nil {
		return "", err
	}
	if _, err = traceid.ParseTrace(q.Get("trace")); err != nil {
		return "", ErrProtocol
	}
	if !supportedViewFormat(q.Get("format")) {
		return "", ErrProtocol
	}
	if u.Path == "/view/capture" {
		if q.Has("cursor") {
			return "", ErrProtocol
		}
		if _, err = traceid.ParseSpan(q.Get("span")); err != nil {
			return "", ErrProtocol
		}
	} else if q.Has("span") {
		return "", ErrProtocol
	}
	return u.Path + "?" + q.Encode(), nil
}

func supportedViewFormat(format string) bool {
	return format == "" || format == "command-v1" || format == "claude-stream-v1"
}

// TraceViewURL returns a token-free browser locator using this validated origin.
// It requests only a display format, never a write operation or permission.
func (c *Client) TraceViewURL(project, trace string) (string, error) {
	tenant, err := parseOpaque(project)
	if err != nil {
		return "", err
	}
	id, err := traceid.ParseTrace(trace)
	if err != nil {
		return "", err
	}
	return c.base + inspectionURL("/view", ingest.Execution{Tenant: tenant, Trace: id}, "command-v1"), nil
}
