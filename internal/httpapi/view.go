package httpapi

import (
	"crypto/subtle"
	_ "embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"strconv"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/sidsharma96/rewind/internal/access"
	"github.com/sidsharma96/rewind/internal/ingest"
	"github.com/sidsharma96/rewind/internal/record"
	"github.com/sidsharma96/rewind/internal/traceid"
)

const sessionCookie = "__Host-rewind_session"
const challengeCookie = "__Host-rewind_form"

type relationship struct{ Label, URL string }
type row struct {
	ID, Name, Owner, Status, Timing, State, URL string
	Metadata                                    json.RawMessage
	Relationships                               []relationship
}
type preview struct{ State, Text, Encoding string }
type viewData struct {
	TracePage                                         bool
	Login                                             bool
	Error, CSRF, Project, Trace, Title, Next, Refresh string
	Rows                                              []row
	Capture                                           *Capture
	Request, Response                                 preview
	Download                                          string
}

func cookie(name, value string, expires time.Time) *http.Cookie {
	return &http.Cookie{Name: name, Value: value, Path: "/", Secure: true, HttpOnly: true, SameSite: http.SameSiteStrictMode, Expires: expires}
}
func (s *Server) challenge(w http.ResponseWriter, r *http.Request) (string, error) {
	for _, c := range r.CookiesNamed(challengeCookie) {
		if len(r.CookiesNamed(challengeCookie)) == 1 && len(c.Value) == 64 {
			if raw, err := hex.DecodeString(c.Value); err == nil && hex.EncodeToString(raw) == c.Value {
				http.SetCookie(w, cookie(challengeCookie, c.Value, time.Now().Add(time.Hour)))
				return c.Value, nil
			}
		}
	}
	token, _, err := access.NewToken()
	if err != nil {
		return "", err
	}
	http.SetCookie(w, cookie(challengeCookie, token, time.Now().Add(time.Hour)))
	return token, nil
}
func (s *Server) form(w http.ResponseWriter, r *http.Request) error {
	if r.Header.Get("Origin") != s.origin || r.Header.Get("Content-Encoding") != "" {
		return ErrProtocol
	}
	typ := r.Header.Get("Content-Type")
	if typ != "application/x-www-form-urlencoded" {
		return ErrProtocol
	}
	r.Body = http.MaxBytesReader(w, r.Body, int64(min(s.limits.MaxRequestBytes, 8192)))
	if r.ParseForm() != nil || len(r.URL.RawQuery) != 0 {
		return ErrProtocol
	}
	csrf := r.PostForm["csrf"]
	cookies := r.CookiesNamed(challengeCookie)
	if len(csrf) != 1 || len(cookies) != 1 || len(csrf[0]) != 64 || subtle.ConstantTimeCompare([]byte(csrf[0]), []byte(cookies[0].Value)) != 1 {
		return ErrProtocol
	}
	return nil
}
func (s *Server) view(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/style.css" && r.Method == http.MethodGet {
		if len(styles) > s.limits.MaxResponseBytes {
			s.fail(w, r, ingest.ErrTooLarge, false)
			return
		}
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
		_, _ = w.Write([]byte(styles))
		return
	}
	if r.URL.Path == "/login" {
		switch r.Method {
		case http.MethodGet:
			if r.URL.RawQuery != "" {
				s.fail(w, r, ErrProtocol, false)
				return
			}
			token, err := s.challenge(w, r)
			if err != nil {
				s.fail(w, r, err, false)
				return
			}
			s.render(w, r, viewData{Login: true, Title: "Sign in", CSRF: token})
		case http.MethodPost:
			if s.form(w, r) != nil || len(r.PostForm) != 2 || len(r.PostForm["credential"]) != 1 {
				s.fail(w, r, ErrProtocol, false)
				return
			}
			id, expires, err := s.sessions.Login(r.Context(), r.PostForm.Get("credential"))
			if err != nil {
				s.fail(w, r, err, false)
				return
			}
			if old := r.CookiesNamed(sessionCookie); len(old) == 1 {
				s.sessions.Logout(old[0].Value)
			}
			http.SetCookie(w, cookie(sessionCookie, id, expires))
			http.SetCookie(w, &http.Cookie{Name: challengeCookie, Value: "", Path: "/", Secure: true, HttpOnly: true, SameSite: http.SameSiteStrictMode, MaxAge: -1})
			http.Redirect(w, r, "/", http.StatusSeeOther)
		default:
			s.fail(w, r, ErrProtocol, false)
		}
		return
	}
	if r.URL.Path == "/logout" {
		if r.Method != http.MethodPost || s.form(w, r) != nil || len(r.PostForm) != 1 {
			s.fail(w, r, ErrProtocol, false)
			return
		}
		if cookies := r.CookiesNamed(sessionCookie); len(cookies) == 1 {
			s.sessions.Logout(cookies[0].Value)
		}
		http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: "", Path: "/", Secure: true, HttpOnly: true, SameSite: http.SameSiteStrictMode, MaxAge: -1})
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	if r.Method != http.MethodGet || (r.URL.Path != "/" && r.URL.Path != "/view" && r.URL.Path != "/view/capture" && r.URL.Path != "/view/response") || r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
		s.failure(w, r, 404, "not_found", "refused")
		return
	}
	ctx, err := s.authenticate(r, false)
	if err != nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	r = r.WithContext(ctx)
	challenge, err := s.challenge(w, r)
	if err != nil {
		s.fail(w, r, err, false)
		return
	}
	if r.URL.Path == "/" {
		if r.URL.RawQuery != "" {
			s.fail(w, r, ErrProtocol, false)
			return
		}
		s.render(w, r, viewData{Title: "Inspect a shared trace", CSRF: challenge})
		return
	}
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil || len(query) > 4 {
		s.fail(w, r, ErrProtocol, false)
		return
	}
	for key, values := range query {
		if len(values) != 1 || key != "project" && key != "trace" && key != "span" && key != "cursor" {
			s.fail(w, r, ErrProtocol, false)
			return
		}
	}
	project, err := parseOpaque(query.Get("project"))
	if err != nil {
		s.fail(w, r, err, false)
		return
	}
	trace, err := traceid.ParseTrace(query.Get("trace"))
	if err != nil {
		s.fail(w, r, ErrProtocol, false)
		return
	}
	e := ingest.Execution{Tenant: project, Trace: trace}
	data := viewData{Title: "Shared trace", CSRF: challenge, Project: Opaque(project), Trace: trace.String(), Refresh: viewURL("/view", e)}
	switch r.URL.Path {
	case "/view":
		data.TracePage = true
		if query.Has("span") {
			s.fail(w, r, ErrProtocol, false)
			return
		}
		page, err := s.readTrace(ctx, e, query.Get("cursor"))
		if err != nil {
			s.fail(w, r, err, false)
			return
		}
		if page.Next != "" {
			data.Next = viewURL("/view", e) + "&cursor=" + url.QueryEscape(page.Next)
		}
		for _, item := range page.Items {
			itemExecution := e
			itemExecution.Span, _ = traceid.ParseSpan(item.Span)
			v := row{ID: item.Span, State: item.State, Name: "No final span", Owner: "Unavailable", Status: "Unavailable", Timing: "Unavailable", URL: viewURL("/view/capture", itemExecution)}
			if item.State == "available" {
				packet, err := s.metadata.Decode(item.Metadata)
				if err != nil {
					s.fail(w, r, err, false)
					return
				}
				span, err := record.SpanAt(packet, 0, 0, 0)
				if err != nil {
					s.fail(w, r, err, false)
					return
				}
				v.Name = span.Metadata.Name
				if v.Name == "" {
					v.Name = "Unnamed operation (unknown name)"
				}
				v.Metadata = item.Metadata
				v.Owner = displayOpaque(item.Receipt.Instance) + " · run " + displayOpaque(item.Receipt.Run)
				v.Status = "Unset"
				if span.Metadata.Status != nil {
					switch int32(span.Metadata.Status.Code) {
					case 0:
						v.Status = "Unset"
					case 1:
						v.Status = "OK"
					case 2:
						v.Status = "Error"
					default:
						v.Status = "Unknown status " + strconv.FormatInt(int64(span.Metadata.Status.Code), 10)
					}
				}
				if nanos, ok := span.DurationNanos(); ok {
					v.Timing = strconv.FormatUint(nanos, 10) + " ns (reported)"
				} else {
					v.Timing = "Unknown duration / check reported timestamps"
				}
				if parent, err := traceid.SpanFromBytes(span.Metadata.ParentSpanId); err == nil {
					target := e
					target.Span = parent
					v.Relationships = append(v.Relationships, relationship{"Reported parent " + parent.String() + " (target may be outside this page)", viewURL("/view/capture", target)})
				}
				for _, link := range span.Metadata.Links {
					target, usable, err := record.TargetOfLink(link)
					if err != nil {
						s.fail(w, r, err, false)
						return
					}
					if !usable {
						v.Relationships = append(v.Relationships, relationship{Label: "Link without a usable target"})
						continue
					}
					other := ingest.Execution{Tenant: e.Tenant, Trace: target.Trace, Span: target.Span}
					v.Relationships = append(v.Relationships, relationship{"Reported link " + target.Trace.String() + "/" + target.Span.String() + " (not proof of existence)", viewURL("/view/capture", other)})
				}
				kind := int32(span.Metadata.Kind)
				if kind < 0 || kind > 5 {
					v.Status += "; unknown span kind " + strconv.FormatInt(int64(kind), 10)
				}
			}
			data.Rows = append(data.Rows, v)
		}
	case "/view/capture", "/view/response":
		if query.Has("cursor") {
			s.fail(w, r, ErrProtocol, false)
			return
		}
		e.Span, err = traceid.ParseSpan(query.Get("span"))
		if err != nil {
			s.fail(w, r, ErrProtocol, false)
			return
		}
		if r.URL.Path == "/view/response" {
			result, err := s.readResult(ctx, e)
			if err != nil {
				s.fail(w, r, err, false)
				return
			}
			bytes, err := base64.StdEncoding.Strict().DecodeString(result.Data)
			if err != nil {
				s.fail(w, r, err, false)
				return
			}
			if len(bytes) > s.limits.MaxResponseBytes {
				s.fail(w, r, ingest.ErrTooLarge, false)
				return
			}
			receiptJSON, _ := json.Marshal(result.Receipt)
			w.Header().Set("Rewind-Receipt", base64.RawURLEncoding.EncodeToString(receiptJSON))
			w.Header().Set("Rewind-Request-State", string(result.RequestState))
			w.Header().Set("Rewind-Response-State", "present")
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"rewind-%s-response.bin\"", e.Span.String()))
			_, _ = w.Write(bytes)
			return
		}
		capture, err := s.readCapture(ctx, e)
		if err != nil {
			s.fail(w, r, err, false)
			return
		}
		data.Capture = &capture
		data.Title = "Captured request and response"
		data.Request = bodyPreview(capture.Request)
		data.Response = bodyPreview(capture.Response)
		if capture.Response.State != nil && *capture.Response.State == record.BodyPresent {
			data.Download = viewURL("/view/response", e)
		}
	}
	s.render(w, r, data)
}
func viewURL(path string, e ingest.Execution) string {
	q := url.Values{"project": {Opaque(e.Tenant)}, "trace": {e.Trace.String()}}
	if e.Span.Valid() {
		q.Set("span", e.Span.String())
	}
	return path + "?" + q.Encode()
}
func displayOpaque(s string) string {
	b, err := hex.DecodeString(s)
	if err == nil && utf8.Valid(b) {
		for _, r := range string(b) {
			if !unicode.IsPrint(r) {
				return "hex:" + s
			}
		}
		return string(b)
	}
	return "hex:" + s
}
func bodyPreview(b Body) preview {
	out := preview{State: "Invalid"}
	if b.State == nil || b.Data == nil {
		return out
	}
	out.State = string(*b.State)
	if *b.State == record.BodyMissing {
		out.Text = "Request or response bytes were not captured."
		return out
	}
	if *b.State == record.BodyRedacted {
		out.Text = "Bytes were redacted by the producer."
		return out
	}
	data, err := base64.StdEncoding.Strict().DecodeString(*b.Data)
	if err != nil {
		return out
	}
	const maxPreview = 4096
	truncated := len(data) > maxPreview
	if truncated {
		data = data[:maxPreview]
	}
	if utf8.Valid(data) {
		out.Text = string(data)
		out.Encoding = "UTF-8 preview"
	} else {
		out.Text = hex.EncodeToString(data)
		out.Encoding = "Hexadecimal preview"
	}
	if truncated {
		out.Encoding += "; first 4096 retained bytes only (saved capture state unchanged)"
	}
	if len(data) == 0 {
		out.Text = "(zero bytes)"
	}
	return out
}
func (s *Server) renderFailure(w http.ResponseWriter, r *http.Request, status int, code string) {
	messages := map[string]string{
		"unauthenticated":    "The credential or session is invalid or expired. Sign in again.",
		"denied":             "This principal has no current permission for the requested operation.",
		"not_found":          "No accepted record was found at lookup. A span or capture may arrive later.",
		"expired":            "This accepted record has expired.",
		"result_unavailable": "A complete saved response is unavailable. Missing, redacted and truncated results cannot be played back.",
		"busy":               "The service is at its admission limit. Try again later.",
		"invalid_request":    "The request could not be read. Check the fields and refresh the form.",
		"too_large":          "This operation exceeds a configured byte limit.",
		"integrity_failure":  "Accepted evidence could not be verified. This is an error, not a missing record.",
	}
	message := messages[code]
	if message == "" {
		message = "The service could not complete this operation. No successful read is returned."
	}
	csrf, _ := s.challenge(w, r)
	data := viewData{Title: http.StatusText(status), Error: message, Login: r.URL.Path == "/login", CSRF: csrf}
	b := boundedBuffer{max: s.limits.MaxResponseBytes}
	if pageTemplate.Execute(&b, data) != nil {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(status)
		_, _ = w.Write([]byte("Rewind operation failed"))
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(b.Bytes())
}
func (s *Server) render(w http.ResponseWriter, r *http.Request, data viewData) {
	b := boundedBuffer{max: s.limits.MaxResponseBytes}
	if err := pageTemplate.Execute(&b, data); err != nil {
		s.fail(w, r, err, false)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(b.Bytes())
}

// This is an output projection only; no lesson or saved evidence lives in JS.
var pageTemplate = template.Must(template.New("view").Funcs(template.FuncMap{
	"json": func(v any) string { b, _ := json.MarshalIndent(v, "", "  "); return string(b) },
}).Parse(pageHTML))

//go:embed assets/view.html
var pageHTML string

//go:embed assets/style.css
var styles string
