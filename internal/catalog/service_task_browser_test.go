//go:build unix

package catalog

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"html"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/sidsharma96/rewind/internal/httpapi"
)

// The browser gets the producer's token-free link and a reader credential, not
// plans or store credentials. Model/tool binaries have already been removed.
func inspectTaskBrowser(t *testing.T, server *httptest.Server, locator, token string, captures map[string]httpapi.Capture, revoke, restore func(), snapshot func() string) {
	t.Helper()
	if !strings.HasPrefix(locator, server.URL+"/view?") || strings.Contains(locator, token) {
		t.Fatal("producer emitted no safe task link")
	}
	browser := *server.Client()
	browser.Jar, _ = cookiejar.New(nil)
	browser.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	get := func(path string) ([]byte, *http.Response) {
		t.Helper()
		resp, err := browser.Get(server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		return b, resp
	}
	path := strings.TrimPrefix(locator, server.URL)
	_, resp := get(path)
	if resp.StatusCode != 303 {
		t.Fatal("signed-out trace access accepted")
	}
	login := resp.Header.Get("Location")
	b, resp := get(login)
	if resp.StatusCode != 200 {
		t.Fatal("task sign-in unavailable")
	}
	field := func(name string) string {
		t.Helper()
		match := regexp.MustCompile(`name="` + name + `" value="([^"]*)"`).FindSubmatch(b)
		if len(match) != 2 {
			t.Fatal("missing login field", name)
		}
		return html.UnescapeString(string(match[1]))
	}
	form := url.Values{"csrf": {field("csrf")}, "return": {field("return")}, "credential": {token}}
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/login", strings.NewReader(form.Encode()))
	req.Header.Set("Origin", server.URL)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := browser.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 303 {
		t.Fatal("task sign-in failed", resp.StatusCode)
	}
	b, resp = get(resp.Header.Get("Location"))
	if resp.StatusCode != 200 || !bytes.Contains(b, []byte("format=command-v1")) {
		t.Fatal("task link lost through sign-in")
	}
	for id, c := range captures {
		capturePath := "/view/capture?" + url.Values{"project": {c.Project}, "trace": {c.Trace}, "span": {id}, "format": {"command-v1"}}.Encode()
		b, resp = get(capturePath)
		if resp.StatusCode != 200 {
			t.Fatal("task capture unavailable", resp.StatusCode)
		}
		var request struct {
			Tool string `json:"tool"`
		}
		raw, _ := base64.StdEncoding.DecodeString(*c.Request.Data)
		if json.Unmarshal(raw, &request) != nil {
			t.Fatal("invalid test request")
		}
		raw, _ = base64.StdEncoding.DecodeString(*c.Response.Data)
		var command struct{ Stdout, Stderr []byte }
		if json.Unmarshal(raw, &command) != nil {
			t.Fatal("invalid command capture")
		}
		if request.Tool == "claude print" {
			var result struct{ Result string }
			if json.Unmarshal(command.Stdout, &result) != nil || result.Result == "" {
				t.Fatal("missing CLI result")
			}
			if !bytes.Contains(b, []byte(html.EscapeString(result.Result))) {
				t.Fatal("model answer remains encoded")
			}
		} else if utf8.Valid(command.Stdout) && !bytes.Contains(b, []byte(html.EscapeString(string(command.Stdout)))) {
			t.Fatal("search stdout remains encoded")
		}
		if utf8.Valid(command.Stderr) && !bytes.Contains(b, []byte(html.EscapeString(string(command.Stderr)))) {
			t.Fatal("stderr remains encoded")
		}
		if !bytes.Contains(b, []byte("Command exit does not establish answer correctness")) {
			t.Fatal("command status conflated with correctness")
		}
		download := "/view/response?" + url.Values{"project": {c.Project}, "trace": {c.Trace}, "span": {id}}.Encode()
		saved, resp := get(download)
		if resp.StatusCode != 200 || !bytes.Equal(saved, raw) || resp.Header.Get("Rewind-Receipt") == "" {
			t.Fatal("readable projection changed exact saved bytes")
		}
	}
	before := snapshot()
	revoke()
	_, resp = get(path)
	if resp.StatusCode != 403 || snapshot() != before {
		t.Fatal("task view bypassed current reader permission")
	}
	for id, c := range captures {
		_, resp = get("/view/capture?" + url.Values{"project": {c.Project}, "trace": {c.Trace}, "span": {id}, "format": {"command-v1"}}.Encode())
		if resp.StatusCode != 403 || snapshot() != before {
			t.Fatal("command interpretation bypassed permission before object access")
		}
		break
	}
	restore()
	t.Log("ordinary task browser: producer link survives sign-in; model results/search streams readable; saved responses unchanged with tools removed; revoked reader refused before object access")
}
