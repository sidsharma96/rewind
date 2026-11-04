package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sidsharma96/rewind/internal/traceid"
)

func fixtureBase() baseConfig {
	return baseConfig{
		DSN:         "postgres://rewind_drill:synthetic-local-only-password-at-least-32-bytes@127.0.0.1:5555/rewind?sslmode=disable",
		R2Endpoint:  "https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com",
		R2AccessKey: "synthetic-access-key",
		R2SecretKey: "synthetic-secret-key",
	}
}

func TestLoadConfigPrivateBoundedSingleDocument(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		mode os.FileMode
		want bool
	}{
		{"private", `{}`, 0600, true},
		{"size boundary", `{}` + strings.Repeat(" ", configLimit-2), 0600, true},
		{"oversized", `{}` + strings.Repeat(" ", configLimit-1), 0600, false},
		{"group readable", `{}`, 0640, false},
		{"world readable", `{}`, 0604, false},
		{"unknown field", `{"writer_token":"synthetic"}`, 0600, false},
		{"second document", `{} {}`, 0600, false},
		{"truncated", `{"dsn":`, 0600, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "private.json")
			if err := os.WriteFile(path, []byte(tc.body), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(path, tc.mode); err != nil {
				t.Fatal(err)
			}
			var c baseConfig
			err := loadConfig(path, &c)
			if tc.want && err != nil || !tc.want && !errors.Is(err, errConfig) {
				t.Fatalf("loadConfig success = %v, want %v", err == nil, tc.want)
			}
		})
	}
	var c baseConfig
	if err := loadConfig(t.TempDir(), &c); !errors.Is(err, errConfig) {
		t.Fatal("directory accepted as private configuration")
	}
}

func TestBaseConfigRestrictsDatabaseAndEndpoint(t *testing.T) {
	base := fixtureBase()
	if !base.valid() {
		t.Fatal("synthetic local configuration rejected")
	}
	for _, tc := range []struct{ name, old, replacement string }{
		{"other database", "/rewind?", "/production?"},
		{"other user", "rewind_drill:", "postgres:"},
		{"remote host", "127.0.0.1", "192.0.2.1"},
		{"hostname", "127.0.0.1", "localhost"},
		{"no explicit port", ":5555", ""},
		{"privileged port", ":5555", ":543"},
		{"invalid port", ":5555", ":65536"},
		{"short password", "synthetic-local-only-password-at-least-32-bytes", "short"},
		{"no password", ":synthetic-local-only-password-at-least-32-bytes@", "@"},
		{"ssl default", "?sslmode=disable", ""},
		{"ssl override", "sslmode=disable", "sslmode=require"},
		{"query host override", "sslmode=disable", "sslmode=disable&host=192.0.2.1"},
		{"service lookup", "sslmode=disable", "sslmode=disable&service=production"},
		{"duplicate mode", "sslmode=disable", "sslmode=disable&sslmode=disable"},
		{"encoded database path", "/rewind?", "/%72ewind?"},
		{"fragment", "sslmode=disable", "sslmode=disable#ignored"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := base
			c.DSN = strings.Replace(c.DSN, tc.old, tc.replacement, 1)
			if c.valid() {
				t.Fatal("out-of-scope database configuration accepted")
			}
		})
	}
	for _, endpoint := range []string{
		"http://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com",
		base.R2Endpoint + ":443",
		base.R2Endpoint + "/bucket",
		base.R2Endpoint + "?",
		base.R2Endpoint + "?region=auto",
		base.R2Endpoint + "#fragment",
		strings.Replace(base.R2Endpoint, "https://", "https://user:password@", 1),
		strings.Replace(base.R2Endpoint, "0123456789abcdef", "0123456789ABCDEF", 1),
		base.R2Endpoint + ".example.com",
		"https://localhost",
	} {
		c := base
		c.R2Endpoint = endpoint
		if c.valid() {
			t.Error("non-default R2 endpoint accepted")
		}
	}
	for _, field := range []string{"access", "secret"} {
		c := base
		if field == "access" {
			c.R2AccessKey = ""
		} else {
			c.R2SecretKey = ""
		}
		if c.valid() {
			t.Errorf("empty %s key accepted", field)
		}
	}
}

func preparedDirectory(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "new-drill")
	if err := prepare(context.Background(), fixtureBase(), dir); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestPreparePrivateExclusiveInputs(t *testing.T) {
	dir := preparedDirectory(t)
	st, err := os.Stat(dir)
	if err != nil || st.Mode().Perm() != 0700 {
		t.Fatalf("private directory: stat=%v, err=%v", st, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 5 {
		t.Fatalf("prepared file count = %d, err=%v", len(entries), err)
	}
	for _, entry := range entries {
		st, err := entry.Info()
		if err != nil || !st.Mode().IsRegular() || st.Mode().Perm() != 0600 {
			t.Fatalf("private output %s: stat=%v, err=%v", entry.Name(), st, err)
		}
	}
	path := filepath.Join(dir, "setup.json")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := prepare(context.Background(), fixtureBase(), dir); err == nil {
		t.Fatal("prepare reused an existing directory")
	}
	if err := exclusiveJSON(path, struct{}{}); err == nil {
		t.Fatal("private file overwritten")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("exclusive creation changed existing configuration")
	}
	var setup setupConfig
	if err := loadConfig(path, &setup); err != nil || !setup.valid() {
		t.Fatal("prepared setup rejected")
	}
	var previousTrace string
	for _, phase := range []string{"ack", "lost"} {
		var w writeConfig
		var r readConfig
		if err := loadConfig(filepath.Join(dir, "write-"+phase+".json"), &w); err != nil || !w.valid() {
			t.Fatalf("prepared %s writer rejected", phase)
		}
		if err := loadConfig(filepath.Join(dir, "read-"+phase+".json"), &r); err != nil || !r.valid() {
			t.Fatalf("prepared %s reader rejected", phase)
		}
		if w.Trace == previousTrace || w.Trace != r.Trace || w.Schema != setup.Schema || r.Project != setup.Project {
			t.Fatal("phases need distinct traces and paired deployment/trace inputs")
		}
		previousTrace = w.Trace
		if _, err := traceid.ParseTrace(w.Trace); err != nil {
			t.Fatal("SDK trace ID invalid")
		}
		seen := make(map[string]bool)
		for i, name := range []string{"handoff", "attempt-failed", "attempt-succeeded"} {
			call := w.Calls[i]
			if call.Name != name || seen[call.Span] {
				t.Fatal("call names or independent SDK span IDs changed")
			}
			if _, err := traceid.ParseSpan(call.Span); err != nil {
				t.Fatal("SDK span ID invalid")
			}
			seen[call.Span] = true
			if i == 0 && call.Parent != "" || i > 0 && call.Parent != w.Calls[0].Span {
				t.Fatal("attempts must share the handoff parent")
			}
		}
		readerBytes, err := os.ReadFile(filepath.Join(dir, "read-"+phase+".json"))
		if err != nil {
			t.Fatal(err)
		}
		for _, token := range w.Tokens {
			if bytes.Contains(readerBytes, []byte(token)) {
				t.Fatal("reader input leaked a writer token")
			}
		}
	}
}

func TestReaderInputExcludesWriterState(t *testing.T) {
	dir := preparedDirectory(t)
	var r readConfig
	if err := loadConfig(filepath.Join(dir, "read-ack.json"), &r); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"calls", "span", "parent", "receipt", "receipts", "object", "objects", "tokens", "writer_token"} {
		t.Run(field, func(t *testing.T) {
			b, err := json.Marshal(r)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(b, &fields); err != nil {
				t.Fatal(err)
			}
			fields[field] = json.RawMessage(`null`)
			path := filepath.Join(t.TempDir(), "reader.json")
			if err := exclusiveJSON(path, fields); err != nil {
				t.Fatal(err)
			}
			var candidate readConfig
			if err := loadConfig(path, &candidate); !errors.Is(err, errConfig) {
				t.Fatal("reader accepted forbidden retained writer state")
			}
		})
	}
	var w writeConfig
	if err := loadConfig(filepath.Join(dir, "write-ack.json"), &w); err != nil {
		t.Fatal(err)
	}
	if !validCredentials(r.Credentials, "reader", "denied") {
		t.Fatal("reader must contain exactly reader and denied credentials")
	}
	for _, principal := range w.Credentials {
		candidate := r
		candidate.Credentials = append(append([]credential(nil), r.Credentials...), principal)
		if candidate.valid() {
			t.Fatal("reader accepted an additional writer credential")
		}
		candidate.Credentials = []credential{r.Credentials[0], principal}
		if candidate.valid() {
			t.Fatal("writer credential substituted for denied credential")
		}
	}
}

func TestDeploymentNamespaceGuard(t *testing.T) {
	const schema = "rewind_drill_0123456789abcdef01234567"
	d := deployment{baseConfig: fixtureBase(), Schema: schema, Project: schema}
	if !d.valid() {
		t.Fatal("synthetic drill namespace rejected")
	}
	for _, name := range []string{"public", "rewind_drill_", schema + "0", strings.ToUpper(schema), schema + `"; DROP SCHEMA public;--`} {
		candidate := d
		candidate.Schema, candidate.Project = name, name
		if candidate.valid() {
			t.Fatal("unbounded or unscoped namespace accepted")
		}
	}
	d.Project = "other-project"
	if d.valid() {
		t.Fatal("project differs from disposable schema")
	}
}

func TestConfigurationTokensStayBoundToNamedPrincipals(t *testing.T) {
	dir := preparedDirectory(t)
	var r readConfig
	var w writeConfig
	if err := loadConfig(filepath.Join(dir, "read-ack.json"), &r); err != nil || !r.valid() {
		t.Fatal("prepared reader rejected")
	}
	if err := loadConfig(filepath.Join(dir, "write-ack.json"), &w); err != nil || !w.valid() {
		t.Fatal("prepared writer rejected")
	}
	r.ReaderToken, r.DeniedToken = r.DeniedToken, r.ReaderToken
	if r.valid() {
		t.Fatal("reader and denied tokens accepted under swapped principal names")
	}
	for _, swap := range []bool{false, true} {
		candidate := w
		candidate.Tokens = make(map[string]string, len(w.Tokens))
		for principal, token := range w.Tokens {
			candidate.Tokens[principal] = token
		}
		candidate.Tokens["alice"] = w.Tokens["bob"]
		if swap {
			candidate.Tokens["bob"] = w.Tokens["alice"]
		}
		if candidate.valid() {
			t.Fatalf("writer token reassignment accepted (swap=%v)", swap)
		}
	}
}
