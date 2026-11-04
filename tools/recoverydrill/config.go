package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/sidsharma96/rewind/internal/access"
	"github.com/sidsharma96/rewind/internal/traceid"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

var errConfig = errors.New("recoverydrill: invalid private configuration")

const configLimit = 64 << 10

// These are deliberately small fixture settings, not production defaults.
const phaseTimeout = 3 * time.Minute

type baseConfig struct {
	DSN         string `json:"dsn"`
	R2Endpoint  string `json:"r2_endpoint"`
	R2AccessKey string `json:"r2_access_key"`
	R2SecretKey string `json:"r2_secret_key"`
}

type deployment struct {
	baseConfig
	Schema  string `json:"schema"`
	Project string `json:"project"`
}

type credential struct {
	Principal string    `json:"principal"`
	Digest    string    `json:"digest"`
	ExpiresAt time.Time `json:"expires_at"`
}

type setupConfig struct {
	deployment
	Credentials []credential `json:"credentials"`
}

type callPlan struct {
	Name   string `json:"name"`
	Span   string `json:"span"`
	Parent string `json:"parent,omitempty"`
}

type writeConfig struct {
	deployment
	Trace       string            `json:"trace"`
	Calls       []callPlan        `json:"calls"`
	Credentials []credential      `json:"credentials"`
	Tokens      map[string]string `json:"tokens"`
}

// Reader input cannot carry original calls, receipts, objects or writer tokens.
type readConfig struct {
	deployment
	Trace       string       `json:"trace"`
	Credentials []credential `json:"credentials"`
	ReaderToken string       `json:"reader_token"`
	DeniedToken string       `json:"denied_token"`
}

func loadConfig(path string, dst any) error {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > configLimit {
		return errConfig
	}
	f, err := os.Open(path)
	if err != nil {
		return errConfig
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 || st.Size() > configLimit {
		return errConfig
	}
	b, err := io.ReadAll(io.LimitReader(f, configLimit+1))
	if err != nil || len(b) > configLimit {
		return errConfig
	}
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	if dec.Decode(dst) != nil || dec.Decode(new(any)) != io.EOF {
		return errConfig
	}
	return nil
}

func (c baseConfig) valid() bool {
	u, err := url.Parse(c.DSN)
	if err != nil || u.Scheme != "postgres" && u.Scheme != "postgresql" || u.User == nil || u.User.Username() != "rewind_drill" || u.Hostname() != "127.0.0.1" || u.Path != "/rewind" || u.RawPath != "" || u.Fragment != "" {
		return false
	}
	password, ok := u.User.Password()
	port, portErr := strconv.Atoi(u.Port())
	query, queryErr := url.ParseQuery(u.RawQuery)
	if !ok || len(password) < 32 || portErr != nil || port < 1024 || port > 65535 || queryErr != nil || len(query) != 1 || len(query["sslmode"]) != 1 || query.Get("sslmode") != "disable" {
		return false
	}
	return r2Endpoint(c.R2Endpoint) && c.R2AccessKey != "" && c.R2SecretKey != ""
}

func r2Endpoint(value string) bool {
	u, err := url.Parse(value)
	if err != nil || u.Scheme != "https" || u.User != nil || u.Port() != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawPath != "" || u.Path != "" && u.Path != "/" {
		return false
	}
	account, ok := strings.CutSuffix(u.Host, ".r2.cloudflarestorage.com")
	return ok && lowerHex(account, 32)
}

func lowerHex(value string, length int) bool {
	if len(value) != length {
		return false
	}
	for i := range value {
		if !(value[i] >= '0' && value[i] <= '9' || value[i] >= 'a' && value[i] <= 'f') {
			return false
		}
	}
	return true
}

func (c deployment) valid() bool {
	return c.baseConfig.valid() && c.Project == c.Schema && strings.HasPrefix(c.Schema, "rewind_drill_") && lowerHex(strings.TrimPrefix(c.Schema, "rewind_drill_"), 24)
}

func verifier(entries []credential) (*access.Credentials, error) {
	var converted []access.Credential
	for _, entry := range entries {
		if !lowerHex(entry.Digest, 64) {
			return nil, errConfig
		}
		raw, _ := hex.DecodeString(entry.Digest)
		var digest [32]byte
		copy(digest[:], raw)
		converted = append(converted, access.Credential{Digest: digest, Principal: entry.Principal, ExpiresAt: entry.ExpiresAt})
	}
	v, err := access.NewCredentials(8, converted, time.Now)
	if err != nil {
		return nil, errConfig
	}
	return v, nil
}

func validCredentials(entries []credential, principals ...string) bool {
	if len(entries) != len(principals) {
		return false
	}
	seen := make(map[string]bool)
	for _, entry := range entries {
		if seen[entry.Principal] {
			return false
		}
		seen[entry.Principal] = true
	}
	for _, p := range principals {
		if !seen[p] {
			return false
		}
	}
	_, err := verifier(entries)
	return err == nil
}

func (c setupConfig) valid() bool {
	return c.deployment.valid() && validCredentials(c.Credentials, "host", "alice", "bob", "collector", "reader", "denied")
}

func (c writeConfig) valid() bool {
	if !c.deployment.valid() || !validCredentials(c.Credentials, "host", "alice", "bob", "collector") || len(c.Tokens) != 4 || len(c.Calls) != 3 {
		return false
	}
	tid, err := traceid.ParseTrace(c.Trace)
	if err != nil || !tid.Valid() {
		return false
	}
	seen := make(map[traceid.Span]bool)
	for i, call := range c.Calls {
		sid, err := traceid.ParseSpan(call.Span)
		if err != nil || seen[sid] || call.Name != callNames[i] || i == 0 && call.Parent != "" || i > 0 && call.Parent != c.Calls[0].Span {
			return false
		}
		seen[sid] = true
	}
	v, _ := verifier(c.Credentials)
	for _, p := range []string{"host", "alice", "bob", "collector"} {
		if _, err := v.Authenticate(context.Background(), c.Tokens[p]); err != nil || !tokenNames(c.Credentials, p, c.Tokens[p]) {
			return false
		}
	}
	return true
}

func (c readConfig) valid() bool {
	if !c.deployment.valid() || !validCredentials(c.Credentials, "reader", "denied") {
		return false
	}
	if _, err := traceid.ParseTrace(c.Trace); err != nil {
		return false
	}
	v, _ := verifier(c.Credentials)
	_, a := v.Authenticate(context.Background(), c.ReaderToken)
	_, b := v.Authenticate(context.Background(), c.DeniedToken)
	return a == nil && b == nil && tokenNames(c.Credentials, "reader", c.ReaderToken) && tokenNames(c.Credentials, "denied", c.DeniedToken)
}

func tokenNames(entries []credential, principal, token string) bool {
	digest := sha256.Sum256([]byte(token))
	for _, entry := range entries {
		if entry.Principal == principal {
			return entry.Digest == hex.EncodeToString(digest[:])
		}
	}
	return false
}

var callNames = [...]string{"handoff", "attempt-failed", "attempt-succeeded"}

func prepare(ctx context.Context, base baseConfig, dir string) error {
	if !base.valid() || dir == "" {
		return errConfig
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		return errors.New("recoverydrill: output directory must be new")
	}
	var random [12]byte
	if _, err := rand.Read(random[:]); err != nil {
		return errors.New("recoverydrill: random namespace failed")
	}
	schema := "rewind_drill_" + hex.EncodeToString(random[:])
	d := deployment{baseConfig: base, Schema: schema, Project: schema}
	var all []credential
	tokens := make(map[string]string)
	expiry := time.Now().UTC().Add(2 * time.Hour)
	for _, principal := range []string{"host", "alice", "bob", "collector", "reader", "denied"} {
		token, digest, err := access.NewToken()
		if err != nil {
			return errors.New("recoverydrill: token generation failed")
		}
		tokens[principal] = token
		all = append(all, credential{Principal: principal, Digest: hex.EncodeToString(digest[:]), ExpiresAt: expiry})
	}
	if err := exclusiveJSON(filepath.Join(dir, "setup.json"), setupConfig{deployment: d, Credentials: all}); err != nil {
		return err
	}
	provider := sdktrace.NewTracerProvider()
	defer provider.Shutdown(context.Background())
	tracer := provider.Tracer("rewind-live-recovery-fixture")
	for _, label := range []string{"ack", "lost"} {
		if err := ctx.Err(); err != nil {
			return err
		}
		parent, root := tracer.Start(ctx, callNames[0])
		_, failed := tracer.Start(parent, callNames[1])
		_, succeeded := tracer.Start(parent, callNames[2])
		tid := root.SpanContext().TraceID().String()
		rootID := root.SpanContext().SpanID().String()
		calls := []callPlan{{Name: callNames[0], Span: rootID}, {Name: callNames[1], Span: failed.SpanContext().SpanID().String(), Parent: rootID}, {Name: callNames[2], Span: succeeded.SpanContext().SpanID().String(), Parent: rootID}}
		failed.End()
		succeeded.End()
		root.End()
		w := writeConfig{deployment: d, Trace: tid, Calls: calls, Credentials: all[:4], Tokens: make(map[string]string)}
		for _, p := range []string{"host", "alice", "bob", "collector"} {
			w.Tokens[p] = tokens[p]
		}
		r := readConfig{deployment: d, Trace: tid, Credentials: all[4:], ReaderToken: tokens["reader"], DeniedToken: tokens["denied"]}
		if !w.valid() || !r.valid() {
			return errConfig
		}
		if err := exclusiveJSON(filepath.Join(dir, "write-"+label+".json"), w); err != nil {
			return err
		}
		if err := exclusiveJSON(filepath.Join(dir, "read-"+label+".json"), r); err != nil {
			return err
		}
	}
	return nil
}

func exclusiveJSON(path string, value any) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return errors.New("recoverydrill: private output creation failed")
	}
	err = json.NewEncoder(f).Encode(value)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		return errors.New("recoverydrill: private output write failed")
	}
	return nil
}
