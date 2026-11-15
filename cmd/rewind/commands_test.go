package main

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPrivateCredentialCreationAndNoOverwrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	var out bytes.Buffer
	if err := run([]string{"token", "--out", path}, &out); err != nil {
		t.Fatal(err)
	}
	token, err := os.ReadFile(path)
	if err != nil || len(token) != 65 {
		t.Fatal("bad token file", err)
	}
	if bytes.Contains(out.Bytes(), bytes.TrimSpace(token)) {
		t.Fatal("raw credential printed")
	}
	before := append([]byte(nil), token...)
	if err := run([]string{"token", "--out", path}, &out); err == nil {
		t.Fatal("token replaced")
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Fatal("token changed")
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := readPrivate(path, 65); err == nil {
		t.Fatal("public credential file accepted")
	}
}
func TestServiceConfigurationHasExplicitBoundsAndTLS(t *testing.T) {
	yes := true
	c := serviceConfig{Database: "postgres://user:password@db.example/test", DatabaseTLS: &yes, MaxConnections: 8, MaxActive: 4, MaxRequest: 4096, MaxResponse: 8192, TimeoutSeconds: 5,
		SessionCount: 8, SessionSeconds: 60, TraceCandidates: 2, TraceStoredBytes: 4096}
	if !c.valid() {
		t.Fatal("bounded config rejected")
	}
	no := false
	c.DatabaseTLS = &no
	if c.valid() {
		t.Fatal("cleartext remote database allowed")
	}
	c.Database = "postgres://user:password@127.0.0.1/test"
	if !c.valid() {
		t.Fatal("explicit loopback tunnel refused")
	}
	c.MaxActive = 0
	if c.valid() {
		t.Fatal("unbounded activity accepted")
	}
}

type readyWriter struct {
	once  sync.Once
	ready chan struct{}
}

func (w *readyWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.ready) })
	return len(p), nil
}

func TestNativeServiceStartupHTTP1AndJoinedShutdown(t *testing.T) {
	dsn := os.Getenv("REWIND_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("requires explicitly disposable native PostgreSQL")
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	u.RawQuery = ""
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	certServer := httptest.NewTLSServer(http.NotFoundHandler())
	certificate := certServer.TLS.Certificates[0]
	certServer.Close()
	dir := t.TempDir()
	certPath, keyPath := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate[0]}), 0600); err != nil {
		t.Fatal(err)
	}
	key, err := x509.MarshalPKCS8PrivateKey(certificate.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), 0600); err != nil {
		t.Fatal(err)
	}
	no := false
	c := serviceConfig{Listen: address, Origin: "https://" + address, CertFile: certPath, KeyFile: keyPath,
		Database: u.String(), DatabaseTLS: &no, Schema: "rewind_service_fixture",
		R2Endpoint: "https://synthetic.r2.cloudflarestorage.com", R2Bucket: "test-bucket", R2Access: "synthetic", R2Secret: "synthetic",
		MaxConnections: 4, MaxActive: 2, MaxRequest: 8192, MaxResponse: 16384, TimeoutSeconds: 2, SessionCount: 4, SessionSeconds: 60, TraceCandidates: 2, TraceStoredBytes: 8192}

	config, _ := json.Marshal(c)
	path := filepath.Join(dir, "config")
	if err := os.WriteFile(path, config, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	out := &readyWriter{ready: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- runContext(ctx, []string{"serve", "--config", path}, out) }()
	select {
	case <-out.ready:
	case err := <-done:
		t.Fatal("startup", err)
	case <-time.After(10 * time.Second):
		t.Fatal("startup did not finish")
	}
	client := *certServer.Client()
	client.Timeout = 5 * time.Second
	response, err := client.Get(c.Origin + "/login")
	if err != nil {
		cancel()
		<-done
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != 200 || response.ProtoMajor != 1 || response.TLS.NegotiatedProtocol == "h2" {
		t.Fatal("connection cap can be bypassed by another protocol")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown did not join")
	}
	if conn, err := net.DialTimeout("tcp", address, time.Second); err == nil {
		_ = conn.Close()
		t.Fatal("listener survived shutdown")
	}
}

func TestHelpDescribesImplementedEntryPoints(t *testing.T) {
	var out bytes.Buffer
	if err := run([]string{"help"}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "save-capture") || strings.Contains(out.String(), "Storage and replay are not implemented") {
		t.Fatal("stale help")
	}
}

func TestDiagnoseRulesArePublishedVerbatimAndFlagsBounded(t *testing.T) {
	var out bytes.Buffer
	if err := run([]string{"diagnose", "--rules"}, &out); err != nil {
		t.Fatal(err)
	}
	doc, err := os.ReadFile("../../engineering/DIAGNOSIS.md")
	if err != nil {
		t.Fatal(err)
	}
	if out.String() != diagnosisRules || !strings.Contains(string(doc), "```text\n"+diagnosisRules+"```") {
		t.Fatal("printed rules and engineering/DIAGNOSIS.md differ")
	}
	for _, args := range [][]string{
		{"diagnose", "--rules", "--trace", "x"},
		{"diagnose", "--max-spans", "0"},
		{"diagnose", "--max-spans", "10001"},
		{"diagnose", "--interpret", "guess"},
		{"diagnose", "--timeout", "1ms"},
		{"diagnose", "--project", "a", "--project-hex", "61"},
		{"diagnose", "extra"},
	} {
		if err := run(args, &out); err == nil {
			t.Errorf("accepted %v", args)
		}
	}
}
