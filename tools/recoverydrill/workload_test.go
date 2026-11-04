package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sidsharma96/rewind/internal/catalog"
	"github.com/sidsharma96/rewind/internal/ingest"
)

type memoryObjects struct {
	mu   sync.Mutex
	data map[ingest.ObjectID][]byte
}

func (m *memoryObjects) Create(ctx context.Context, id ingest.ObjectID, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if sha256.Sum256(data) != id.Digest {
		return ingest.ErrIntegrity
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if original, found := m.data[id]; found && !bytes.Equal(original, data) {
		return ingest.ErrIntegrity
	}
	m.data[id] = bytes.Clone(data)
	return nil
}

func (m *memoryObjects) ReadExact(ctx context.Context, id ingest.ObjectID, size int) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	data, found := m.data[id]
	if !found {
		return nil, ingest.ErrObjectMissing
	}
	if len(data) != size || sha256.Sum256(data) != id.Digest {
		return nil, ingest.ErrIntegrity
	}
	return bytes.Clone(data), nil
}

type drillFixture struct {
	pool    *pgxpool.Pool
	c       *catalog.Catalog
	ack     writeConfig
	lost    writeConfig
	reads   [2]readConfig
	objects *memoryObjects
}

func nativeDrill(t *testing.T) drillFixture {
	t.Helper()
	dsn := os.Getenv("REWIND_DRILL_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("set REWIND_DRILL_TEST_POSTGRES to an explicitly dedicated rewind database/rewind_drill role")
	}
	base := fixtureBase()
	base.DSN = dsn
	dir := filepath.Join(t.TempDir(), "prepared")
	if err := prepare(t.Context(), base, dir); err != nil {
		t.Fatal("prepare dedicated fixture failed")
	}
	var cfg setupConfig
	if err := loadConfig(filepath.Join(dir, "setup.json"), &cfg); err != nil {
		t.Fatal(err)
	}
	pool, c, err := openCatalog(t.Context(), cfg.deployment)
	if err != nil {
		t.Fatal("open dedicated fixture catalog failed")
	}
	t.Cleanup(pool.Close)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := pool.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{cfg.Schema}.Sanitize()+" CASCADE"); err != nil {
			t.Error("remove owned drill schema failed")
		}
	})
	if err := setup(t.Context(), cfg, pool); err != nil {
		t.Fatal("dedicated fixture setup failed")
	}
	if err := setup(t.Context(), cfg, pool); err == nil {
		t.Fatal("setup reused an existing schema")
	}
	f := drillFixture{pool: pool, c: c, objects: &memoryObjects{data: make(map[ingest.ObjectID][]byte)}}
	for _, pair := range []struct {
		name   string
		target any
	}{
		{"write-ack.json", &f.ack}, {"write-lost.json", &f.lost}, {"read-ack.json", &f.reads[0]}, {"read-lost.json", &f.reads[1]},
	} {
		if err := loadConfig(filepath.Join(dir, pair.name), pair.target); err != nil {
			t.Fatal(err)
		}
	}
	return f
}

func (f drillFixture) session(t *testing.T, credentials []credential, threshold int) *session {
	t.Helper()
	s, err := newSession(f.c, f.objects, credentials, threshold)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func events(t *testing.T, data []byte) []event {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader(data))
	var out []event
	for {
		var value event
		err := d.Decode(&value)
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatal("malformed event stream")
		}
		out = append(out, value)
	}
}

func receipts(t *testing.T, values []event, kind string) map[string]ingest.Receipt {
	t.Helper()
	out := make(map[string]ingest.Receipt)
	for _, value := range values {
		if value.Type != kind {
			continue
		}
		key := value.Name + "/" + value.Kind
		if value.Receipt == nil || out[key] != (ingest.Receipt{}) {
			t.Fatal("receipt absent or duplicated in output")
		}
		out[key] = *value.Receipt
	}
	return out
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

func TestNativeDrillPhasesReadBothBeforeRetry(t *testing.T) {
	f := nativeDrill(t)
	var acknowledged, withheld, control bytes.Buffer
	if err := write(t.Context(), f.ack, f.session(t, f.ack.Credentials, 32768), &acknowledged, io.Discard, false, false); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	barrier := writerFunc(func(p []byte) (int, error) {
		n, err := control.Write(p)
		cancel()
		return n, err
	})
	if err := write(ctx, f.lost, f.session(t, f.lost.Credentials, 32768), &withheld, barrier, true, false); err == nil {
		t.Fatal("withheld acknowledgment unexpectedly completed")
	}
	ackRows := receipts(t, events(t, acknowledged.Bytes()), "accepted")
	lostRows := receipts(t, events(t, withheld.Bytes()), "accepted")
	if len(ackRows) != 6 || len(lostRows) != 5 {
		t.Fatal("writer exposed wrong acknowledgment count")
	}
	if _, found := lostRows["attempt-succeeded/capture"]; found {
		t.Fatal("withheld final receipt leaked")
	}
	controlRows := events(t, control.Bytes())
	if len(controlRows) != 1 || controlRows[0].Type != "control" || controlRows[0].Phase != "final-ack-withheld" || controlRows[0].Receipt != nil {
		t.Fatal("control barrier included a receipt or wrong phase")
	}
	for _, row := range events(t, withheld.Bytes()) {
		if row.Type == "summary" {
			t.Fatal("withheld write emitted success summary")
		}
	}
	var recovered [2]map[string]ingest.Receipt
	// Complete BOTH reads before permitting either retry. Fresh sessions have
	// only reader configs and no original producer calls/receipts.
	for i, cfg := range f.reads {
		var output bytes.Buffer
		s := f.session(t, cfg.Credentials, 32768)
		if err := read(t.Context(), cfg, s, &output); err != nil {
			t.Fatal(err)
		}
		recovered[i] = receipts(t, events(t, output.Bytes()), "recovered")
		if len(recovered[i]) != 6 || s.objects.snapshot() != (objectMethods{Reads: 7}) {
			t.Fatal("fresh read changed record count or object calls")
		}
		for _, row := range events(t, output.Bytes()) {
			if row.Type == "denied" && (row.Methods == nil || *row.Methods != (objectMethods{})) {
				t.Fatal("denied read used object operations")
			}
		}
	}
	for key, expected := range ackRows {
		if recovered[0][key] != expected {
			t.Fatal("acknowledged acceptance changed across fresh reader")
		}
	}
	for key, expected := range lostRows {
		if recovered[1][key] != expected {
			t.Fatal("known acceptance changed after withheld reply")
		}
	}
	for i, cfg := range []writeConfig{f.ack, f.lost} {
		var output bytes.Buffer
		s := f.session(t, cfg.Credentials, 1)
		if err := write(t.Context(), cfg, s, &output, io.Discard, false, true); err != nil {
			t.Fatal(err)
		}
		rows := receipts(t, events(t, output.Bytes()), "retried")
		if len(rows) != 6 || s.objects.snapshot().Creates != 0 || s.objects.snapshot().Reads == 0 {
			t.Fatal("semantic retry did not use accepted objects only")
		}
		for key, got := range rows {
			if !got.Duplicate {
				t.Fatal("retry returned new acceptance")
			}
			got.Duplicate = false
			if got != recovered[i][key] {
				t.Fatal("retry changed original reference/attribution/times")
			}
		}
	}
	t.Log("two traces saved; final application reply withheld; fresh readers recovered both before retries; seven reads per trace; retries created no objects and retained original receipts")
}

func TestNativeDrillRetryCannotRecreateUnacceptedRecords(t *testing.T) {
	f := nativeDrill(t)
	s := f.session(t, f.ack.Credentials, 1)
	if err := registerCalls(t.Context(), f.ack, s); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := read(t.Context(), f.reads[0], f.session(t, f.reads[0].Credentials, 32768), &output); err == nil {
		t.Fatal("fresh read claimed recovery from registrations only")
	}
	output.Reset()
	if err := write(t.Context(), f.ack, s, &output, io.Discard, false, true); err == nil {
		t.Fatal("retry created absent acceptance")
	}
	if len(f.objects.data) != 0 {
		t.Fatal("retry wrote an object while repairing missing state")
	}
	var count int
	if err := f.pool.QueryRow(t.Context(), "SELECT count(*) FROM "+pgx.Identifier{f.ack.Schema, "records"}.Sanitize()).Scan(&count); err != nil || count != 0 {
		t.Fatal("retry created an accepted record")
	}
}

func TestGuardRejectsWrongActualDatabase(t *testing.T) {
	dsn := os.Getenv("REWIND_DRILL_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("set dedicated test database configuration")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal("parse dedicated test configuration failed")
	}
	cfg.ConnConfig.Database = "postgres"
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal("construct wrong-database test pool failed")
	}
	defer pool.Close()
	if err := guardDatabase(t.Context(), pool); err == nil {
		t.Fatal("actual database identity was not checked")
	}
}

func TestRunRefusesUnknownAndIncompatibleArguments(t *testing.T) {
	for _, args := range [][]string{
		nil, {"-phase", "drop", "-config", "synthetic"}, {"-phase", "read", "-config", "synthetic", "-withhold-final-ack"},
		{"-phase", "setup", "-config", "synthetic", "-out-dir", "unused"}, {"-phase", "prepare", "-config", "synthetic", "extra"},
	} {
		var out, diagnostics bytes.Buffer
		if err := run(t.Context(), args, &out, &diagnostics); err == nil || out.Len() != 0 || diagnostics.Len() != 0 {
			t.Fatal("invalid arguments reached work or exposed diagnostics")
		}
	}
	// Configuration errors never echo a token, DSN or provider error text.
	var out bytes.Buffer
	if err := run(t.Context(), []string{"-phase", "read", "-config", filepath.Join(t.TempDir(), "absent")}, &out, io.Discard); !errors.Is(err, errConfig) {
		t.Fatal("missing private configuration was not redacted")
	}
}
