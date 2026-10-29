package catalog

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sidsharma96/rewind/internal/ingest"
	"github.com/sidsharma96/rewind/internal/traceid"
)

func TestBinaryIDValidationBeforeDatabase(t *testing.T) {
	c, err := New(&pgxpool.Pool{}, "owned", Limits{1, time.Second})
	if err != nil {
		t.Fatal(err)
	}
	valid := ingest.Registration{
		Execution: ingest.Execution{Tenant: "project", Trace: traceid.Trace{1}, Span: traceid.Span{1}},
		Owner:     ingest.Owner{Run: "run", Instance: "alice"}, RegisteredBy: "host",
	}
	for _, zeroTrace := range []bool{true, false} {
		bad := valid
		if zeroTrace {
			bad.Execution.Trace = traceid.Trace{}
		} else {
			bad.Execution.Span = traceid.Span{}
		}
		if _, _, err := c.RegisterOwnership(t.Context(), bad); !errors.Is(err, ingest.ErrInvalid) {
			t.Fatal("zero ID reached registration database")
		}
		id := ingest.Identity{Tenant: bad.Execution.Tenant, Trace: bad.Execution.Trace, Span: bad.Execution.Span}
		if _, err := c.Lookup(t.Context(), id, "reader"); !errors.Is(err, ingest.ErrInvalid) {
			t.Fatal("zero ID reached lookup database")
		}
	}
	for _, badString := range []string{"", strings.Repeat("x", 257)} {
		for field := range 4 {
			bad := valid
			parts := []*string{&bad.Execution.Tenant, &bad.Owner.Run, &bad.Owner.Instance, &bad.RegisteredBy}
			*parts[field] = badString
			if _, _, err := c.RegisterOwnership(t.Context(), bad); !errors.Is(err, ingest.ErrInvalid) {
				t.Fatal("invalid opaque ID reached database")
			}
		}
	}
}

func TestBinaryKeysPersistAsRawBytes(t *testing.T) {
	f := database(t)
	r := f.r
	var err error
	r.Execution.Trace, err = traceid.ParseTrace("00ff102030405060708090a0b0c0d0ef")
	if err != nil {
		t.Fatal(err)
	}
	r.Execution.Span, err = traceid.ParseSpan("00ff1020304050ab")
	if err != nil {
		t.Fatal(err)
	}
	f.seed(t, r)
	if _, dup, err := f.c.RegisterOwnership(t.Context(), r); err != nil || dup {
		t.Fatalf("first registration: duplicate=%v err=%v", dup, err)
	}
	var rawTrace, rawSpan []byte
	if err := f.pool.QueryRow(t.Context(), "SELECT trace_id,span_id FROM "+f.c.table("bindings")).Scan(&rawTrace, &rawSpan); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(rawTrace, r.Execution.Trace[:]) || !bytes.Equal(rawSpan, r.Execution.Span[:]) {
		t.Fatal("SQL key is not the exact raw binary ID")
	}
	// Seed a receipt through trusted setup, not a claim of implemented Publish.
	f.r = r
	original := f.seedRecord(t, ingest.SpanRecord, false, false)
	freshPool, err := pgxpool.NewWithConfig(t.Context(), f.pool.Config())
	if err != nil {
		t.Fatal("open fresh pool failed")
	}
	defer freshPool.Close()
	fresh, err := New(freshPool, f.c.schema, Limits{1, time.Second})
	if err != nil {
		t.Fatal(err)
	}
	r.Execution.Trace, _ = traceid.ParseTrace(strings.ToUpper(r.Execution.Trace.String()))
	r.Execution.Span, _ = traceid.ParseSpan(strings.ToUpper(r.Execution.Span.String()))
	if got, dup, err := fresh.RegisterOwnership(t.Context(), r); err != nil || !dup || got != r {
		t.Fatalf("fresh-client case-equivalent duplicate: %v", err)
	}
	if got, err := fresh.Lookup(t.Context(), original.Reference.Identity, "reader"); err != nil || got != original {
		t.Fatalf("raw-key lookup: %v", err)
	}
}

func TestSQLRefusesMalformedExecutionKeys(t *testing.T) {
	f := database(t)
	f.seed(t, f.r)
	if _, _, err := f.c.RegisterOwnership(t.Context(), f.r); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"creator_grants", "bindings"} {
		for _, column := range []struct {
			name  string
			width int
		}{{"trace_id", 16}, {"span_id", 8}} {
			for _, bad := range [][]byte{{}, bytes.Repeat([]byte{1}, column.width-1), bytes.Repeat([]byte{1}, column.width+1), make([]byte, column.width)} {
				_, err := f.pool.Exec(t.Context(), "UPDATE "+f.c.table(table)+" SET "+column.name+"=$1", bad)
				var pgErr *pgconn.PgError
				if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
					t.Fatalf("%s.%s length=%d not refused by CHECK: %v", table, column.name, len(bad), err)
				}
			}
		}
	}
	if got, dup, err := f.c.RegisterOwnership(t.Context(), f.r); err != nil || !dup || got != f.r {
		t.Fatal("failed SQL writes changed original binding")
	}
}
