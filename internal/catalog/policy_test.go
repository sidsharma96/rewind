package catalog

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sidsharma96/rewind/internal/ingest"
)

// This fixture is trusted service setup, not a policy-control API. A future record
// publisher must hold this same shared gate until its acceptance commit finishes.
func readPolicy(ctx context.Context, c *Catalog, tx pgx.Tx, project string) (int64, int64, error) {
	if err := c.gate(ctx, tx, project, "SHARE"); err != nil {
		return 0, 0, err
	}
	var payload, marker int64
	err := tx.QueryRow(ctx, "SELECT payload_retention_us,marker_window_us FROM "+c.table("projects")+" WHERE id=$1", []byte(project)).Scan(&payload, &marker)
	return payload, marker, err
}
func policySnapshot(t *testing.T, pool *pgxpool.Pool, c *Catalog, project string) (int64, int64) {
	t.Helper()
	tx, err := pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(tx)
	payload, marker, err := readPolicy(t.Context(), c, tx, project)
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	return payload, marker
}
func TestProjectRetentionDefaultsScopeAndPersistence(t *testing.T) {
	f := database(t)
	f.seed(t, f.r)
	other := f.r
	other.Execution.Tenant = "other-project"
	f.seed(t, other)
	payload, marker := policySnapshot(t, f.pool, f.c, f.r.Execution.Tenant)
	if payload != ingest.DefaultPayloadRetention.Microseconds() || marker != ingest.DefaultMarkerWindow.Microseconds() {
		t.Fatalf("SQL policy defaults differ: payload=%d marker=%d", payload, marker)
	}
	f.policy(t, "UPDATE "+f.c.table("projects")+" SET payload_retention_us=$2,marker_window_us=$3 WHERE id=$1", []byte(f.r.Execution.Tenant), (30 * 24 * time.Hour).Microseconds(), (2 * time.Hour).Microseconds())
	// A fresh pool has no policy cache or connection state from that update.
	fresh, err := pgxpool.NewWithConfig(t.Context(), f.pool.Config())
	if err != nil {
		t.Fatal("open fresh policy pool failed")
	}
	t.Cleanup(fresh.Close)
	gotPayload, gotMarker := policySnapshot(t, fresh, f.c, f.r.Execution.Tenant)
	if gotPayload != (30*24*time.Hour).Microseconds() || gotMarker != (2*time.Hour).Microseconds() {
		t.Fatal("updated policy did not survive a new pool")
	}
	otherPayload, otherMarker := policySnapshot(t, fresh, f.c, other.Execution.Tenant)
	if otherPayload != payload || otherMarker != marker {
		t.Fatal("one project's policy changed another project's defaults")
	}
}
func TestProjectRetentionRejectsInvalidDurations(t *testing.T) {
	f := database(t)
	f.seed(t, f.r)
	maxUS := int64(^uint64(0)>>1) / int64(time.Microsecond)
	for _, column := range []string{"payload_retention_us", "marker_window_us"} {
		for _, value := range []int64{0, -1, maxUS + 1} {
			tx := f.holdPolicy(t)
			_, err := tx.Exec(t.Context(), "UPDATE "+f.c.table("projects")+" SET "+column+"=$2 WHERE id=$1", []byte(f.r.Execution.Tenant), value)
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
				t.Fatalf("invalid %s value was not rejected by a CHECK constraint", column)
			}
			if err = rollback(tx); err != nil {
				t.Fatal("rollback invalid policy failed")
			}
		}
	}
	payload, marker := policySnapshot(t, f.pool, f.c, f.r.Execution.Tenant)
	if payload != ingest.DefaultPayloadRetention.Microseconds() || marker != ingest.DefaultMarkerWindow.Microseconds() {
		t.Fatal("failed policy updates changed original settings")
	}
	// Exercise both legal bounds, including conversion without duration overflow.
	for _, value := range []int64{1, maxUS} {
		f.policy(t, "UPDATE "+f.c.table("projects")+" SET payload_retention_us=$2,marker_window_us=$2 WHERE id=$1", []byte(f.r.Execution.Tenant), value)
		payload, marker = policySnapshot(t, f.pool, f.c, f.r.Execution.Tenant)
		if payload != value || marker != value || time.Duration(payload)*time.Microsecond <= 0 || time.Duration(marker)*time.Microsecond <= 0 {
			t.Fatal("legal duration boundary failed exact positive conversion")
		}
	}
}
func TestProjectPolicyUpdateWaitsForSharedHolder(t *testing.T) {
	f := database(t)
	f.seed(t, f.r)
	reader, err := f.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(reader)
	first, _, err := readPolicy(t.Context(), f.c, reader, f.r.Execution.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	done := make(chan error, 1)
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		tx, err := f.pool.Begin(ctx)
		if err != nil {
			done <- err
			return
		}
		defer rollback(tx)
		if err = f.c.gate(ctx, tx, f.r.Execution.Tenant, "UPDATE"); err == nil {
			_, err = tx.Exec(ctx, "UPDATE "+f.c.table("projects")+" SET payload_retention_us=$2 WHERE id=$1", []byte(f.r.Execution.Tenant), (30 * 24 * time.Hour).Microseconds())
		}
		if err == nil {
			err = tx.Commit(ctx)
		}
		done <- err
	}()
	t.Cleanup(func() {
		cancel()
		_ = rollback(reader)
		select {
		case <-joined:
		case <-time.After(12 * time.Second):
			t.Error("policy writer did not stop")
		}
	})
	blocked(t, f.pool, reader.Conn().PgConn().PID())
	still, _, err := readPolicy(t.Context(), f.c, reader, f.r.Execution.Tenant)
	if err != nil || still != first {
		t.Fatal("waiting update changed the protected policy view")
	}
	if err = reader.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-done:
		if err != nil {
			t.Fatal("waiting policy update failed")
		}
	case <-ctx.Done():
		t.Fatal("policy update did not finish")
	}
	newer, _ := policySnapshot(t, f.pool, f.c, f.r.Execution.Tenant)
	if newer != (30 * 24 * time.Hour).Microseconds() {
		t.Fatal("fresh reader did not see committed project policy")
	}
}
