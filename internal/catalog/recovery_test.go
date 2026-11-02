package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/sidsharma96/rewind/internal/ingest"
)

type recoveryChild struct {
	cmd         *exec.Cmd
	in          *os.File
	out         *os.File
	decode      *json.Decoder
	encode      *json.Encoder
	done        chan error
	cancel      context.CancelFunc
	waited      bool
	dir         string
	diagnostics recoveryDiagnostics
}

// os/exec drains this writer before Wait returns. A noisy failed child cannot
// retain unbounded stderr; race diagnostics must not disappear when the writer
// is deliberately killed before its normal test exit.
type recoveryDiagnostics struct{ data []byte }

func (d *recoveryDiagnostics) Write(p []byte) (int, error) {
	n := min(len(p), (16<<10)-len(d.data))
	d.data = append(d.data, p[:n]...)
	return len(p), nil
}

func startRecoveryChild(t *testing.T, config recoveryConfig) *recoveryChild {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal("locate test executable failed")
	}
	stdin, input, err := os.Pipe()
	if err != nil {
		t.Fatal("open worker input pipe failed")
	}
	output, stdout, err := os.Pipe()
	if err != nil {
		_ = stdin.Close()
		_ = input.Close()
		t.Fatal("open worker output pipe failed")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	dir := t.TempDir()
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestRecoveryWorker$", "-test.count=1")
	cmd.Dir = dir
	// No home, credentials, accepted-state files or Go toolchain is inherited.
	cmd.Env = []string{"REWIND_RECOVERY_WORKER=1", "TMPDIR=" + dir, "GOTRACEBACK=single", "GORACE=halt_on_error=1 atexit_sleep_ms=0"}
	cmd.Stdin, cmd.Stdout = stdin, stdout
	cmd.WaitDelay = 2 * time.Second
	child := &recoveryChild{cmd: cmd, in: input, out: output, done: make(chan error, 1), cancel: cancel, dir: dir,
		decode: json.NewDecoder(io.LimitReader(output, 64<<10)), encode: json.NewEncoder(input)}
	cmd.Stderr = &child.diagnostics
	if err := cmd.Start(); err != nil {
		cancel()
		_ = stdin.Close()
		_ = stdout.Close()
		_ = input.Close()
		_ = output.Close()
		t.Fatal("start recovery worker failed")
	}
	_ = stdin.Close()
	_ = stdout.Close()
	go func() { child.done <- cmd.Wait() }()
	t.Cleanup(func() {
		child.cancel()
		_ = child.in.Close()
		_ = child.out.Close()
		if !child.waited {
			_ = child.cmd.Process.Kill()
			<-child.done
			child.waited = true
		}
	})
	if err := child.encode.Encode(config); err != nil {
		t.Fatal("send worker configuration failed")
	}
	return child
}

func (c *recoveryChild) event(t *testing.T, phase string) recoveryEvent {
	t.Helper()
	var event recoveryEvent
	if err := c.decode.Decode(&event); err != nil || event.Phase != phase {
		c.cancel()
		// Worker errors are deliberately fixed messages, never provider errors,
		// configuration or payload bytes. Bound diagnostics after cancellation.
		diagnostic, _ := io.ReadAll(io.LimitReader(io.MultiReader(c.decode.Buffered(), c.out), 4096))
		t.Fatalf("worker did not reach %s (got %q): %s", phase, event.Phase, diagnostic)
	}
	return event
}

func (c *recoveryChild) finish(t *testing.T, killed bool) {
	t.Helper()
	if killed {
		if err := c.cmd.Process.Kill(); err != nil {
			t.Fatal("writer did not remain alive until forced termination")
		}
	}
	err := <-c.done
	c.waited = true
	if len(c.diagnostics.data) != 0 {
		t.Fatalf("worker stderr (including race diagnostics): %s", c.diagnostics.data)
	}
	if killed {
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.Success() {
			t.Fatal("writer was not forcibly terminated and reaped")
		}
	} else if err != nil {
		t.Fatal("replacement worker failed")
	}
	_ = c.in.Close()
	_ = c.out.Close()
	c.cancel()
	if err := os.RemoveAll(c.dir); err != nil {
		t.Fatal("remove worker-local directory failed")
	}
	if _, err := os.Stat(c.dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("worker-local directory survived removal")
	}
}

func TestWriterProcessRecovery(t *testing.T) {
	for _, fault := range []string{"acknowledged", "put-reply", "commit-reply", "before-publish"} {
		t.Run(fault, func(t *testing.T) {
			f := database(t)
			writer := f.writer(t)
			peer := &recoveryObjects{peer: &publicationObjects{data: make(map[string][]byte)}, drop: fault == "put-reply"}
			server := httptest.NewServer(peer)
			t.Cleanup(server.Close)
			config := recoveryConfig{DSN: f.pool.Config().ConnString(), Schema: f.c.schema, ObjectURL: server.URL,
				Registration: f.r, Role: "writer", Fault: fault, CaptureAccepted: fault != "before-publish"}
			var proxy *recoveryProxy
			if fault == "commit-reply" {
				proxy = newRecoveryProxy(t, f.pool.Config())
				config.ProxyAddress = proxy.address
			}
			child := startRecoveryChild(t, config)
			switch fault {
			case "before-publish":
				child.event(t, "uploaded")
			case "commit-reply":
				child.event(t, "publishing")
				proxy.armed.Store(true)
				if err := child.encode.Encode(recoveryEvent{Resume: true}); err != nil {
					t.Fatal("resume publication failed")
				}
				child.event(t, "uncertain")
				if proxy.dropped.Load() != 1 {
					t.Fatal("proxy did not observe and withhold COMMIT plus idle ReadyForQuery")
				}
			default:
				child.event(t, "acknowledged")
			}
			child.finish(t, true) // Kill, join and remove cwd BEFORE creating replacement.
			bodies, _ := recoveryBodies(t, f.r)
			s, c := int64(len(bodies[0])), int64(len(bodies[1]))
			missing := int64(len("<Error><Code>NoSuchKey</Code></Error>"))
			before := peer.snapshot()
			want := recoveryTraffic{GET: 4, PUT: 2, Request: s + c, Response: 2*missing + s + c}
			if before != want || peer.peer.count() != 2 || peer.lost != (fault == "put-reply") {
				t.Fatalf("writer traffic/fault differs: got %+v want %+v", before, want)
			}
			ids := [2]ingest.Identity{f.reference(ingest.SpanRecord, "").Identity, f.reference(ingest.CaptureRecord, "").Identity}
			var original [2]ingest.Receipt
			for i, id := range ids {
				var err error
				original[i], err = f.c.Lookup(t.Context(), id, "reader")
				if i == 1 && !config.CaptureAccepted {
					if !errors.Is(err, ingest.ErrNotFound) {
						t.Fatal("pre-publication crash accepted capture")
					}
				} else if err != nil {
					t.Fatal("committed reference did not survive writer death")
				}
			}
			// This original receipt is an oracle only. It is NOT in config, stdin,
			// the new cwd, or any other input supplied to the replacement.
			config.Role, config.ProxyAddress = "replacement", ""
			replacement := startRecoveryChild(t, config)
			result := replacement.event(t, "recovered")
			replacement.finish(t, false)
			after := peer.snapshot()
			delta := after.minus(before)
			want = recoveryTraffic{GET: 7, PUT: 1, Request: s + 1, Response: 4*s + 2*c + 1 + missing}
			if !config.CaptureAccepted {
				want.GET-- // Catalog absence must not fetch the unaccepted object.
				want.Response -= c
			}
			if delta != want || peer.peer.count() != 3 {
				t.Fatalf("replacement traffic differs: got %+v want %+v", delta, want)
			}
			for i, receipt := range result.Receipts {
				if (i == 0 || config.CaptureAccepted) && !sameRecoveryReceipt(receipt, original[i]) {
					t.Fatal("replacement changed original receipt")
				}
				original[i] = receipt // Includes the first capture acceptance after pre-publish crash.
			}
			f.policy(t, "DELETE FROM "+f.c.table("reader_grants"))
			f.policy(t, "DELETE FROM "+f.c.table("writer_grants"))
			config.Role = "denied"
			denied := startRecoveryChild(t, config)
			denied.event(t, "denied")
			denied.finish(t, false)
			if peer.snapshot() != after {
				t.Fatal("revoked permissions reached object storage")
			}
			f.grantWriter(t, writer)
			f.policy(t, "INSERT INTO "+f.c.table("reader_grants")+" VALUES($1,$2)", []byte(f.r.Execution.Tenant), []byte("reader"))
			for i, id := range ids {
				receipt, err := f.c.Lookup(t.Context(), id, "reader")
				if err != nil || !sameRecoveryReceipt(receipt, original[i]) {
					t.Fatal("denied operation changed accepted state")
				}
			}
			t.Logf("writer killed/reaped; cwd removed; writer %+v; replacement read/retry/conflict %+v; denial zero object requests; orphan retained", before, delta)
		})
	}
}
