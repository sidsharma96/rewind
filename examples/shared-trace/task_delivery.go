package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/sidsharma96/rewind/internal/httpapi"
)

func taskPlanName(i int) string { return fmt.Sprintf("%02d.plan", i+1) }
func syncDirectory(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return errors.New("task directory parent unavailable")
	}
	syncErr, closeErr := d.Sync(), d.Close()
	if syncErr != nil || closeErr != nil {
		return errors.New("task directory synchronization failed")
	}
	return nil
}

func readTaskPlans(o taskOptions) ([]plan, error) {
	info, err := os.Lstat(o.Directory)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("task directory must be private")
	}
	directory, err := os.Open(o.Directory)
	if err != nil {
		return nil, errors.New("task directory unavailable")
	}
	entries, readErr := directory.ReadDir(6)
	closeErr := directory.Close()
	if readErr != nil && !errors.Is(readErr, io.EOF) || closeErr != nil || len(entries) == 0 || len(entries) > 5 {
		return nil, errors.New("task directory must hold 1..5 saved observations")
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	expected := []string{"alice", "bob", "bob", "bob", "alice"}
	var plans []plan
	for i, entry := range entries {
		if entry.Name() != taskPlanName(i) {
			return nil, errors.New("unexpected task file; never regenerate a torn or incomplete plan")
		}
		b, err := private(filepath.Join(o.Directory, entry.Name()), 1<<20)
		if err != nil {
			return nil, err
		}
		var saved []plan
		if httpapi.DecodeDocument(b, &saved) != nil || len(saved) != 1 {
			return nil, errors.New("invalid saved observation; never rerun the task to repair it")
		}
		p := saved[0]
		if p.Execution.Project != httpapi.Opaque(o.Project) || p.Owner.Run != httpapi.Opaque(o.Run) || p.Owner.Instance != httpapi.Opaque(expected[i]) || p.Span.Ended == nil || !*p.Span.Ended || p.Capture.Request == nil || p.Capture.Response == nil {
			return nil, errors.New("saved task scope or observation invalid")
		}
		if i > 0 && p.Execution.Trace != plans[0].Execution.Trace {
			return nil, errors.New("saved task changed trace")
		}
		plans = append(plans, p)
	}
	return plans, nil
}

func deliverTask(ctx context.Context, plans []plan, host, alice, bob *httpapi.Client) error {
	for _, p := range plans {
		writer := alice
		if p.Owner.Instance == httpapi.Opaque("bob") {
			writer = bob
		}
		if _, err := host.Register(ctx, p.Execution, p.Owner); err != nil {
			return fmt.Errorf("task registration: %w; use task-retry with the saved directory", err)
		}
		span, err := writer.SaveSpan(ctx, p.Execution, p.Span)
		if err != nil {
			return fmt.Errorf("task span delivery: %w; use task-retry with the saved directory", err)
		}
		capture, err := writer.SaveCapture(ctx, p.Execution, p.Capture)
		if err != nil {
			return fmt.Errorf("task capture delivery: %w; use task-retry with the saved directory", err)
		}
		traceURL, err := writer.TraceViewURL(p.Execution.Project, p.Execution.Trace)
		if err != nil {
			return err
		}
		if err = json.NewEncoder(os.Stdout).Encode(struct {
			TraceURL  string            `json:"trace_url"`
			Execution httpapi.Execution `json:"execution"`
			Span      httpapi.Receipt   `json:"span_receipt"`
			Capture   httpapi.Receipt   `json:"capture_receipt"`
		}{traceURL, p.Execution, span, capture}); err != nil {
			return errors.New("task receipt output failed; use task-retry")
		}
	}
	return nil
}
