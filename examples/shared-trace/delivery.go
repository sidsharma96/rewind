package main

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/sidsharma96/rewind/internal/httpapi"
)

// deliverRecords requires confirmed registration and a saved retry plan. Span
// and capture are independently accepted records. Borrow both inputs read-only
// until both calls finish; a failure can leave either or both records accepted.
// One helper and the caller share the parent deadline and always join before
// return. Do not cancel a valid peer write merely because one reply failed.
func deliverRecords(ctx context.Context, writer *httpapi.Client, p plan) (span, capture httpapi.Receipt, err error) {
	if err := ctx.Err(); err != nil {
		return span, capture, err
	}
	var spanErr error
	var joined sync.WaitGroup
	joined.Go(func() {
		span, spanErr = writer.SaveSpan(ctx, p.Execution, p.Span)
	})
	capture, captureErr := writer.SaveCapture(ctx, p.Execution, p.Capture)
	joined.Wait()
	if spanErr != nil {
		spanErr = fmt.Errorf("span delivery: %w", spanErr)
	}
	if captureErr != nil {
		captureErr = fmt.Errorf("capture delivery: %w", captureErr)
	}
	return span, capture, errors.Join(spanErr, captureErr)
}
