# Go practices

Use standard Go formatting, ordinary control flow and names that describe the
domain. Prefer concrete types until a caller needs a genuine substitution or
capability boundary. Keep packages cohesive; avoid generic repository/service
layers, one interface per type and hidden mutable globals.

Return operational failures as errors. Add useful context with `%w`; use
`errors.Is` and `errors.As` for wrapped errors. Avoid secrets and captured payloads
in error messages. Check required writes, syncs and closes separately. Cleanup
must still run after a preceding operation fails; rollback does not undo remote
effects. Reserve panic/Must helpers for programmer-owned constants and invariants.

Pass contexts through blocking operations. Document who owns each goroutine,
buffer, connection and reader; who cancels, closes or releases it; and who waits
for completion. Cancellation is a request, not a join. Bound queued work, waiting
callers, bytes, goroutines, retries and metric-label cardinality. Explain overload
behavior rather than assuming a worker limit bounds memory.

Validate input at boundaries before allocating or using it. Bound streams before
decoding, check semantic relationships after parsing, and return explicit errors
for truncation or corruption. A typed identifier does not validate arbitrary casts.
Document byte encodings, checksums, limits and compatibility before persisting data.
Never normalize captured request bytes silently and still call them exact.

Put authorization before storage access. Keep event identity separate from content
hashes. Any deduplication or cache needs a stated tenant scope and retention policy.
Use synthetic fixtures; production prompts, responses and credentials do not
belong in tests, logs, benchmark artifacts or screenshots.

For algorithms, write down invariants and complexity with the relevant variables.
Include space, allocations, I/O requests, transferred bytes and worst-case inputs.
Use a straightforward implementation as the oracle. Fuzz decoders and malformed
inputs; use fault injection for publication/recovery boundaries and race tests for
actual concurrent state. Do not use arbitrary sleeps to establish event ordering.

Measure before optimizing. Distinguish a microbenchmark from a complete request,
allocated bytes from retained memory, and a warm cache from a cold read. Keep
workload, toolchain, hardware, sample count, variation, outcomes and limits with
the evidence. Negative and inconclusive results are useful results.

Sources: [Effective Go](https://go.dev/doc/effective_go),
[Go memory model](https://go.dev/ref/mem), and
[context cancellation](https://pkg.go.dev/context#CancelFunc).
