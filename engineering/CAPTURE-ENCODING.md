# Capture encoding experiment

Status: approved experiment, not a selected storage format. No public codec,
normalizer, endpoint or integration with ingest is introduced. Measurements cannot
select OpenTelemetry field semantics, canonical equality or a compatibility promise.

## Question and baseline

Compare one full JSON object (Go's `encoding/json`, byte slices encoded as Base64)
with JSON metadata followed by unchanged binary request and response bodies. Use
the same illustrative typed metadata and body values in both. This metadata is a
small workload fixture, not the agreed OpenTelemetry-compatible record model.

Put reproducible experiment code in `tools/capturelab` as test-only Go code. Normal
tests check the experimental invariants; benchmarks run only when explicitly
requested. Nothing in the application imports this lab. Use standard libraries;
the experiment does not add dependencies or persist a selected representation.

## Candidate representation and invariants

The hybrid candidate has a 30-byte header: four bytes `RWL1` (experimental version),
one request-presence byte, one response-presence byte, then three little-endian
unsigned 64-bit lengths for JSON metadata, request and response. Append the three
sections in that order. Presence is zero for missing and one for present. A missing
body must have length zero; present zero-length bodies remain distinguishable.
Redacted and truncated body states are outside this experiment, not collapsed into
missing or empty. A production format must define all those states separately.

Reject unknown versions, invalid presence, inconsistent lengths, trailing bytes,
invalid metadata JSON and truncated input. Limit metadata to 256 KiB, each decoded
body to 8 MiB, and each encoded object to 24 MiB. Check lengths against bounds and
remaining input before converting to `int`, slicing or allocating. JSON has the
same encoded-object and decoded-component bounds; its standard decoder can allocate
within the encoded-object cap before component checks. Neither codec is a streaming
network decoder. Callers own their input; returned encoded and decoded buffers are
independent, with no shared mutable state or goroutines.

Compress each complete encoding separately using standard-library gzip BestSpeed
and DefaultCompression as additional comparisons, not a production choice. The
default level checks whether an apparent format advantage depends on compressor
settings. Bound decompression at the
encoded-object cap plus one byte before decoding the inner representation. Reject
decompression errors and excess output. Do not pool buffers or reuse output across
iterations: measure straightforward independently owned results in both candidates.

## Workload and measurement

Use only synthetic fixtures: metadata-only small span, small text capture, repetitive
large text, varied large text, deterministic high-entropy binary, and already-gzipped
text bodies. Publish exact sizes and generation rules, fixed seed, Go version,
OS/architecture and hardware with evidence. These fixtures have no production
distribution or OTLP compatibility claim. Nil and empty body cases are correctness
fixtures, not evidence of a complete capture state model.

For JSON and hybrid, without gzip and with each gzip level, measure encoded bytes and repeated
encode/decode time, allocated bytes and allocation count. Precompute inputs outside
timed loops. Each decode receives a complete in-memory object; no network, disk,
catalog, hashing, authorization, admission or object-provider latency is measured.
Use five benchmark samples and report median and range rather than one result.
Record compressed-size comparisons separately from uncompressed storage ratios.

Round-trip against the original typed metadata and byte slices as the oracle. Test
all body-presence combinations, exact arbitrary bytes, bounds, bad framing and
gzip corruption. Fuzz each decoder with valid and malformed seeds: a successful
decode must re-encode and decode to the same supported values without panic.
Check output ownership by mutating input and output independently.

## Cost and decision boundary

For N encoded bytes and M metadata bytes, straightforward encoding and decoding
take O(N + M) work and O(N + M) allocated space; gzip adds compression state and
CPU work, with measured allocation distinct from retained process memory. Length
arithmetic must be checked even when input is malicious. Compression decoding must
not allocate in proportion to an unchecked advertised or expanded size.

Under an unchanged one-object-per-record policy all candidates use the same object
request count. Fewer bytes can reduce storage and transfer, but these benchmarks
measure neither provider billing nor end-to-end latency. No accepted object is
rewritten; corruption prevents decoding, and there is no catalog publication or
cross-store recovery experiment here.

Recommend a direction only after reviewing results. Revisit when real metadata,
body-size/content distributions, compression needs, range reads or an object layout
change are available. The simplest credible alternative remains full JSON; a
smaller hybrid result does not by itself justify a public binary protocol.

## Observed result and limits

Go 1.27.2, darwin/arm64, Apple M1, 16 GiB memory; GOMAXPROCS=1, five samples,
150 ms per benchmark. Construction is outside timed loops; each operation returns
independently owned data. There are six fixtures, six codec variants and two
operations (72 benchmark cases). Raw logs remain in ignored local evidence.

| Fixture (decoded body bytes) | JSON bytes | Hybrid bytes | JSON gzip default | Hybrid gzip default |
| --- | ---: | ---: | ---: | ---: |
| Span (0) | 348 | 334 | 251 | 236 |
| Small text (4,608) | 6,492 | 4,942 | 363 | 316 |
| Repeated text (327,680) | 437,256 | 328,014 | 1,917 | 1,327 |
| Varied text (327,680) | 437,256 | 328,014 | 75,541 | 56,256 |
| Binary (1,114,112) | 1,485,832 | 1,114,446 | 1,437,264 | 1,114,556 |
| Precompressed (69,854) | 93,484 | 70,188 | 77,295 | 70,218 |

In the uncompressed binary fixture, JSON encoding took median 630.671 microseconds
(range 627.184–654.151), allocating 1,491,330 bytes in three allocations. Hybrid
encoding took 69.472 microseconds (68.002–70.641), allocating 1,122,925 bytes in four.
JSON decoding took 2,654.361 microseconds (2,636.788–2,663.299); hybrid decoding took
54.054 (45.822–57.825). Both decoders made five allocations and about 1,114,412
allocated bytes per operation. More allocations does not necessarily mean more
allocated bytes. Timing is local and some other cases varied substantially.

The hybrid saves about 25% of uncompressed JSON size for large bodies, but only
14 bytes for the metadata-only fixture. Compression helps repetitive content and
costs CPU and working allocations; neither tested level saves space on the hybrid
binary or precompressed fixtures. For example, fresh default gzip state raises
hybrid binary encode allocation to median 4,484,660 bytes, versus 1,122,925 without
gzip. Allocations are not a retained-memory or peak-memory measurement.

This supports proposing JSON metadata plus raw bodies, with compression deferred.
It does not approve that format. There is no claim about every gzip level, another
compressor or a production distribution. A public format still needs all capture
states, exact supported metadata semantics, normalization, framing compatibility
and retries across encoder versions. In particular, gzip output is not covered by
Go's byte-for-byte compatibility promise; hashing compressed bytes does not settle
logical record equality across implementations or versions.
