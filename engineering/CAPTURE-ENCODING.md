# Capture encoding experiment

Status: the user selected the hybrid layout: versioned framing, JSON metadata and
unchanged request/response bytes. This selects the representation direction, not
the lab's header, limits or illustrative metadata as a frozen production format.
No public codec, normalizer, endpoint or integration with ingest is introduced.
OpenTelemetry field semantics, canonical equality and compatibility remain open.
Retry comparison uses the accepted object when stored bytes differ; no separate
logical-content digest is selected. This is a design decision, not implemented
behavior; see [retry comparison](#retry-comparison-direction).
The selected compression policy has two gates: below a size threshold, store raw;
at or above it, try gzip and keep it only if smaller. The selected starting
threshold is configurable, defaulting to 32 KiB (32,768 bytes) for the complete
hybrid record before compression or an outer encoding marker. The gzip setting
and production representation remain open; the gzip variants are still test-only.

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

The selected direction follows the reviewed results below. Revisit when real metadata,
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

The user selected JSON metadata plus raw bodies and the two-gate compression
policy, with a configurable 32 KiB starting threshold. The gzip setting remains
undecided. There is no claim about every gzip level, another
compressor or a production distribution. A public format still needs all capture
states, exact supported metadata semantics, normalization, framing compatibility
and retries across encoder versions. In particular, gzip output is not covered by
Go's byte-for-byte compatibility promise; hashing compressed bytes does not settle
logical record equality across implementations or versions.

## Threshold experiment design

The user selected a size gate followed by a savings gate. Measure the size of the
complete hybrid record before compression, not just one body. At an exact threshold
the compressor runs. Keep raw on equal size as well as larger size; compression
errors fail the experiment's operation rather than silently producing success.
No attempt is made to modify an accepted object. A future raw/gzip representation
must carry an explicit encoding marker and bound expanded reads independently of
compressed size. It must not change defined logical record equality.

Extend only the test lab. Test selector boundaries and both outcomes with controlled
compressor callbacks, plus actual round trips with both gzip settings. The selector
may borrow its input; the record encoder first owns a new hybrid buffer and returns
only owned results. No pooling, retained caller buffers or goroutines. Codecs and
selection are experimental helpers, not public APIs or ingest integration.

Use fixed-seed synthetic repeated text, sixteen-word varied text, random binary and
already-gzipped random binary, with the existing illustrative metadata. Make complete
hybrid records exactly 4, 8, 16, 32, 48, 64, 128 and 256 KiB, plus one byte below and
above each candidate threshold (8, 32 and 64 KiB). Request bodies take one fifth of
the available body bytes; responses take the rest. Already-compressed bodies must
be valid separate gzip streams, not truncated prefixes. Publish fixture generation
rules and seed; this is not a production content or size distribution.

Compare never, always, 8 KiB, 32 KiB and 64 KiB compression policies, with BestSpeed
and DefaultCompression separately. Count the one-byte experimental encoding marker
in all selected-object sizes, whether raw or gzip; this is accounting, not a frozen
envelope format. Measure detailed compression encode/decode time and allocations
at the power-of-two sizes (4, 8, 16, 32, 64, 128 and 256 KiB). Derive the threshold
outcome for each case from its measured raw and compression-attempt paths, and
check those derived outcomes against direct selector runs at all sizes. There is
no weighted aggregate or production throughput claim. Do not time thousands of
equivalent threshold paths just to count the same compression work repeatedly.

Precompute only fixture values and decoder inputs outside timed loops; the raw
hybrid encoding remains inside each encoder benchmark. Include rejected compression
attempts in timing and allocations even though their stored result is raw. Use
five samples, 100 ms targets, GOMAXPROCS=1; report median and range. Run correctness,
race and fuzz work before timing, not concurrently with it. Reuse tested bounded
gzip decoding. A short fuzz run on the original codecs does not prove the new
policy, so boundary/fallback/error/ownership tests must exercise that policy itself.

For R raw bytes, selection without compression is constant work after the O(R)
hybrid encoding. A compression attempt adds work over R and holds raw and compressed
buffers plus gzip state; keeping raw afterward does not recover that CPU or past
allocation. Object request count is unchanged. Large incompressible records are
the negative control: every qualifying attempt costs work and saves no bytes.
The simplest alternatives remain never and always compress. Recommend a provisional
threshold from the measured trade-offs; selecting a production default still needs
its workload and budgets. Revisit with actual distributions, concurrent memory and
latency budgets, or another compressor. No production threshold or gzip level is
approved merely by this experiment.

## Threshold observations

The threshold lab ran with Go 1.27.2, darwin/arm64, Apple M1, 16 GiB memory,
GOMAXPROCS=1, five 100 ms samples per case. The 56 exact-size fixtures produce
504 directly checked policy outcomes. Timing covers 168 cases: 28 power-of-two
fixtures, three baseline paths and two operations. Threshold choices map to those
paths; no aggregate throughput or production size/content weighting is inferred.
Reported selected sizes include one experimental marker byte on both paths.

| Varied-text hybrid size before compression | Selected bytes, BestSpeed | Encode microseconds, median (range) | Encode allocated bytes |
| --- | ---: | ---: | ---: |
| 8 KiB | 2,129 | 62.780 (62.706–65.695) | 830,694 |
| 32 KiB | 7,416 | 135.067 (132.931–136.708) | 863,463 |
| 64 KiB | 14,395 | 237.942 (235.300–240.995) | 912,617 |

Those paths include hybrid encoding and the compression attempt. Raw encoding at
the same sizes took medians 1.579, 2.959 and 4.610 microseconds. At 32 KiB,
DefaultCompression stored 6,066 bytes, taking 222.612 microseconds
(221.891–225.061), with 1,125,614 allocated bytes versus BestSpeed's 863,463.
Fresh gzip state is relatively expensive for small records. These allocation
counts are not peak or retained memory, and no pooling was used.

The negative control still pays. At 32 KiB, random binary kept 32,769 selected
bytes on every policy. BestSpeed's attempted encode took 122.700 microseconds
(122.341–123.539) and allocated 888,296 bytes, compared with raw's 2.916
microseconds (2.900–2.970) and 33,344 bytes. Both random and already-gzipped binary
kept raw at every tested size, even when compression ran. This says nothing about
all binary formats or other metadata distributions.

An 8 KiB threshold compresses the 8 and 16 KiB text fixtures; a 32 KiB threshold
skips them, avoiding their compression attempts but giving up those savings. A
64 KiB threshold also skips 32 and 48 KiB. At 64 KiB and above all three thresholds
attempt the same work. Boundaries were checked at one byte below, exactly at and
one byte above each threshold. Equal-size compressed results and operational
compression failures were separately injected; an error is not a raw success.

The selected starting threshold is configurable with a 32 KiB default: avoid fresh
gzip state on small records while retaining substantial text savings above the
gate. Apply it to the complete hybrid record, including metadata and framing;
exactly 32,768 bytes is eligible for a compression attempt. This is a size gate,
not a maximum accepted record size or a bound on concurrent memory. An 8 KiB gate
favors small-text storage; 64 KiB skips more work. The selection is a tuning
hypothesis, not a measured optimum. Real size/content frequency, concurrent memory
and latency budgets can change it. BestSpeed remains the provisional recommendation
for the separate gzip-setting choice, which is not yet selected. Compression level
trades CPU/allocation for further size savings. No production codec or runtime
configuration is introduced by recording this default.

## Retry comparison direction

The selected strategy keeps the existing stored-object SHA-256 digest without
adding a separate logical-content digest to each catalog row. An authorized
retry with the same identity and matching stored digest/size can take the quick
path. If the stored bytes differ, read the accepted object, verify its stored
digest/size, decode within explicit bounds, then compare defined metadata values
and exact captured bytes and body states. Equal contents return the original
receipt; different contents conflict without replacing the accepted record.
Tenant/trace/span/kind remains record identity. A digest grants no authority.

This needs a writer-scoped catalog check, not the public reader lookup: a writer
does not automatically have project-reader permission. Only trusted coordinator
code may read accepted contents for this comparison; it must not return them to
the writer. Keep object reads and decoding outside catalog transactions. Final
resolution must acquire the normal catalog locks and recheck current ownership,
grant, run state, expiry and the accepted reference that was compared. A prior
comparison is not authority to bypass a later revocation or saved expiry. If the
reference changed during the comparison, do not reuse that result; fail explicitly
or retry within a fixed bound. A cleanup/read failure or corrupt object is an
operational/integrity error, not evidence of absence, equality or conflict. Preserve
original attribution, acceptance time and expiry on a duplicate. Do not rewrite
accepted objects or delete uncertain uploads.

The expectation is few different-byte, same-identity comparisons in healthy steady
state with one stable encoding. New identities have no accepted object to compare;
ordinary identical-byte retries need no extra comparison read; trace reads and
replays do not enter this ingest path. Mixed encoders, compression changes and
producer bugs can create bursts. There is no production traffic or measured
comparison frequency. Do not confuse this conditional rate with overall retry rate.

The alternative is a second digest over normalized contents, allowing equality
checks in the catalog without reading the accepted object. It adds hashing work
and retained catalog bytes, but avoids comparison object I/O and simplifies final
resolution. A disposable layout probe on PostgreSQL 16.13 Linux/arm64 used 100,000
synthetic rows based on the current record column types, with identical primary
keys and no digest index. Adding a 32-byte bytea digest increased heap plus index
size from 24,313,856 to 27,361,280 bytes, about 12.5% for that fixture. Padding and
page packing make this a layout-specific result, not a universal bytes-per-row rule.

A separate Go 1.27.2 darwin/arm64 Apple M1 probe used the lab's varied-text hybrid
fixtures, BestSpeed, GOMAXPROCS=1 and five 150 ms samples. At 32 KiB, an additional
SHA-256 over already-normalized bytes took median 15.297 microseconds
(14.815–16.843), with no allocation. Verifying, expanding and comparing an old
object took 186.448 microseconds (151.272–248.604), allocating 122,992 bytes.
Those are local component costs, not end-to-end strategies: network, authorization,
normalization, version conversion, database writes, WAL and replication were not
timed. The selected strategy can use less CPU as well as catalog space when extra
comparisons are uncommon. At a high comparison rate, object reads, decompression,
allocation and waiting can dominate. No fixed rate or monetary break-even follows.

For W eligible attempts per second and fraction q requiring an extra comparison,
the selected path adds up to W*q comparison reads per second. Each reads S stored
bytes and does O(S + R) work with O(S + R) transient space for R expanded bytes,
both bounded. The alternative adds an O(R) content hash on each attempt and a
digest on each retained row. Bound active work, including the comparison, rather
than launching an unbounded goroutine per retry. Revisit using measured comparison
rates, encoder versions, bytes, latency and failures, especially during upgrades.

The current opaque-byte coordinator still requires an identical returned reference.
Neither typed equality nor the selected fallback exists yet. Schema, normalization,
version compatibility, concrete capability/API changes and fault tests must precede
integration; this decision does not freeze them or add a production codec.
