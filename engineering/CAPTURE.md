# Capture direction and OpenTelemetry boundary

Status: the user selected a small explicit Rewind capture path first, with an
OpenTelemetry-compatible internal record and a later OTLP adapter. No public capture
schema or receiver is implemented. Internal typed ended-span admission and its
version-1 saved payload are implemented. Internal typed captures and saved-byte
playback are also implemented; public receiver schemas remain open. The selected
span-metadata model reuses OpenTelemetry's generated Go types, including resource,
scope and typed attributes. Rewind owns validation, normalization, exact capture
bodies, ownership and the saved format. The narrow JSON adapter below is selected;
the internal capture schema is specified below. The selected saved layout is versioned framing followed by JSON metadata and exact raw request
and response bytes. Compression uses a size gate, then keeps gzip only if smaller;
the selected starting threshold is configurable with a 32 KiB default for the
complete hybrid record before compression or an outer encoding marker. The selected
gzip starting setting is configurable BestSpeed. The internal envelope framing is now implemented by `record.EnvelopeCodec` below;
`record.SpanCodec` implements its final-span payload and `ingest.Spans` the
internal typed span save/read/retry path. `record.CaptureCodec` and
`ingest.Captures` implement the internal capture schema and saved-response operation.
Public capture admission and production byte limits remain open. The older `ingest.Store` still receives opaque bytes.
An isolated [encoding experiment](CAPTURE-ENCODING.md) compares full JSON with
JSON metadata and raw bodies, including compression. The user selected the hybrid
layout from its results. Its earlier body experiments remain test-only; the runtime
capture codec implements the bounded layout below and the span codec uses the
existing metadata JSON adapter and envelope.

## Typed final-span acceptance

`record.SpanCodec` defines one internal version-1 span payload: the existing JSON
metadata encoding of exactly one ResourceSpans, one ScopeSpans and one Span, with
inline resource/scope context and both schema URLs, under the span envelope tag.
Decode requires those exact cardinalities rather than silently selecting the first
record in a batch. Span and parent/link ID widths, nonzero execution IDs, supported
bounded graphs, unknown-wire refusal and ordered unique attributes are checked.
It preserves the selected numeric, flags, floating, timestamp, optional-shell and
unavailable-link evidence rules. This is a supported internal representation, not
a general OTLP semantic-conformance validator or a public capture endpoint.

Blank operation names remain blank and mean unknown descriptive evidence. Never
insert a placeholder into saved metadata; any eventual display fallback must be
identified as presentation. A later nonempty name under the same execution is
changed retry content. Revisit if a required consumer cannot safely expose an
unknown name, bringing that consumer and concrete failure before changing admission.
As inspected in November 2026, OTLP's pinned
[Span.name definition](https://github.com/open-telemetry/opentelemetry-proto/blob/a8951735f7801e8adfaec5c0ace9262771cfec6e/opentelemetry/proto/trace/v1/trace.proto#L138-L149)
calls for nonempty names while defining empty as unknown. Jaeger's pinned
[empty-name sanitizer](https://github.com/jaegertracing/jaeger/blob/2bef72022bbad6c692ac3aaf6c6a372c4b495b86/internal/jptrace/sanitizer/emptyspanname.go#L20-L44)
substitutes a label before trace storage. Rewind preserves the report because its
workload retains execution evidence; that separation of stored facts and eventual
display labels is our design, not Jaeger's behavior or an OTLP admission mandate.
Requiring a name would catch omissions earlier but refuse otherwise useful records.

Prepare bounds/checks borrowed input before explicitly cloning it, normalizes only
the owned clone, encodes metadata then the envelope, and returns owned metadata
and stored bytes or no partial results. Borrowed descendants must remain stable
throughout. Decode returns an independently owned graph after framing, parsing,
shape, identity and ordered-attribute checks; it never repairs saved metadata.
Callers supply independent positive metadata/graph/depth/stored/expanded bounds;
byte caps are not an exact peak-memory or elapsed-time guarantee.

`ingest.Spans.WriteSpan` takes the expected project/trace/span execution and an
explicit EndedSpan declaration. It enters the underlying Store's nonwaiting active
limit before graph work, matches IDs, obtains trusted writer attribution and calls
writer-scoped catalog inspection before preparation. No input owner label grants
authority. New records follow immutable object create then catalog publication.
The ordinary opaque Store retains exact-byte behavior, including for synthetic
capture fixtures. No arbitrary legacy opaque bytes become typed or authorized.

An existing exact reference resolves without object I/O. Different stored bytes
require one exact-size/digest accepted-object read outside catalog locks, bounded
span decoding, key cross-checking and MetadataCodec.Compare. Only trusted coordinator
code sees those accepted bytes; producer write access grants no reader permission.
InspectSpan returns an original receipt or absence; PublishSpan can discover a
concurrent winner that needs comparison; ResolveSpan rechecks current owner/grant,
run, saved expiry and the precise compared reference under the existing lock order.
Equal returns the original duplicate receipt; different conflicts. Changed or absent
comparison references, read/close/parse errors and cancellation cannot become
comparison outcomes. A racing absent write performs at most one winner comparison.
Original bytes, attribution, acceptance and expiry never change on a retry. Lost
publication replies remain uncertain; uncertain uploads are retained.

`ReadSpan` requires explicit project-reader permission, reads a verified catalog
reference through the same admission bound, decodes the span kind and checks the
saved IDs against the requested execution before returning an owned graph. It
looks up one known record, not all operations in a trace, and makes no capture or
trace-completeness claim. Tenant and acceptance authority remain in the catalog.

For existing differently encoded records this adds one comparison GET transferring
the accepted stored bytes; identical references add no object request. New records
retain the object adapter's pre-read/conditional PUT/verification sequence. These
are bounded operation counts, not latency measurements. Preparation copies bounded
metadata and sorts keyed attributes using the existing algorithm; active admission
covers preparation, JSON/envelope buffers, accepted-object decoding and comparison.
No workers, queues, new cache, shared engine, deadline policy or logical digest
column is added. Encoding then calling opaque Write is simpler but would falsely
conflict on equivalent presence/compression representations. Revisit the selected
comparison strategy for measured comparison frequency and object costs.

Native tests exercise two registered owners in one shared trace, separate reader
grants, exact and different-encoding retries, conflicts, concurrent collectors and
revocation/run/expiry/reference changes before final resolution. The process drill
submits a typed failed span and separately opaque capture, kills/reaps the writer,
removes its directory and decodes through fresh process clients, including lost
stored-PUT and actual typed PostgreSQL COMMIT responses. Surviving local services
and controlled principals do not establish machine loss, provider durability,
remote catalog recovery or real credential verification. Public capture-body
admission remains later work. Bounded trace enumeration and internal capture
playback now exist, with the limits described below.

## Typed capture and saved response

`record.Capture` contains one execution's trace/span IDs, request Body and response
Body. Project, owner, submitting principal and acceptance remain authoritative
catalog facts. Bodies preserve arbitrary raw bytes; they need not be JSON or UTF-8.
Each body has one required `BodyState`:

| State | Permitted bytes | Meaning for the saved-result consumer |
| --- | --- | --- |
| Missing | Zero bytes | No body was supplied |
| Redacted | Zero bytes | Producer reports deliberate withholding |
| Truncated | Any bounded partial bytes, including zero | Incomplete evidence, with no promised prefix, offset or original length |
| Present | Any bounded complete bytes, including zero | Complete body as reported by the producer |

Zero/unknown states are invalid. States describe the producer's report; they prove
neither truth nor authority. No automatic redaction/truncation is performed. Nil and
empty byte slices within the same state compare alike. A Present zero-byte response
is distinct from Missing. Accepted capture remains immutable; later complete bodies
cannot upgrade an accepted unavailable capture under the same execution identity.

The internal version-1 CaptureObject payload follows the common RWND envelope:
three little-endian unsigned 64-bit lengths for header, request and response, then
those sections in order. The 24-byte framing overhead is derived from three
eight-byte fields. The JSON header has exactly four required string keys:
`trace_id`, `span_id`, `request_state`, `response_state`. The writer emits lowercase
hex IDs. Decode accepts equivalent ID case, header field order and whitespace, but
refuses unknown/case-folded keys, duplicate decoded keys, missing fields, wrong
types, invalid UTF-8, invalid/nonzero-ID widths, unsupported states, inconsistent
states/bytes, extra JSON values and trailing/truncated frames. Earlier opaque or
experimental RWL1 payloads are not migrated or guessed. Version 1 covers this
internal payload as well as its envelope; incompatible changes need a version
decision, and this is not a frozen public receiver format.

`CaptureCodec` requires positive metadata/per-body caps and an explicitly configured
EnvelopeCodec. Actual header/body totals are checked against the expanded cap by
subtraction before body allocation; decode checks each uint64 against its component
cap and remaining input before conversion/slicing. Stored/expanded caps are independent,
and hypothetical component maxima need not all fit at once. No production defaults
are chosen. Prepare returns separately owned prepared bodies and stored bytes;
Decode returns separately owned bodies, so retaining a response does not pin the
entire expanded record. Compare validates both records before comparing IDs, states
and exact bytes. Header formatting/compression differences alone are equivalent.

The parser uses `encoding/json.Decoder.Token` for four exact string fields, rather
than ordinary struct decoding. Installed Go 1.27.2's `encoding/json/decode.go`
documents case-insensitive struct-key matching and later duplicate replacement;
both would hide ambiguous metadata here. Each accepted key/value is checked before
continuing, and the closing object must be followed by EOF. This bounded flat
schema adds no generic JSON tree framework.

`ingest.Captures` borrows a configured Spans coordinator's private Store, sharing
the complete-operation admission gate, authorizer, objects and catalog without
requiring a saved span. `CaptureCatalog` exposes only trusted kind-checked
InspectCapture, PublishCapture and ResolveCapture. Native wrappers share the
existing typed catalog transaction modes with spans; both kinds retain their own
guards. No table/index or object contract changes. Comparison reads occur outside
locks; final resolution repeats current owner/grant/run/expiry/reference checks.
New records create immutable objects before publication. Retries preserve original
bytes, receipt, attribution and times. Changed state or raw bytes conflict; parser,
integrity and operational errors are not comparison outcomes. Legacy opaque data
under an accepted capture identity fails closed without rewriting its acceptance.

ReadCapture returns owned decoded bodies with the exact authorized receipt used
for verification. Missing/expired records, object loss, corruption, unsupported
format, permission and backend failures remain errors, never fabricated body states.
Playback uses that same authorized read within one admission slot. It returns
`SavedResult{Execution, Receipt, Bytes, RequestState}` only for a Present response.
A Missing/Redacted/Truncated response returns `ErrResultUnavailable` with a zero
result. A Present zero-byte response succeeds. A Missing/Redacted/Truncated request
does not prevent returning a complete response; mandatory RequestState prevents
that result from implying a complete captured pair. No incoming request matching,
live fallback, span-status-derived exception or replay of arbitrary Go errors exists.
Inspect unavailable evidence through ReadCapture instead.

The simplest stricter alternative requires both bodies Present. It makes the pair
complete by construction but discards useful saved results solely because input
was withheld. The selected consumer returns saved bytes by authorized execution
identity. Revisit if a required consumer must match a new request, reconstruct a
tool exception or guarantee a complete pair, bringing its concrete failure before
changing this operation.

Pinned primary comparisons inform the boundary, not this wire format.
[OpenTelemetry semantic conventions v1.38.0](https://github.com/open-telemetry/semantic-conventions/blob/8e16df7981f94b70ed1b9795c995094af995184f/docs/gen-ai/gen-ai-spans.md)
describe input/output content separately as opt-in and discuss sensitive/large
content and external storage. That makes selective evidence a realistic workload;
it prescribes no Rewind playback rule. [VCR.py 8.4.0's cassette](https://github.com/kevin1024/vcrpy/blob/9daef64c0497f5ce1d125fe53ec83afccba4027c/vcr/cassette.py#L246-L274)
finds a recorded HTTP response using configured incoming-request matchers and raises
when none matches. Its default matcher list is URI/method, not every original byte.
That substitution workload differs from Rewind's explicit execution lookup and
does not supply our ownership or durability guarantees. Our inference is that
selective input retention fits saved-result inspection, while request substitution
needs its own matching contract.

Tests cover all 16 state pairs, arbitrary binary/empty bodies, ambiguous headers,
length overflow/truncation, independent caps, ownership and fuzz round trips. Native
tests check independent span/capture arrival, original receipts, compression retries,
current permission/run/expiry/reference failures and kind guards. The process drill
records Alice's handoff and Bob's separate failed/successful calls to a controlled
tool. Both writers are killed/reaped, directories removed and tool stopped before a
new process receives only project/trace and storage settings. It discovers all
three spans, refuses the missing failed result and returns the successful saved
bytes with RequestState=Redacted and original acceptance. The same sequence covers
actual capture COMMIT reply loss. Recovery performs seven GETs and no PUT/tool
requests in the controlled workload. Surviving local PostgreSQL/HTTP services do
not establish physical host loss, remote catalog placement or provider durability.

Preparation/decoding/comparison are linear in bounded header plus raw bytes, with
owned copies and gzip buffers, not an exact heap or hard CPU bound. A cold capture
read needs one authorized lookup and one GET of its stored bytes; exact stored
retries need no object request, while differently encoded retries need one original
GET and locked recheck. These are derived costs, not timing measurements. Public
admission, credentials, streaming/lifecycle records, application replay adapters,
production limits and full host/catalog recovery remain separate work.

## Saved-object kind discrimination

Use a small common envelope carrying a format version, record kind and encoding
marker. Each kind keeps its own payload schema. After verifying stored-object
integrity, a reader must check the declared kind against the kind expected from
the authorized catalog lookup before interpreting the payload. Refuse unsupported
versions, unsupported kinds and kind mismatches explicitly; never try a new kind
as a final span. The label is a cross-check, not a second authority for the record.
It cannot prove ownership, event identity, acceptance or reconstruct the catalog.
Envelope kind means span versus capture, not the span's descriptive OTLP kind such
as CLIENT or INTERNAL. Unknown numbers in that metadata field still follow the
separate preservation policy; refusing an unsupported record decoder changes none
of those numeric-value rules.

The internal `record.EnvelopeCodec` implements the selected fixed seven-byte
prefix: ASCII `RWND` at offsets 0-3, version 1 at offset 4, object kind at offset
5 (span 1, capture 2), and encoding at offset 6 (plain 0, gzip 1). These object
kind tags are separate from both `ingest.Kind` and OTLP span kind. Map the catalog
kind explicitly; do not cast its internal enum to a wire tag.

The version selects the whole saved-format revision, including the prefix and
kind-specific payload schemas together. A future incompatible change to either
requires an explicit supported-version decision. EnvelopeCodec frames bytes without
parsing their payload; SpanCodec now supplies the internal version-1 span schema.
Earlier synthetic composed-test payloads do not freeze a public format. Production byte
limits and public capture admission remain open; CaptureCodec supplies the internal
version-1 capture schema. Span and capture remain independent
records. Future lifecycle events need their separate identities and contracts;
reserving the ability to distinguish them does not implement or accept them.
No accepted object is rewritten. SpanCodec and ingest.Spans now implement typed
final-span preparation, payload decoding and logical retry resolution. Capture-body
schemas remain separate and CaptureCodec now implements them. Envelope Encode borrows stable prepared bytes and returns an
independent stored buffer. Decode expects caller-verified size/digest and a
catalog-authorized kind, returning independent payload bytes or nil on every
error. It checks the whole stored cap, minimum header length and magic, supported
version/kind/encoding, and the expected kind before expanding anything. Malformed
framing, unsupported labels, kind mismatch and excessive bytes have distinct
errors. Success still requires the caller to parse and validate the payload.

`EnvelopeLimits` require explicit positive expanded limits, a stored cap of at
least seven bytes, and room for framing/probe arithmetic. The zero codec refuses
operations. `DefaultCompressionPolicy()` supplies the agreed 32 KiB threshold
and gzip BestSpeed; callers may supply a positive threshold and a supported gzip
level. The threshold counts the uncompressed payload plus six non-encoding
prefix bytes. Equality attempts gzip; only a smaller complete object wins.
A bounded candidate writer stops when gzip cannot fit the stored cap or cannot
beat plain bytes. That size outcome falls back to plain when it fits; actual
compressor failures return no output. This limits the candidate's written length,
not buffer capacity, gzip's internal buffers, total allocations or elapsed CPU.

The codec has immutable configuration and no shared request state, object calls,
catalog calls or workers. Header dispatch has constant work; compression and
expansion depend on bounded input/output. The native composition test exercises
both encodings through real catalog publication, fresh authorized reads and the
controlled object peer, then checks the expected kind before metadata decoding.
It also preserves original encoded retries, separate capture objects and conflicts.
Store remains opaque; the separate Spans coordinator compares different encodings
for typed final-span retries.

The simpler alternative supplies kind only from catalog/request context and uses
separate typed decoders. That can be safe when every reader retains the correct
context. We keep the label so decoding can cross-check that context and an object
inspector can identify the payload type. Parsing a fixed bounded header has
constant work and auxiliary space and adds no object or catalog request. The prefix adds seven stored bytes (derived from its layout);
latency is unmeasured. Including kind in the bytes can prevent
cross-kind reuse of otherwise identical payload objects; content equality never
made span and capture the same event. Revisit a common envelope if concrete
per-kind layouts or measured overhead justify separating formats, retaining
explicit supported-kind dispatch under either design.

As inspected in October 2026, Protobuf's
[`Any` definition](https://github.com/protocolbuffers/protobuf/blob/a79f2d2e9fadd75e94f3fe40a0399bf0a5d90551/src/google/protobuf/any.proto#L128-L161)
puts a type identifier beside serialized message bytes. Its workload is embedding
heterogeneous protobuf messages; the mechanism supports explicit dispatch, not
Rewind's exact-body layout or storage guarantees. We are not adopting `Any` or
runtime schema fetching. OTLP's separately typed
[`TraceService.Export`](https://github.com/open-telemetry/opentelemetry-proto/blob/a8951735f7801e8adfaec5c0ace9262771cfec6e/opentelemetry/proto/collector/trace/v1/trace_service.proto#L27-L42)
and
[`LogsService.Export`](https://github.com/open-telemetry/opentelemetry-proto/blob/a8951735f7801e8adfaec5c0ace9262771cfec6e/opentelemetry/proto/collector/logs/v1/logs_service.proto#L27-L42)
illustrate the alternative: surrounding service context selects a message type.
That is telemetry transport, not standalone persisted-object identification.
Choosing the explicit label for Rewind is our inference from its multiple saved
record kinds and inspection needs, not a requirement imposed by those projects.

For the fixed header rather than a structured header, Prometheus v3.0.0
[`newReader`](https://github.com/prometheus/prometheus/blob/c5d009d57fcccb7247e1191a0b10d74b06295388/tsdb/chunks/chunks.go#L590-L605)
checks its time-series segment header's minimum size, magic and version before
chunk access. This supports bounded prefix dispatch, not its segment layout or
storage guarantee for Rewind. The credible structured alternative is illustrated
by etcd v3.6.0's
[`Record`](https://github.com/etcd-io/etcd/blob/f5d605a93abe2fdfa13880aab0f1f3a4203628dd/server/storage/wal/walpb/record.proto#L14-L18)
and [length-framed encoding](https://github.com/etcd-io/etcd/blob/f5d605a93abe2fdfa13880aab0f1f3a4203628dd/server/storage/wal/encoder.go#L108-L128):
a recovery log stores typed Protobuf records inside length-delimited frames.
Our whole-object workload needs three mandatory labels, not extensible header
fields or log alignment. We keep the fixed prefix to avoid another bounded parser
and generated schema. Revisit for an actual consumer requiring optional header
fields independent of payload changes; no speed advantage has been measured.

### Stored and expanded byte limits

The first common envelope will omit an exact uncompressed-payload length. Bound
stored input and actual expanded output separately, then fully decode and validate
the kind-specific payload. An envelope label still grants no authority. The
selected version/kind/encoding checks precede decompression after object integrity
and authorized expected-kind checks.

The two choices both require real limits. A declared length additionally permits
early refusal of an honestly declared oversized payload, supports size inspection
without decompression and can be checked against the result. Omitting it keeps
one less field and consistency rule in the whole-object format, at the cost of
possibly decoding up to the output limit before discovering excess. The extra
field's byte overhead would be small; no performance advantage is claimed for
omitting it. Revisit if a concrete reader needs expanded size before decoding,
block/range access is selected, or measurements justify early refusal.

Both the runtime envelope decoder and the lab decoder enforce an expanded-output cap with one extra probe byte and
check read/close failures before payload parsing. Input/output byte caps do not
bound total allocations, decompressor buffers or elapsed CPU time. Limit
configuration must be positive and leave room for the probe byte without integer
overflow; no production defaults are selected here. Successful gzip must reach
its validated end, since Close alone does not check the data checksum.

As inspected in October 2026, Parquet's pinned
[`PageHeader`](https://github.com/apache/parquet-format/blob/8c91fac200074c47af7ea5f1a87b0511245bb8a9/src/main/thrift/parquet.thrift#L830-L838)
contains compressed and uncompressed page sizes. That columnar page workload
supports the declared-size alternative; Rewind currently decodes whole objects.
[RFC 1952 section 2.3.1](https://www.rfc-editor.org/rfc/rfc1952) puts gzip's size in
the trailer modulo 2^32, so it supplies no trusted upfront allocation bound. These
mechanisms explain the choices; omitting another size field is our inference for
Rewind's agreed read path, not a requirement imposed by either format.

### Gzip member sequences

Accept one or more valid gzip members and parse their joined plaintext as one
kind-specific payload. A member is an independently compressed part with its own
header and trailer. Empty members are allowed. The stored cap covers every member
and header; the expanded cap covers their combined output. Validate every member
through the complete input before calling the payload parser. Refuse checksum/size
failures, truncated later members and non-member trailing bytes, including zero
padding. Return no usable payload on any failure. Two members do not become two
records, and compression grants no authority or retry identity.

Our writer emits one member; many incremental writes can feed that same compressor.
The reader follows ordinary gzip concatenation rather than imposing an extra
single-member restriction without a concrete consumer need. Requiring exactly one
member would narrow the format but needs an explicit decoder mode and a check for
remaining input. We have no measured speed or safety advantage for either policy.
Revisit if a required consumer cannot support complete member sequences or a
measured workload justifies a narrower rule. Changing a published format would
require an explicit compatibility/version decision.

As inspected in October 2026, [RFC 1952 section 2.2](https://www.rfc-editor.org/rfc/rfc1952#section-2.2)
defines consecutive members for general gzip interchange. Go 1.27.0's
[`Reader` and `Multistream`](https://github.com/golang/go/blob/go1.27.0/src/compress/gzip/gunzip.go#L60-L139)
join their output by default; its single-member mode supports formats needing
separate stream boundaries. Those mechanisms support both alternatives. Our choice
keeps standard reader behavior because Rewind needs one logical payload, not one
compression part. Runtime envelope and lab tests check joined output, whole-input/combined-output
limits and refusal before payload parsing when a later member fails. SpanCodec parses final-span
payloads; CaptureCodec now parses capture payloads. Byte caps alone do not bound elapsed CPU or peak memory;
empty members and headers consume stored bytes even when they add no output.

## Two entrances to one acceptance path

The selected metadata operations now live in one concrete `record.MetadataCodec`,
configured with explicit positive byte, traversal-work and nesting limits. Its
`Check`, `Encode`, `Decode`, `NormalizeOwned` and `Compare` operations keep the
existing bounded guard, JSON adapter, keyed ordering and direct comparison rules.
`Check` is a structural/representation guard, not complete semantic admission;
`Decode` parses owned metadata and does not bypass later checking. The copying
comparison remains a separate lab oracle. The move alone selects no production limits or public OTLP receiver. The separate
`EnvelopeCodec` handles framing; `SpanCodec` now defines the internal final-span
payload and the typed coordinator integrates acceptance.

The codec owns no connections, workers or mutable request state. Encoding and
comparison borrow stable inputs; decoding returns an independent graph. Owned
normalization checks the complete graph before changing attribute order and
requires exclusive ownership of every descendant and backing array. Retaining
callers explicitly clone before normalization. Errors remain errors, not evidence
of a conflicting retry. The lab delegates these operations and retains its
synthetic workload and independent oracles. The composed catalog test uses
the codec for its synthetic payload and fresh read, while Store remains opaque.

```text
Explicit Rewind capture -> decode ----+
                                     +-> validate record -> encode -> ingest
OTLP export -----------> translate --+
```

The second entrance is future work. Authentication supplies trusted tenant context;
neither a span's attributes nor its trace ID grants storage authority. Each entrance
must bound encoded and decoded input before passing owned record bytes to ingest.
The accepted record needs defined metadata normalization and exact-payload equality
independent of compression. Keep the stored-object digest for integrity and the
identical-bytes retry path. For different stored bytes under an existing identity,
the selected direction is to decode the accepted object and compare defined
contents; do not add a second logical-content digest. See the
[comparison design](CAPTURE-ENCODING.md#retry-comparison-direction) for authorization,
final rechecks and failure boundaries. This is not implemented. Do not compare raw
transport JSON or assume arbitrary protobuf serialization is canonical.

The record design must account for trace/span/parent identity, timestamps, kind,
status, typed attributes, events, links, flags, trace state, dropped-data counts,
resource context, instrumentation scope and their schema URLs. This is a list of
compatibility concerns, not an approved Go struct or support claim. Specify which
fields affect equality, supported value types, ordering/default rules and treatment
of unsupported fields under the selected refusal policy before freezing a version. Equal span IDs in unrelated tenants must not share
acceptance state.

Payloads are separate concepts from these metadata fields. Store exact captured
request/response bytes only when available, and distinguish missing, redacted,
truncated and present data, including a present zero-byte body. Those states do not
by themselves establish that a particular replay adapter has all required inputs.
Do not manufacture a body by serializing attributes and call it the original bytes.

## One span with its context

The user selected a logical span record that includes its resource, instrumentation
scope and both schema URLs alongside one generated span. Do not replace that
context with a reference to a separately retained context object. This repeats
context bytes between saved span records, but decoding a span's metadata does not
need a second context-object lookup or a shared-context retention protocol.
Physical batching and packing remain separate choices. Captured bodies and trusted
execution attribution remain separate from this descriptive metadata.

`internal/record` defines a concrete span/context representation and `SpanAt`,
selecting one span at zero-based resource/scope/span indexes in generated
`TracesData`. Selection takes context from that exact path, retain nil versus
present empty context messages and both schema URLs, and rejects invalid paths with
no partial result. Its `OTLP` method rebuilds a one-resource/one-scope/one-span generated
packet for the existing lab codec, without duplicating generated field definitions.
This is an internal association operation, not a complete saved format or validation
boundary. Do not add storage, authentication or ingest dependencies to it.

The identity operation `Span.IDs()` returns checked, owned
`traceid.Trace` and `traceid.Span` arrays from that selected generated span's binary
slices. It must return both zero values on every error, including a valid trace
paired with an invalid span. Nil metadata returns `record.ErrInvalid`; invalid
width/all-zero IDs return `traceid.ErrInvalid`. It neither alters nor retains the
original ID buffers. Callers keep the borrowed graph stable during extraction.
Context labels, parent IDs and links cannot supply the selected execution's key.

This operation is independent of completion: trusted ownership registration needs
an identity before the call ends, and future lifecycle events will use that same
execution. It grants no authority and does not accept a record. The selected typed
acceptance direction saves ended spans, including failures, once; unfinished
progress belongs in separately identified immutable lifecycle events when their
contract is implemented. Neither selection, reconstruction nor ID extraction
enforces ended-span admission yet. Do not confuse embedded OTLP span events with
separately acknowledged lifecycle records or treat a crash as a successful end.

Selection and reconstructed wrappers borrow resource/scope/span pointers, and are
read-only while used. New wrapper messages/slices do not imply a deep snapshot or
exclusive ownership of the reachable graph. Callers retaining a mutable input must
explicitly clone before in-place normalization. An owner can normalize its whole
fresh packet before selecting read-only views. Encoded bytes are the independent
output; no mandatory full-message clone is introduced by association itself.

Selection refuses unknown wire fields on the selected packet/resource-group/scope-group
wrappers before reconstruction can drop them. Unknown fields inside retained
resource/scope/span messages remain attached and the existing encoder guard must
refuse them. Unselected siblings are not part of the selected record; this operation
does not accept/reject an OTLP batch. Pin wrapper field coverage to the existing
protocol schema so a dependency upgrade cannot quietly discard a new known field.

Selection costs O(1) path access and unknown-slice length checks; reconstruction
allocates a constant number of wrapper messages and single-element slices. It does
not traverse or copy attributes or bodies. Encoding/validation retain their separate
costs. No object requests, catalog operations, workers or recovery effects occur in
this operation. Missing external shared context would be a new failure dependency
under the rejected alternative. Tests cover multiple resource/scope/span paths with
distinct contexts/URLs, optional-context presence, malformed paths, wrapper unknowns,
retained nested unknowns, pointer ownership and independent reconstructed shells.
The maintained lab encodes individual records with both grouping URLs and Resource
entity references, then decodes them after original input mutation. A property
fuzzer compares selected reconstruction against cloning/pruning the input, with
individual codec round trips for encodable selections. That is memory/format evidence, not host-loss
or remote durability evidence. SpanCodec and the typed coordinator now use this association for bounded internal
final-span admission; the internal capture schema is now implemented, while public
receiver semantics remain open.

## Explicit ended-span declaration

The selected final-span path treats submission as the authorized producer's or
scoped collector's explicit declaration that the operation has ended, including
an ended failure. It does not infer completion from status, timestamps, embedded
events, labels, registration or first arrival. It does not independently prove
that remote work stopped. The accepted final record will retain that report;
there is no prerequisite per-execution catalog completion transition. Existing
owner/delegation, run-admission, delivery and retention checks remain necessary.
A span's completion does not complete its run or start the run's delivery window.

`Span.DeclareEnded() (EndedSpan, error)` implements the internal declaration step.
It checks `Span.IDs()` and returns no declaration on missing/invalid execution
identity, preserving those errors. All status values and reported timestamps are
left unchanged; status support and remaining field validity are separate checks.
The call itself makes the declaration. Neither `SpanAt`, `IDs`, `DurationNanos`
nor reconstruction calls it implicitly. It does not call an SDK's `End` method.
`EndedSpan` has private contents; its zero value has no declaration and `View`
returns `ErrInvalid` with no partial span. A successful `View` returns the same
borrowed read-only metadata and inline context. Callers keep that whole reachable
graph stable while used. This is no immutable snapshot, ownership transfer,
authenticated capability or complete validated record. No IDs are cached beside
mutable metadata and no mandatory deep copy is introduced. The encoding lab takes
a declaration, reconstructs its one-span packet, and produces independently owned
bytes; a declaration does not bypass unknown-field or other encoder checks.
The internal typed coordinator now joins bounded SpanCodec preparation to current
owner-aware publication. The opaque `Store.Write` remains internal.

Declaration and view access use O(1) work/space, with fixed 24-byte identity checks
in the declaration, no graph traversal, catalog/object request or persistent
state. There is no throughput measurement. Tests cover explicit declaration with
unset/OK/error status and unknown/reversed/equal/ordered times, zero-value refusal,
invalid IDs returning no partial declaration, borrowed context, independent codec
output and unsupported-field refusal after declaration. These are memory and
encoding checks, not remote acceptance or host-loss evidence.

### Research and alternative

As inspected in October 2026, OpenTelemetry's
[`End` and `OnEnd` contract](https://github.com/open-telemetry/opentelemetry-specification/blob/b4bb23c6f2a5e30d3b729cfc425531d4262a6afa/specification/trace/sdk.md#L979-L989)
and Go [`recordingSpan.End`](https://github.com/open-telemetry/opentelemetry-go/blob/983defdf55ffda64c2d3ddebcb85c5a5af2eb08a/sdk/trace/span.go#L396-L475)
provide local completion signaling before export, separately from error status.
They do not establish receiver authentication or durable storage. Jaeger's
[`storageExporter.pushTraces`](https://github.com/jaegertracing/jaeger/blob/2bef72022bbad6c692ac3aaf6c6a372c4b495b86/cmd/jaeger/internal/exporters/storageexporter/exporter.go#L52-L54)
sanitizes trace data and calls its trace writer; this inspected boundary has no
separate per-span completion operation. It is a trace-storage comparison, not a
whole-system absence proof or a model for our timestamp/authorization policies.

The credible alternative is authoritative execution-lifecycle state. Temporal's
[`respondactivitytaskcompleted.Invoke`](https://github.com/temporalio/temporal/blob/b03650523460b098cbaacf97192fc45a0c48d995/service/history/api/respondactivitytaskcompleted/api.go#L54-L117)
checks running state and attempt information and adds a completion event in a
workflow update. Its
[`AddActivityTaskCompletedEvent`](https://github.com/temporalio/temporal/blob/b03650523460b098cbaacf97192fc45a0c48d995/service/history/workflow/mutable_state_impl.go#L3656-L3699)
includes the result and removes the pending activity when applying the event.
That workload needs completion to advance orchestration. A completion transition
can be published with the result; two client requests and a separately visible
completed-without-result interval are not inherent costs of this alternative.
It still consumes a worker's report rather than independently witnessing physical
cessation. Rewind keeps final-span submission as a report because its current
contract is storage and replay of evidence. Revisit authoritative per-execution
transitions if the finished system must gate attempt eligibility, advance work or
expose completion independently of final-span delivery. Independently identified
lifecycle records remain a required future path under either choice.

## Reported timestamps and derived duration

The selected policy keeps original span timestamps when end is before start,
exposes a timing warning and unavailable duration, and excludes that unusable
duration from duration calculations and timed replay. This does not relax identity,
authorization, bounds or unsupported-metadata rules. AcceptedAt and retention remain
catalog-owned; producer clock values cannot move those decisions.

Zero-valued endpoints remain raw zero but mean unknown for timing use. Preserve
the other endpoint if it is nonzero; do not fill unknown time from arrival time,
infer completion or reject an otherwise admissible final span for this reason alone.
This intentionally gives up timing a genuine exact Unix-epoch-zero endpoint from
this representation. Interpreting zero literally would instead turn an omitted
start into a seemingly measured decades-long call. A missing span is still invalid;
this policy does not create a span when metadata is absent.

The `Span.DurationNanos() (uint64, bool)` operation derives an unsigned
nanosecond difference only after checking both endpoints are nonzero and ordered.
Nil metadata, either zero endpoint or end before start returns `(0, false)`;
equal nonzero timestamps return `(0, true)`. The boolean must
be checked: unavailable is different from a measured numeric zero. Return the exact
unsigned difference, without narrowing it to Go's signed `time.Duration` or
floating point. No normalization, mutation, cached warning field or saved schema
change is needed. The borrowed metadata must remain stable while read.

This helper applies the timing interpretation and order check only. A true result
does not establish clock accuracy, ended status, authorization or complete record
validity. Unknown is a Rewind interpretation, not proof that the sender omitted a
field. A future warning consumer must first distinguish missing metadata, unknown
endpoints and reversed nonzero endpoints by inspecting the unchanged fields; the
availability boolean is not a warning code. Runtime acceptance,
warning presentation, aggregates and replay consumers remain unimplemented.

The operation uses O(1) fixed-width work and space with no allocation, graph copy,
object/catalog request, transferred bytes or recovery state. An unconditional
unsigned subtraction can wrap to a huge positive duration when end is smaller.
A signed duration conversion can lose values above its positive range. Compare
with a signed arbitrary-precision subtraction oracle in maintained tests; round-trip
reversed, unknown and known zero-duration metadata through the lab codec without
changing it. Check the explicit nonzero precondition separately from the arithmetic
oracle; full-range nonzero subtraction must not narrow to a signed value.

### External evidence and its limits

- The [OTLP span schema](https://github.com/open-telemetry/opentelemetry-proto/blob/a8951735f7801e8adfaec5c0ace9262771cfec6e/opentelemetry/proto/trace/v1/trace.proto#L168-L181)
  uses proto3 `fixed64` timestamp fields without `optional`. They represent Unix
  epoch nanoseconds and are semantically required; the comments do not reserve zero
  as missing. [Protobuf presence rules](https://protobuf.dev/programming-guides/field_presence/#semantic-differences)
  explain that the generated API cannot distinguish omitted and explicitly zero
  scalars. This establishes ambiguity, not a standard mandate to reject zero.
  Rewind chooses unknown for inspection/playback while preserving the raw fields.
- Jaeger's `adjustTimestamps` at revision
  `2bef72022bbad6c692ac3aaf6c6a372c4b495b86`,
  `cmd/jaeger/internal/extension/jaegerquery/internal/adjuster/clockskew.go`, shifts
  start/end/events at query time and records a warning. Its RPC alignment assumes
  equal request/response network latency. This is presentation correction for
  cross-host skew, not evidence of a general reversed-span ingestion policy.
- Tempo's latency subprocessor at `d0e737c714489ab62e0a2bb00402687161b92132`,
  `modules/generator/processor/spanmetrics/spanmetrics.go:248`, and Collector's
  `aggregateMetrics` at `f953484a157760796bb45c80972eb32ddc7bce6e`,
  `connector/spanmetricsconnector/connector.go:450`, use zero latency for reversed
  timestamps. Their workload is metric generation. Rewind chooses unavailable
  duration for inspection/playback rather than presenting a fabricated zero;
  future metrics must account explicitly for omitted duration samples.
- OpenTelemetry Go's `monotonicEndTime` and `recordingSpan.End` at
  `983defdf55ffda64c2d3ddebcb85c5a5af2eb08a`, `sdk/trace/span.go`, use elapsed
  monotonic time for ordinary completion; an explicit end timestamp overrides
  that path. A wall-clock adjustment does not normally imply a negative span
  duration when the SDK's start/end path retains monotonic readings.
- Honeycomb's [event-latency investigation](https://www.honeycomb.io/blog/ingest-timestamps-debug-event-latency)
  compares reported event time with API acceptance time. Its documented Lambda
  export delay illustrates diagnostic value of separate times, not a policy for
  accepting reversed span timestamps.

These mechanisms inform the selected policy; they do not establish a universal
observability ingestion rule. Revisit explicit timestamp presence if exact epoch-zero
timing becomes a supported workload, and revisit admission, query-time presentation
and aggregate sample accounting with their own concrete workloads. Do not infer
causality or silently adjust stored evidence from a timestamp-only comparison.

## Generated metadata and JSON

Use `go.opentelemetry.io/proto/otlp` v1.9.0 for the selected generated types. Its
source repository's protocol submodule is exactly the existing reference commit
below. Use protobuf v1.36.12, a compatible patch in the runtime family required by
that module; both require Go 1.23 or later. This pin is a reproducible starting
point, not a claim of compatibility with every newer protocol field. Generated
messages are mutable: accepted records will need validation and owned snapshots.

The test-only JSON check in `tools/capturelab/otlp_json_test.go` uses fixed synthetic
generated messages, without adding an application API or changing the earlier
toy-type size/timing fixtures. It exercises resource,
scope and span with both schema URLs, typed attributes, events, links, flags,
status, counts and exact integer timestamps. It compares decoded messages with their
originals for structural value fidelity, not as a complete logical retry rule.
For floating values this excludes NaN diagnostic bits, as the
[floating-value contract](#floating-metadata-values) defines.
It checks integer 12 versus text "12", the pinned AnyValue alternatives including
empty, large signed integers and metadata bytes. Captured bodies remain separate.

The protocol-aware `protojson` candidate round-trips the tested known fields and
types; ordinary `encoding/json` fails to decode the generated oneof value types.
The check inspects integer and ID representations and independent buffer/message
ownership. ProtoJSON emits large integers as decimal strings, IDs as Base64 and
enums as names by default. Its JSON round trip loses the injected unknown protobuf
wire field at both the root and a nested attribute, while preserving known fields.
Default ProtoJSON decoding refuses an unknown JSON field but accepts an empty Span
without IDs. These are observed library limitations; the selected internal
unsupported-field policy below refuses that loss before encoding. Successful
conversion cannot replace acceptance validation. Fixed
in-memory fixtures are not a bounded network decoder or an OTLP receiver. No
performance or remote-durability claim follows.

## Selected narrow JSON adapter

Use `protojson` with numeric enums, adapting IDs only along the generated trace
schema's resource/scope/span path and the span's direct links. Convert trace IDs,
span IDs and parent span IDs between Base64 and hexadecimal; leave metadata
`bytesValue` in Base64 and ordinary attributes named `traceId` unchanged. Reuse the
generated schema instead of maintaining parallel structs for every field.
This selects encoding conventions, not an OTLP endpoint or the complete saved record.

`record.MetadataCodec` in `internal/record/metadata_json.go` implements conversion.
The lab delegates to it on bounded in-memory JSON fixtures. It preserves tested typed values and context, uses
raw JSON fragments rather than floating-point maps, rejects duplicate keys before
map conversion and checks nonempty ID lengths. Tests inspect exact output fields,
compare an independently produced Collector JSON fixture, exercise malformed input
and ownership, and fuzz the paired decoder. An input-value traversal budget and
depth check stop oversized or cyclic Go messages before serialization; these bounds
cover known fields, not opaque unknown protobuf storage. The fixture byte/depth
bounds are experimental, not selected production admission limits. An encoder's
check after serialization limits returned bytes, not peak memory used
to serialize an already allocated Go message. A separate temporary module checked
the maintained helpers against Collector pdata v1.70.0 in both directions for 22
synthetic value cases (44 structural comparisons). No Collector dependency was added;
normal tests retain one synthetic inline peer fixture. The temporary module was
removed after its compact evidence was saved.

A full handwritten mapping is the simplest schema-specific alternative but duplicates
field definitions and conversions. Collector `pdata` provides a public OTLP JSON
codec with a different data model; bridging from the selected canonical generated
types needs protobuf encoding/decoding or field mapping. No new runtime dependency
or Collector service is selected. The Go SDK's generated-type OTLP JSON codec is
`internal` and cannot be imported by Rewind.

For B metadata JSON bytes the narrow conversion adds linear scans/copies at fixed
schema depth and O(B) temporary space; no object requests, catalog transactions,
workers or remote I/O. It does not process captured bodies. There is no measured
performance advantage over plain ProtoJSON. Do not depend on byte stability across
builds or confuse compact/sorted outer JSON with canonical logical contents: the
selected retry strategy still needs its decoded comparison fallback.

The selected internal-record policy refuses unsupported metadata before saving,
rather than save only the known portion. The metadata encoder checks unknown
protobuf wire bytes at every visited message before calling ProtoJSON. That includes
resource/scope context, span, events, links, status, nested arrays and key/value
values, not only the packet root. Refusal must preserve the original message and
return no encoded bytes. Ordinary new attribute names remain supported: their keys
are data inside known schema fields. Numeric kind/status values follow the
[preservation policy](#numeric-kind-and-status-values) below; remaining semantic
validation is a separate contract.

Default strict ProtoJSON decoding already refuses unknown JSON field names in the
metadata decoder; it must not enable `DiscardUnknown` for saved metadata. This
is different from OTLP JSON reception, whose standard requires ignoring unknown
transport fields. A future adapter must document that projection and cannot claim
the JSON saved record preserves every original transport field. A public OTLP adapter is absent; internal typed final-span acceptance now uses
these strict saved-metadata rules. The encoder returns `record.ErrMetadataUnsupported`
for visited unknown wire metadata; public error mapping remains unselected.
`otlp_unknown_test.go` checks 25 message positions, direct injection and a real
binary-protobuf parse, default JSON refusal and unchanged input. The raw ProtoJSON
loss check remains the simple negative oracle; it is not a permitted save path.

The simplest alternative is dropping unsupported portions, which accepts more
mixed-version input but loses evidence before retry comparison or replay can inspect
it. Opaque preservation would require an explicitly versioned representation and
is not selected. A schema upgrade can explicitly add supported fields later; it
must revisit this check and the saved-format compatibility rules. The recursive
unknown-field portion uses the existing bounded walk: one visit per known field
and value, O(depth) stack, and constant work to check the length of each message's
unknown byte slice. It neither copies nor parses that slice and does no object or
catalog operation. The uniqueness check below adds its own time and memory costs. Parent-only
checking misses nested loss; clearing unknown bytes would violate the policy.

Exact accepted field names, semantic validation, normalization and saved-version
rules remain open. The paired lab decoder is not a standards-complete
network parser. Aliases for converted IDs or their parent paths are refused so
ProtoJSON cannot reinterpret unconverted hexadecimal text as Base64. Other accepted
ProtoJSON spellings/defaults do not define OTLP reception.
OTLP JSON's transport rule to ignore unknown fields is separate from the policy
that refuses unsupported fields still present in an internal record. Error/corruption
must not turn into absence or equality. Revisit the narrow adapter if special cases grow
or real profiling shows material overhead; changing the core model requires review.

Pinned implementation references:

- [Collector pdata v1.70.0 JSON codec](https://github.com/open-telemetry/opentelemetry-collector/blob/pdata/v1.70.0/pdata/ptrace/json.go),
  `JSONMarshaler.MarshalTraces` and `JSONUnmarshaler.UnmarshalTraces`.
- [Collector protobuf bridge](https://github.com/open-telemetry/opentelemetry-collector/blob/pdata/v1.70.0/pdata/ptrace/pb.go),
  `ProtoMarshaler` and `ProtoUnmarshaler`.
- [Go SDK v1.47.0 internal codec](https://github.com/open-telemetry/opentelemetry-go/blob/v1.47.0/exporters/otlp/otlptrace/otlptracehttp/internal/otlpjson/export_trace_service_request.go),
  `MarshalExportTraceServiceRequest` and the parallel field mappings.

## Numeric kind and status values

Preserve the exact signed 32-bit numbers in `Span.Kind` and `Status.Code`, including
numbers absent from the pinned enum definitions. They are uninterpreted evidence,
not proof of a valid future category. Do not silently coerce them to INTERNAL, OK,
ERROR or UNSET. Future viewers, category-dependent queries/aggregates and supported
replay adapters must handle an explicit unknown case and report when the operation
needs a meaning they cannot supply. In particular, `code != ERROR` must not imply
success. This is a selected acceptance/consumer contract; the current implementation
is test-lab encoding, not runtime validation, warning presentation or replay.

Revisit this policy if a **required consumer cannot safely expose an unknown
category**. At that integration checkpoint, name the consumer, show its concrete
failure case, and compare safe explicit unknown handling with narrower acceptance
before changing the policy. Do not silently broaden refusal or invent an ordinary
category to make the consumer work. This remains an open integration risk until
actual consumers are implemented and checked.

The current codec retains these numbers through owned attribute ordering, JSON
encoding and decoding. Signed 32-bit limits are inclusive. The paired decoder
refuses out-of-range or non-integral numeric input and unsupported enum names, with
no partial decoded message. A future input path must check range before narrowing
instead of accepting a wrapped value. Known zero-valued kind UNSPECIFIED and status
UNSET are distinct from arbitrary unknown numbers; their raw values stay unchanged.
Optional-status presence follows the comparison-only rule below; remaining
default/equality rules are separate work.
Different unknown numbers must remain different defined contents for eventual retry
comparison; the production equality fallback is not implemented here.

This policy applies only to numbers in those two known fields. Unknown protobuf
wire fields still fail before JSON encoding, because that path would lose them;
unknown saved-JSON fields and new record kinds remain refused by their own checks.
It grants no permission, completion proof or exception to any other validation.
The separate flags policy below does not select a public saved format.

As inspected in October 2026, protobuf-go v1.36.12's
[`unmarshalEnum`](https://github.com/protocolbuffers/protobuf-go/blob/cdd4c5f7406e82462949c7a65defa9f3029c162d/encoding/protojson/decode.go#L498-L522)
accepts a numeric token in signed 32-bit range without requiring enum membership,
and its [encoder](https://github.com/protocolbuffers/protobuf-go/blob/cdd4c5f7406e82462949c7a65defa9f3029c162d/encoding/protojson/encode.go#L327-L336)
writes unknown values numerically. Collector pdata v1.70.0's
[`ReadEnumValue`](https://github.com/open-telemetry/opentelemetry-collector/blob/f532e15ef29b6ed7fefad2b23a0fefd27120ef9b/pdata/internal/json/iterator.go#L213-L231)
also reads numeric values directly; its
[status codec](https://github.com/open-telemetry/opentelemetry-collector/blob/f532e15ef29b6ed7fefad2b23a0fefd27120ef9b/pdata/internal/generated_proto_status.go#L135-L149)
writes them numerically. These support representation/forwarding, not validity of
unknown categories or safe interpretation by all downstream consumers. Collector's
[`StatusCode.String`](https://github.com/open-telemetry/opentelemetry-collector/blob/f532e15ef29b6ed7fefad2b23a0fefd27120ef9b/pdata/ptrace/status_code.go#L20-L30)
returns empty text for unknown values, illustrating why a familiar label cannot
be assumed. The OTLP [enum definitions](https://github.com/open-telemetry/opentelemetry-proto/blob/a8951735f7801e8adfaec5c0ace9262771cfec6e/opentelemetry/proto/trace/v1/trace.proto#L153-L178)
give zero kind a defined meaning; that does not define a meaning for number 99.

The simplest alternative refuses an entire final span if either number is unknown.
It limits accepted categories and avoids ordinary storage writes for that refused
attempt, but can discard useful metadata under version skew or instrumentation
bugs. Preservation uses the normal accepted-record path without an enum sidecar or
extra metadata lookup. It does not promise the same total I/O as early refusal.
Both strategies need bounded scalar checks; neither has a measured throughput
advantage here. Codec costs remain linear in bounded metadata size with no new
workers, catalog operations or recovery state in the lab.

`otlp_enum_test.go` checks known zero, ordinary known values, unknown 99, negatives,
both signed boundaries and a pair of unknowns. Whole-message comparison against a
pre-encoding clone checks numeric fidelity, context and input nonmutation. Decoding
after original-message mutation checks independent encoded output. Negative controls
cover overflow, fractional input, unsupported names and wrong types. Unknown numbers
do not bypass unknown-wire refusal. A property fuzzer varies both signed 32-bit
values through ordering/encoding/decoding. These are representation and refusal
checks; no full admission, consumer-safety, performance or remote-durability result
follows.

## Floating metadata values

Compare floating metadata by exact numeric value, with every NaN equal to every
other NaN and positive/negative zero equal. Distinct finite nonzero values, including
adjacent representable values, remain different. Signed infinities remain distinct.
Never use a tolerance, round an attribute before comparison or equate different
numeric types: integer 12, double 12 and text "12" are different typed contents.
NaN must not become zero, missing data, success or an ordinary finite value.

The selected JSON metadata representation writes NaN as one explicit string value.
It deliberately does not preserve NaN diagnostic payload, sign or signaling bits.
Ordinary double bit patterns, both zero signs and signed infinities survive the
codec. Saving does not rewrite a zero sign just because the signs compare equally
for retries. Equivalent retries retain the original accepted encoding, attribution,
reference and deadlines. Exact captured request/response bytes and their states
keep their separate rules; this is not a numeric interpretation of capture bodies.

The current lab codec already has these representation properties. The maintained
comparison projection uses `proto.Equal` on owned projections, which supplies the
selected float semantics. New tests compare it with an independent oracle on the
input IEEE 754 bit classes: zeros form one class, NaNs form one class, all other
bits must match. Tests cover 15 patterns and 225 paired comparisons, signed zero,
multiple NaNs, extreme finite/subnormal values, signed infinities and adjacent
finite numbers. Non-NaN round trips require exact output bits, not just value
equality; encoding/ordering must leave original input bits unchanged even for NaNs.
Decoded output must survive source mutation. Nested checks cover resource, scope,
span, event and link attributes plus array/key/value positions; different types
and malformed double input remain distinct or refused. A fuzzer varies two uint64
patterns through codec and comparison against that bit-class oracle. These are
bounded local checks of the metadata comparator, not complete semantic admission,
consumer validation or remote durability evidence.

The simplest alternative requires exact float bits as evidence and retry identity.
It can preserve numerical debugging details, but needs a saved representation that
retains NaN bits before comparison; comparing bits after JSON decoding cannot
recover them. Zero sign can affect operations such as division, and NaN diagnostic
bits may carry useful low-level evidence. We choose value comparison for typed
tracing metadata under the selected JSON direction, accepting those limits.
Revisit if a required consumer or supported workload needs zero sign or NaN bits
as retry-significant evidence. Name it, reproduce the failure and compare a
bit-preserving representation or narrower support before changing this contract.
Actual numeric/replay consumers remain unimplemented.

Scalar value comparison has constant time/space work and adds no stored metadata,
worker or object request. Existing bounded traversal, sorting, copying and decoding
keep their own costs. Exact-bit comparison is also constant per scalar; its saved
representation/version costs depend on the design and are unmeasured. The selected
different-bytes retry read and final authority/expiry/reference recheck still apply.

As inspected in October 2026, protobuf-go v1.36.12's
[`equalFloat`](https://github.com/protocolbuffers/protobuf-go/blob/cdd4c5f7406e82462949c7a65defa9f3029c162d/reflect/protoreflect/value_equal.go#L81-L87)
uses numeric equality plus a both-NaN case. That is protobuf value comparison,
not bit-forensic identity or Rewind authorization. Its JSON
[`appendFloat`](https://github.com/protocolbuffers/protobuf-go/blob/cdd4c5f7406e82462949c7a65defa9f3029c162d/internal/encoding/json/encode.go#L148-L170)
and Collector pdata v1.70.0's JSON
[`WriteFloat64`](https://github.com/open-telemetry/opentelemetry-collector/blob/f532e15ef29b6ed7fefad2b23a0fefd27120ef9b/pdata/internal/json/stream.go#L89-L107)
serialize all NaNs using one spelling. Those transport codecs support the selected
representation's feasibility, not its suitability for every numerical consumer.
Protobuf-go's binary
[`appendDouble`](https://github.com/protocolbuffers/protobuf-go/blob/cdd4c5f7406e82462949c7a65defa9f3029c162d/internal/impl/codec_gen.go#L4693-L4697)
writes `Float64bits` as fixed64, illustrating a credible bit-preserving alternative.
That observation does not select a different metadata format for Rewind.

## Span and link flags

Preserve the exact unsigned 32-bit `Span.Flags` and each `Span_Link.Flags` received
as metadata. Unknown bits remain uninterpreted evidence; do not clear them, narrow
the whole value to a byte or refuse a record solely because a bit is unfamiliar.
Compare each complete value for eventual retry equality. For example, `0x80000001`
and `1` both have the sampled bit set but are different recorded flags. This is a
Rewind content rule, not an OTLP definition of retry identity.

Future consumers interpret documented bits with masks while retaining the original
value. Read bit 0 for sampling; unknown high bits must not make that result true.
For parent/link remoteness, the bit-8 known marker gates interpretation of bit 9:
without the known marker, remoteness is unknown. Span flags describe the parent
context's remoteness; link flags describe the linked context. Do not interpret an
unfamiliar bit as a category, success, completion, ownership or permission. This
contract covers preservation, not every flag combination's semantic validity, a
new-span constructor or implemented consumers.

The existing lab ordering and JSON codec already retain uint32 values. Maintained
tests now cover 100 independent span/link pairs, inclusive zero/maximum and bit 31,
unchanged inputs during encoding and decoding after source mutation. All 32
single-bit changes to each field remain distinct in comparison projections. The
paired decoder refuses negative, out-of-range, fractional or wrong-type flags
without returning a partial result; positive controls include values above int32
range. Unknown bits do not bypass unsupported-wire refusal on either message.
`FuzzOTLPFlags` varies both uint32 fields against exact whole-message round trips.
These are representation checks. Typed final-span preparation and retry comparison
now use the codec; display/replay interpretation and future-version rules remain open.

The simplest alternative refuses records containing unsupported flag bits. It
limits the accepted domain and avoids normal storage writes for refused attempts,
but loses other evidence and compatible forwarded flags. Preservation adds constant
scalar work per field to the existing bounded metadata walks, with no sidecar,
worker or extra object request. It still saves ordinary records that refusal would
drop. Binary fixed32 holds four bytes; JSON decimal spelling can use more digits.
No latency, allocation or throughput comparison between policies has been measured.
The selected future different-bytes retry read and final authority/expiry/reference
recheck remain required.

Revisit if a required consumer cannot safely separate known flag interpretation
from preserved unknown bits. Name the consumer, reproduce its failure and compare
a safe adapter with narrower acceptance before changing this policy. The separate
unknown-category and optional-message-presence checkpoints remain in force.

As inspected in October 2026, the pinned OTLP
[span flags definition](https://github.com/open-telemetry/opentelemetry-proto/blob/a8951735f7801e8adfaec5c0ace9262771cfec6e/opentelemetry/proto/trace/v1/trace.proto#L106-L136)
recommends copying equivalent forwarded flags as-is and tells readers not to assume
high bits are zero; its [link flags](https://github.com/open-telemetry/opentelemetry-proto/blob/a8951735f7801e8adfaec5c0ace9262771cfec6e/opentelemetry/proto/trace/v1/trace.proto#L270-L290)
likewise warn readers not to assume those bits are zero. These protocol statements
support forwarding compatibility, not the whole Rewind acceptance protocol.
Collector pdata v1.70.0's local JSON
[writer](https://github.com/open-telemetry/opentelemetry-collector/blob/f532e15ef29b6ed7fefad2b23a0fefd27120ef9b/pdata/internal/generated_proto_span.go#L198-L200)
and [reader](https://github.com/open-telemetry/opentelemetry-collector/blob/f532e15ef29b6ed7fefad2b23a0fefd27120ef9b/pdata/internal/generated_proto_span.go#L281-L282)
carry the whole uint32 without a known-bit mask. This does not establish every
Collector pipeline's behavior or downstream safety. By contrast, OpenTelemetry Go's
[`buildSpanFlagsWith`](https://github.com/open-telemetry/opentelemetry-go/blob/983defdf55ffda64c2d3ddebcb85c5a5af2eb08a/exporters/otlp/otlptrace/internal/tracetransform/span.go#L179-L187)
constructs fresh flags from byte-sized runtime trace flags and documented remoteness
markers. That creation workload does not justify erasing high bits in received
OTLP evidence; the protocol distinguishes creation from equivalent forwarding.

## Links without usable targets

Keep every bounded, structurally supported received link, even a completely empty
message, when either target ID is absent or correctly sized but all zero. Preserve
partial IDs, typed attributes, trace state, full flags, dropped counts and list
position. Derive an explicit no-usable-target result instead of fabricating a key
or dropping evidence. Empty IDs and fixed-width all-zero IDs remain distinct retry
contents. A nil pointer in a message list is still malformed, not an empty message.
Nonempty trace IDs must be 16 bytes and span IDs 8 bytes; wrong widths are refused,
even if the other ID is absent. This is not a policy for malformed parent IDs.

The internal `record.TargetOfLink` operation returns a copied `LinkTarget`, a
usability boolean and an error. Both correctly sized nonzero IDs yield a usable
key. Empty/zero IDs yield a zero target, false and no error; nil links return
`record.ErrInvalid`, wrong widths `traceid.ErrInvalid`. No error or unavailable
result exposes partial IDs. It checks both widths before deciding unavailable,
reads only ID buffers, and neither mutates nor retains them. The caller keeps the
borrowed link stable. Unknown fields remain intact for the independent complete
metadata guard: target extraction does not certify complete record validity.

A usable target is not proof that the linked span exists, is retained, belongs to
the current project or can be read. No lookup or automatic target registration is
performed here. The containing span still needs its own valid execution IDs and
ordinary ownership/authorization. Neither partial nor valid link IDs supply those
requirements. Future readers must show an explicit unavailable-target case; a valid
key whose authorized lookup finds nothing is a different state.

The lab's complete guard now checks link ID widths before normalization, encoding
or direct comparison. This moves existing JSON shape refusal earlier and prevents
an obvious difference from hiding malformed IDs on either input. Codec and
comparison tests preserve all nine absent/zero/nonzero target combinations and
empty message entries, keep all pairs distinct by original ID bytes, and retain
link fields and sequence order when no target is usable. Unknown fields, wrong
widths, nil entries and invalid own identity still fail their separate boundaries.
The target helper has a byte-length/nonzero oracle fuzzer; the lab fuzzer varies ID
shapes, full flags and integer attributes through round-trip and comparison.
These are helper and lab checks, not runtime admission, viewer or replay support.

As inspected in October 2026, OpenTelemetry's pinned
[Link API rule](https://github.com/open-telemetry/opentelemetry-specification/blob/36ccfcfb33aca1e9c70a2cfd26e031ed9180949a/specification/trace/api.md#L801-L828)
recommends recording empty/all-zero contexts when attributes or trace state contain
information. The Go SDK's pinned
[`recordingSpan.AddLink`](https://github.com/open-telemetry/opentelemetry-go/blob/983defdf55ffda64c2d3ddebcb85c5a5af2eb08a/sdk/trace/span.go#L720-L752)
retains that case while recording, applying attribute limits; it drops an invalid
context with neither attributes nor trace state. Those mechanisms concern producer
recording. Keeping even empty link messages that reached Rewind is our stronger
received-evidence rule, not an upstream requirement.

The alternatives are dropping such links (keeping the span but losing evidence)
or rejecting the entire span (simpler target-ID requirements but rejection of
legitimate producer metadata). We retain the evidence and require consumers to
separate it from navigation. Revisit if a named required consumer cannot safely
represent no usable target, using its concrete failure before changing acceptance.
Classification checks/copies at most 24 bytes per link: constant time and auxiliary
space per link, linear work across the bounded link list. Retention/encoding and
comparison still include all received fields under existing metadata limits. No
object request, transferred byte, sidecar, lock, wait or recovery metadata is added.
These are operation bounds, not measured throughput or a strict peak-memory cap
for the entire metadata path.

## Direct metadata comparison

Compare stable, prepared span metadata directly without cloning its descendants.
`record.MetadataCodec.Compare` takes two borrowed
`record.Span` views and returns `(bool, error)`. It checks IDs, reconstructs only
one-span wrappers, then runs the existing complete bounded recursive guard and a
read-only attribute-order check for each input. Unknown fields, malformed pointers,
cycles/excessive visits or depth, duplicate keys and unordered attributes return
false plus an error. Both inputs pass these checks before any field difference can
return false with no error. A caller must inspect the error first; invalid or
unsupported metadata cannot become a retry conflict or an equal retry.

The caller keeps both complete graphs stable throughout. Owned decoding and
attribute ordering precede comparison; borrowed data requires an explicit copy
before that earlier mutation. Comparison itself never sorts, clears fields or
rewrites presence. It compares both schema URLs and complete Resource and Scope
values, supplying local empty messages for their absent cases. Status uses the
same local default, and every other Span field compares its presence and typed
value. Special handling looks up the Status descriptor by name, rather than
skipping an unexplained numeric tag. A schema fence pins every current Span field's
name, number, kind and cardinality. A dependency upgrade requires reviewing that
fence and the generated-field mutation coverage.

`spanComparisonProjection` remains a deliberately copying oracle, separately
normalizing its owned clones and filling the three permitted default shells.
Direct tests compare against it in both directions, cover each current Span field,
context URLs, numeric unknowns, full flags, floating values, narrow shell presence
and sequence order, and check deterministic binary snapshots for nonmutation
(including NaN payload bits and zero signs). Error tests put an obvious context
mismatch before invalid data in either argument. Every keyed container is tested
for refusal when unordered. Unknown wrapper fields fail `SpanAt`; unknown fields
at each selected descendant fail the checked comparison. The fuzzer varies
presence, double bits, enum numbers, flags, text, bytes, events and links against
the copying oracle. These checks do not establish complete semantic admission,
record-version/body equality, runtime acceptance or remote durability.

For the comparison walk, F is the fixed set of Span descriptors, N the number of
visited graph values, B byte content and S examined string bytes. Both algorithms
have O(F+N+B+S) worst-case comparison time; direct reflection still allocates and
makes no allocation-free or strict peak-space promise. Copying additionally
allocates O(N+B) data; immutable string backing bytes are shared. Common bounds,
uniqueness checks and attribute ordering have their own costs described below;
they must not be omitted to make the direct walk faster. There are zero object
requests in this lab. Real different-byte retries still need the selected bounded
accepted-object read outside catalog locks and final authority/expiry/reference
recheck. No catalog, authorization or stored reference changes here.

As inspected in October 2026, protobuf-go v1.36.12's
[`proto.Equal`](https://github.com/protocolbuffers/protobuf-go/blob/cdd4c5f7406e82462949c7a65defa9f3029c162d/proto/equal.go#L42-L66)
reads existing messages through reflection or generated equality methods without
cloning. Its workload is protobuf value equality; it supplies neither Rewind's
bounds nor our three optional-message exceptions. Its testing package
[`protocmp.transformMessage`](https://github.com/protocolbuffers/protobuf-go/blob/cdd4c5f7406e82462949c7a65defa9f3029c162d/testing/protocmp/xform.go#L258-L277)
constructs comparison maps for flexible assertions and diffs. That test-only
workload supports keeping a projection oracle, not putting the same map transform
in ingestion. These are observed upstream mechanisms; using a checked direct
comparison and a copying oracle together is our design.

We choose direct comparison to avoid copying whole already-prepared graphs merely
to read them. The simpler copying comparator remains credible, particularly when
comparison is negligible beside decoding and validation. Revisit if representative
profiles or schema-maintenance costs outweigh the saved copying; keep semantics
unchanged if the implementation strategy changes. The earlier component prototype
is evidence about its measured comparison phase, not the latency of this checked
operation or a complete request.

## Optional descriptive message comparison

For retry-content comparison only, an absent Resource, InstrumentationScope or
Status is equivalent to a present completely default-valued message at that same
position in one span and its context. This is a narrow Rewind equality rule, not
a rule to erase all empty messages. Preserve the original accepted encoding and
its presence information; an equivalent retry must retain the original reference,
attribution and deadlines. Never mutate a borrowed source graph for comparison.

All supported fields must be default: a scope with an empty name but a version,
attribute or dropped count is not empty. Resource attributes, dropped counts and
entity references remain evidence. Status message text, nonzero known codes and
unknown signed codes remain different contents. Both grouping schema URLs compare
independently. Missing span metadata or invalid execution IDs cannot become an
empty valid span. Kind zero is not rewritten to INTERNAL. AnyValue alternatives,
array/event/link order and missing versus present zero-byte bodies retain their
separate rules. Unsupported wire fields are refused before projection, not erased.

The maintained `spanComparisonProjection` lab baseline reconstructs a one-span
packet, validates the borrowed graph before `proto.Clone`, orders attributes in the
owned clone, then fills only those three missing positions. The shared guard also
refuses invalid reflective messages: nil entries in repeated message lists and
selected message-valued oneof arms with nil pointers can otherwise turn into empty
messages during copying/serialization. Ordinary absent optional fields stay valid.
This baseline is not a runtime equality API, semantic validator, saved codec or
selection of all remaining equality/version rules. `proto.Equal` on projections
is the scoped test oracle, not a frozen full-record contract.

Tests exercise all eight presence forms and 64 paired comparisons, keeping each
distinct encoding intact through decoding; negative controls preserve other fields
and sequence order. Tests also cover source nonmutation, independent output,
idempotence and refusal before copying cyclic, excessive, duplicate or unsupported
graphs. The fuzzer varies presence masks and every signed 32-bit status code against
the simple rule that only zero equals the default status. These are local bounded
representation checks, not acceptance/recovery or consumer-safety evidence.

For metadata size M, the traversal/copy/comparison work takes O(M) time and O(M)
transient space at this baseline. Existing uniqueness checks add expected hashing
costs, with their hash-collision worst-case limitation; keyed attribute sorting adds
the sum of O(k log k) comparison costs and key-byte reads. Bounds count graph visits,
including shared descendants. Copies may repeat
shared subgraphs; this is not a peak-memory cap or a measured performance result.
There is no I/O, persistent sidecar or catalog operation in the lab. The future
different-bytes fallback still needs its already-selected bounded accepted-object
read outside catalog locks and final authority/expiry/reference recheck. A runtime
comparison need not copy whole graphs just because this baseline does.

The credible alternative keeps presence as retry content and uses structural
comparison after attribute ordering. That retains an empty-shell declaration as a
conflict but also conflicts on serializer-shape changes that convey the same default
facts. We choose the narrow equivalence because these three messages can convey
no additional information when all their fields are default. Revisit if a required
consumer needs empty-shell presence itself to affect retry-content identity; name
that consumer and its concrete failure before changing the rule. The independent
unknown-category consumer checkpoint remains in force.

As inspected in October 2026, the pinned OTLP
[Status definition](https://github.com/open-telemetry/opentelemetry-proto/blob/a8951735f7801e8adfaec5c0ace9262771cfec6e/opentelemetry/proto/trace/v1/trace.proto#L301-L303)
assigns absent status the UNSET meaning, and its
[group definitions](https://github.com/open-telemetry/opentelemetry-proto/blob/a8951735f7801e8adfaec5c0ace9262771cfec6e/opentelemetry/proto/trace/v1/trace.proto#L48-L83)
describe absent scope as an empty unknown name and absent resource as no known
resource information. Resource empty-equivalence is our inference, not an OTLP
global equality guarantee. The SDK's resource-grouping
[`Resource.Equal`](https://github.com/open-telemetry/opentelemetry-go/blob/983defdf55ffda64c2d3ddebcb85c5a5af2eb08a/sdk/resource/resource.go#L161-L173)
also treats nil as empty, but ignores schema URL differences; we cannot copy its
whole identity rule for saved evidence. Protobuf-go's structural
[`proto.Equal`](https://github.com/protocolbuffers/protobuf-go/blob/cdd4c5f7406e82462949c7a65defa9f3029c162d/proto/equal.go#L14-L41)
distinguishes nil from present empty messages without OTLP semantics. These systems
support the distinction between representation and meaning; they do not implement
Rewind's immutable retry protocol.

## Compatibility references and required checks

The upstream reference is OpenTelemetry proto v1.9.0, commit
`a8951735f7801e8adfaec5c0ace9262771cfec6e`:

- [examples/trace.json](https://github.com/open-telemetry/opentelemetry-proto/blob/a8951735f7801e8adfaec5c0ace9262771cfec6e/examples/trace.json)
  supplies one server span with a parent, resource attributes and scope attributes.
- [trace/v1/trace.proto](https://github.com/open-telemetry/opentelemetry-proto/blob/a8951735f7801e8adfaec5c0ace9262771cfec6e/opentelemetry/proto/trace/v1/trace.proto),
  symbols `ResourceSpans`, `ScopeSpans` and `Span`, defines the nesting, 16-byte trace
  IDs, 8-byte span IDs and integer nanosecond timestamps.

Use these references to check that a record retains resource and scope context,
including their schema URLs. Moving a span between batch positions or changing an
ID's hex case must not create a different execution identity. Preserve integer
nanosecond timestamps without converting them through floating point. The example
has no captured request/response; its metadata cannot reconstruct those bytes.

A projection of one JSON example cannot establish OTLP compatibility. Before
freezing the record, exercise typed values and unknown/default fields against the
defined equality rules. Before implementing the receiver, use generated protocol
types and a full fixture matrix for JSON/protobuf decoding, bounds and protocol
responses. Retaining raw metadata is not a record normalization algorithm.

## Separate span and capture records

Consider one finished model call. Its span is accepted without captured bodies;
the producer later supplies the captured request and response for that same call.
This is additional evidence about the existing execution, not a new span.

The user selected separate immutable span metadata and capture records. Capturing
additional evidence must not mutate an accepted span or invent another execution.
Start with one logical capture record associated with one tenant/trace/span, rather
than a collection of independently replaceable payload parts. Concrete body states,
encoding and the producer-ownership mechanism still need design before a public
capture endpoint. Producer isolation is a selected first-version requirement below.

Each record has its own acceptance result. A span acknowledgment covers its metadata;
a capture acknowledgment covers delivered captured bytes and their recoverable link.
Neither promises durability of bytes that have not arrived, or proves replay readiness.
An identical retry of either record succeeds; changed bytes under that record's
identity conflict without changing either previously accepted record.

The local coordinator includes a span/capture kind in the internal tenant/trace/span
key. It is a bounded storage primitive, not an attachment API: it
does not parse bodies, join records or check parent existence. Both kinds use the
same execution-owner checks, admission and immutable object-before-catalog sequence.
The kind is part of catalog identity, not the object digest or the execution identity. Unsupported kinds must fail before storage access.
This key is not a selected wire format, persisted schema or OpenTelemetry type.

The simplest alternative was one immutable snapshot containing metadata and available
bodies. It needs fewer publication operations, but requires producer coordination
before sending and cannot accept late capture under the existing identity. Separate
records retain the span when capture fails, at the cost of extra publication, query
assembly and failure/retention states. Two different records that pass their
preliminary checks require two lookups, two creates and two publications in the
controlled baseline; no comparative latency measurement
exists. Revisit multiple captures or independently submitted request/response parts
only after a concrete producer needs them.

## Either arrival order

Accept authorized span and capture records in either order. The producer supplies
stable execution identity and retries delivered records; Rewind owns their durable
acceptance, duplicate/conflict handling and association. Capture acceptance must not
depend on finding an already accepted span. The stored tenant/trace/span association
survives replacement of ingest-local state; no in-memory wait list is required.

Before both records arrive, reads of a known accepted record still succeed and
lookup of its absent peer reports absence. A read error is not evidence of absence.
After both arrive, their shared identity permits association without rewriting either
record. This does not define a combined snapshot read or imply replay readiness.
If the second publication fails or its reply is lost, the first remains accepted;
retry the uncertain record using its unchanged identity and contents.

Requiring the span first was the simpler referential-integrity alternative, but
would force the producer to buffer or retry capture merely because delivery was
reordered. The chosen contract adds partial states that need storage limits,
retention and clear query results. It does not authorize an arbitrary producer to
attach data to another producer's execution. The selected ownership requirement
below is exercised through controlled authorizer/catalog capabilities. This internal
seam must not be exposed as a public attachment API.

## Producer authority

The first working version must enforce producer/execution ownership inside Rewind.
Project membership alone must not permit a writer to claim another producer's
execution. Project permissions govern allowed operations; attribution identifies
whose execution a record describes. The selected read policy requires an explicit
project-reader permission covering span metadata and capture across that project.
An execution owner or producer writer gains no automatic read access. This lets an
authorized debugger follow several agents' records while keeping writing authority
restricted to the correct instance. Narrower per-run/instance read grants would
reduce reader exposure but add grant management and potentially partial views;
they are deferred. Store now requires these separate capabilities; tests exercise
them with controlled identity and grant state. An additional internal credential
adapter now verifies separate principals and checks current native grants.

For example, A owns an execution and B belongs to the same project. B guesses its
trace/span identity and sends capture before A sends the span. Rewind must refuse
that unauthorized claim. Accepting whichever producer writes first would preserve
an immutable false association. Both span and capture must use the same authorized
owner regardless of arrival order. Several producers may own different spans in
one trace; owning a span does not imply owning the trace.

Distinguish the attributed execution owner from the authenticated submitter. They
may be the same for a direct write. A collector submitting for an agent needs an
explicit grant for that scope; its credentials alone do not prove every agent label
in a batch. Obtain identities and authority from trusted authentication/grants,
never an arbitrary telemetry attribute. Credential rotation must not silently
change execution identity.

Writing authority is scoped to one agent instance in one run in one project. An
instance is one particular running copy of an agent. Two planner copies in the
same run must have different instance identities. Keep a stable agent identity for
grouping, but it does not grant write access across those instances or runs. A run
is not assumed to be the same as a trace. A retry or credential rotation preserves
the original instance and execution identities; a newly started instance does not
inherit old ownership merely because it has the same agent name. This selects the
authority boundary, not a credential lifetime or a resumption protocol.

Trusted authentication and permissions must establish that project/run/instance
scope. Even a valid instance scope does not prove ownership of every trace/span
it names. A trusted execution creator must register that exact binding in the
authoritative catalog before record acceptance. The local registration coordinator
and owner-aware record publication share a binding contract, exercised with one
controlled catalog. Normal completion will close new call registration and permit
authorized delivery for already registered calls during a finite window; explicit
revocation closes delivery immediately. The configurable default is ten minutes
from trusted completion. A fixed window value and controlled catalog exercise
delivery timing. Joined run-admission tests now close new registration and allow
only authorized identical registration retries during the open window. Real
authenticated lifecycle operations and general provider-clock guarantees remain
absent. The registration adapter now persists bindings and elapsed closure; record
publication now uses native owner-aware SQL alongside controlled tests; see [delayed delivery](AUTHORIZATION.md#run-completion-and-delayed-delivery).
Real grant issuance, revocation propagation, delegation protocols and durable
representation remain unresolved; no token format or new API is selected.

Publication must atomically enforce the authoritative owner and applicable grant
before returning duplicate/conflict outcomes. A preliminary lookup is not ownership
proof. Lookup and read paths must enforce their permissions before disclosing record
state. Identical contents do not authorize another writer, and no owner may rewrite
an accepted original. Retain trustworthy owner/submitter attribution with acceptance
so replacing the ingest host does not erase it. The record catalog now requires
original owner/submitter attribution and returns it unchanged for authorized retries.
Registration's catalog contract separately requires original creator attribution.

The simpler alternative was one trusted writer for an entire project. It requires
less identity and grant state, but leaves producer isolation to that writer. The
selected requirement adds authentication, durable ownership and permission checks;
no request-count or latency measurement exists for that future implementation.
Compared with long-lived agent-wide permission, instance-scoped permission limits
authority across runs and concurrent copies at the cost of more identity/grant
lifecycle state. Review that cost against actual producer and collector delivery,
including delayed retries and rotation, before implementing credential lifetimes.
Attribution helps root cause analysis and replay interpretation. It does not prove
that self-reported actions happened, that capture is complete or that replay is
possible. Independent gateway observations remain a later extension.

This follows the distinction between authentication and resource-specific permission
in [OWASP's authorization guidance](https://cheatsheetseries.owasp.org/cheatsheets/Authorization_Cheat_Sheet.html).
[Collector authentication](https://opentelemetry.io/docs/collector/extend/custom-component/extension/authenticator/)
provides a connection-authentication boundary, not Rewind execution ownership.

### Catalog registration experiment

`Registrar.Register` in `internal/ingest/registration.go` establishes one immutable
binding through controlled capabilities. `Execution` contains tenant/trace/span;
`Owner` contains run/instance. Leaving kind out of the binding gives span and capture
one owner. The selected identity representation is a 16-byte trace ID and an
8-byte span ID, each nonzero, matching OpenTelemetry. `internal/traceid`
provides comparable array values, strict hex parsing and checked copying from
generated messages' byte slices. Hex case denotes the same bytes. Parsing must
reject wrong lengths, malformed hex and all-zero IDs before authorization or
catalog access; array zero values must also fail at those boundaries. Parent IDs
are not execution keys and their optional semantics need separate validation.
Project, run, instance and principal IDs remain bounded opaque strings.
This does not freeze a public request or saved-record format. `Tenant` supplies
the project's scope for this experiment, not a new
organization/project hierarchy.

Use fixed arrays rather than carrying canonicalized hex strings internally:
an execution remains a comparable map key without retaining a mutable byte slice
or allocating hex for each SQL request. Passing PostgreSQL the array's bytes
preserves its existing `bytea` representation. The installer constrains new
schemas to exact nonzero trace/span widths. It still refuses existing schemas;
there is no automatic migration or reinterpretation of earlier symbolic IDs.
Fixed-width parsing, validity checks, copies and formatting have constant cost
and bounded output; they perform no object or catalog I/O. This change does not
validate metadata contents, prove ownership or implement an OTLP receiver.

After validation and bounded admission, the registration authorizer must approve
this exact execution/owner for a trusted creator and return its authenticated
principal. Ordinary agent write permission is insufficient. Authorization precedes
any catalog access; callers cannot supply `RegisteredBy` through Register's arguments.
The ownership catalog checks current creator permission and run admission atomically
with the binding outcome. A running run may create a binding; a normally completed
run permits only an identical existing execution/owner retry before its fixed
delivery deadline. That retry is a duplicate even if another authorized registrar
submits it; the original creator attribution remains unchanged. New/changed bindings,
closed or revoked runs and unknown run state return `ErrDenied` before a duplicate
or conflict outcome. An owner change returns `ErrOwnershipConflict` only when both
applicable runs admit the operation. Neither result changes the original binding.

`ErrDenied` and `ErrOwnershipConflict` confirm refusal of this attempt. Any other
catalog error can hide a committed registration and returns
`ErrRegistrationUncertain` with the cause preserved. Retry the same execution/owner
to recover the original result. A malformed successful reply also returns uncertain
plus integrity failure, rather than a successful receipt. Confirmed success remains
success if cancellation arrives after commit. No rollback or deletion is attempted.
Replacing Registrar retains only state in the supplied catalog; tests model that
replacement in one process, not real host loss or database durability.

The operation uses one authorization call and one catalog operation, with no object
request, payload upload, worker, waiting queue or automatic retry. Its active limit
is independent of Store's; combined catalog concurrency can reach the sum of their
limits. Active limits do not bound accumulated bindings. The registration-only
fixture uses a mutex-protected map with expected constant-time lookup and O(E)
entries for E executions. The combined model also stores runs and grants, with
costs in [the authorization design](AUTHORIZATION.md). No worst-case CPU, row-size,
latency or throughput claim follows.

Store now refuses an error from the preliminary owner-aware CheckWrite before
upload. Publish must repeat owner/grant checks atomically before duplicate/conflict
outcomes and preserve the original submitter on a retry by another authorized
collector. Lookup separately requires an explicit project-reader grant. Controlled
tests exercise registration plus both record kinds, denial, concurrent retries,
lost replies and a grant change during upload; [the enforcement design](AUTHORIZATION.md)
states the contracts and costs. Legacy rows without attribution fail closed rather
than acquiring history from a later binding. Real credential/grant lifetimes,
admission quotas, binding retention and provider enforcement remain unresolved.
The native registration adapter now persists bindings and checks creator grants
and both run states in PostgreSQL; see [the SQL scope](POSTGRES.md#first-native-operation).
The same adapter now implements owner-aware CheckWrite and Publish, reads records
under exact project-reader grants and saves expiry when needed. The composed local
test joins that catalog to Store and the object SDK. Internal credential
verification and native permission checks now also compose with typed saving and
reading; public reception, remote catalog deployment and combined host-loss
recovery remain absent. Do not
substitute an unrelated binding store and treat it as enforced record acceptance.
Its installer requires a new owned schema; it migrates no legacy dataset.


For OTLP, keep the standard span format and authenticated exporter transport. The
agreed integration direction is a small helper in the trusted host application
that creates a normal span and registers its actual IDs. `producer.Host` now
registers an actual SDK SpanContext for its configured project/run/instance; it
exports no metadata. Separate `host_creator_grants` give that trusted host creation
authority without changing exact-call creator grants or implying writer/read
permission. Registration repeats the current scope check under its project gate.
`access.Credentials` verifies opaque per-principal tokens; public reception,
credential distribution/rotation and a collector forwarding protocol remain open. A shared collector must
preserve verified attribution rather than trust arbitrary agent labels; its own
credential authenticates only that collector. Standard exporter headers identify
the submitting credential but do not establish the execution-to-owner binding.

## Retaining capture whose span never arrives

Arrival order must not turn a successful save into a transient memory buffer.
Unmatched accepted captures are required durable records, unlike uploaded objects
that were never published. There is currently no automatic deletion or expiry.
Matched and unmatched captures use the same normal record-retention policy. Missing
span metadata does not shorten the accepted capture's lifetime; a later match does
not reset its deadline. An identical retry is the same acceptance, not another
retention period. Use admission/storage quotas to bound new work rather than silently
shortening the retention promised for accepted records. These are selected rules,
not an implemented expiry or quota mechanism. The selected configurable payload
default is seven elapsed days from each record's first authoritative acceptance,
with the existing 24-hour marker window after payload expiry.
The selected configurable default for an expired-record marker is 24 hours after
payload expiry. This extra identity memory does not extend payload availability.

The simpler alternative of a shorter unmatched-capture timeout reclaims unused data
earlier, but makes late span delivery change whether recorded inputs survive. A timer
by itself does not prove a peer will never arrive. The normal policy still needs a
authoritative acceptance timing, clock authority, policy-change and late-arrival
semantics. The payload duration default is seven elapsed days.
Cleanup must account for concurrent readers/writers, uncertain publication and
objects shared by other accepted records. See [retention boundaries](RETENTION.md).

Exercise both arrival orders, retry after a lost reply, metadata supplied
without bodies, present empty bodies, exact byte changes and tenant isolation.
Before OTLP reception, design batch/partial-success/retry mapping separately:
[OTLP acknowledgments](https://opentelemetry.io/docs/specs/otlp/) cover adjacent
client/server nodes, not end-to-end delivery through collectors. Do not interpret
a producer's successful export to an intermediary as Rewind's durable acceptance.

## Owned attribute ordering

The selected normalization direction sorts keyed attribute collections by their
case-sensitive keys, including resource, scope, span, event and link attributes
and nested key/value collections. Preserve array elements, events, links, spans
and resource/scope group order. This is one normalization operation, not complete
logical equality or a canonical byte encoding.

A fresh decoded message can be normalized in place while Rewind exclusively owns
its entire reachable object graph. An in-process caller retaining its message
needs an explicit copy before that operation; a pointer alone does not transfer
ownership. The existing encoder stays non-mutating. Do not remove the opaque
Store's separate byte snapshot under this metadata decision.

`record.MetadataCodec.NormalizeOwned` validates unknown fields and configured
traversal bounds over the entire graph before any mutation, so refusal cannot
leave a half-reordered message. A bounded second walk sorts only known keyed
collections. This is not integrated with acceptance. Repeated keys within one
collection must be refused, including identical values, before any reordering or
JSON encoding. This covers the same six keyed container types and nested maps;
reusing a key in a different collection is allowed. Case-distinct keys remain
distinct. The encoder must share this guard so callers cannot bypass it by skipping
normalization. Parsing alone need not establish semantic validity.

The metadata guard reuses the bounded read-only traversal. After a message's known
fields pass the existing budget/depth check, inspect its keyed collection with a
local Go set (`map[string]struct{}`). A repeated key returns `ErrDuplicateAttribute`,
without including keys or values in the error text. No value comparison, winner
rule or silent repair is needed. Strings are referenced, not copied. The set is
local to one collection/check, with O(k) auxiliary entries and expected O(k) lookups
plus key hashing. Go does not guarantee constant-time map operations; hashing can
degenerate, so the overall validator must not inherit the sort's worst-case bound.
A simple pairwise scan is the exact test oracle (O(k²), constant auxiliary space).
Copying just key headers and sorting them is a deterministic O(k log k) alternative
if bounded worst-case CPU becomes a requirement. Measure the selected checker on
bounded fixtures; no caching, pooling or full metadata cloning is introduced.
Other invalid semantic inputs remain outside the equivalence claim.

The runtime ordering operation checks with `slices.IsSortedFunc` before calling
`slices.SortFunc`; the lab retains the matched strategy experiment.
Matched microbenchmarks cover direct sorting and check-then-sort on sorted,
reversed, shuffled and nearly sorted lists, including long key prefixes. The check
reduces fully sorted-list time in these samples but can slow nearly sorted lists;
Go's direct sort is already adaptive. This is a provisional local choice, without
a cache or retained state between independent submissions. For collections of sizes k_i, comparison work is
at worst O(sum k_i log k_i) for ordering alone, plus traversal and the
separately described uniqueness check. String comparisons also
read key bytes. In-place sorting avoids a full message clone but still uses stack
space; it neither avoids decoding/encoding nor promises an allocation-free request.
There are no object requests, transferred bytes, catalog locks or recovery effects
in this metadata operation. A failed later encoding publishes nothing. Revisit the
sort strategy if measured final workloads or the Go implementation change.

The independent insertion-sort oracle checks all keyed container paths and nested
maps while preserving ordered sequences and original pair pointers/backing arrays.
Tests cover explicit caller cloning, fresh decoder ownership, shared internal
references and non-mutating refusal for unsupported fields, oversize and cycles.
Fuzzed permutations must reach the same unique-key reference result. Duplicate
refusal must leave the entire graph unchanged, including unrelated collections,
and return no encoder output; check identical/different typed values, adjacent
and separated repeats, and all keyed container paths against the pairwise oracle. The maintained
benchmarks compare restored identical inputs and report restoration separately;
no disposable probes or comparison images are needed.
