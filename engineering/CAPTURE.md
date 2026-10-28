# Capture direction and OpenTelemetry boundary

Status: the user selected a small explicit Rewind capture path first, with an
OpenTelemetry-compatible internal record and a later OTLP adapter. No public capture
schema, saved format, receiver or record normalizer is implemented. The selected
span-metadata model reuses OpenTelemetry's generated Go types, including resource,
scope and typed attributes. Rewind owns validation, normalization, exact capture
bodies, ownership and the saved format. The narrow JSON adapter below is selected;
a complete record schema remains open. The selected saved layout is versioned framing followed by JSON metadata and exact raw request
and response bytes. Compression uses a size gate, then keeps gzip only if smaller;
the selected starting threshold is configurable with a 32 KiB default for the
complete hybrid record before compression or an outer encoding marker. The selected
gzip starting setting is configurable BestSpeed. Header details, complete metadata/body-state
rules and limits also remain open. The current
ingest coordinator receives opaque bytes and does not know OpenTelemetry types.
An isolated [encoding experiment](CAPTURE-ENCODING.md) compares full JSON with
JSON metadata and raw bodies, including compression. The user selected the hybrid
layout from its results; its test-only codecs are not integrated with acceptance.

## Two entrances to one acceptance path

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
originals for structural round-trip fidelity, not as a chosen logical retry rule.
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

The paired codec in `tools/capturelab/otlp_codec_test.go` implements conversion on
bounded in-memory JSON fixtures. It preserves tested typed values and context, uses
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
rather than save only the known portion. The lab encoder checks unknown
protobuf wire bytes at every visited message before calling ProtoJSON. That includes
resource/scope context, span, events, links, status, nested arrays and key/value
values, not only the packet root. Refusal must preserve the original message and
return no encoded bytes. Ordinary new attribute names remain supported: their keys
are data inside known schema fields. Numeric enum values remain representable by
the codec; semantic value validation is a separate contract.

Default strict ProtoJSON decoding already refuses unknown JSON field names in the
paired lab decoder; it must not enable `DiscardUnknown` for saved metadata. This
is different from OTLP JSON reception, whose standard requires ignoring unknown
transport fields. A future adapter must document that projection and cannot claim
the JSON saved record preserves every original transport field. No adapter or real
record acceptance exists yet. The encoder returns the distinct lab error
`errUnsupportedMetadata` for visited unknown wire metadata; public error mapping is not selected by that test helper.
`otlp_unknown_test.go` checks 25 message positions, direct injection and a real
binary-protobuf parse, default JSON refusal and unchanged input. The raw ProtoJSON
loss check remains the simple negative oracle; it is not a permitted save path.

The simplest alternative is dropping unsupported portions, which accepts more
mixed-version input but loses evidence before retry comparison or replay can inspect
it. Opaque preservation would require an explicitly versioned representation and
is not selected. A schema upgrade can explicitly add supported fields later; it
must revisit this check and the saved-format compatibility rules. The recursive
check uses the existing bounded walk: O(visited known fields and values) time,
O(depth) stack, constant work to check the length of each message's unknown byte
slice, no copying or parsing that slice and no object/catalog operation. Parent-only
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
them with controlled identity and grant state, not real authentication.

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
publication still uses controlled backends; see [delayed delivery](AUTHORIZATION.md#run-completion-and-delayed-delivery).
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
one owner. These are bounded opaque internal IDs, not a selected public
wire encoding. `Tenant` supplies the project's scope for this experiment, not a new
organization/project hierarchy.

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
The same adapter now reads trusted seeded record rows under exact project-reader
grants and saves expiry when needed. It still has no Store publication implementation
and must not be combined with an unrelated binding store and treated as enforced
record acceptance.
Its installer requires a new owned schema; it migrates no legacy dataset.


For OTLP, keep the standard span format and authenticated exporter transport. The
agreed integration direction is a small helper in the trusted host application
that creates a normal span and registers its actual IDs. The helper, credentials,
receiver and collector delegation are not implemented. A shared collector must
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

The test-only `normalizeOwnedOTLP` helper validates unknown fields and existing
traversal bounds over the entire graph before any mutation, so refusal cannot
leave a half-reordered message. A bounded second walk sorts only known keyed
collections. This is not integrated with acceptance. It must preserve every key/value pair; duplicate
key interpretation remains unresolved, and ordering duplicates cannot establish
retry equality. Invalid semantic inputs remain outside the equivalence claim.

The lab checks order with `slices.IsSortedFunc` before calling `slices.SortFunc`.
Matched microbenchmarks cover direct sorting and check-then-sort on sorted,
reversed, shuffled and nearly sorted lists, including long key prefixes. The check
reduces fully sorted-list time in these samples but can slow nearly sorted lists;
Go's direct sort is already adaptive. This is a provisional local choice, without
a cache or retained state between independent submissions. For collections of sizes k_i, comparison work is
at worst O(sum k_i log k_i), plus linear bounded traversal. String comparisons also
read key bytes. In-place sorting avoids a full message clone but still uses stack
space; it neither avoids decoding/encoding nor promises an allocation-free request.
There are no object requests, transferred bytes, catalog locks or recovery effects
in this lab operation. A failed later encoding publishes nothing. Revisit the
sort strategy if measured final workloads or the Go implementation change.

The independent insertion-sort oracle checks all keyed container paths and nested
maps while preserving ordered sequences and original pair pointers/backing arrays.
Tests cover explicit caller cloning, fresh decoder ownership, shared internal
references and non-mutating refusal for unsupported fields, oversize and cycles.
Fuzzed permutations must reach the same unique-key reference result. Duplicate
pairs are retained, without a duplicate equality or winner policy. The maintained
benchmarks compare restored identical inputs and report restoration separately;
no disposable probes or comparison images are needed.
