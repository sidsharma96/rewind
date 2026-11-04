# Rewind architecture direction

This describes agreed contracts and implementation boundaries. The CLI provides
help/version only. The internal ingest coordinator runs against controlled test
backends. Ownership registration, record publication and known-record lookup have a native-pgx
PostgreSQL adapter; immutable objects have an AWS SDK v2 R2 adapter. A composed
PostgreSQL/controlled-HTTP test exercises saving, fresh reads and retries. The internal typed ended-span coordinator now validates, saves and reads known
span records, including equivalent differently encoded retries. An internal bounded
trace reader now discovers final spans from project and trace ID alone. Internal
typed captures save exact request/response bytes with explicit availability, and
authorized recorded-byte playback returns complete saved responses. A public
capture format and application replay runtime remain unimplemented. Internal opaque
bearer verification, native permission checks and a trusted SDK-ID registration
helper now compose with the typed record path; there is no public receiver.
Its separate-process drill also kills/reaps a writer and removes its working
directory before recovering accepted records through new clients. Controlled
HTTP PUT-response loss and actual local PostgreSQL COMMIT-response loss are
exercised. These local process tests leave their storage services alive. A separate
[live host-recovery driver](HOST-RECOVERY.md) now uses verified opaque credentials,
remote PostgreSQL and live R2, destroys the ingest VM, then discovers both agents'
records and saved responses on a fresh VM before copying writer retry inputs.
Original receipts survive semantic retries with no object creation. This tests
ingest-VM replacement with surviving remote stores, not catalog disaster recovery
or provider-wide durability. Its lost application acknowledgment is distinct from
the local test's actual COMMIT-response fault.
An internal ownership-registration coordinator shares its binding contract with
owner-aware publication and explicit project-reader lookups. Controlled capabilities
exercise earlier integration tests. `access.Credentials` and `access.Authorizer`
now verify distinct principals and current native grants; `producer.Host` registers
actual SDK IDs under separately granted host creation scope. Trusted setup still
installs grants and credential snapshots; distribution, rotation and authenticated
run controls remain unimplemented.

## Long-term outcome

Rewind is a multiplayer trace store and recorded-playback system. Multiple agents
contribute their own operation records to one shared trace. An authorized reader
can inspect the agents' work together, follow reported parent and link relationships,
find available captured inputs and results, and play back saved results for a
supported workload. Later, authorized users should be able to find recurring behavior
across runs and preserve selected evidence as regression-test examples. This is the
intended outcome; the internal components described above do not yet deliver that
complete experience.

Use one acceptance scenario to keep implementation steps connected to that outcome:

1. Alice and Bob are different agent instances working in one project. Each has
   registered executions and submits its own spans into the same trace. Each span
   describes one operation; a separately accepted capture holds its exact bodies.
   A permitted collector can deliver for either agent under explicit scoped authority.
2. Alice cannot claim or overwrite Bob's execution by learning its IDs. A retry
   of an accepted record preserves its original acceptance; conflicting contents
   do not replace it. Owning or writing an execution grants no implicit read access.
3. Success covers the accepted record's remotely saved bytes and the catalog
   metadata needed to find them. A new authorized reader can find that record
   after acknowledgment, including after replacement of the ingest machine.
   The live host-recovery drill exercises this boundary with a surviving remote
   catalog and R2; catalog loss/restore and failover remain separate obligations.
4. A reader with project permission can retrieve both agents' accepted records and
   inspect their relationships in one trace. Records and captures may arrive late
   or independently. Show available evidence and unavailable relationships/captures
   explicitly; finding some spans must not claim the entire trace is complete.
   Reported timestamps alone cannot establish a global causal order.
5. Recorded playback uses the available saved results for the supported workload.
   Missing or redacted capture remains explicit. Running a model or application
   again is a separate operation with its own adapter and comparison contract.

This scenario is a completion target. Public producer integration, managed
credentials, complete trace assembly and an inspection interface remain open.
Internal synthetic producers, saved-byte playback and live ingest-VM replacement
exercise parts of it.
The precise first-release interface, supported playback workload, operating limits
and deployment/recovery acceptance tests still need concrete scopes before release.

The learning outcome remains part of the project: implement and explain storage
formats, buffering and indexing choices, compaction, recovery and query algorithms
against concrete workloads and credible baselines, counting memory, requests and
transferred bytes as well as CPU work. A useful first release is an intermediate
milestone; it does not silently remove later agreed learning goals. The roadmap's
unselected mechanisms and illustrative performance numbers are not completion gates.

Separate immutable lifecycle events remain a required extension direction for
reported start/progress evidence. Ended spans alone do not supply live progress.
Agent orchestration, tool gateways and run stopping belong to separately scoped
extensions; shared tracing does not itself implement those capabilities. Likewise,
Concordance's shared object library is a bounded reuse direction. It must preserve
the application outcome above and justify its concrete consumer or maintenance
benefit; a shared cache, service or engine is not selected by that direction.

The internal typed ended-span checkpoint joins checked one-span payloads to
owner-aware publication and fresh decoding. Its native composition test saves two
independently owned agents' records in one trace; its process drill replaces the
writer and recovers a typed failed span. These use controlled principals and
surviving local services. A separate trace-ID-only process drill now discovers both
agents' final spans after both writers are killed/reaped and their working directories
removed. The capture drill adds Alice's handoff and Bob's separate failed/successful
tool attempts, shuts down both writers and the controlled tool, then discovers and
plays back the available response from project/trace alone. Actual capture COMMIT
reply loss returns uncertainty before the new process finds its original acceptance.
These local process drills leave machine replacement untested. The separate live
driver now verifies opaque credentials and current native grants while recovering
the shared trace and saved results after ingest-VM destruction. Catalog recovery,
public capture and the complete acceptance scenario remain open; see the
[observed sequence and limits](HOST-RECOVERY.md#one-run-across-two-hosts).

### Current capture and saved-result operation

`record.CaptureCodec` implements the internal version-1 capture payload described in
[the capture contract](CAPTURE.md#typed-capture-and-saved-response). Request and response
each carry Missing, Redacted, Truncated or Present state, with exact raw bytes where
permitted. Present includes a valid zero-byte body; unavailable capture is distinct
from an absent/expired record or a failed read. Capture publication is independent
of final-span arrival and retains one immutable capture per execution.

`ingest.Captures` shares the typed span coordinator's admission bound and native
catalog. It saves/reads typed captures and resolves retries by decoded state and
exact body bytes, independently of header formatting and compression. It preserves
the original receipt and repeats current authority/expiry/reference checks after
outside-lock comparison. Its known-execution `Playback` returns a complete Present
response with mandatory request availability, execution and original receipt. An
unavailable request does not block that response; an unavailable response returns
`ErrResultUnavailable`. No incoming request matching, live tool fallback or fabricated
exception is part of this operation. Trace pages discover execution IDs but do not
freeze a resumable playback membership set. Public admission, production limits and
application-specific replay consumers remain open.

### Current trace-read pages

`ingest.TraceReader` uses the same coordinator admission slot as typed span writes
and known-span reads. Each request checks current project-reader permission and
seeks accepted execution IDs in increasing span-ID byte order through the catalog.
The cursor is a scope-bound position, not permission or frozen membership. Late
records ahead of it may appear on a later page; late records behind it require a
new scan from the beginning. Exhausting the observed keys never establishes trace
completeness or causal order. Fixed-membership export/playback selection remains
a separate future consumer contract. This live-page choice does not relax future
cleanup/reader-lifetime obligations for immutable accepted references.

The page bounds candidate executions, aggregate stored object bytes and total
blocking time. Capture-only and retained expired identities count as candidates;
registration alone does not. Each candidate's final span goes through current
permission and saved-expiry lookup. Available items carry an owned decoded span
and the exact original receipt used for verification; unavailable items explicitly
mean no final span at lookup or confirmed final-span expiry. Missing accepted
objects, corrupt bytes, unsupported formats and operational failures fail the
whole page with no partial results or continuation. No capture bodies are fetched.

The existing `(project, trace_id, span_id, kind)` primary key supports distinct-key
seeking across both retained kinds. Native tests record an ordered index-only
scan under PostgreSQL 16.13, without timings. Logical limits do not hard-bound
physical database work: planner choices, dead rows and lock waits still matter.
Sequential GETs cap call count and stored bytes; per-span codec limits multiplied
by the candidate cap bound expansion/graph work, not exact heap use or hard CPU
time. A span that cannot fit an empty page is an explicit limit error; a span
that only exceeds remaining space is left for continuation. No production limits
or public token encoding are selected. Revisit fixed membership when a required
consumer needs reproducible resumable selection rather than refreshed inspection.

### Later investigation and regression-test goals

After the usable capture, inspection and recorded-playback path, support finding
patterns across runs and preserving selected examples for regression tests. For example,
find shared Alice/Bob runs containing tool failures, inspect the available inputs
and results, then preserve examples with enough context to test a future change.
These are agreed later goals, not implemented features or extra first-release gates.

Search should cover relevant structured fields and supported captured text under
current project-reader permission. Queries must distinguish conditions on one span
from conditions satisfied by different spans in a trace. Verified ownership comes
from the catalog, not descriptive agent labels. Query syntax, indexes and analytical
freshness remain open; immediate trace-ID lookup after acknowledgment stays required.

A reproducible example needs an explicit selection of accepted record identities
and verified references, plus the resource versions and application state its
consumer requires. A live page cursor is not that selection. Preserve missing,
redacted and truncated evidence explicitly. Examples need
an export or retention contract that outlives their source records where necessary;
normal retries still cannot extend expiry. Recorded-byte playback and evaluating a
new execution remain different operations. Export shape, scoring, state capture and
retention mechanisms require a concrete consumer design before implementation.

Braintrust's [Trace Lake article](https://www.braintrust.dev/blog/trace-lake) motivates
searching execution history and turning failures into tests. Its production claims
are not Rewind measurements or selected operating targets. Grafana Tempo's
[pinned architecture](https://github.com/grafana/tempo/blob/c1722c24a0ae27a6966b19ee249d18d6c1173e97/docs/sources/tempo/introduction/architecture.md)
uses object-storage Parquet blocks for trace-ID lookup and cross-trace queries;
its telemetry workload does not supply Rewind's producer ownership or playback
contract. These examples inform later workloads, not a new database, SQL engine,
background investigation service or shared Concordance engine. Select mechanisms
against actual queries and measured costs in the existing learning stages.

## Agreed requirements

Rewind is a standalone trace store and replay project. Accepted ingest batches must
be remotely durable before success is acknowledged. That includes captured payloads
and the metadata required to recover them after losing the ingest machine. Data
still buffered in the producer is outside this guarantee.

Success also promises immediate availability through trace lookup: a new authorized
lookup begun after acknowledgment must be able to resolve the accepted span, even
on another ingest host. Publish the recovery metadata and trace references together
before replying. Until columnar indexes exist, lookup may read the original remote
batch. It must not depend on the original host's memory, a lagging indexer or a stale
catalog replica. This does not promise trace completeness or immediate availability
in future analytical queries; those contracts remain to be designed.

Recorded playback reads saved results. Model re-execution reconstructs a captured
request and compares a fresh result. Application re-execution requires an explicit
adapter and dependency-substitution policy. Do not promise that external models
produce identical output. A trace may omit inputs required for replay; expose
capture completeness and redaction rather than inventing missing state.

The internal typed span path accepts ended spans, including ended failed calls,
as one immutable final record. Registration may precede completion; it establishes
authority, not a saved span or evidence that the call finished. Never save a start
snapshot into that final span slot and overwrite it later. A process crash before
delivery may leave the final span missing; do not fabricate its outcome.

Design for separate immutable lifecycle events linked to the same registered
execution. Such events need their own event identities, retry/conflict rules,
authorization, retention, ordering and delivery guarantees before implementation.
Neither a new span ID for each update nor rewriting final metadata supplies that
contract. Existing embedded OTLP span events arrive with their exported span;
they do not by themselves implement independently durable live progress. Lifecycle
events are a required extension direction, not a new implemented record kind,
live trace stream, atomic run log or runtime messaging service.

Final-span submission is an explicit declaration by the authorized producer or
scoped collector that the operation has ended. No timestamp, status, registration
or arrival-order inference supplies that declaration. It records reported
completion, not independently witnessed cessation of external work. Do not require
a separate per-execution catalog completion transition before saving the final
span. Existing ownership, run admission, delivery and retention checks still apply.
The internal `Span.DeclareEnded` operation returns a borrowed `EndedSpan` after
checking the execution IDs; a zero declaration cannot expose a view. The encoding
lab and internal typed coordinator consume this declaration. `record.SpanCodec`
prepares and decodes a bounded supported one-span payload; it is not a general
OTLP semantic-conformance validator. See the
[declaration contract](CAPTURE.md#explicit-ended-span-declaration).

Preserve reported span timestamps even when the end precedes the start. Derive a
timing warning and unavailable duration; do not silently replace the timestamps
or turn unknown duration into a measured zero. Such spans must not contribute an
unusable duration to future duration calculations or timed replay.
Zero-valued endpoints remain unchanged and mean unknown for timing use, because
generated OTLP metadata cannot distinguish an omitted scalar from explicit zero.
Keep any nonzero endpoint; do not replace unknown time with acceptance time or
infer whether the operation finished. Equal nonzero endpoints have zero duration.
Catalog-owned acceptance, retention and permission clocks remain separate.
Cross-host clock alignment is a different problem: ordered local times do not
establish cross-agent chronology.
The [timing contract](CAPTURE.md#reported-timestamps-and-derived-duration) defines
the duration helper, not a complete admission, metrics or replay path.

An eventual lifecycle key combines the authorized project, registered trace/span
execution and a stable event ID; identical retries cannot become new progress.
Its writer must satisfy the execution owner/delegation boundary. An acknowledgment
must cover that event's remotely saved bytes and catalog reference independently
of the final span. Version/kind dispatch must distinguish events from final spans;
an unsupported kind must not be decoded as a span. Keep explicit missing evidence
when assembling a run; arrival order and wall-clock timestamps alone do not prove
cross-agent causality. These are extension constraints, not a frozen event schema.

Tenant-scoped span identity controls retries and conflicts. Content hashes identify
payload bytes and do not replace event identity or authorization. After authorization,
an identical retry succeeds without creating another logical span. Different contents
under an accepted span identity return a conflict and preserve the original; do not
overwrite it, append an automatic revision or silently assign a new identity.

A genuinely new execution receives a new span identity, even if its inputs and
outputs match an earlier execution. Changed contents alone do not establish that
a new execution happened: they might reflect a correction, a bug or tampering.
Execution keys now use comparable 16-byte trace and 8-byte span arrays, with
all-zero IDs refused before authorization/catalog access. Hex parsing is case
insensitive; PostgreSQL keeps raw bytes. Other identifiers remain opaque bounded
strings. These keys do not authenticate a producer or validate metadata contents.
Compare defined metadata fields by their typed values, independently of outer
transport JSON field order or spacing. Compare captured request and response bytes
exactly; do not silently rewrite them. The concrete field schema, public identity encoding,
deterministic record encoding, correction mechanism and duplicate response shape
still need design. These are agreed contracts, not implemented behavior.

Keep one stored-object digest in the catalog. For an authorized same-identity
retry whose stored bytes differ, the selected direction is to verify and decode
the accepted object and compare defined contents, rather than persist a second
logical-content digest. Identical stored bytes take the quick path. Comparison
I/O must stay outside catalog locks; final resolution rechecks current authority,
expiry and the compared reference. Writer-scoped comparison must not require or
confer project-reader access. The [comparison design](CAPTURE-ENCODING.md#retry-comparison-direction)
records the trade-off and failure boundaries. `ingest.Spans`, `ingest.Captures` and
the native catalog implement it for final-span metadata and typed capture bodies.
The legacy opaque coordinator retains exact stored-byte equality.

## Capture and OpenTelemetry

Build a small explicit Rewind capture path first, with an internal record designed
against OpenTelemetry's span model from the start. A later OTLP receiver translates
into the same record and acceptance path. This chooses implementation order, not a
complete public schema, dependency or endpoint. Check real OTLP examples before
freezing the saved format. Preserve resource and instrumentation-scope context as
well as span fields; a four-field request/response struct is not that record model.
The selected span-metadata model uses OpenTelemetry's generated Go types for span,
resource, scope and typed attributes. Rewind owns validation, normalization, capture
bodies, authorization and saved-format rules. This avoids maintaining parallel
protocol field definitions but adds OTLP/protobuf dependencies and still needs an
explicit JSON serialization boundary. The [metadata investigation](CAPTURE.md#generated-metadata-and-json)
checks that boundary. The selected serializer direction is a narrow adapter around
`protojson`: numeric enums and hexadecimal IDs at actual span/link fields, without
a parallel handwritten metadata schema or Collector data model. The concrete `record.MetadataCodec` now implements that boundary; the maintained
lab and internal typed span coordinator delegate to it. Its byte, graph-work and
nesting limits are explicit configuration, not selected production defaults. The selected internal-record
policy refuses unsupported metadata rather than discard it: unknown protobuf wire
fields anywhere in the metadata must fail before JSON encoding. Known kind/status
fields preserve unknown signed 32-bit enum numbers without inventing their meaning;
consumers must handle an explicit unknown category. Revisit this policy if a required
consumer cannot safely expose an unknown category, using its concrete failure case
before changing acceptance. The [numeric-value contract](CAPTURE.md#numeric-kind-and-status-values)
records that integration checkpoint. Lab round-trip, refusal and property tests
cover representation; the typed span coordinator now applies these rules in
preparation and retry comparison. Display/replay consumers remain unimplemented.
Received span and link flags retain all 32 bits, including unfamiliar bits; each
full unsigned value remains retry content. Future consumers interpret documented
bits through masks without replacing the saved value or inferring authority.
The [flags contract](CAPTURE.md#span-and-link-flags) records the lab checks and
consumer integration checkpoint. Floating metadata compares by exact numeric value,
with both NaNs equal and both zero signs equal. Different numeric types stay distinct.
The JSON metadata direction preserves ordinary double bits and zero signs when
saving, but does not preserve NaN diagnostic bits. The
[floating-value contract](CAPTURE.md#floating-metadata-values) records that limit,
lab coverage and the consumer-driven revisit condition. OTLP JSON
transport compatibility remains a separate adapter responsibility. Attribute ordering normalizes exclusively owned messages in place; caller-owned
messages require an explicit copy first. The [ordering direction](CAPTURE.md#owned-attribute-ordering)
preserves sequence order and keeps the encoder non-mutating. Repeated attribute
keys, including identical values and nested map keys, are refused before sorting
or encoding; separate collections may reuse keys. Each logical span record includes
its resource, scope and both schema URLs directly; do not add shared-context object
references. The [span context direction](CAPTURE.md#one-span-with-its-context) defines
an internal one-span association operation before codec integration. For retry
comparison only, absent and completely default Resource, Scope and Status messages
are equivalent at those three positions; original encoded presence stays intact.
The [comparison projection](CAPTURE.md#optional-descriptive-message-comparison)
remains the copying test oracle. The selected
[direct comparison](CAPTURE.md#direct-metadata-comparison) checks both bounded,
supported, attribute-ordered span views before comparing without copying their
contents. The metadata codec implements it and the lab checks it against a copying oracle,
with stable borrowed inputs and explicit errors; the typed coordinator now uses
it for runtime final-span retry resolution. Other fields and schema URLs remain evidence. Revisit if a
required consumer needs empty-shell presence itself to affect retry-content identity.
Links with absent or correctly sized all-zero target IDs retain their received
evidence, including empty link messages; derive an explicit unavailable target.
The internal `record.TargetOfLink` helper copies a key only for two nonzero IDs,
and refuses wrong nonempty widths. A usable key grants no authority or guarantee
of existence; the containing span's own IDs stay strict. The
[link contract](CAPTURE.md#links-without-usable-targets) covers preservation, retry
distinctions and the consumer integration checkpoint. Typed span preparation/decoding preserves these targets; viewer navigation and
replay integration remain unimplemented.
The internal saved final-span schema is bounded one-span metadata JSON under the
version-1 envelope. Internal capture bodies now have their separate payload schema;
public admission and future-version
compatibility remain open. Blank operation names remain unknown evidence; never
insert a saved placeholder. A changed name is changed retry content. Revisit for
a required consumer unable to safely expose an unknown name and show its concrete
failure before changing acceptance.
The selected saved layout is hybrid: versioned framing, JSON
metadata and exact raw bodies. The [encoding experiment](CAPTURE-ENCODING.md)
supports this direction. The internal `record.EnvelopeCodec` uses a fixed seven-byte common envelope for the format version,
record kind and encoding, with a separate payload schema for each kind. After
stored-object integrity verification, readers must cross-check the declared kind
against the authorized catalog's expected kind and refuse unsupported or mismatched
kinds before payload interpretation. This label supplies no authority or catalog
reconstruction. The [kind-discrimination contract](CAPTURE.md#saved-object-kind-discrimination)
records the choice; its `RWND` prefix and explicit version/kind/encoding tags are implemented, while
the internal capture payload schema is implemented and production limits remain open. Version 1 denotes
the whole format revision, not only the header; synthetic test payloads do not
freeze a public payload format. The codec requires caller-verified integrity and
a catalog-authorized expected kind. The first common envelope omits an exact expanded-payload
length: readers enforce separate stored-input and actual expanded-output limits,
then fully validate the payload. This saves a field and its consistency rules,
while giving up early refusal from an honest oversized declaration and size
inspection without decoding. Revisit for a concrete consumer needing that size
upfront, block/range access or measured early-refusal value. Gzip readers will accept
complete valid member sequences as one logical payload, including empty members;
the stored and expanded limits cover the whole input and combined output. Invalid
later members and non-member trailing bytes fail before payload parsing. The writer
will emit one member. The [member contract](CAPTURE.md#gzip-member-sequences) records
the compatibility choice and runtime framing checks; final-span payload decoding
uses `SpanCodec`, and capture payload decoding uses `CaptureCodec`.
The selected compression policy skips records below a size
threshold and otherwise keeps gzip only if smaller. Its selected starting default
is a configurable 32 KiB for the complete uncompressed hybrid record, including
metadata and framing but excluding any outer encoding marker; equality with the
threshold attempts compression. Use configurable gzip BestSpeed as the starting
setting, favoring lower compression CPU and allocation over the extra size savings of
DefaultCompression. The metadata codec includes owned attribute ordering, but
the envelope codec now exposes explicit byte bounds and compression policy.
Internal typed final-span admission and logical retry resolution are implemented.
The internal capture schema is implemented; public interfaces and production limits remain open.

Keep captured bytes distinct from tracing metadata. A span can lack the inputs
needed for replay; missing capture must not be represented as an empty request or
response. Span metadata and one logical capture record are accepted independently
and remain immutable. A span acknowledgment does not cover undelivered capture;
a capture acknowledgment must cover its delivered bytes and recoverable link.
Either record may arrive first. Accepted capture must not wait for span publication;
their durable shared identity supplies the association. Matched and unmatched capture
use the same normal retention policy; matching or identical retries do not reset a
capture's deadline. Span and capture each use a configurable seven elapsed days
from their own first authoritative acceptance. Expired-record markers have a configurable default window of
24 hours after payload expiry; after marker removal, old-identity recognition is
not guaranteed. Confirmed record expiry must also be saved before it is reported,
so a backward catalog-clock correction cannot revive a retained expired identity.
The local Lifetime value and controlled catalog tests exercise this state transition,
including save failure and a saved transition whose reply is lost. Native Lookup
now shares ordinary binding reads and restarts exclusively once to save newly
observed expiry, rechecking permission and fresh state. SQL tests seed records
through trusted setup; native writes now also save expiry before confirming it. A false forward jump can expire early, and an
uncommitted observation gives no protection. Save receipts now carry original catalog-owned AcceptedAt and
ExpiresAt instants. The coordinator validates their ordering and returns them
unchanged on retries; controlled tests keep both with the original reference and
attribution. Native publication chooses those times from project policy once,
and SQL Lookup reads the same saved receipts. A public receipt encoding remains unimplemented. Rewind must enforce authenticated producer/execution ownership in
the first working version, with writing authority scoped to one agent instance
in one run in one project, as described in [the capture design](CAPTURE.md#producer-authority).
Trusted catalog registration is the selected ownership binding. The local Registrar
exercises its retry/refusal contract. Store passes authenticated writer attribution
to the catalog, which checks the binding and current grant atomically with acceptance.
Read access requires an explicit project-reader permission across the project's
span and capture records; producer write permission does not imply read permission.
Controlled tests exercise the separate permissions, original submitter attribution
and refusal after a grant changes during upload; see [ownership enforcement](AUTHORIZATION.md).
Normal completion will stop new call registration and allow a bounded delivery
window for already registered calls, with current ownership and grant checks.
Explicit revocation closes submissions immediately at the catalog boundary.
The selected configurable default is ten minutes from trusted normal completion.
An internal fixed window and controlled catalog tests exercise the delivery boundary.
A catalog-owned run-admission value and controlled catalog now also close new
registrations, resolve authorized identical registration retries during drain and
enforce revocation. An explicit elapsed-closed value now preserves saved closure
across supplied clock regression; once durably recorded, closure must not reopen.
This cannot correct an unobserved clock jump and can retain an early closure caused
by a false forward jump. The registration adapter now persists bindings and elapsed closure in PostgreSQL.
Authenticated run-control operations and general provider
clock/failover guarantees remain unimplemented.
Cleanup, credential distribution/rotation, grant lifecycles and combined query semantics remain
to be designed. Lookup can save expiry; no autonomous expiry worker or storage quota
mechanism exists yet.
OTLP reception still needs its own bounded decoding, batch handling and
protocol responses. Reusing the storage sequence does not implement those contracts.
The scoped mapping investigation and next unresolved choice are described in
[the capture design](CAPTURE.md).

## Initial storage choice

One Go process owns ingest, storage, queries and replay orchestration. The agreed
first version uses PostgreSQL for the authoritative catalog and R2 for immutable
objects. Both stores hold required durable state: listing R2 objects cannot recover
which objects PostgreSQL accepted. The private `rewind-dev` R2 bucket is
provisioned; its scoped adapter probe passed creation/retry, conditional
overwrite rejection, a locally discarded successful upload reply and fresh-client
reads. A dedicated remote PostgreSQL development catalog and R2 now also support
the [live ingest-host replacement](HOST-RECOVERY.md). Registration tests still use
disposable local PostgreSQL, and object fault tests drive the SDK through controlled
HTTP peers. The smaller live R2 probe tests only the object adapter.
The [object contract](OBJECTS.md) specifies immutable writes and bounded reads. A catalog
on the sole ingest machine would violate the machine-loss requirement. The live
drill exercises that remote placement and replacement boundary; catalog
backup/restore and failover remain untested.

Use native pgx with explicit SQL for the PostgreSQL adapter. The selected
coordination is pessimistic locking with a shared project gate and an exclusive
execution binding lock: different calls in a project may proceed concurrently;
the same call's mutations take turns. Permission and run-state changes acquire
the project gate exclusively. Object I/O stays outside these transactions.
The [PostgreSQL design](POSTGRES.md) specifies the lock order, first-registration
race and failure boundaries. Native registration, publication and lookup implement
this order; throughput measurements remain future work.

The publication sequence is bounded admission and authorization, a preliminary
catalog lookup, complete remote batch/payload writes, catalog commit including
trace references, then acknowledgment. The lookup stops known expired identities
and catalog failures before upload; it does not reserve acceptance. Final publication
must recheck current expiry and conflicts. A remote journal
or raw batch can establish durability and basic lookup before columnar indexing
and compaction. Do not rely on an asynchronous
flush from a local WAL to justify the acknowledgment. Specify retry identity,
ambiguous remote outcomes, catalog atomicity and backpressure before implementing
that sequence. A local WAL may be useful staging, but is not the durable truth.

The first coordination experiment and its failure boundaries are specified in
[the ingest design](INGEST.md). An object upload and a PostgreSQL transaction are
not one cross-store transaction. Upload failures prevent publication; catalog
failures can leave unreferenced objects, and a lost commit response must be treated
as uncertain. Retain uncertain objects rather than deleting data that may be live.

Queries need a stable view of published data. Compaction publishes replacements
before retiring inputs. Readers and cleanup need an explicit lifetime protocol;
immutable objects alone do not settle deletion races. Start with one writer and
one compactor unless an experiment establishes a need for more concurrency.

Create packages as operations arrive: command wiring, ingest admission, storage,
query and replay are prospective responsibilities, not a mandate for five empty
packages. Transport must not own format rules. Storage must not import CLI code.

## Coordination with Concordance

The agreed direction is a small in-process Go object-storage library. Candidate
shared operations are immutable writes, bounded full/range reads, integrity checks,
provider errors and storage fault tests. Each application keeps its current
scope-to-object-key mapping in a trusted adapter. The selected common verified
read requires exact stored size and digest plus an independent configured byte
cap; Rewind's consumer-owned object operation is explicitly ReadExact. It does
not add an expanded-length field to the record envelope. Both projects now have
controlled writer-process/lost-COMMIT sequences; complete the joint contract and
design review before extraction. Keep journals, catalogs, publication
transactions, compaction, retention, tenant policy and acknowledgments with each
application. Defer shared caching, a separate service and a shared storage engine.
Do not add cross-checkout imports or local module replacements to public source.

Coordinate development before changing either object adapter's scope, read bounds,
integrity, error/retry contract, admission or timeout behavior, and before adding
range reads, deletion, verification reuse or caching. Recheck readiness as soon as
native record publication and ingest-host replacement work, rather than waiting
for replay or a viewer. Both now have Rewind evidence. The
[extraction checkpoint](OBJECTS.md#extraction-checkpoint)
is the maintained handoff: compare current revisions, concrete sequences, measured
requests/bytes, differences and unresolved outcomes with Concordance's development
agent before proposing a shared API. A local note is not a delivered message or
agreement. Begin extraction only after both recovery examples establish a contract
and the joint API/design is reviewed. This coordination does not create runtime
communication between application agents.

## Deferred agent safety extension

Preserve trustworthy execution evidence as a future direction. Build the trace-store
and replay foundation first, then evaluate a small multiagent workload with one
mediated tool gateway and a measurable run-stop experiment. This direction does
not approve a gateway, agent runtime, detector or credential service for the first
implementation.

Accepted original records must be immutable to agent principals. Derive producer
identity and authority from authenticated context, distinguish agent reports from
independently observed actions, and preserve corrections as separate linked records.
Several agents may contribute their own spans to one trace; a trace ID grants no
authority over another producer's records. Immutability after acceptance does not
prove truthful or complete capture, and content hashes alone do not prevent deletion.

Enforcement belongs outside agent control. A future gateway, credential broker and
runtime supervisor would enforce run-scoped authority, including descendants.
Rewind supplies evidence; immediate lookup and ordinary completed-span export do
not provide pre-action authorization. A kill contract must distinguish refusal of
new operations, termination of running work, credential-revocation propagation and
external effects already in flight. Do not promise simultaneous global stopping.

Apply explicit permission and budget rules before attempting to infer intent.
Repeated policy denials can justify a scoped pause without proving maliciousness.
Monitoring decisions need attributable evidence and a policy version. Replay tests
use recorded tool results to compare interventions and false positives; they do not
establish general safety. Review the trust model, retention, failure policy and
bounded read/admission costs before implementing this extension.

## First design experiments

1. Define accepted-batch identity, completeness and recovery metadata, and specify
   the bounded read path that meets the agreed trace-lookup guarantee.
2. Demonstrate remote write, publication, lost-reply retry and recovery without the
   ingest machine. Exercise upload success followed by catalog failure.
3. Compare an append buffer plus sorting with an ordered memtable on the same
   ingest/read/flush workload before selecting a skip list.
4. Add a simple segment format, trace lookup and tenant/time scans. Use those
   measurements to choose ordering, encoding and indexes.
5. Add replay for a small supported capture format; then broaden query and aggregate
   operations. Approximation needs exact baselines and stated error/window semantics.

These experiments need independent design review before implementation. The roadmap's
compression ratios, object latency and market gaps are hypotheses, not measurements
or product guarantees. Gateway development, enterprise identity and distributed
query execution are deferred.
