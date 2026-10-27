# Rewind architecture direction

This describes agreed contracts and implementation boundaries. The CLI provides
help/version only. The internal ingest coordinator runs against controlled test
backends. Ownership registration has a native-pgx PostgreSQL adapter and immutable
objects have an AWS SDK v2 R2 adapter. Record publication, a public capture format and replay
runtime remain unimplemented.
Its tests model failures rather than establish remote durability.
An internal ownership-registration coordinator shares its binding contract with
owner-aware publication and explicit project-reader lookups. Controlled capabilities
exercise this integration; real credential verification remains absent.

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

Tenant-scoped span identity controls retries and conflicts. Content hashes identify
payload bytes and do not replace event identity or authorization. After authorization,
an identical retry succeeds without creating another logical span. Different contents
under an accepted span identity return a conflict and preserve the original; do not
overwrite it, append an automatic revision or silently assign a new identity.

A genuinely new execution receives a new span identity, even if its inputs and
outputs match an earlier execution. Changed contents alone do not establish that
a new execution happened: they might reflect a correction, a bug or tampering.
Compare defined metadata fields by their typed values, independently of outer
transport JSON field order or spacing. Compare captured request and response bytes
exactly; do not silently rewrite them. The concrete field schema, identity encoding,
deterministic record encoding, correction mechanism and duplicate response shape
still need design. These are agreed contracts, not implemented behavior.

## Capture and OpenTelemetry

Build a small explicit Rewind capture path first, with an internal record designed
against OpenTelemetry's span model from the start. A later OTLP receiver translates
into the same record and acceptance path. This chooses implementation order, not a
public schema, encoding, dependency or endpoint. Check real OTLP examples before
freezing the saved format. Preserve resource and instrumentation-scope context as
well as span fields; a four-field request/response struct is not that record model.

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
including save failure and a saved transition whose reply is lost; native expiry
transactions remain unimplemented. A false forward jump can expire early, and an
uncommitted observation gives no protection. Save receipts now carry original catalog-owned AcceptedAt and
ExpiresAt instants. The coordinator validates their ordering and returns them
unchanged on retries; controlled tests keep both with the original reference and
attribution. Native SQL record persistence and a public receipt encoding remain
unimplemented. Rewind must enforce authenticated producer/execution ownership in
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
Authenticated run-control operations, record-side persistence and general provider
clock/failover guarantees remain unimplemented.
Cleanup, real authentication/grant lifecycles and combined query semantics remain
to be designed. No expiry or quota mechanism exists yet.
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
reads. No remote PostgreSQL
catalog is provisioned. Registration tests use disposable local PostgreSQL and
object fault tests drive the SDK through controlled HTTP peers. The live R2
probe does not test the record catalog or replacement of the ingest host.
The [object contract](OBJECTS.md) specifies immutable writes and bounded reads. A catalog
on the sole ingest machine would violate the machine-loss requirement. The first
storage experiment must include a recoverable metadata placement and recovery test.

Use native pgx with explicit SQL for the PostgreSQL adapter. The selected
coordination is pessimistic locking with a shared project gate and an exclusive
execution binding lock: different calls in a project may proceed concurrently;
the same call's mutations take turns. Permission and run-state changes acquire
the project gate exclusively. Object I/O stays outside these transactions.
The [PostgreSQL design](POSTGRES.md) specifies the lock order, first-registration
race and failure boundaries. The registration slice implements this order; record-side SQL and throughput
measurements remain future work.

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
provider errors and storage fault tests. Establish one write/read/recovery sequence
from each project before extracting it. Keep journals, catalogs, publication
transactions, compaction, retention, tenant policy and acknowledgments with each
application. Defer shared caching, a separate service and a shared storage engine.
Do not add cross-checkout imports or local module replacements to public source.

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
