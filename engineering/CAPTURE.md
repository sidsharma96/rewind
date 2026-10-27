# Capture direction and OpenTelemetry boundary

Status: the user selected a small explicit Rewind capture path first, with an
OpenTelemetry-compatible internal record and a later OTLP adapter. No public capture
schema, saved format, receiver or record normalizer is implemented. The current
ingest coordinator receives opaque bytes and does not know OpenTelemetry types.

## Two entrances to one acceptance path

```text
Explicit Rewind capture -> decode ----+
                                     +-> validate record -> encode -> ingest
OTLP export -----------> translate --+
```

The second entrance is future work. Authentication supplies trusted tenant context;
neither a span's attributes nor its trace ID grants storage authority. Each entrance
must bound encoded and decoded input before passing owned record bytes to ingest.
The accepted record must have a defined deterministic encoding before its digest
can implement the agreed metadata-value and exact-payload equality rule. Do not
hash raw transport JSON or assume arbitrary protobuf serialization is canonical.

The record design must account for trace/span/parent identity, timestamps, kind,
status, typed attributes, events, links, flags, trace state, dropped-data counts,
resource context, instrumentation scope and their schema URLs. This is a list of
compatibility concerns, not an approved Go struct or support claim. Specify which
fields affect equality, supported value types, ordering/default rules and treatment
of unknown fields before freezing a version. Equal span IDs in unrelated tenants
must not share acceptance state.

Payloads are separate concepts from these metadata fields. Store exact captured
request/response bytes only when available, and distinguish missing, redacted,
truncated and present data, including a present zero-byte body. Those states do not
by themselves establish that a particular replay adapter has all required inputs.
Do not manufacture a body by serializing attributes and call it the original bytes.

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
revocation closes delivery immediately. The window duration is not selected, and
no lifecycle operation exists yet; see [delayed delivery](AUTHORIZATION.md#run-completion-and-delayed-delivery).
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
one owner. These are bounded opaque internal IDs, not a persisted schema or selected
wire encoding. `Tenant` supplies the project's scope for this experiment, not a new
organization/project hierarchy.

After validation and bounded admission, the registration authorizer must approve
this exact execution/owner for a trusted creator and return its authenticated
principal. Ordinary agent write permission is insufficient. Authorization precedes
any catalog access; callers cannot supply `RegisteredBy` through Register's arguments.
The ownership catalog then atomically creates the binding or returns the original.
An identical owner retry is a duplicate even if another authorized registrar submits
it; the original creator attribution remains unchanged. A different owner returns
`ErrOwnershipConflict` without changing the binding.

Any other catalog error can hide a committed registration and returns
`ErrRegistrationUncertain` with the cause preserved. Retry the same execution/owner
to recover the original result. A malformed successful reply also returns uncertain
plus integrity failure, rather than a successful receipt. Confirmed success remains
success if cancellation arrives after commit. No rollback or deletion is attempted.
Replacing Registrar retains only state in the supplied catalog; tests model that
replacement in one process, not real host loss or database durability.

The operation uses one authorization call and one catalog operation, with no object
request, payload upload, worker, waiting queue or automatic retry. Its active limit
is independent of Store's; combined catalog concurrency can reach the sum of their
limits. Active limits do not bound accumulated bindings. The test catalog uses a
mutex-protected map with expected constant-time lookup and O(E) entries for E
executions; no worst-case CPU, row-size, latency or throughput claim follows.

Store now refuses an error from the preliminary owner-aware CheckWrite before
upload. Publish must repeat owner/grant checks atomically before duplicate/conflict
outcomes and preserve the original submitter on a retry by another authorized
collector. Lookup separately requires an explicit project-reader grant. Controlled
tests exercise registration plus both record kinds, denial, concurrent retries,
lost replies and a grant change during upload; [the enforcement design](AUTHORIZATION.md)
states the contracts and costs. Legacy rows without attribution fail closed rather
than acquiring history from a later binding. Real credential/grant lifetimes,
admission quotas, binding retention and provider enforcement remain unresolved.

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
not an implemented expiry or quota mechanism; no payload duration has been selected.
The selected configurable default for an expired-record marker is 24 hours after
payload expiry. This extra identity memory does not extend payload availability.

The simpler alternative of a shorter unmatched-capture timeout reclaims unused data
earlier, but makes late span delivery change whether recorded inputs survive. A timer
by itself does not prove a peer will never arrive. The normal policy still needs a
defined start, clock authority, payload duration, policy-change and late-arrival semantics.
Cleanup must account for concurrent readers/writers, uncertain publication and
objects shared by other accepted records. See [retention boundaries](RETENTION.md).

Exercise both arrival orders, retry after a lost reply, metadata supplied
without bodies, present empty bodies, exact byte changes and tenant isolation.
Before OTLP reception, design batch/partial-success/retry mapping separately:
[OTLP acknowledgments](https://opentelemetry.io/docs/specs/otlp/) cover adjacent
client/server nodes, not end-to-end delivery through collectors. Do not interpret
a producer's successful export to an intermediary as Rewind's durable acceptance.
