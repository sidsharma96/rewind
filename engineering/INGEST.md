# First ingest coordination experiment

Status: implemented as an internal coordinator with controlled test backends.
PostgreSQL and R2 are the chosen architecture. Internal adapters now cover
ownership registration, record publication/lookup and immutable objects. Internal
typed span/capture acceptance and saved-byte playback now compose with these adapters;
opaque credential verification and current native grants are implemented. The
[live host-recovery driver](HOST-RECOVERY.md) also composes them with a remote
PostgreSQL catalog and live R2 across actual ingest-VM replacement.
The controlled experiment isolates ordering and error handling. A composed native
test now joins Store, PostgreSQL registration/publication/lookup and the object SDK
against a controlled HTTP peer. It covers discarded successful publication replies,
fresh-client reads/retries and grant removal during upload. A separate-process
drill now kills and reaps the writer, removes its private working directory, and
recovers through a new process with surviving PostgreSQL and controlled HTTP
services. These tests establish neither provider durability nor recovery after
real host loss; the separate live driver tests that boundary with surviving
remote stores. No public endpoint,
wire format or shared storage library is added.

## One saved record

The operation accepts one immutable record at a time. An identity contains a tenant,
trace, span and kind: span metadata or one logical capture for that execution.
Kind separates their catalog entries without creating another execution. Span
acceptance does not imply capture acceptance. The caller supplies opaque bytes; the
coordinator does not validate them as span metadata or captured bodies. In this local
experiment only, equal means exactly equal bytes. The agreed production rule is
to compare defined metadata fields by their typed values and captured request and
response bytes exactly. Outer transport JSON field order and spacing must not
create a conflict; formatting differences within captured bytes still matter.
Trace and span keys are fixed-size nonzero binary OpenTelemetry IDs; project
identifiers remain opaque bounded strings. Validation precedes authorization and
storage access. The legacy Store still does not parse opaque contents against the key. The typed
Spans and Captures coordinators validate their own payloads and compare decoded
contents with the rules in [the capture contract](CAPTURE.md#typed-capture-and-saved-response).
They share the Store admission gate and native transaction state machine. TraceReader
discovers execution IDs from current project/trace pages; Captures.Playback returns
a complete saved response with request availability, using current reader permission.
Public admission and application-specific replay remain unimplemented.

Separate authorization methods approve a writer for the exact execution or an
explicit project reader before storage access. Tests supply controlled principals;
no network authentication is implemented. The catalog checks current grants and the
[registered execution owner](AUTHORIZATION.md) at its authoritative operation boundary.
Store never infers authority from a tenant, trace ID or telemetry label.

The operation follows this order:

```text
Authorize and admit bounded work
              |
              v
Check current writer grant, registered owner and expiry
              |
              v
Create immutable object (tenant + content digest)
              |
              v
Atomically check authority and publish reference + original attribution
              |
              v
Return success
```

The preliminary CheckWrite avoids uploads for unauthorized or known-expired records
and stops writes when the catalog cannot be consulted. Success permits upload for
either an absent or live record; it returns no duplicate receipt or conflict. Every
error stops the write. The check reserves nothing. Grant changes, expiry or another
writer can change the result before final publication, which checks state again.

The object store capability creates an immutable object or confirms an identical
object already exists. It must not overwrite different bytes. Its success requires
completed durable bytes and verified integrity; the adapter must demonstrate
this contract with the actual provider. A tenant and SHA-256 digest identify an
object logically; the [R2 adapter](OBJECTS.md) supplies the versioned, hex-encoded
tenant namespace. The digest is not span
identity. Identical bytes in different spans still produce separate catalog entries.

Immutable bytes could later be cached within a tenant after authorization. A cache
would only be an expendable copy: eviction must not remove accepted durable data,
and a cache hit cannot replace catalog publication for a new span. Sharing stored
bytes is deduplication; serving an expendable copy is caching. Neither makes two
executions the same event. No cache is implemented or selected by this experiment.

The catalog capability publishes a single identity atomically. It either creates
the reference, recognizes an identical accepted reference, or returns a conflict
without changing the original. Confirmed permission denial and expiry are also
refusals of this attempt. Any other publication error has an unknown outcome:
the catalog may have committed before the response was lost. No automatic retry or
rollback is attempted. The caller must retry the same identity and captured bytes.
A successful commit remains success even if cancellation arrives immediately after.
The receipt includes catalog-owned `AcceptedAt` and `ExpiresAt`, recorded once
with the original reference and attribution. An identical retry or lookup returns
those original times, even after trusted retention configuration changes. Both
must be specified and expiry must follow acceptance. A malformed publish receipt
is uncertain plus integrity failure; a malformed lookup stops before object read.
The coordinator does not sample its local clock to decide availability or rebuild
an expiry from current configuration. Controlled tests exercise this result
contract; it does not implement durable timestamp storage or automatic expiry.
An explicit catalog expired-record result is also a confirmed refusal, not uncertain
publication. A retained expired identity refuses identical or changed contents without
altering the original acceptance. Lookup returns expired before any object read.
Span and capture publication are independent. Failure or conflict of one record
must not overwrite or roll back the other. No combined acceptance operation exists.

The coordinator's read operation resolves one known record through the catalog, then
reads its object with a byte limit and checks its size and digest. Catalog absence
is different from catalog unavailability or a missing referenced object. The latter
cases must not be reported as an absent span. Trace enumeration, pagination and
multi-span batch atomicity are not selected by this per-record experiment. It does
not join span/capture records or check parent existence.
Either record may be accepted first, as specified in [the capture design](CAPTURE.md).
The absent peer does not block acceptance or reading of the existing record.
This opaque primitive does not itself implement credential verification, typed
body validation, automatic retention or trace assembly; it must not be exposed
directly as a product attachment API. The typed coordinators and internal access
adapter supply the implemented body and authority checks described above.

## When one step fails

| Interruption | Possible durable state | Coordinator behavior |
| --- | --- | --- |
| Authorization or admission fails | Neither store accessed | Return the failure |
| Preliminary lookup returns expired or fails | This attempt has uploaded or published nothing | Return the lookup error; do not label it uncertain publication |
| Upload fails, possibly after storing bytes | Unreferenced object may exist | Do not publish; return the upload failure |
| Upload succeeds; catalog fails before commit | Unreferenced object exists | Return uncertain publication; do not delete the object |
| Catalog commits; response is lost | Accepted span exists | Return uncertain publication; identical retry resolves it |
| Current grant or registered owner check refuses publication | Uploaded bytes may be unreferenced | Return denied before disclosing duplicate/conflict/expiry; retain objects |
| Catalog returns a content conflict | Original span remains accepted; new bytes may be unreferenced | Return conflict; preserve both stores' existing live data |
| Identity expires after preliminary lookup | Uploaded bytes may be unreferenced | Final publication returns expired; preserve original acceptance |
| Ingest instance is replaced | Catalog and objects remain available | Fresh coordinator can read and resolve retries |
| Catalog read fails | Acceptance state cannot be established | Return error, not absence |
| Referenced object is unavailable | Catalog reference exists | Return read failure, not absence or fabricated data |
| Referenced bytes are corrupt | Catalog reference exists | Return integrity failure |

No deletion capability is provided to the coordinator. Cleanup requires a separate
protocol that accounts for in-flight writes, uncertain commits, readers and retention.
A timeout or negative object read does not establish that a previous in-flight write
cannot still commit. Do not delete uncertain objects based on elapsed time alone.

Active-operation and byte limits do not bound accumulated orphan objects. Repeated
conflicting uploads can create more objects even though the catalog rejects them.
This is a known limit of the local experiment. Real ingestion needs per-tenant
storage/rate controls and a reviewed cleanup protocol. The preliminary lookup does
not short-circuit live duplicates or conflicts; publication still decides them.

Known expired retries stop before upload. Expiry after the check still prevents
logical acceptance but may leave newly uploaded, unreferenced bytes. The
[retention experiment](RETENTION.md) uses a
validated deadline value and controlled catalog to test expired refusals, not actual
expiry, durable marker storage or safe reclamation of shared objects.

## Bounds and ownership

Use explicit limits on record bytes and active operations, with no internal waiting
queue, goroutines or retry loops. When all operation slots are occupied, return a
busy error. Tests supply their own limits; they are not service defaults or a public
SLA. The future transport must bound bodies before decoding; this library receives
an already allocated buffer. Identity strings also have a fixed internal length cap
for the experiment, not a selected external identifier format.

The caller must not modify input during a write. After admission and validation,
the coordinator copies captured bytes before hashing and passing them to storage.
Backends may not retain or mutate arguments. The read result belongs to the caller.
The active-operation limit bounds coordinator-owned work, not results a caller keeps
after return or data retained by a backend. Context cancellation is propagated to
capabilities; each capability must honor it. The coordinator creates no goroutines
and cannot force a noncompliant capability to stop.

For n captured bytes, hashing and copying take O(n) work and O(n) transient space.
A write that passes the preliminary check performs one catalog lookup, one object
create and one catalog publication. A failed preliminary check performs one lookup
and no object request or publication. A read performs one catalog lookup and one
bounded object read. No throughput or latency claim is made.
Accepting both span and capture uses two writes. Limits are shared across both kinds;
unknown kinds fail validation before authorization or storage capability calls.
Compare the later adapters against this simple baseline before batching or caching.
If a later coordinator uses a smaller read limit, an older accepted record may
exceed it. Report the limit error without calling that valid record corrupt.

## Evidence required beyond the local experiment

The controlled backends retain state when a coordinator is replaced; that is a model
of losing ingest-local state, not proof of remote recovery. Exercise before- and
after-commit failures, concurrent duplicates and conflicts, cancellation, tenant
isolation, oversized references, missing objects and corrupt bytes using explicit
barriers rather than sleeps. Do not count this test double as a PostgreSQL adapter.

### Writer-process recovery

`internal/catalog/recovery_test.go:TestWriterProcessRecovery` uses the real catalog,
coordinator, envelope/metadata codecs and object SDK. PostgreSQL and the controlled
HTTP object service outlive the writer subprocess. Each case kills the writer,
waits for its exit, removes its working directory, and starts a new test executable
process with fresh clients. The replacement receives connection configuration,
the known execution key and original synthetic producer inputs for retries, not a
saved receipt, catalog reference, object map or writer-local files. The CLI still
does not ingest. Trusted fixture principals do not verify real credentials.

| Interruption | State resolved by the replacement |
| --- | --- |
| Both span and capture acknowledged | Both records retain their bytes, references, original attribution and times |
| Conditional PUT stored, HTTP response disconnected | The adapter verifies with GET; accepted records survive writer termination |
| Typed span COMMIT response lost on the PostgreSQL connection | Spans reports uncertain with no receipt; its committed span and earlier capture are found after writer termination |
| Capture uploaded, writer killed before Publish | Span remains accepted; capture lookup is absent despite the retained object; explicit retry creates its first acceptance |

The original drill arms the local PostgreSQL fault proxy at typed span publication;
the typed-capture drill also arms it at capture publication. It
observes `CommandComplete(COMMIT)` and idle `ReadyForQuery`, withholds both and
disconnects. This exercises pgx's actual commit-response error path rather than
discarding a successful Go method result. It is limited to explicitly plaintext
loopback/Unix disposable servers, never a TLS downgrade. The HTTP peer likewise
disconnects after storing an actual conditional PUT. No failure point relies on a
sleep. Child processes are killed and joined on failure; proxy connection pairs,
message bodies and lifetimes are bounded and their goroutines are joined.

In every case changed typed span metadata conflicts and leaves the accepted span
unchanged without uploading a known-conflicting candidate. A further process using
revoked reader/writer grants is refused without object requests, and original
receipts remain unchanged after the test restores the grants. Span and capture
still have independent acceptance; this does not introduce an atomic pair or batch.

The fixture counts HTTP body bytes read/written, excluding headers, protocol/SDK
buffers and guaranteed network delivery. For its 187-byte gzip span envelope and
12-byte plain legacy capture envelope, the initial writes use four GETs, two PUTs,
199 request-body bytes and 273 response-body bytes. Replacement reads, exact retries,
one rejected typed conflict and a final preserved-span read use five GETs, zero
PUTs and 585 response-body bytes. Before-publication crash uses four GETs and 573
response bytes because catalog absence stops the read; the explicit capture retry
then performs its first catalog acceptance using the retained object. These are
recorded synthetic operation counts, not timings or production estimates.

The typed-capture drill extends this existing child-process harness. Alice records
a handoff, and Bob records separate failed and successful tool invocations in the
same trace. The controlled wrapper exposes no result on its first HTTP failure;
the second result is complete with its request deliberately redacted. After both
writers are killed/reaped and their directories removed, and after the tool is
stopped, a new reader receives only project/trace plus storage configuration. It
finds all three spans, inspects unavailable evidence, refuses failed-result playback
and returns the exact saved successful response with mandatory request availability.
Original receipts and independent owners survive both acknowledged publication and
an actual capture COMMIT reply loss. This workload makes seven GETs and zero PUTs
or tool calls during recovery; current-reader denial makes no object request.

This establishes a process boundary with surviving test services. The test parent
still owns the in-memory object service, PostgreSQL remains available, and both
may share the same physical host. It does not establish ingest-machine loss,
remote catalog placement, storage-service loss, real R2/provider faults, database
failover/restore or zero-loss backups. The separate
[live drill](HOST-RECOVERY.md) now tests actual ingest-VM destruction with remote
PostgreSQL and R2 surviving; catalog and provider loss remain outside its evidence.
The smaller existing in-process test remains the baseline.

PostgreSQL 16's [message-flow contract](https://www.postgresql.org/docs/16/protocol-flow.html#PROTOCOL-FLOW-SIMPLE-QUERY)
defines the completion/ready boundary; its
[commit settings](https://www.postgresql.org/docs/16/runtime-config-wal.html#GUC-SYNCHRONOUS-COMMIT)
separate WAL flush from other durability modes. The proxy uses the existing
pgx v5.11.0 `pgproto3.Frontend.Receive` and `SetMaxBodyLen`, not a new wire parser.
These mechanisms establish the fault placement, not provider durability.

The live drill verifies immutable R2 writes, checksum/size handling, PostgreSQL
transactions and synchronous durability settings with the catalog outside the
destroyed ingest VM. Actual COMMIT-response loss remains a separate local fault
test; the live driver suppresses an application reply after commit. Define and
test database failover and restore guarantees:
restoring an older catalog can lose accepted references even if every R2 object
survives. Backups are not automatically a zero-loss failover protocol. Arbitrary
catalog-service or R2 data loss is outside the agreed ingest-host-loss guarantee
and needs an explicit disaster plan before stronger claims.
