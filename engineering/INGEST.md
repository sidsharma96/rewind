# First ingest coordination experiment

Status: implemented as an internal coordinator with controlled test backends.
PostgreSQL and R2 are the chosen architecture, but adapters and remote infrastructure
do not exist yet.
This experiment implements the ordering and error handling with controlled test
backends. It cannot establish provider durability, SQL isolation or recovery after
real host loss. It adds no public endpoint, wire format or shared storage library.

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
The field schema, identity validation and deterministic record encoding need a
separate checkpoint. The coordinator does not parse or normalize transport JSON.

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
completed durable bytes and verified integrity; a future adapter must demonstrate
this contract with the actual provider. A tenant and SHA-256 digest identify an
object logically; no bucket naming scheme is chosen here. The digest is not span
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
Real authentication, body validation, automatic retention and query assembly are
still absent; this primitive must not be exposed directly as a product attachment API.

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

Before any real success response, verify immutable R2 writes, checksum/size handling,
PostgreSQL transactions and synchronous durability settings against deployed services.
Place the catalog outside the ingest host. Test connection loss around COMMIT and
replacement of the ingest host. Define database failover and restore guarantees:
restoring an older catalog can lose accepted references even if every R2 object
survives. Backups are not automatically a zero-loss failover protocol. Arbitrary
catalog-service or R2 data loss is outside the agreed ingest-host-loss guarantee
and needs an explicit disaster plan before stronger claims.
