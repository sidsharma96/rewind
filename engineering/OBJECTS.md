# Immutable R2 objects

Status: internal adapter implemented and exercised against controlled HTTP peers;
no R2 bucket or credentials are provisioned. AWS SDK for
Go v2 is the selected client. This adapter implements `ingest.Objects`, not record
acceptance, authentication, retention or a shared Concordance library.

## One write and its retry

Alice's saved bytes have a SHA-256 digest. The object key is
`v1/tenants/<hex tenant bytes>/sha256/<hex digest>`. Tenant identifiers are exact
opaque bytes, between one and 256 bytes; hex encoding prevents path ambiguity and
preserves non-UTF-8 identifiers. A key grants no authority. Store authorizes before
calling Objects; the adapter is an internal trusted capability.

Create first reads that key. If the bytes already match exactly, it succeeds.
Only a structured `NoSuchKey` error with HTTP 404 permits a conditional PUT with
`If-None-Match: *`. It then reads and verifies the bytes, including after an
unsuccessful PUT response. A timeout can hide a completed write; reading identical
bytes resolves that uncertainty. A failed reconciliation returns an uncertain
object outcome (`objectstore.ErrUncertain`) and never authorizes catalog
publication. Store preserves that error through wrapping; callers can inspect it
and safely retry the same identity and bytes. This differs from the catalog-only
`ingest.ErrUncertain`. It never deletes.
A missing read after PUT does not prove that no write remains in flight.

The simpler alternative is conditional PUT then GET on every call. It saves a
request for a new object but uploads all bytes again on an identical retry. The
read-first sequence uses at most three SDK operations (GET, PUT, GET), one for an
existing object. Revisit after measuring duplicate frequency, object sizes and
request cost on a representative capture workload; no performance result exists.

A successful Create verifies SHA-256 and exact byte equality against its input.
A successful Read verifies SHA-256 and returns owned bytes. Read's caller supplies
an upper byte bound, not an expected length; Store separately checks the catalog's
exact size. Empty objects are allowed. ETag is not used as SHA-256. Wrong bytes,
truncation and oversized responses never produce a successful result.

## Ownership and limits

A concrete adapter borrows an `s3.Client` and a bucket from trusted configuration.
The owner supplies the R2 endpoint, region, credentials and bounded HTTP transport,
and owns connection cleanup. The adapter performs no ambient credential discovery.
It adds no worker, cache or queue. Limits cap active method calls, per-object bytes
and total operation time. Admission is nonblocking, before object allocation/I/O;
full admission returns `ingest.ErrBusy`. This limit is separate from Store's
admission; configure it to accommodate the Store calls sharing this adapter. The
caller must not mutate Create's input until return. The pinned SDK closes its safe request-body
wrapper before returning, detaching the caller's byte reader even if the transport
retains that wrapper. The SDK and configured transport own connection cleanup.

Each SDK operation disables SDK retries and request/response logging, and selects
only required protocol checksums for R2 compatibility. A response wrapper caps
control/error XML before SDK decoding (64 KiB) and successful GET bodies at the
read limit plus one probe byte. The HTTP transport remains responsible for header
limits, TLS, connection buffers and any transport-level retry. All returned bodies
are closed, including failed reads. SDK calls inherit the operation timeout.

For n input bytes, hashing/comparison is O(n); a live operation retains O(n) bytes.
For A active calls and maximum B bytes, adapter working memory is O(A*B), excluding
callers' buffers/results and transport/SDK overhead. A new successful object uses
one PUT of n bytes and two GET operations, the first absent; a duplicate uses one
GET of n bytes. Recovery requires a new client reading the same tenant/key; it
needs no ingest-local manifest. Application recovery still requires PostgreSQL's
accepted reference, attribution and trace index. This adapter cannot supply them.

## Errors and evidence

Read maps only structured NoSuchKey + 404 to `ingest.ErrObjectMissing`; access
denial, generic 404 and unavailable service remain failures. An oversized read
returns `ingest.ErrTooLarge`, invalid identifiers/configuration return ErrInvalid,
and digest/contents mismatch returns ErrIntegrity. Provider text is redacted in
normal errors; wrapped causes remain available for inspection and must not be
logged indiscriminately. An unresolved Create uses its own uncertainty sentinel,
never catalog absence or a claim that no object was saved.

Fault tests drive the real SDK through controlled HTTP responses: concurrent
conditional writes, lost PUT reply, subsequent absence, replacement client,
tenant separation, wrong bytes, huge XML/bodies, truncation, cancellation and body
closure. Source/race checks plus independent design and implementation reviews are
required. These tests establish adapter behavior against an emulator, not R2
compatibility, provider durability or a complete remotely durable ingest system.
A real R2 write/read/recovery experiment and remote catalog publication remain
necessary before exposing acceptance.

Sources: [R2 S3 compatibility](https://developers.cloudflare.com/r2/api/s3/api/)
(conditional PUT is atomic against committed state), and
[AWS SDK endpoint configuration](https://docs.aws.amazon.com/sdk-for-go/v2/developer-guide/configure-endpoints.html).
The same read/create/read sequence exists in Concordance's object adapter; no code
is extracted until both applications have exercised their own recovery contract.
