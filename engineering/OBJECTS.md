# Immutable R2 objects

Status: internal adapter exercised against controlled HTTP peers and the private
`rewind-dev` R2 bucket. The scoped live probe passed creation/retry, conditional
overwrite rejection, a locally discarded successful PUT reply and fresh-client
reads of both original and reconciled bytes.
Public r2.dev access is disabled, no custom domain is attached and no saved-object
expiry rule is configured. The default unfinished-multipart abort rule is not
accepted-record retention. AWS SDK for Go v2 is the selected client. This adapter implements `ingest.Objects`, not record
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
A successful ReadExact verifies the expected stored length and SHA-256 and returns
owned bytes. Its caller supplies the exact size from the accepted catalog reference;
Store repeats that reference's length/hash checks. The adapter's independent configured
byte cap refuses larger expectations before object I/O. A stored length mismatch
is ErrIntegrity, including a shorter body whose hash matches; an expectation above
the cap is ErrTooLarge. Zero means exactly empty, not an unspecified length. This
counts stored bytes, before envelope expansion; expanded-payload limits stay separate.
ETag is not used as SHA-256. Wrong bytes,
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

ReadExact maps only structured NoSuchKey + 404 to `ingest.ErrObjectMissing`; access
denial, generic 404 and unavailable service remain failures. An expected length above
the configured byte cap
returns `ingest.ErrTooLarge` before I/O; a negative expectation or invalid
identifier/configuration returns ErrInvalid. Header/actual-length or digest/contents
mismatch returns ErrIntegrity, with no successful result bytes. Provider text is
redacted in
normal errors; wrapped causes remain available for inspection and must not be
logged indiscriminately. An unresolved Create uses its own uncertainty sentinel,
never catalog absence or a claim that no object was saved.

Fault tests drive the real SDK through controlled HTTP responses: concurrent
conditional writes, lost PUT reply, subsequent absence, replacement client,
tenant separation, wrong bytes, huge XML/bodies, truncation, cancellation and body
closure. Source/race checks plus independent design and implementation reviews are
required. Controlled tests establish behavior against a modeled peer. The opt-in live
probe also observed this small sequence on R2; neither establishes a complete
remotely durable ingest system, host-loss recovery or failover. Remote catalog
publication and combined recovery remain necessary before exposing acceptance.

Sources: [R2 S3 compatibility](https://developers.cloudflare.com/r2/api/s3/api/)
(conditional PUT is atomic against committed state), and
[AWS SDK endpoint configuration](https://docs.aws.amazon.com/sdk-for-go/v2/developer-guide/configure-endpoints.html).
The same read/create/read sequence exists in Concordance's object adapter; no code
is extracted until both applications have exercised their own recovery contract.

### Extraction checkpoint

The comparison against Concordance revision
`d6e7b19c29faf4816169067a545ba993b27ca5cd`, `internal/objectstore/store.go`
(`EnsureVerified`, `Read`), finds the same strict GET/conditional-PUT/GET baseline.
Both projects now have controlled writer-process recovery sequences and an actual
delivered comparison. That evidence is not a joint shared-library test or approval
to extract. Rewind's process drill merged at
`b0b53e7566d95af121808303955074b91138f319`; its object adapter now explicitly
requires exact stored size through ReadExact. Neither application changes its
catalog acceptance or acknowledgment by making that object-read contract explicit.

Applications retain their existing scope-to-key mapping in trusted adapters:
Rewind's opaque tenant bytes and `v1/tenants` layout, Concordance's qualified
collection and `v1/collections` layout. The selected shared direction accepts a
resolved object key; it does not infer authority from that key. Endpoint, bucket,
client and credential routing remain trusted owner configuration. There is no
namespace migration or new shared routing API. Exact stored size and digest, with
an independent byte cap, are the selected common read direction. Final shared
signatures, write-reference construction, admission/deadline ownership and error
mapping still need joint design review before extraction.

| Boundary | Rewind | Concordance |
| --- | --- | --- |
| Authorized namespace | Per-call opaque tenant bytes | Store-bound globally qualified UTF-8 collection |
| Read length | ReadExact verifies expected stored size within an independent cap; Store repeats catalog checks | Reference carries exact size, checked by adapter within its cap |
| Admission and timeout | Adapter bounds active calls and elapsed time | Caller owns admission and supplies a deadline |
| Publication | Record catalog must accept identity/owner/reference | Catalog publishes an entire generation |
| Recovery evidence | Separate writer killed/reaped, working directory removed; replacement finds committed span/capture through PostgreSQL and controlled objects | Writer killed/reaped and directory removed; fresh process recovers catalog-published generation and exact quotes; actual final COMMIT response loss exercised |

Rewind's process drill observes both acknowledged acceptance and an actual lost
PostgreSQL COMMIT response, plus a disconnected stored PUT and a crash before
publication. It counts HTTP bodies/requests and checks current permission refusal
before object access. The controlled HTTP service and database survive, on the
same test host: this is not real R2/host-loss recovery. See
[the recovery experiment](INGEST.md#writer-process-recovery) for sequence, counts
and limits. Concordance's [counterpart drill](https://github.com/sidsharma96/concordance/blob/d6e7b19c29faf4816169067a545ba993b27ca5cd/engineering/experiments/writer-process-recovery.md)
likewise uses surviving local PostgreSQL/HTTP objects and records per-phase request
and body-byte counts. Its completed retry returns the original generation without
object requests; an unpublished candidate remains invisible until explicit retry.
Different record/generation fixtures are not comparative adapter performance.

Concordance also has verification receipts and a reuse component above its strict
object path. Those do not establish Rewind's authorization, retention or acceptance
and are not approved for shared extraction. Both process examples start with
catalog state after replacing the writer, resolve ambiguous object and final catalog
commit outcomes, and record requests/body bytes. Keep the remaining scope,
write-reference, admission/deadline and error-mapping differences explicit before
choosing the complete library API. Neither adapter currently supplies range reads
or safe deletion.

The coordination trigger is any proposed change to the object operations or their
scope, integrity, bounds, retry/error, admission or timeout contract, including
range reads, deletion and reuse/cache shortcuts. Review this checkpoint with the
Concordance development agent before such work diverges. Also revisit immediately
after Rewind's first native record publication and ingest-host replacement drill;
do not wait for replay or the trace viewer. The shared code remains a small
in-process library; this is not a message bus for agents running in either product.

The handoff must name both source revisions and cover each application's authorized
write, committed catalog reference, replacement-writer read, lost write/commit
replies, requests, transferred bytes and remaining failures. Keep application
acknowledgments distinct. Propose the smallest common API only when both sequences
are exercised; review the differences above and require an explicit response from
the other project's agent before calling that API agreed. The delivered exchange
and selected key/read directions are readiness evidence, not approval of the
complete API or extraction.

## Opt-in development-bucket probe

`TestR2LiveImmutableObjects` only targets `rewind-dev`. Supply
`REWIND_R2_ENDPOINT`, `REWIND_R2_ACCESS_KEY_ID` and
`REWIND_R2_SECRET_ACCESS_KEY` as explicit local environment variables. Use an R2
Object Read & Write credential restricted to this bucket. The test accepts only
the default HTTPS Cloudflare R2 account endpoint, blocks redirects, bounds the
transport and does not discover credentials or load an environment file.
Keep credentials and their local files out of logs and source history.

```sh
REWIND_R2_LIVE_TEST=1 go test -race -count=1 -v ./internal/objectstore \
  -run '^TestR2LiveImmutableObjects$'
```

Predict the result before running: a new object and identical retry must read as
the same bytes from a fresh client. A direct conditional PUT of wrong bytes to
that saved key must return PreconditionFailed/412 and preserve the original.
For a second object, a local wrapper closes and discards a successful real PUT
response; Create must reconcile through GET, and another fresh client must read
those bytes. The local response fault is not an actual provider outage.

The test retains two small synthetic objects in a random tenant namespace and
never lists or deletes objects or changes bucket settings. Later runs use new
namespaces. The live test skips unless explicitly enabled; normal checks use
controlled peers. The live pass covers the observed provider sequence, not
machine loss, remote catalog publication, permanent immutability or failover.
The S3 credential has storage privileges; it is not an agent execution credential.

Credential setup: [Cloudflare R2 API tokens](https://developers.cloudflare.com/r2/api/tokens/).
