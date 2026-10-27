# Ownership at record acceptance

Status: implemented as internal coordination with controlled test capabilities. It exercises
already selected producer ownership and explicit project-reader rules; it selects
no credential format, grant lifetime, endpoint or database schema.

Alice owns call 42. A trusted host registers its tenant/trace/span to Alice's
run/instance before either record arrives. Bob has writing permission for his own
instance in the same project. Knowing call 42's IDs must not let Bob submit either
kind, even with identical bytes. A collector may submit for Alice only with explicit
authority for Alice's project/run/instance. Its authenticated principal remains
separate from the execution owner.

```text
trusted request -> authorize writer -> preliminary catalog check
                                             |
                                      immutable upload
                                             |
                         catalog: current writer grant + registered owner
                                  + acceptance + original attribution
                                             |
                                          receipt

trusted request -> authorize project reader -> authorized catalog lookup
                                             |
                                      bounded object read
```

## Internal capabilities

Store replaces the tenant-only authorizer and catalog contracts, rather than retaining
an optional unprotected mode. Write's caller still supplies only identity and bytes.
The authorizer approves the exact execution and returns trusted attribution: owner
(run/instance) and submitting principal. Read uses a separate authorization method
for the explicit project-reader permission and returns the reader's principal.
Validate these bounded identities before calling storage. No caller-provided label
becomes a principal. Actual credential verification remains an adapter responsibility.

The record catalog has three operations:

- CheckWrite checks current writer authority and the registered execution owner,
  then expiry, before upload. A missing record with a valid binding is allowed.
  It returns no acceptance receipt and reserves nothing.
- Publish checks the same authority and binding atomically with the record outcome,
  before duplicate, conflict or expired results. Save reference and original owner and
  submitting principal together. Return the original receipt for an identical retry,
  even if another authorized collector submits it; do not replace attribution.
- Lookup checks the current explicit project-reader grant before exposing absence,
  expiry or a reference. It returns a stored reference with original attribution;
  the coordinator validates it before fetching any object.

Registration and record publication must share authoritative binding state in a real
adapter. No publication may create a missing binding, accept a mismatched owner or
retroactively attribute a legacy tenant-only record. Missing/invalid attribution in
an existing row fails closed. Existing experiments contain synthetic process-local
state only; there is no deployed dataset to migrate.

Confirmed denial is an explicit error and must not be wrapped as an uncertain
publication. An arbitrary catalog failure may hide a commit and stays uncertain.
Malformed successful receipts also return uncertain plus integrity failure. A
duplicate preserves the original submitter, but must still match the requested
reference and authorized owner. A new receipt must match the current submitter.
Confirmed success remains success after cancellation, as before.

## Failure and cost model

Removing permission between the preliminary check and Publish must prevent new
acceptance, and may leave an uploaded unreferenced object. Retain it: another
accepted record may share its bytes, and no cleanup protocol exists. Repeated
unauthorized attempts stopped at the first check upload no bytes. A denied current
attempt says nothing about an earlier attempt's acceptance. Unavailable permission
state is an error, never an allow or a fabricated absence.

The read permission check orders authorization with catalog lookup. It does not
cancel a read already authorized or recall bytes already returned. This is not a
global kill switch. Credential expiry, revocation propagation and active-run shutdown
remain unresolved. The selected normal-completion delivery policy is described below;
a deadline predicate and controlled catalog exercise delivery timing, not a full
run lifecycle.

## Run completion and delayed delivery

Selected policy: normal completion closes new execution registration
and starts a finite delivery window for already registered calls. Authorized direct
writers and explicitly delegated collectors can deliver span or capture during that
window; the same owner and current-grant checks still apply. No collector-only
bypass or transfer of ownership is introduced. The selected configurable default
is ten minutes from trusted normal completion, separate from payload retention and
the 24-hour expired-record marker window.

An optional collector gathers records, queues or batches delivery and retries
failures. It does not become the execution owner. Finishing the producer's work
does not establish that every record has reached Rewind. There is no collector
integration or run-completion operation yet.

| Run condition | New call registration | Delivery for an existing binding |
| --- | --- | --- |
| Running | Allowed with existing creator permission | Allowed with existing writer permission |
| Completed normally, before the delivery deadline | Closed | Allowed with existing writer permission |
| Delivery deadline reached | Closed | Closed |
| Explicitly revoked | Closed | Closed immediately at the authoritative operation boundary |

The trusted catalog must establish completion and its fixed deadline together.
Producer timestamps cannot move that deadline. Repeated completion or delivery
must not extend it; revocation cannot reopen it. A write that begins before the
deadline but reaches publication after it must be refused, possibly leaving an
uploaded object. Checking only at upload admission is insufficient. An identical
record retry still needs delivery permission, even if the record was accepted earlier.

This policy does not extend payload retention, expired-record markers, credentials
or grants. It does not grant read access or delete accepted records. Registering a
call beforehand does not prove its delivered contents were produced before normal
completion. A bounded window deliberately permits authorized new evidence for those
existing bindings. Explicit revocation overrides the remaining delivery window;
it does not recall reads, terminate agents or cut external credentials by itself.

Compared with closing submissions immediately on normal completion, the window
reduces rejection of delayed evidence but retains submission authority for longer.
It cannot guarantee that every collector queue empties before the deadline. Revisit
its duration against observed queue age, retry budgets, publication latency and
acceptable authority duration. Catalog clock/isolation rules, trusted lifecycle
control and the behavior of registration retries still need an implementation design.

The implemented internal slice is a fixed `DeliveryWindow` value, constructed from
a trusted completion instant and an explicit positive duration. Its default constant
is ten minutes; callers still supply policy explicitly. It compares a supplied
instant against the original interval, including completion and excluding the
deadline. Zero values and unspecified times fail closed. The value has no clock,
worker, timer, mutation or completion/retry operation that could move its deadline.
It is not a persisted encoding or a real run lifecycle.

A controlled catalog uses this value under its existing mutex after owner/grant
checks at CheckWrite and Publish. Tests move a supplied time during upload,
check both span and capture, and remove the grant during the window. This isolates
the deadline rule without deciding registration-retry responses or introducing a
new lifecycle endpoint. The model leaves reads and original attribution intact.
New-registration closure and trusted completion/revocation operations remain future
integration, not guarantees established by this value.

For a run completed at 12:00 with the default duration, publication at 12:09:59 is
eligible and publication at 12:10 is refused. An upload that starts at 12:09:59 but
publishes at 12:10 is refused too. An immediate-close alternative loses that last
delivery opportunity; a timer that closes a local gate can lag or disappear with
the ingest host. Comparing the deadline at final authoritative publication is the
simple baseline. The predicate takes O(1) time and space with no extra object
request or transferred bytes. Real catalog clock/isolation and recovery still need
provider tests; a wall-clock rollback must not be assumed safe merely because a
local value compares times correctly.

For n record bytes, retain O(n) copy/hash work and transient memory. Each permitted
write uses one preliminary catalog operation, one object create of n bytes, and one
atomic publication. Each permitted read uses one catalog operation and one bounded
object read. Ownership and grant checks increase catalog work without another
coordinator round trip. A simple test catalog holds bindings, grants and records
under one mutex: expected O(1) map operations, O(E + G + R) entries for E executions,
G grants and R records. It serializes tests, not a production storage strategy or
latency result. No new queue, goroutine, cache, retry loop or dependency is needed.

The credible simpler alternative is one permission check followed by the old
publication primitive. It saves the final grant check but accepts a record when
authority changes during upload. Another alternative is a trusted project-wide
writer; the user already chose instance isolation instead. Revisit these boundaries
when a real catalog must define transaction isolation, revocation ordering and
recovery under competing writers.

## Controlled evidence

Existing upload/publication/integrity/retention tests use the owner-aware seam.
`authorization_test.go` exercises registered Alice versus Bob across both record
kinds, wrong tenant/run/instance, missing registration, an explicitly delegated
collector, changed submitting principals, original attribution after a lost reply and retry, reader-only versus
writer-only permission, and unavailable authorization state. Denial precedes
object access and duplicate/conflict disclosure. A deterministic barrier
changes a grant while upload is blocked; final publication refuses it for new,
duplicate and conflicting requests. Concurrent collectors retain one original
submitter. Tests refuse legacy rows with missing attribution rather than inferring
their history from a later registration.

These experiments model trusted capabilities in one process. Real authentication,
PostgreSQL transactions, R2 durability and trace enumeration remain absent.
