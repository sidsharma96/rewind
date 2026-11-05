# Ownership at record acceptance

Status: internal credential verification and catalog authorization are implemented.
They enforce selected producer ownership and explicit project-reader rules. Native
PostgreSQL CheckWrite/Publish now repeat current writer, binding and run checks;
Lookup checks an exact project-reader grant and saved ownership. The composed
coordinator test publishes real SQL rows against a controlled object peer. The
authentication adapter verifies separate opaque bearer credentials for individual
principals, with permissions kept authoritative in PostgreSQL. A trusted host
helper registers actual OpenTelemetry SDK span IDs. Operator grant/credential
setup is explicit. The [Rewind service](SERVICE.md) now receives records through
verified credentials and exposes read-only browser sessions. Credential
distribution/rotation and authenticated run controls remain unimplemented.

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

`access.Credentials` maps each verified bearer credential to one submitting
principal. Its direct HTTP adapter requires TLS and exactly one Authorization
header; forwarded protocol/owner/principal headers supply no identity. TLS client
certificate validation belongs to the caller's client configuration. Alice, Bob,
a collector and a project reader remain distinguishable. A credential does not establish execution
ownership, grant project reading or replace the catalog's current permission checks.
Credential rotation must preserve the principal and existing execution identities.

We choose this direction to integrate the first trusted application without adding
an identity provider. OpenTelemetry Collector's
[bearer verifier](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/f412fad84ccd3cd7ffe1bcb5bc2608940984effa/extension/bearertokenauthextension/bearertokenauth.go#L234-L254)
checks a configured credential but returns the original context, so copying that
mechanism alone would not supply Rewind's separate principals. Its
[OIDC verifier](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/f412fad84ccd3cd7ffe1bcb5bc2608940984effa/extension/oidcauthextension/extension.go#L216-L247)
verifies the credential before extracting identity; neither mechanism establishes
Rewind ownership. Revisit an existing OIDC integration when a real application
needs its managed issuance, rotation or identity system. Opaque credential
distribution and rotation remain our responsibility; no instantaneous global
credential-revocation guarantee is selected.

Tokens contain 32 random bytes encoded as 64 lowercase hexadecimal characters.
Server snapshots retain SHA-256 digests of that text, principal names and exclusive
expiry instants, not raw tokens. The configured credential count and lifetimes
have no production defaults. Authentication scans every configured digest with
constant-time comparison/selection; malformed-token parsing and expiry checks do
not have that timing promise. Verification costs O(C) for C configured credentials;
snapshot space is O(C). Each later protected operation rechecks the private
context's expiry and cancellation. Snapshot replacement does not invalidate old
contexts; an already-authorized operation may finish after expiry, and wall-clock
rollback can extend apparent validity. Current catalog grant removal remains
ordered at authoritative operation boundaries.

`access.Authorizer` queries the native catalog for current permissions. WriterFor
derives the owner from the immutable registered binding and checks the verified
principal's writer grant. CheckReader requires its independent project grant;
CheckCreator permits an exact-call grant or separately granted trusted-host scope.
Each preliminary check adds one bounded catalog transaction, no object request,
and reserves no acceptance. Store and Registrar still repeat current policy at
their final operation boundaries. The adapter creates no policy cache or worker.

`producer.Host` copies a configured project/run/instance and registers the actual
SpanContext returned by the standard SDK. It changes no IDs and exports no span.
Host creation permission is explicit in `host_creator_grants`, scoped to one
project/run/instance/principal, separately from exact-call `creator_grants` and
writer/reader grants. A trusted operator installs/removes these grants under the
exclusive project gate. Registration checks either current creator grant under
the shared gate before its existing binding and run-admission decision. No
per-call policy row is synthesized; no agent-facing grant-management API is added.
Only trusted host code may hold this credential. It does not isolate malicious
code sharing that credential-bearing process or attest truthful execution.

Compared with operator approval for each freshly generated call ID, host scope
allows normal applications to start calls without an administrator round trip.
It gives the trusted host broader creation authority within that one instance;
ordinary writer and collector credentials gain none. Revisit if the actual host
cannot protect that credential or requires narrower creation authority. Retry an
uncertain registration with the same SDK IDs and owner; starting another span
would create a different execution. Removing creator scope does not implicitly
remove delivery or reading authority, which retain independent grants.

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
an existing row fails closed. Native read tests use synthetic state in disposable PostgreSQL; there is no deployed
accepted-record dataset to migrate.

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
authenticated control still needs an implementation design. The selected registration retry rule
permits an authorized identical existing binding during the open delivery window.
It returns the original registrar without adding a binding or moving the deadline.
New or changed bindings are refused after normal completion. At the deadline or
after revocation, registration retries are refused too.

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
the deadline rule without introducing a new lifecycle endpoint. The joined run
admission below now exercises registration retries too. The model leaves reads and original attribution intact.
Trusted completion/revocation operations remain future integration, not guarantees
established by this value.

The implemented internal slice adds a concrete `RunAdmission` value: explicitly running,
normally completed with an original DeliveryWindow, or revoked. Its operations
complete a running value once, preserve that original completion on retries, revoke
without reopening, and check record or registration admission at a supplied instant.
Zero/unknown state fails closed. It is a value owned by a catalog operation, not a
concurrent state holder or a trusted controller. No new endpoint, run-control grant,
credential, automatic clock or provider encoding is selected.

The combined controlled catalog keeps known project/run states and exact
creator grants alongside bindings. Under one mutex, registration must check current
creator permission and both applicable run states before returning duplicate or
conflict outcomes or creating a binding. An existing binding's run cannot be
bypassed by proposing an owner in a different running run. Only an identical
execution/owner retry is admitted during the completed run's open window; otherwise
it is refused. Original registrar attribution remains unchanged. A running run may
create a new binding. Unknown or revoked runs refuse every registration attempt.

CheckWrite and Publish consult the same run state after owner/grant checks.
The previous test-only window hook is replaced by that operation. Reads still
use their independent grants. The trusted test fixture installs completion or
revocation while holding the same catalog mutex; this does not implement an
authenticated lifecycle controller. A deadline remains a supplied-time comparison,
so real catalog isolation, durable state and recovery remain open. The selected
elapsed-closure policy is permanent once saved: the internal CloseElapsed operation
returns an explicit closed value at/after the original deadline. The catalog must
save that value; CheckRecord alone remains a pure predicate and changes no state.
Completion/closure retries preserve closure and original timing. Revocation remains
a distinct state. Controlled tests save closure, regress supplied time, replace
coordinators and refuse record/registration retries while leaving reads intact.
Registration and record writes now persist observed elapsed closure in PostgreSQL.
An autonomous lifecycle controller remains unimplemented. A false forward jump can
close early; a backward jump before saved closure remains outside this protection.

Confirmed `ErrDenied` from registration means refusal of this attempt, like a
confirmed ownership conflict. Registrar must not add ErrRegistrationUncertain to
that result. Every other catalog error may hide a commit. Missing grants/state or
deadline closure must be checked before revealing duplicate/conflict outcomes;
permission-state unavailability remains an error rather than absence or permission.

The admission value uses O(1) time/space and adds no object calls. The controlled
catalog adds known-run and creator-grant maps with expected constant-time operations
under its existing mutex, not a worst-case performance guarantee. Total model state is O(E + G + R + U)
entries for executions, grants, records and known runs; active admission does not
bound those accumulated entries. Registration uses
one catalog operation and no object bytes; it checks at most the proposed and
original run. Tests compare running, draining, deadline and revoked states on
the same lost-reply/retry workload and force completion/revocation between initial
authorization and the final atomic operation. A single precheck followed by the old
registration write would admit a new call after completion and is insufficient.

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

Those model experiments supply trusted capabilities in one process. Native
publication tests additionally exercise PostgreSQL transactions. The authenticated
integration now generates real SDK IDs, verifies distinct opaque credentials,
registers owner bindings without exact-call grants, saves both agents' span/capture
fixtures and recovers the shared trace and saved response through fresh verifier,
catalog and coordinator objects. It checks cross-owner denial, scoped collector
retry attribution and grant removal before object access. Separate HTTP tests use
a certificate-verifying TLS client. The native objects remain a controlled HTTP
peer; these tests do not supply a public receiver, automatic SDK exporter, remote
catalog deployment or actual ingest-host destruction.

The separate [live host-recovery driver](HOST-RECOVERY.md) now verifies distinct
opaque credentials and the same native grants with remote PostgreSQL and live
R2. A replacement VM discovers both owners' records and saved responses after
the original ingest VM is destroyed; denied-reader probes make no object calls.
Its trusted operator setup and synthetic principals do not implement credential
distribution, rotation, OTLP reception or authenticated run controls. The explicit
Rewind service uses the same native checks for each protected request.
