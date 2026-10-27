# Retention boundaries

Status: matched and unmatched capture use the same normal retention policy. Missing
span metadata does not shorten a capture's lifetime. A later match or identical retry
must not create a new acceptance time or reset its deadline. The selected configurable
default for expired-record markers is 24 hours after payload expiry. This is not the
payload-retention duration. The selected payload default is seven elapsed days
(168 hours) from each span or capture record's first authoritative acceptance.
Both durations are trusted configurable policy; normal retries and later matches
preserve original deadlines. Native catalog dates use PostgreSQL timestamptz.
Public record encoding, an autonomous expiry worker, reclamation and quotas remain
unimplemented.
The local implementation classifies deadlines and propagates confirmed catalog
expiry refusals in controlled tests. The current coordinator has no deletion
operation, authoritative expiry clock or durable lifetime metadata.

## Project policy authority

Each project's trusted retention policy belongs in PostgreSQL. New record
publication must read that policy while holding the shared project gate, then
save original acceptance, payload expiry and marker deadline together. A policy
change takes the project gate exclusively and affects subsequent first
acceptances only. Retrying a record or accepting its peer keeps the old record's
stored deadlines. An ingest server's configuration must not override this policy.

The project schema stores `payload_retention_us` and `marker_window_us` in the
project gate row, with seven-day and 24-hour defaults. Store elapsed durations as
positive integer microseconds bounded by Go's representable duration; avoid
calendar months/days and silent conversion overflow. This policy precision is
independent of captured span timestamps and request/response bytes. The existing
pure Go deadline value remains independent of this SQL representation.

The fresh-schema installer enforces those bounds and provides defaults; it
does not migrate or modify an existing schema. Native PostgreSQL tests use trusted
setup under the existing project-gate convention to verify project isolation,
persistence through a new pool, rejected invalid settings and policy changes waiting for
shared holders. No application policy-management API or SQL record publication
is implied by those schema tests.

## Remember confirmed expiry

Once the authoritative catalog has saved and confirmed that a record expired,
a backward database-clock correction must not make it live again while its identity
is retained. The chosen state is one saved expired flag beside the original fixed
deadlines. Check current permission first. An authorized lookup, preliminary write
check or final publication that first discovers expiry must persist that transition
before confirming ErrExpired. Do not change the original receipt or either deadline.
This can require a catalog write during a read. Serialization and transaction failure
handling belong to the backend, not an ingest-host timer.

If saving expiry fails or its commit is uncertain, return an operational error
rather than a confirmed expiry, absence or live receipt. The coordinator already
stops a failed lookup/check before object access; a non-refusal Publish error is
uncertain. A saved transition whose reply was lost still prevents revival, but an
observation that never committed cannot supply that guarantee.

Remembering expiry cannot detect a clock jump that happens before any saved expiry.
A false forward jump can permanently expire a record early; correcting the clock
will not undo saved expiry. No object deletion follows from the flag. The original
marker deadline is unchanged, and protection does not outlive identity removal.
The local value and controlled tests exercise this transition. Native Lookup reads
trusted seeded records and commits expiry with one exclusive restart after its
shared read. SQL tests exercise expiry update/commit failure and fresh-pool reads;
native record publication and automatic cleanup remain unimplemented. The simplest
alternative, comparing only the current clock with expiry on every request, permits
revival after a backward correction and was not selected.

## Acceptance, expiry and byte reclamation

An accepted capture whose span has not arrived is required durable data. It is
different from an object uploaded by a write that never published a catalog record.
Quota exhaustion must refuse new admissions according to an explicit contract,
rather than silently evict accepted records to make room. Admission byte/operation
limits currently do not bound accumulated durable data or unpublished objects.

Three operations still need separate backend contracts: deciding that a
record is no longer available, retaining enough identity metadata to handle retries,
and reclaiming bytes that no live reference, reader or in-flight writer still needs.
Deleting a catalog row does not by itself prove its object is safe to remove. Span
and capture can share an object within a tenant, as can separate executions with
identical bytes. No ingest-host timer or negative lookup establishes global absence
of future publication. No cleanup protocol is approved here.

The selected receipt includes catalog-owned `AcceptedAt` and `ExpiresAt` times,
separate from captured contents and producer-supplied span timestamps. Each first
acceptance records them with its reference and original attribution; successful
identical retries and lookups return those original values. Changing trusted
retention configuration affects new records only. Lookup still requires explicit
project-reader permission; receipt metadata grants no permission.

The coordinator requires both times to be specified and expiry to
be strictly later than acceptance. A malformed successful publication returns
uncertain plus integrity failure, because acceptance may already have committed.
A malformed lookup returns integrity failure before object access. The coordinator
does not compare these times with its local clock or require the current default
retention duration: another host's clock or changed policy must not invalidate an
original receipt. This is an internal result contract, not an HTTP/OTLP encoding,
public record schema or autonomous expiry service. Controlled tests supply a catalog
clock and preserve original times. Native SQL Lookup stores expiry on trusted
seeded records; ordinary record publication remains future work.

`DefaultPayloadRetention` supplies the seven-day policy constant. A future catalog
can derive `acceptedAt.Add(payloadRetention)` and pass that expiry plus its marker
window to `NewLifetime`. It must do this only on first acceptance, not on a retry.
Span accepted on Monday and capture accepted on Tuesday have separate seven-day
lifetimes; capture arrival does not extend the span. The constant does not run a
clock or change the existing controlled catalog into an automatic expiry service.

## Identity after payload expiry

Consider an illustrative ten-minute retention period, not a selected service default:
capture is accepted at minute zero and expires at minute ten. The original caller
misses an acknowledgment and retries the same identity and bytes at minute fifteen.

If cleanup has forgotten both payload and acceptance identity, the write can look
new and receive another lifetime. Availability has changed from expired to accepted
merely because an old delivery was repeated. Content addressing alone cannot tell
this retry from first acceptance after all catalog evidence is gone.

Retain a compact expired-record marker containing identity and the metadata needed
for the chosen retry response. Its default deadline is payload expiry plus 24 elapsed
hours; retries and later span matches do not move either deadline. An expired record
returns an explicit expired result without accepting it again. Marker publication/removal,
concurrent retries, authority, quota accounting and disaster recovery must be part
of the catalog contract. A local map example cannot establish those properties.

Markers also consume storage. A bounded marker window protects only that window;
after it, forgetting identities restores the same ambiguity unless the protocol
has another way to reject old identities. Permanent markers avoid that forgetting
but accumulate indefinitely. Do not claim permanent duplicate protection from a
finite marker window. After marker removal, recognition of an old identity is no
longer guaranteed. The choice trades a finite metadata lifetime for that explicit
limit. Configuration changes must not silently reconstruct deadlines of existing
records; define policy changes separately. Fresh schemas now hold original record
deadlines for Lookup; a public policy-control API, migrations and cleanup remain
unimplemented.

## Local classification and catalog refusal experiment

The local Lifetime value represents fixed payload/marker deadlines and saved expiry.
PhaseAt classifies a supplied time as before payload expiry, within the marker window,
or after the marker window. Expiry starts at its exact deadline; the marker window
ends at its exact deadline. Being after the marker window permits later forgetting,
not automatic deletion or a requirement to accept old requests. Use explicit supplied
times in tests; this value is not a clock, scheduler, cleanup worker or persistence
protocol. ObserveExpiry returns an updated copy whose Expired flag cannot revert;
PhaseAt remains a pure clock/deadline classification and is not an availability
predicate. The catalog must save the returned copy before confirming expiry. The
authoritative acceptance/clock placement still needs backend design.

The catalog contract includes a confirmed expired-record refusal. A retained expired
identity must refuse publication under that identity without changing the original
reference, regardless of submitted contents. Reads must return expired before object
access, rather than absence. The coordinator must preserve this confirmed outcome
instead of labeling it an uncertain commit. Authorization remains before catalog
or object access. This is an internal error contract, not a selected HTTP/OTLP status.

The write first checks the catalog after authorization. An already expired identity
or lookup failure stops before copying or uploading bytes. This adds one lookup per
write. A live or absent result permits upload but reserves nothing: the record can
expire before final publication, which must refuse it then. That race can still
leave an unreferenced object. It does not resurrect an accepted record, and refusal
must not delete the object: another live reference or in-flight publication might
need it. Controlled tests exercise both early refusal and expiry during upload;
they do not solve cleanup or quotas. Live duplicates and conflicts still reach final
publication rather than being decided by the preliminary check.

For N records processed over time, retaining all expired identities uses O(N)
metadata; a bounded window retains the identities within that window, whose count
still requires rate/storage controls. Dropping all metadata has lower steady-state
cost but permits the delayed-retry failure above. No bytes-per-marker, throughput
or storage-cost measurement is claimed. Revisit the strategy against a concrete
arrival/retry workload before choosing indexes, partitions or compaction.
