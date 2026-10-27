# Retention boundaries

Status: matched and unmatched capture use the same normal retention policy. Missing
span metadata does not shorten a capture's lifetime. A later match or identical retry
must not create a new acceptance time or reset its deadline. The selected configurable
default for expired-record markers is 24 hours after payload expiry. This is not the
payload-retention duration. The selected payload default is seven elapsed days
(168 hours) from each span or capture record's first authoritative acceptance.
Both durations are trusted configurable policy; normal retries and later matches
preserve original deadlines. Clock/epoch representation, automatic expiry,
reclamation and quotas remain unimplemented.
The local implementation classifies deadlines and propagates confirmed catalog
expiry refusals in controlled tests. The current coordinator has no deletion
operation, authoritative expiry clock or durable lifetime metadata.

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

The authoritative catalog must eventually preserve the original acceptance result
across duplicate retries and lost commit replies, including whatever retention
metadata the selected policy requires. Keep that server-owned metadata separate
from captured contents and producer-supplied span timestamps. The existing catalog
interface returns only a reference/duplicate result; it does not persist timestamps
or expiry receipts. Design its next contract before adding timestamp fields or a
persisted schema.

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
records; define policy changes separately. No service configuration or persisted
deadline schema is implemented yet.

## Local classification and catalog refusal experiment

The local Lifetime value represents fixed payload/marker deadlines and classifies
a supplied time as before payload expiry, within the marker window,
or after the marker window. Expiry starts at its exact deadline; the marker window
ends at its exact deadline. Being after the marker window permits later forgetting,
not automatic deletion or a requirement to accept old requests. Use explicit supplied
times in tests; this value is not a clock, scheduler, cleanup worker or persistence
protocol. The authoritative acceptance/clock placement still needs backend design.

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
