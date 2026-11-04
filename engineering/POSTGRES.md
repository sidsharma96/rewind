# PostgreSQL catalog coordination

Status: ownership registration and record Lookup have an internal native-pgx
PostgreSQL adapter, including CheckWrite/Publish. There is no public
endpoint. Internal opaque credential verification now composes with native
preliminary permission checks and authoritative operations. R2 objects have a separate adapter,
with a private development bucket and a passing scoped write/retry/read probe. The first
native integration tests use disposable PostgreSQL. A separate dedicated remote
development catalog now survives actual ingest-VM destruction in the
[live host-recovery drill](HOST-RECOVERY.md); production catalog restore and
failover remain untested.

## Different calls may proceed together

Alice saves call 42 while Bob saves call 99 in the same project. Both may make
catalog decisions concurrently. Two attempts to change call 42 must take turns.
A permission change must be ordered with both decisions, so a removed grant
cannot fit between a check and acceptance.

Use a durable gate row per known project. Ordinary catalog transactions lock that
row `FOR SHARE`, then lock their execution binding `FOR UPDATE` before making
record mutations. The binding key is project/trace/span; span and capture share
that lock but remain separately accepted records. Neither requires its peer.
All grants and run-state changes lock the project row `FOR UPDATE` first.

```text
record transaction:   shared project gate -> call binding -> checks -> commit
another call:         shared project gate -> other binding -> checks -> commit
policy transaction: exclusive project gate -> change policy -> commit

object upload/read: outside every catalog transaction
```

This is pessimistic locking: reserve access before deciding, rather than detect a
competing change afterward and retry. Shared project locks coexist. The exclusive
policy lock waits for admitted catalog transactions to finish; a later ordinary
transaction must observe the committed policy. A policy change is effective at
its catalog commit, not when its request arrives. It does not cancel object
uploads, terminate agents or recall bytes already read.

## Order and visibility

Use explicit `ReadCommitted` transactions. Acquire the project gate first, then
use subsequent statements to read current grants. For record writes, acquire the
binding before the final run, expiry and record decision. Check current owner and
grant before exposing a duplicate, conflict, expiry or absence. A project-reader
lookup checks its separate grant before disclosing record state; a shared binding
lock can coordinate its read with mutations without excluding other readers.

PostgreSQL takes a fresh Read Committed snapshot for each statement. A policy
query started before waiting on the gate is insufficient; obtain the gate first
and query policy afterward. Keep acquired locks until commit or rollback. Do not
upgrade a shared project lock to exclusive inside an ordinary operation. A policy
transaction takes the exclusive project lock from the outset and does not need
to acquire call locks. Each record/registration operation concerns one project
and one execution; multi-call transactions are outside this design.

Every service path that changes grants, runs, bindings or accepted metadata must
obey this convention, including trusted setup and future maintenance. Database
access belongs to the service, not agent principals. Normal operations must not
delete or recreate a project gate or execution binding. Their lifetime and cleanup
need a separate design. An unknown project/run fails closed; ordinary record
delivery cannot provision either or create ownership.

## Project retention policy

The project gate row is also the selected authority for payload retention and
expired-marker duration. The fresh-schema installer stores positive integer
microsecond counts with defaults of seven elapsed days and 24 elapsed hours.
Counts may not exceed `9223372036854775`, the whole-microsecond range that converts
into a positive Go duration without overflow. This is an encoding bound, not a
service retention limit or a captured-timestamp format.

Policy writes must take the project gate exclusively. Record publication reads policy after its shared gate is acquired and preserve that view until
commit. The same lock therefore orders permission and policy changes with
acceptance. A policy update does not rewrite original receipts or markers. A
lookup or retry must use saved deadlines rather than today's project policy.
Native PostgreSQL tests validate schema storage and coordination using trusted SQL test
setup. Publication reads this policy for new records; an authenticated policy-control
path remains unimplemented. There is no schema migration or automatic expiry worker.

## Confirmed record expiry needs a commit

Record operations must remember observed expiry while identity is retained, as
specified in [retention](RETENTION.md#remember-confirmed-expiry). A reader cannot
confirm expiry using a read-only clock comparison alone. Lookup
uses shared binding locks for live or already saved expired records. On newly
observed expiry, end the shared transaction and release all its locks, then restart
once with an exclusive binding lock. Recheck the project-reader grant, binding,
record and fresh database time; save expiry before confirming it. Do not upgrade
shared locks in place. Carry one admission slot and one timeout across both attempts.
There is no retry loop. Object reads stay outside both transactions.

If the clock moves back before any expiry is saved, the exclusive attempt may find
the record live and return its original receipt. If another attempt already saved
expiry, the flag refuses it regardless of the new time. Permission removal between
attempts must deny the second attempt before exposing state or changing expiry.
Failed rollback prevents restart; failed or uncertain commit is an operational
error, never a confirmed expiry. A shared live read need not repeat exclusively.

The adapter implements the complete internal ingest.Catalog capability alongside
ownership registration. Lookup still cannot register owners or create accepted
records. Native publication uses current writer grants and the same registered
bindings; composed tests save through Store rather than seed accepted rows.
An internal credential adapter now derives principals and checks current native
permissions. Public reception remains absent; the live recovery driver now uses
these same native operations against its dedicated remote development catalog.
Persist kinds as explicit span/capture labels, digests as 32 bytes, size as a checked
nonnegative bigint, bounded opaque owner/submitter IDs as bytea, and original catalog
acceptance/expiry/marker instants as finite timestamptz values with increasing
order. These catalog-owned clock values have PostgreSQL microsecond precision,
like run deadlines; they do not encode or reduce precision of captured span times.
A boolean remembers expiry; original receipts and both deadlines stay fixed.
Publication chooses these times once from trusted policy under its locks.

## First registration has no row to lock

`SELECT ... FOR UPDATE` does not lock a nonexistent binding. Registration needs
the binding's unique project/trace/span constraint as well as row locking:

1. Acquire the shared project gate and check current exact-call creator permission
   or separately granted host creation scope for that project/run/instance/principal.
2. Tentatively insert the proposed complete binding with `ON CONFLICT DO NOTHING`.
   This never updates an existing owner or original registrar. If another
   registration is still deciding the same identity, the unique insertion waits.
3. In a subsequent statement, read the resulting binding `FOR UPDATE`. Check both
   original and proposed runs where they differ, and evaluate current admission
   after acquiring the lock. Only then classify new, identical or changed ownership.
4. Commit only an allowed result. Roll back a tentative new binding if run state,
   deadline, legacy state or any other check refuses it. A completed run permits
   only an identical existing binding, never a tentative new binding. Do not return
   confirmed denial unless rollback/no-commit is established; unexpected errors
   remain uncertain under the existing registration contract.

Thus two first registrations cannot independently create owners. If the first
commits, the second reads its original attribution; if it rolls back, the second
can insert its own candidate and still must pass every check. This SQL mechanism
does not replace the existing creator, both-run or legacy-unattributed rules.

Record delivery locks an existing binding rather than inserting one. An authorized
reader may find no binding/record and receive absence; writing authority is never
inferred from that absence. Final Publish repeats the preliminary checks after
upload. It saves the reference and original submitter together and leaves uncertain
uploaded objects intact. A database transaction does not include the R2 write.

## Waiting, time and failure

Bound admitted operations and waiting time, pass cancellation through pgx, and
release transactions/connections on every path. A pool bounds connections, not
the number of callers waiting for them. Concrete admission and timeout settings
still need implementation. Cancellation before confirmation cannot establish
whether a commit happened; a confirmed commit remains successful if cancellation
arrives afterward. There is no automatic retry worker in this direction.

Deadline evaluation must follow lock acquisition. PostgreSQL's
`current_timestamp`/`now()` report transaction-start time and can be stale after
waiting. A fresh `clock_timestamp()` query avoids that particular error; it does
not make wall time monotonic. The selected policy remembers elapsed closure:
after the catalog durably records a closed delivery window, clock corrections,
completion retries and later grants must not reopen it. A mistaken forward jump
can permanently close it early; a backward jump before any saved closure can
still extend apparent eligibility. This does not supply monotonic or failover time.

The internal RunAdmission value now has an explicit elapsed-closed state and a
CloseElapsed transition; controlled tests save that value and then move supplied
time backward. The registration adapter persists elapsed closure on an authorized registration
attempt that observes the original completed deadline. Record writes now enforce the same saved state; an autonomous lifecycle controller
remains unimplemented.
The SQL adapter must end the ordinary shared-gate transaction without acceptance,
then acquire the project gate exclusively in a separate policy transaction, verify
the same original completion/deadline and persist closure. Never upgrade a shared
lock in place. Closure is established only after that commit is confirmed; earlier
ordered decisions are not undone. Persistent timestamp encoding and per-record retention use the native schema. Eligibility is decided with a
fresh database time under the acquired locks; an eligible transaction may finish
committing after the deadline. Success still requires confirmed commit. The
deadline does not promise that the final disk flush finishes before a wall-clock
instant. The selected payload default is
seven elapsed days from each record's first authoritative acceptance, followed by
24 hours of expired identity recognition; retries preserve both deadlines. Neither
duration is an automatic cleanup mechanism. This does not establish remote durability.

## First native operation

The concrete `internal/catalog` package uses native pgxpool and implements
`ingest.OwnershipCatalog` and all three protected ingest.Catalog operations. Store
can use the same native catalog for preliminary checks, publication and reads. There is no public
endpoint. Credential verification now lives in the separate internal access adapter.

The caller owns the pool and supplies a trusted schema name; query table names
are explicitly qualified. An explicit installer creates that new owned schema
and its tables in one transaction; no implicit
creation, destructive migration or reuse of unrelated application tables. The
first operation needs project gates, runs, exact and scoped host creator grants and immutable
bindings only. Persist IDs as bytea, preserving exact bytes and avoiding text
collation or UTF-8 normalization. Trace IDs are exactly 16 bytes and span IDs
exactly 8 bytes, with at least one nonzero byte in each ID.
The fresh-schema installer enforces these widths and rejects all-zero values;
it does not migrate existing schemas. Other IDs remain bounded opaque bytes.
Use explicit phase labels and
validated completion/deadline pairs rather than storing a Go enum's ordinal.

Registration follows the shared-gate/unique-insert/locked-read sequence above.
Reconstruct admission through the existing domain value's validated transitions,
not a second set of SQL authorization rules. A deadline refusal that needs saved
closure first rolls back its tentative binding and releases locks, then takes the
exclusive project gate in a separate closure transaction. Match the observed
original deadline before recording closure; preserve existing revocation/closure.
No caller-supplied timestamp establishes that observation. A failed or ambiguous
closure commit must not be presented as confirmed saved closure.

Bound active calls before pool acquisition and give the whole operation an explicit
trusted timeout. Each transaction refuses a replica/read-only server or disabled
fsync, strengthens weak synchronous_commit settings to on, and preserves the
operator's stronger remote_apply setting. These are minimum PostgreSQL checks;
remote placement, storage reliability and failover policy still belong to deployment. Keep rollback cleanup separately bounded after cancellation;
release or close the connection on every path. Ordinary SQL/connection/commit
failures remain uncertain to Registrar; do not wrap an ambiguous outcome with a
confirmed ErrDenied or ErrOwnershipConflict. Confirmed commit remains success
even if cancellation arrives just afterward. New grants and lifecycle operations
will still need authenticated controls; synthetic integration fixtures may seed
policy only under the same project-gate discipline.

Integration tests must opt in to a disposable database and own their fresh test
schema. Exercise exact bounded IDs, both-run checks, simultaneous first insertion,
original registrar preservation, current grants after waiting, remembered closure,
failure/cancellation and pool replacement. Observe lock waits with database blocker
state, not sleeps. A local PostgreSQL restart is not ingest-machine-loss evidence,
and no registration-only test establishes R2 durability or trace lookup.

## Native record publication

The fresh schema adds writer_grants keyed by project/run/instance/principal,
matching the existing controlled writer capability. The run must already exist;
trusted service setup uses the exclusive project gate. These grants permit record
submission for that owner, not reading. The binding still must name that exact
owner; no record operation creates one. No credential or grant-control API exists.

CheckWrite and Publish share the same protected write decision. Each holds one
admission slot and deadline across the ordinary transaction and any separate
elapsed-closure save. They lock the project gate shared, query current writer
grants, lock the existing binding exclusively, check owner/run and query fresh
time. They examine records only after those authority checks. An absent record
is allowed at CheckWrite without reserving anything. Publish inserts the reference,
original attribution and policy-derived times together. Exact-reference retries
return the original receipt; differing references conflict without replacement.
The coordinator still compares opaque bytes here. The selected decoded logical
fallback and typed saved format remain separate work.

A saved/newly observed expired record is refused only after commit. Run closure
uses the existing separate exclusive-gate transaction after ordinary rollback;
no shared lock is upgraded. Statement, commit or closure failures remain
operational errors, never confirmed refusal. No object I/O occurs under SQL locks.

Native tests cover independent span/capture records, duplicate/conflict races,
original receipts across principal/policy changes, denied state disclosure, bounded
waiting with fresh grant checks, saved expiry/closure and failed statement/deferred
commit triggers. The composed path drives real Store, native registration and
catalog operations, and the actual SDK/object adapter against a controlled HTTP
peer. A discarded successful publication reply is followed by a fresh pool/store/
client read and identical retry; conflicting bytes preserve the original, and
removing a grant during upload prevents final publication. The test uses synthetic
span JSON as opaque payload, not a selected saved format. Its in-memory object
peer, live original process and local database do not establish remote durability,
network-COMMIT ambiguity, catalog failover or complete ingest-host replacement.

## Cost, alternative and evidence

The simpler alternative is one exclusive gate for every operation in a project.
It serializes unrelated calls and reader metadata lookups but needs fewer lock
rules. The selected design permits independent call decisions at the cost of a
binding lock, unique-insertion races and policy coordination. Project-wide policy
changes still wait for active shared holders. There is no bounded revocation
latency or throughput claim without bounded transactions and measurements.

For P projects and E executions, gate and binding state occupy O(P + E) entries,
in addition to grants, runs and accepted records. Indexed equality lookups have
tree-search and possible storage I/O costs, not a constant-time map guarantee.
Registration has a unique insert and a binding read; a record mutation locks an
existing binding. Counts of actual queries, transferred rows, WAL bytes, waits
and recovery work have not yet been measured on a representative workload. The lock
protocol adds no object requests and must never hold a database lock across R2.
A successful same-run registration at the default commit setting issues nine
application SQL commands including BEGIN/COMMIT, the durability check, gate,
creator grant, insertion, locked binding read, run read and fresh time. A second
run or a commit-setting upgrade adds a command. Cold pgx preparation, internal
constraint work and closure transactions add work; this is a source-level count,
not a measured network-round-trip or latency result.


A disposable native-pgx probe on PostgreSQL 16.13 exercised different-call
concurrency, same-call waiting, an exclusive policy change waiting for both shared
holders, and a subsequent query seeing the removed grant. Simultaneous first
registrations preserved one original registrar after commit; rolling back the
first candidate let the waiting candidate insert. A synthetic cutoff sampled
while a caller waited distinguished stale transaction-start time from a fresh
database-clock comparison. Explicit barriers and PostgreSQL lock-wait observation
established ordering; no sleep was used to infer it.

These are mechanics observations, not full Rewind integration, authentication,
benchmark, production deadlock analysis, clock-regression or recovery results.
The database was local and disposable; no ingest-machine-loss guarantee was
tested. Revisit lock granularity when representative same-call or policy-change
waiting dominates, when batching requires multiple bindings, or when clock and
cleanup rules require more shared mutable state. Preserve the simpler exclusive
gate as a comparison baseline rather than claiming the chosen design is faster.

Sources: [pgx native API](https://github.com/jackc/pgx/tree/v5.11.0),
[PostgreSQL row locks](https://www.postgresql.org/docs/16/explicit-locking.html#LOCKING-ROWS),
[Read Committed visibility](https://www.postgresql.org/docs/16/transaction-iso.html#XACT-READ-COMMITTED),
and [current time functions](https://www.postgresql.org/docs/16/functions-datetime.html#FUNCTIONS-DATETIME-CURRENT).
