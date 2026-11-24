# Recover accepted records after losing the ingest host

The internal `tools/recoverydrill` driver has recovered Alice and Bob's shared
trace and saved responses on a replacement DigitalOcean VM after the original
ingest VM was destroyed. PostgreSQL on a separate surviving VM held acceptance
and ownership; the private `rewind-dev` R2 bucket held immutable objects. This
demonstrates the tested ingest-host boundary. It does not establish catalog
restore, catalog failover or provider-wide durability.

## Driver and prerequisites

The driver is an operator-only synthetic workload, separate from the help/version
CLI. It exposes no receiver and creates or deletes no infrastructure. Build the
same reviewed source for the operator machine and the guest's OS/architecture,
then verify each guest binary's SHA-256 before running it. Each phase has a
three-minute deadline. Its fixture limits are not production defaults.

An operator must provision a dedicated `rewind` database owned by the restricted
`rewind_drill` login. The setup phase creates a fresh schema and explicit fixture
grants. The driver
refuses a different database/user, a replica, a read-only database or disabled
`fsync`; its sessions require `synchronous_commit=on`. See the
[catalog contract](POSTGRES.md#first-native-operation). PostgreSQL must survive
the ingest VM, rather than reside on it. R2 credentials must be restricted to the
development bucket; the [object adapter](OBJECTS.md) bounds and verifies reads.

For the observed run, PostgreSQL stayed on localhost at the catalog host. A
host-key-verified SSH tunnel connected the ingest VM's `127.0.0.1:15432` to the
catalog host's `127.0.0.1:5432`. The driver's PostgreSQL DSN requires loopback and
`sslmode=disable`: only the local tunnel endpoints use plaintext PostgreSQL; SSH
encrypts and authenticates the network leg. Never point that DSN at a remote
plaintext server. R2 uses its default account HTTPS endpoint with certificate
verification and no redirects.

Use a separate temporary tunnel account/key, restricted to that one TCP
destination, with no sessions, SFTP, PTY, remote forwarding or Unix-socket
forwarding. Keep the administrator private key on the operator machine. Verify
positive database login and negative forwarding/session cases before transferring
storage secrets to a guest. Config files must be regular files readable only by
their owner, at most 64 KiB; use protected directories and remove their secrets
after cleanup. Credential issuance and infrastructure lifecycle are operator
responsibilities, not a maintained deployment service.

## One run across two hosts

The base private config uses `dsn`, `r2_endpoint`, `r2_access_key` and
`r2_secret_key`. The DSN must include the dedicated account and password, local
tunnel port, `rewind` database and only `sslmode=disable`; the R2 endpoint must be
the default HTTPS account endpoint. `prepare` generates the phase-specific files.

The phases accept explicit private config files:

| Phase | Input and result |
| --- | --- |
| `prepare` | Base PostgreSQL/R2 configuration plus a new `-out-dir`; creates separate setup, writer and reader configs for acknowledged and withheld-reply traces |
| `setup` | Creates a fresh schema, project, run and scoped grants; the observed operator ran it through the laptop tunnel before either guest received storage secrets |
| `write` | Registers real SDK-generated execution IDs, verifies distinct opaque credentials, writes three spans and their three captures, and reports acceptance receipts |
| `write -withhold-final-ack` | Commits the same six-record workload but suppresses the final capture's application acknowledgment and waits at an explicit control barrier |
| `read` | Receives project/trace, storage configuration and reader/denied credentials; discovers spans and saved responses without producer plans or receipts |
| `retry` | Receives the original writer plan, disables object creation and changes compression policy; equivalent records must return their original acceptance |

For example, the invocation shape is `recoverydrill -phase read -config
read-ack.json`. Preparation additionally requires `-out-dir`; acknowledgment
suppression is allowed only with `-phase write`. These files are different typed
inputs: the reader decoder rejects writer plans, object references, receipts and
unknown fields. Preparation uses standard OpenTelemetry SDK IDs, but the tool
calls, timestamps and saved bodies are synthetic fixtures. It is not an automatic
SDK exporter or a public capture integration.

Alice records a handoff, then Bob records a failed tool attempt and a successful
attempt in the same trace. The failure has a missing response and refuses saved
playback. The success has a redacted request and the exact saved response
`{"answer":42}`. Each operation owns a separate immutable span and capture;
accepting one does not atomically accept its partner. Current reader permission
is independent of writer ownership, as specified in
[authorization](AUTHORIZATION.md#internal-capabilities).

The observed sequence was:

```text
Operator: prepare fresh plans; setup through the restricted catalog tunnel
A: write acknowledged trace; write second trace to withheld-ack barrier
Operator: destroy A; confirm its exact provider ID is absent; reap local SSH
B: receive only reader configs; discover and read both traces
B: only then receive writer plans; retry both traces; no new object creation
Operator: destroy B; retire tunnel and database login; verify cleanup
```

The writer was still alive immediately before A's destruction request. No
graceful tool or writer shutdown preceded deletion. A's confirmed absence came
before B's creation; at most one temporary ingest VM existed at a time. B never
ran `setup` and received no original producer plans until both reads completed.
Thus its discovery used the surviving catalog, rather than a copied ingest
directory or a producer-supplied list of saved objects.

## Observed result

The complete run recovered six records from each trace. Eleven receipts had
reached the caller before host loss; every recovered reference, attribution,
acceptance time and expiry matched those receipts exactly. The final withheld
capture's receipt was first discovered by B before any retry. Retrying then
preserved all twelve recovered receipts, changing only `Duplicate` to true.

| Operation | Records | Object `Create` calls | Object `ReadExact` calls |
| --- | ---: | ---: | ---: |
| Initial write, acknowledged trace | 6 | 6 | 0 |
| Replacement read, each trace | 6 | 0 | 7 |
| Semantic retry, each trace | 6 | 0 | 6 |

These are observed adapter-method counts, not S3 request counts or transferred
bytes. The withheld write has no final summary, so its initial method totals are
not reported here. One `Create` can perform several provider operations. The extra
recovery read checks that failed-response playback refuses rather than fabricates
a result. Both denied-reader probes, before discovery and against a known
result, made zero object calls. Retry used no tool and created no objects; its
different stored encoding required reading and comparing the accepted contents.

All three temporary VMs, including the stopped first attempt's VM, were confirmed
absent. The original catalog host remained the only VM, with its four existing
services active and their process IDs unchanged. Cleanup left `rewind_drill`
NOLOGIN with zero sessions, removed the temporary SSH users, key directories,
configuration blocks and provider tags, and deleted local private inputs. Catalog
schemas and R2 data remain as evidence. The initial post-B cleanup returned a
generic failure; a separate invocation of the reviewed cleanup completed retirement.
The more specific remote cause was not retained. The temporary compute estimate was $0.03,
including the provider minimum per VM; this is not an invoice or a timing result.

The operator retained a compact evidence summary outside the repository, pinning
the source tree and guest binary hash. It is not a dependency of normal builds or
checks.

## Failure in the harness and remaining limits

The first attempt confirmed A's destruction but stopped before creating B. Its
exception was not retained, so the precise cause is unknown. The harness then
waited for its local SSH child to exit; a dead remote host can leave a local TCP
session waiting for retransmission timeouts. That is the suspected failure,
rather than a demonstrated lost record. Cleanup succeeded. The corrected harness
explicitly killed and reaped its own local SSH child after provider-confirmed
deletion, retained safe phase/error diagnostics, and completed a fresh A/B run.
It reused only the proven retired dedicated catalog, rotated secrets and used a
fresh schema, project, execution IDs and VM identities. Earlier data was retained.

The withheld-reply fault suppresses an application acknowledgment after
`WriteCapture` succeeds. It does not disconnect the remote PostgreSQL COMMIT
response. The separate [local process experiment](INGEST.md#writer-process-recovery)
does inject actual COMMIT-response loss; its PostgreSQL and controlled HTTP
services survive. Neither experiment substitutes for the other's failure boundary.

The live run kept the remote catalog and R2 available. It did not destroy the
catalog, restore a backup, promote a replica, interrupt the provider or establish
physical-host isolation. Restoring an older catalog can lose accepted references
even when all objects survive. Define and test catalog backup/restore and failover
guarantees before making those claims. Public reception, managed credentials,
production operating limits and application replay remain unimplemented. The
[Concordance checkpoint](OBJECTS.md#extraction-checkpoint) can now use this Rewind
recovery sequence; it grants no shared API or extraction approval.
