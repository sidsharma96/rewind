# Shared trace service and inspection

Status: implemented explicit Rewind interface, exercised with native PostgreSQL,
controlled object HTTP, TLS clients and separate SDK-ID producer/CLI processes.
One authenticated Go service supplies terminal commands and a browser trace view.
Browser sign-in uses short-lived sessions. This is not an OTLP receiver or a
provisioned deployment. Browser-driven sign-in still needs inspection with an
operator-trusted certificate; desktop/phone layout checks use actual test responses.

## One operation from two producers to one reader

Alice hands work to Bob in one shared trace. Bob records one failed tool attempt
and a separate successful attempt. A trusted host registers each actual execution
ID under the appropriate project/run/instance creation grant. Alice and Bob each
submit ended spans and independent captures using their own scoped credentials.
Neither receives PostgreSQL or R2 credentials.

The reader starts with a project and trace ID, uses current project-reader
permission, inspects both agents' records, and selects the successful attempt's
saved response. Stopping the producers and controlled tool must not prevent that
operation. The terminal and browser expose the same evidence and refusals.
This completes an ordinary application/client path through the existing
[acceptance scenario](ARCHITECTURE.md#long-term-outcome), rather than another
operator-only storage probe.

A missing reply after acceptance leaves the producer uncertain. It retries the
same execution and contents and receives the original receipt. Each span and
capture has its own acceptance; there is no atomic whole-trace save. No network
success may stand in for the existing remote object and catalog publication.

## Keep transport outside storage

One service process composes the existing Registrar, Spans, Captures and
TraceReader with access.Credentials/access.Authorizer and native catalog/R2
adapters. It owns configuration, network listeners and shutdown. New HTTP handlers
translate bounded public inputs and results; they do not reproduce permission,
retry, retention, codec or publication rules. Clients own only service credentials
and operation inputs. Trusted operator setup still creates projects, runs, grants
and credential digests. Credential distribution and automated rotation remain open.

Use direct TLS initially, matching AuthenticateRequest. A forwarded identity or
TLS header is not authentication. Supporting an identity proxy later needs an
explicit trusted connection and principal mapping contract. No new dependency,
background ingestion queue or shared object library is needed for this operation.
The existing single-process choice does not promise one service replica forever.

### Resource operations

| Operation | Public behavior | Existing operation |
| --- | --- | --- |
| Register one execution | Trusted host submits execution IDs and owner scope; current creator grant required | Registrar.Register |
| Submit one ended span | Explicit end declaration with one bounded span plus inline resource/scope; owner comes from binding | Spans.WriteSpan |
| Submit one capture | Exact request/response bytes with explicit availability; independent of span arrival | Captures.WriteCapture |
| Inspect a trace page | Bounded current page, with explicit no-span/expired entries and scoped continuation position | TraceReader.ReadTrace |
| Inspect one capture | States and permitted bytes with the original receipt | Captures.ReadCapture |
| Retrieve a saved response | Only complete Present response, including zero bytes; request state and original receipt remain mandatory | Captures.Playback |

Use a versioned Rewind API, distinct from a later OTLP receiver and from the saved
RWND object envelope. Do not expose raw trusted Store capabilities or accept
caller-supplied attribution. Registration fields are unavailable to ordinary write
operations. A reader credential grants no registration or write authority.

The wire contract preserves the existing opaque identifier bytes, nonzero
hexadecimal trace/span IDs, numeric enum values and full flag values. It rejects
duplicate/unknown JSON fields, trailing values, malformed encodings and
limits before expensive preparation. One supported metadata projection is not
OTLP batch compatibility. Explicit public projections define bytes, timestamps and original receipts;
configured request/response bounds and stable errors apply independently. Internal
structs are not exported by default JSON reflection.

A public continuation token is a validated project/trace/key position. It is
neither authority nor frozen membership. Restart discovers late arrivals behind
an earlier position. Do not invent a complete-trace flag or accumulate unbounded
pages in a client. Corrupt or missing accepted objects and operational failures
remain errors, not missing capture or a partially successful page.

### Version-one wire contract

The transport is HTTP/1.1 over direct TLS. HTTP/2 is disabled initially so the
configured accepted-connection cap also bounds concurrent handler goroutines;
a separate nonblocking operation gate includes body decoding and response writes.
No CORS or forwarded identity is accepted. Mutation APIs require a Bearer header
and reject browser Origin headers. Read APIs may accept Bearer credentials or a
read-only browser session. A cookie never authenticates registration or writing.

All routes begin `/api/v1/projects/{project_hex}`. Opaque project/run/instance and
principal bytes use lowercase hexadecimal, preserving non-UTF-8 identifiers.
Trace and span IDs have their existing fixed hexadecimal widths; input may use
uppercase and output is lowercase. Public structs are explicit projections.

| Method and remaining path | Input or result |
| --- | --- |
| POST `/executions/{trace}/{span}/registration` | `run_hex`, `instance_hex`; original registration and duplicate flag |
| PUT `/executions/{trace}/{span}/span` | `ended: true`, `metadata`: exactly one resource/scope/span in the existing strict hexadecimal-ID/numeric-enum metadata projection |
| PUT `/executions/{trace}/{span}/capture` | `request` and `response`, each with required `state` and `data_base64` |
| GET `/traces/{trace}?cursor=...` | Items with execution, available/no_span/expired state, original receipt and metadata only when available; next cursor or empty string |
| GET `/executions/{trace}/{span}/capture` | Original receipt, both body states and exact bytes as Base64 |
| GET `/executions/{trace}/{span}/response` | Original receipt, execution, request state and complete response bytes as Base64 |

Body data uses canonical padded Base64; Missing/Redacted require an empty data
string, while Present/Truncated may contain zero bytes. Parsing refuses missing
required fields, null substitutes, duplicate/unknown keys, trailing JSON values,
invalid UTF-8 and excessive input/nesting. A capture is not the saved object
framing, and a span request is not OTLP reception. IDs in metadata/capture must
match the route. Input limits apply before decoding or object access.

A public receipt includes project/trace/span/kind, stored SHA-256 and byte length,
original owner run/instance and submitter as hexadecimal, original catalog
acceptance/expiry as UTC RFC3339Nano strings, and duplicate. It never contains an
object credential or URL. Span/capture success is HTTP 200 only after confirmed
publication. Registration success covers the binding, not either record.

The continuation value is `1.project_hex.trace_hex.after_span_hex`. The complete
scope must match the route; an empty value starts a scan. The internal cursor
has a checked position constructor/accessor, not a wire encoder or authority.
Clients may choose another valid position, which skips evidence; no signature or
membership-snapshot claim is made. Query keys and repeated parameters are strict.

Errors contain only stable code and outcome strings, never provider messages.
Invalid requests (400), invalid credentials (401), denied operations (403), absent
records (404), conflicts (409), confirmed expiry (410), excessive input/result
(413) and overload/operational failure (503) remain distinct. Writes with an
ambiguous storage/transport outcome say `unknown`; confirmed pre-admission/domain
refusals say `refused`. Lost/malformed replies at the client remain uncertain,
regardless of whether the server sent a status. No automatic client retries.
Read failure returns no successful page or consumed continuation.

The server config explicitly supplies positive connection, operation, byte,
whole-request, session-count/lifetime and trace-page bounds. Codecs have independent
stored/expanded/graph limits derived from those bounded settings; exact heap use
is not promised. Operation deadlines span body read through response write.
Normal clients refuse redirects, require TLS, bound response bytes/time and hold
only one trace page. Trusted server configuration supplies TLS files, database
connection/schema, R2 endpoint/bucket/credentials and credential digests; no ambient
credential discovery. Catalog placement and provider durability remain operator
responsibilities: a local sole catalog cannot meet remote durability before ack.

Browser login uses a fixed same-origin form, a Secure/HttpOnly/SameSite=Strict
host-only anti-forgery cookie and matching unpredictable hidden value. Login and
logout require exact configured Origin plus that value. Successful login rotates
the session identifier. Session cookies use the `__Host-` prefix, Path=/, no Domain,
Secure, HttpOnly, SameSite=Strict and expiry no later than credential/session
expiry. Logout deletes the table entry and cookie; requests already authenticated
may finish. Login challenges require no unbounded server table. HTTP response
caching is disabled, and a restrictive content policy forbids scripts and external
resources in the initial server-rendered UI. Captured bytes are escaped previews
or attachment downloads, never executable HTML. No credential is put in a URL.

## Short-lived browser sessions

Reuse an operator-issued Rewind credential for a principal with project-reader
permission. The selected flow is one password-field form submission over TLS,
then a short-lived opaque browser session. No password account, SSO integration,
query-string token or browser-persistent API key is implemented. Existing architecture
already defers enterprise identity; it is not a dependency of the first browser.

The server retains only a random session identifier's digest and a binding
to the authenticating credential entry, not the raw Rewind credential. Its bounded
in-memory session table has an explicit maximum and expiry; no hidden eviction
may create authority. The browser receives a host-only Secure, HttpOnly,
SameSite cookie. HttpOnly prevents page scripts from reading the cookie; it does
not prevent malicious same-origin scripts from acting through a browser session.

Credentials remain an immutable startup snapshot. Credential validity means the
entry is configured and not expired; replacing configuration requires restart,
which clears sessions. No automatic credential revocation is implemented. The
access package owns credential-reference checks and creates read-only authenticated
contexts without retaining the raw token. Write/registration authorization refuses
these contexts even if the original principal also has writing permission.

Every session request must check session expiry, original credential validity and
current project-reader permission. A session cannot last beyond its credential,
restore revoked reader access, or authenticate an API write/registration. Login
always creates a fresh unpredictable session. Logout invalidates that session;
service restart clears all sessions and requires signing in again without losing
accepted records. Sessions are disposable authentication state, not recovery data.

Login/logout require same-origin and anti-forgery checks. Use a fixed configured
public origin, not arbitrary Host or forwarded headers, to validate navigation
and form origins. Reader pages must not accept arbitrary redirect targets. Do not
log credentials, cookies or captured bodies. Escape untrusted labels/content,
serve captured bytes as attachments rather than executable HTML, disable caching
of authenticated data and apply a restrictive content policy. Cookies use Path=/ and exclusive expiry, with session lifetime/count selected
in operator configuration. One login may occupy an operation slot; its class cap
leaves other operation slots free, not a denial-of-service guarantee.

The simpler alternative holds the raw credential only in the browser tab's memory
and sends it as a Bearer header on each request. It needs no server session table,
but page scripts can read the credential and a reload needs another sign-in.
Local storage and session storage are not this alternative. Both alternatives
keep native reader checks and leave managed credential delivery unresolved.
We choose the opaque session: page scripts need no raw Rewind credential. Revisit
for a required client or multi-replica deployment that needs a different login
contract. Session loss on service restart deliberately requires another login.

## A bounded view of the available trace

The browser should show operation names, verified owner instances, reported parent
and link relationships, reported status and timing warnings. Descriptive agent
labels cannot become verified ownership. Unknown numeric kind/status, blank names,
unknown times and unavailable link targets need explicit display cases.

Show which relationships leave the loaded page or trace. Cycles and disconnected
records remain evidence rather than being forced into a fabricated tree. Span-ID
page order is navigation, not causal or time order. Refresh and continuation must
explain late arrivals. Captures are fetched only when selected, not for every span
in a page. Missing, Redacted, Truncated and Present remain distinct from absent or
expired capture and from a failed read. A saved result is recorded-byte playback,
not another invocation of the tool or model.

Use keyboard-operable controls and readable desktop/phone layouts. The initial
page is rendered on the server and uses native HTML disclosures; no JavaScript
or frontend framework is needed. Illustrative views must never claim that data was ingested
or measured. Terminal commands must make unavailable results and failures as
explicit as the browser does.

## Acceptance and limits

Validate one small two-producer tool workload through the actual client/service
boundary with native PostgreSQL and controlled object HTTP. Use independently
scoped credentials, fresh reader clients, both capture/span arrival orders, lost
application replies, conflicting retries, revoked permission, malformed/oversized
requests, overload, credential/session expiry and shutdown. Verify browser layout,
keyboard navigation and untrusted content with actual rendered pages. Keep public
code checks independent of private book/configuration. Teach the implemented
boundary in the private book when behavior changes.

The server needs connection/header/body/concurrency/deadline bounds before reading
untrusted bodies. Service admission must also include response encoding and slow
clients after coordinator calls release their slots. Clients need response and
retained-page bounds. These are required implementation details, not measured
production capacity or selected numeric defaults. A successful HTTP write cannot
be acknowledged from a local queue, and timeouts do not prove nothing committed.

Catalog loss/restore, provider durability, remote COMMIT-response loss, general
application replay, complete lifecycle streams, cross-run search and retained
regression examples remain separate work. The existing live ingest-host drill's
scope does not grow merely because a browser is added. Concordance's object adapter
is unchanged at f7f45ce9320960334152bdd85feb64e1a65378dc versus the previous inspected
bda56deabc651145967099f56cededb2ff410530; no range, deletion, cache or verification
reuse operation is needed here. A prepared handoff is still not joint API approval.

## Primary implementation references

[Jupyter's login handler](https://github.com/jupyter-server/jupyter_server/blob/e39ebd56b3ed2b201aa615448ec6adbeed8fa99c/jupyter_server/auth/login.py#L93-L105)
checks the submitted form and issues a login cookie. Its
[identity provider](https://github.com/jupyter-server/jupyter_server/blob/e39ebd56b3ed2b201aa615448ec6adbeed8fa99c/jupyter_server/auth/identity.py#L386-L400)
uses HttpOnly and HTTPS-aware Secure settings. Jupyter protects interactive
notebooks, not Rewind records; this supports the credential-to-cookie pattern,
not our session table, permission policy or lifetime guarantees.

[Grafana's proxy client](https://github.com/grafana/grafana/blob/af0a912af5e4af0def56ecb4c1ed2a1ddb1b2e28/pkg/services/authn/clients/proxy.go#L78-L92)
checks allowed source addresses before using identity headers. Its
[proxy documentation](https://grafana.com/docs/grafana/latest/setup-grafana/configure-access/configure-authentication/auth-proxy/)
explains that trust boundary. This is a different operator-managed login approach;
copying a header into Rewind would not supply verified identity or project access.
The small first browser should reuse existing verified credentials rather than
introduce that separate deployment contract.

[OWASP's session guidance](https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html)
explains cookie flags, server-enforced expiry and the risks of web storage. It is
security guidance, not a measured Rewind implementation or proof that cookies
prevent all script attacks. The choice of disposable bounded sessions over a
credential held in tab memory is the selected direction; these references do not certify its implementation.

## Operator setup and first run

Use an owned PostgreSQL database/schema, an R2 bucket and separate provider
credentials, and a TLS certificate/key for the fixed public HTTPS origin. The
catalog and objects must survive ingest-host loss; a database on the only ingest
machine is not remote durability. TLS terminates at Rewind. An ordinary identity
or forwarded-protocol header cannot replace it.

`rewind token --out host.token` creates a new owner-readable token file and prints
only the SHA-256 to put in server configuration. Repeat for Alice, Bob and the
reader, preserving distinct tokens. Token files and the server configuration must
be regular files with no group/other permissions. A principal name does not install
its grants.

This configuration is a template. Replace store addresses, secrets, certificate
paths, token digests and expiration instants before use. The numeric settings are
small example bounds, not measured production capacity. The complete encoded HTTP
request counts toward its byte cap, including Base64 overhead.

```json
{
  "listen": "127.0.0.1:8443",
  "origin": "https://rewind.example.com:8443",
  "tls_cert_file": "/private/rewind/cert.pem",
  "tls_key_file": "/private/rewind/key.pem",
  "database_url": "postgres://USER:PASSWORD@CATALOG_HOST/rewind",
  "database_tls": true,
  "database_ca_file": "",
  "schema": "rewind",
  "r2_endpoint": "https://ACCOUNT.r2.cloudflarestorage.com",
  "r2_bucket": "rewind-dev",
  "r2_access_key": "REPLACE",
  "r2_secret_key": "REPLACE",
  "credentials": [
    {"principal_hex":"686f7374","sha256":"HOST_DIGEST","expires_at":"REPLACE_WITH_RFC3339"},
    {"principal_hex":"616c696365","sha256":"ALICE_DIGEST","expires_at":"REPLACE_WITH_RFC3339"},
    {"principal_hex":"626f62","sha256":"BOB_DIGEST","expires_at":"REPLACE_WITH_RFC3339"},
    {"principal_hex":"726561646572","sha256":"READER_DIGEST","expires_at":"REPLACE_WITH_RFC3339"}
  ],
  "max_connections": 16,
  "max_active": 4,
  "max_request_bytes": 1048576,
  "max_response_bytes": 4194304,
  "timeout_seconds": 20,
  "session_count": 32,
  "session_seconds": 900,
  "trace_candidates": 16,
  "trace_stored_bytes": 1048576
}
```

The database URL supplies user, password, host and database, without query
parameters. `database_tls: true` verifies the catalog certificate against the
configured CA or system roots. Cleartext is allowed only through an explicitly
configured loopback connection, for example a trusted tunnel to a remote catalog.
Do not infer remote placement from that address. `PGSERVICE` and `PGSERVICEFILE`
must be unset for the service; it does not import ambient credential profiles.

With a private configuration file, `rewind install --config server.conf` creates
a new catalog schema. It refuses an existing schema and performs no migration or
deletion. Trusted operator SQL then creates the application project/run/grants.
For the example, use an established private operator connection:

```sh
psql "service=rewind-operator" -v schema=rewind -f examples/shared-trace/setup.sql
bin/rewind serve --config server.conf
```

The SQL is for a new `demo` project and running `demo-run`; it fails rather than
replace an existing project. It gives trusted `host` creation scope for Alice/Bob's
instances, gives each writer only its own instance, and gives `reader` project-read
permission. Use the same project gate when changing live grants;
[ownership enforcement](AUTHORIZATION.md) explains the locking rule. The service
never installs these grants from span metadata.

`serve` prints the listening origin, serves HTTP/1 over TLS, and joins admitted
handlers at shutdown before closing borrowed catalog/object connections. It does
not renew certificates, distribute credentials, create infrastructure or run a
background flush queue. Configuration requires explicit positive limits;
validation also caps request/response documents at 64 MiB each, connections at
4096, active operations at 1024, trace candidates at 1024, session entries at
65536 and session duration at one hour. These are validation ceilings, not
recommended settings or an exact heap budget.

## Two-producer example

The example is a trusted application host making synthetic calls. It receives
separate creation and writer credentials; these must stay outside untrusted agent
code. The descriptive `agent.name` label is never authorization. It uses actual
SDK-generated IDs and sends the explicit Rewind API, not OTLP export.

Build `go build -o bin/shared-trace ./examples/shared-trace`. Start its controlled
loopback tool in one terminal:

```sh
bin/shared-trace --role tool
```

Alice creates a handoff, saving a private plan before submitting records:

```sh
bin/shared-trace --role alice --endpoint https://rewind.example.com:8443 \
  --host-token-file host.token --token-file alice.token \
  --plan-file alice.plan
```

Copy the returned `execution.trace_id` and `execution.span_id` into Bob's command:

```sh
bin/shared-trace --role bob --endpoint https://rewind.example.com:8443 \
  --host-token-file host.token --token-file bob.token \
  --trace TRACE_ID --parent ALICE_SPAN_ID --plan-file bob.plan
```

The controlled tool refuses its first call with body `busy` and answers the next
with body `42`. They receive different span IDs, captures and reported statuses.
Stop the tool and producer processes. `rewind trace` discovers their records using
only project/trace and a reader credential, and `rewind playback --span SPAN_ID`
returns a complete saved response without contacting the tool.

Plans preserve IDs, owner scope, metadata and exact bodies. If a reply is lost,
rerun the producer with `--role retry` and the same plan/host/writer token files.
It neither creates new SDK spans nor invokes the tool again. Plans are sensitive
local staging, outside Rewind's remote acknowledgment guarantee, not a replay
dataset or a remote backup. Do not edit a plan to work around a conflict.

Use `--ca-file` on sample/client commands for a private CA. A browser separately
needs a trusted certificate for the service origin; do not put a credential in
its URL. Sign in with `reader.token`'s existing credential, enter the project key
shown in receipts (`64656d6f` for `demo`) and trace ID, and inspect capture states.
Download response attachments carry `Rewind-Request-State`,
`Rewind-Response-State` and a Base64url-encoded JSON `Rewind-Receipt` header.
The receipt digest covers the stored envelope, not the downloaded body alone.

The CLI accepts project/run/instance names as literal strings; `--project-hex`
supports opaque project bytes. The HTTP API uses hexadecimal opaque IDs directly.
Client commands cap request/response documents at 16 MiB and do not auto-retry.
`playback --out FILE` creates a new owner-readable file and prints its original
receipt/request state; it refuses to overwrite. File close is checked, but that
export is not a disk-sync or remote-backup promise.
