# Rewind

Rewind records independently owned agent operations in a shared trace. A reader
with project permission can inspect spans and captured inputs/results through
terminal commands or a browser, then retrieve a complete saved response without
running the tool or model again.

The storage engine is also a learning project: formats, indexing, recovery and
their costs are implemented and examined explicitly. The service uses PostgreSQL
for authoritative acceptance and R2 for immutable objects. Both must survive the
ingest machine for an acknowledgment to mean remote durability.

Rewind is an active learning project, not a supported production service. Wire
formats and the catalog schema may still change.

## Getting started

Install Go 1.27 or newer and Git, then build:

```sh
make check
make build
bin/rewind help
```

`make check` needs no database or cloud account. The quickest way to exercise the
whole service locally is the opt-in integration suite: point
`REWIND_TEST_POSTGRES` at a disposable PostgreSQL database and run the service
tests in `./internal/catalog`, which start TLS clients, separate producer/reader
processes and browser sessions against controlled object storage (see
[CONTRIBUTING.md](CONTRIBUTING.md)).

Follow the [service setup](engineering/SERVICE.md#operator-setup-and-first-run) to
configure TLS, remote stores and separately scoped credentials. Once the service
is running, inspect one current page:

```sh
bin/rewind trace --endpoint https://rewind.example.com \
  --token-file reader.token --project demo --trace TRACE_ID
```

To hand a whole trace to an agent, print every readable span and capture as one
evidence document, and the root-cause rules it is designed for:

```sh
bin/rewind diagnose --endpoint https://rewind.example.com \
  --token-file reader.token --project demo --trace TRACE_ID
bin/rewind diagnose --rules
```

It states coverage, absent relationships and failures explicitly; see
[diagnosing a run](engineering/DIAGNOSIS.md) for what it does and does not claim.

The browser uses the same service origin and project-reader policy. The
[two-producer example](engineering/SERVICE.md#two-producer-example) records a
handoff and separate failed/successful tool attempts, with saved plans for
same-identity retry.

A trace page is available evidence, not proof of a complete run or causal order.
Missing, redacted or truncated responses refuse recorded playback. Credential and
grant provisioning remain operator tasks; there is no OTLP receiver, automatic
credential rotation or general application re-execution. Building does not
provision or deploy a service.

See [CONTRIBUTING.md](CONTRIBUTING.md) for contributor setup and checks, and the
[architecture](engineering/ARCHITECTURE.md) for implemented and remaining boundaries.

## License

[MIT](LICENSE)
