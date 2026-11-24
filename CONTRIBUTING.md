# Contributing

Use Go 1.27 or newer and Git. No database, cloud account or private documentation
is needed to build and check the current source.

```sh
make check
make race
make build
make hooks-install
```

`make check` checks formatting, runs vet and tests, and checks the Git index for
private/generated artifacts and oversized source files. `make race` runs the tests
with Go's race detector. The optional hook checks the index and whitespace before
a commit; installation refuses to replace an existing custom hook setting.

PostgreSQL registration, publication, lookup and authenticated service integration
tests are opt-in. Use
an explicitly disposable database whose test role may create schemas, and set
`REWIND_TEST_POSTGRES` to its connection string before running:

```sh
go test -race -count=1 ./internal/catalog ./cmd/rewind
```

Catalog tests create fresh randomly named schemas and remove only those schemas.
The service tests also run TLS clients, separate producer/reader processes and
browser-session requests against controlled object storage; no cloud credentials
are needed.
Without the variable, database tests skip; normal checks still require no database.
Keep connection strings and credentials out of source and check logs.

The [host-recovery driver](engineering/HOST-RECOVERY.md#driver-and-prerequisites)
has separate native tests. Set `REWIND_DRILL_TEST_POSTGRES` to an explicitly
disposable `rewind` database owned by `rewind_drill` before running
`go test -race ./tools/recoverydrill`. These tests need no cloud credentials and
remove only their randomly named schemas.

The optional [R2 probe](engineering/OBJECTS.md#opt-in-development-bucket-probe)
needs S3 credentials restricted to the private `rewind-dev` bucket. It is disabled
by default and retains two small synthetic objects in a new namespace on each run.
Normal checks require no cloud access.

Follow the [Go practices](engineering/GO.md). Keep changes focused and branch names
descriptive, such as `batch-recovery`. Explain the problem, resulting behavior and
relevant checks in a pull request. Include a concrete failure sequence when fixing
a bug, and distinguish implemented behavior from a proposal.

The hosted Checks workflow runs `make check`, `make race` and `make build` on every
pull request and on pushes to `main`. It does not run the opt-in PostgreSQL, drill
or R2 tests; say in the pull request which of those you ran.

Use synthetic data. Keep credentials, captured production payloads, raw measurements
and generated output out of Git. Shared repository tools use Go; shell entry points
and YAML/TOML configuration are appropriate where needed. The index check applies
a 1 MiB per-file limit and excludes JSON/JSONL/NDJSON and Python artifacts; runtime
serialization formats are unaffected.
