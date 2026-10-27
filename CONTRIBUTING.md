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

Follow the [Go practices](engineering/GO.md). Keep changes focused and branch names
descriptive, such as `batch-recovery`. Explain the problem, resulting behavior and
relevant checks in a pull request. Include a concrete failure sequence when fixing
a bug, and distinguish implemented behavior from a proposal.

Hosted Actions currently runs only on request because its included minutes are
exhausted. Pull requests use a `local/source` status for checks run against their
exact commit; the PR records the commands and platform. This is local verification,
not a GitHub-hosted CI result.

Use synthetic data. Keep credentials, captured production payloads, raw measurements
and generated output out of Git. Shared repository tools use Go; shell entry points
and YAML/TOML configuration are appropriate where needed. The index check applies
a 1 MiB per-file limit and excludes JSON/JSONL/NDJSON and Python artifacts; runtime
serialization formats are unaffected.
