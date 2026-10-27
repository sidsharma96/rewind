# Local independent review

The private runner is adapted from Turnstile's staged-source reviewer. Roles and
upstream observations are local configuration. They are not required for builds
or CI and are never staged. Provision them separately in a fresh clone.

Stage only intended public source. Put scope, checks actually run, results, skipped
checks and unresolved questions in `.tools/reviews/evidence.txt`. Never put secrets
or private book prose in public-source evidence. The runner snapshots staged blobs,
excludes private paths, disables inherited Claude customization/MCP configuration
and permits only Read, Grep and Glob. Reviewers cannot run tests.

```sh
go run ./.tools/reviewer run --reviewer code --base origin/main \
  --evidence .tools/reviews/evidence.txt --report .tools/reviews/code-1.json
go run ./.tools/reviewer run --reviewer architecture --base origin/main \
  --evidence .tools/reviews/evidence.txt --report .tools/reviews/architecture-1.json
```

Use a new report path per round. Before the initial commit, use `--base empty-tree`;
the runner rejects that special base once HEAD exists. After the initial commit
but before a remote exists, use `--base HEAD`. Ordinary feature reviews compare
with the actual target branch, refreshed before merging.

Use `--purpose design` for substantial design checkpoints or `--purpose baseline`
for an existing architecture assessment. Neither approves future implementation.
An APPROVE must correspond to the final change and required role. Resolve material
findings or provide a concrete rebuttal; missing evidence requires the specific
check or narrower claim. After two unresolved substantive exchanges, discuss the
disagreement with the user.

Immediately before an authorized commit or push, replace `run` with `verify` using
the same evidence, base and final report. Changes to any bound input invalidate the
approval. Reports are local workflow checks, not tamper-proof attestations.

For algorithms and architecture, ask why this structure fits the workload, what
work remains, what grows, and which failure invalidates the conclusion. Include
recovery, cold reads, misses, cancellation, contention and the simplest credible
alternative. Pin upstream references to a revision, file and symbol; explain both
the relevant observation and its limit. A famous repository is not evidence that
Rewind needs its architecture.

Review private book prose in its own context. Read it for reader understanding,
then verify claims and inspect the delivered pages. Do not copy the private book
into the source snapshot or treat code approval as editorial approval.

## Local checks while hosted minutes are unavailable

The user authorized reviewed PRs and merges without buying more Actions minutes.
Hosted Checks is manual-only. Use a separate `local/source` commit status for actual
local results; never report a GitHub-hosted run as passing when it did not execute.
GitHub refused branch protection on this private repository's current plan. These
are maintainer-enforced steps, not server-enforced or tamper-proof attestations.

After final independent approvals, commit and push the source, then make an isolated
clone containing only tracked files at the exact PR head SHA. Run `make check`,
`make race` and `make build` there. Normal checks must work without the private
book or Claude configuration. For local Linux verification, use an official Go
image pinned by digest and record its Go version and actual OS/architecture.
Mount only the isolated clone; do not mount home directories, private documentation,
credentials or the Docker socket. No persistent Actions runner is required.

Keep the full check output in ignored `.tools/reviews/`. Record source commit/tree,
commands, platform, result and limitations in the PR. Publish `local/source=success`
only for the tested SHA after every required command succeeds and the clone remains
unchanged. If any step fails, do not publish success or merge. Changes to the head
require new verification. Keep private reviewer configuration and book prose out of
the PR evidence; summarize actual review verdicts and scope instead.

Before merge, verify the final local approvals against the current staged tree and
the reviewed comparison base again. Fetch the target branch, confirm its SHA and
the PR head still match the reviewed/tested inputs, and merge with the expected
head SHA. A green local status does not authorize a different commit or stale base.
Preserve user changes and remove the temporary clone after saving its evidence.
Revisit hosted CI or a dedicated runner when continuous verification is needed.
