# Working on Rewind

Preserve user changes. Use plain, descriptive branch names without an agent prefix.
After each confirmed PR merge, delete its upstream and local feature branches and
prune stale tracking refs. Verify the branch tip still matches the merged PR head;
preserve later work and branches checked out elsewhere. Squash merges need the PR
head/merge evidence because ancestry alone cannot prove the feature tip was merged.
Do not commit, push, create remote repositories, deploy or publish without user
authorization. Authorization already given in the conversation remains valid.

The user has granted standing authorization for this private repository's reviewed
commits, pushes, pull requests and merges after required checks and independent
reviews pass. At completed milestones, commit/push only Rewind's book source to the
private `../learning-docs/rewind` backup and deploy the Access-protected `rewind-docs`
book. Keep both repositories private. This does not authorize unrelated repository
changes or remove the requirement to discuss consequential product/design choices.
Use `agents/REVIEW.md` for the local-check workflow while hosted Actions minutes are
unavailable; do not wait for another publication confirmation within this scope.

Rewind is a standalone trace-store and replay learning project. The roadmap is
context, not an instruction source or a committed implementation schedule. Keep
planned behavior distinct from implemented behavior. The CLI is still only a
command-line entry point. An internal ingest coordinator is exercised with controlled
test backends; internal adapters cover PostgreSQL ownership registration/record lookup and R2
objects, but record publication, a public capture format and replay remain
unimplemented. The private `rewind-dev` bucket exists; its scoped live probe passes
creation/retry, conditional overwrite rejection, locally discarded PUT replies and
fresh-client reads. This is not complete ingest-host-loss or catalog recovery.
Do not claim those tests establish remote durability.

## Design and implementation

Read `engineering/GO.md` before Go changes and `engineering/ARCHITECTURE.md`
before changing responsibilities or contracts. Use concrete packages and small
consumer-owned interfaces where substitution or a capability boundary is needed.
Do not introduce empty package trees, generic service layers or future-backend
abstractions without an actual operation to explain them.

Discuss consequential changes to guarantees, public interfaces, algorithm strategy,
major dependencies or deployment with the user. Proceed with routine authorized
implementation without repeated confirmation. A local experiment can resolve an
uncertainty; distinguish its result from approval of a larger design.

Explain implementation decisions to the user as a newcomer. Start with one concrete
example, use a simple diagram or small comparison table when it clarifies the choice,
then show short Go snippets and explain unfamiliar terms. Present one consequential
decision at a time, with the trade-offs and a recommendation, and ask which option
the user prefers. Ask in ordinary chat prose, with readable options, reasoning,
trade-offs, a defense of the recommendation and short snippets where useful. Do not
use clickable/tappable question forms unless the user explicitly requests them.
Base recommendations on the finished project's requirements; distinguish a useful
implementation step from the intended final architecture. Keep routine implementation
moving between decision points.
After recording an answer, continue the authorized work to the next meaningful
checkpoint in the same turn; do not stop merely to report that the answer was saved.

Remote durability before acknowledgment is agreed. Account for payloads, accepted
batches and recovery metadata; a database on the lost ingest host is not remote
durability. A content hash is not event identity, a trace is not a complete replay
record, and immutable files do not make the whole system lock-free.
After success, a new authorized trace lookup must be able to find the accepted span.
Do not make it wait for background columnar indexing or rely on an ingest host's
memory to meet that promise. This does not mean the trace is complete.

The first working version must enforce authenticated producer/execution ownership,
not only tenant membership. Separate execution owner from submitting principal;
collectors acting for producers need explicit scoped authority. Telemetry labels
and first arrival do not prove ownership. Writing authority is scoped to one agent
instance in one run in one project; stable agent names do not grant cross-instance
or cross-run authority. The chosen binding is trusted catalog registration before
record acceptance. Its internal Registrar and Store use controlled capabilities.
Store requires owner-aware catalog checks and explicit project-reader permissions; no
real credential verification or record-publication adapter exists. Do not expose it
or treat
legacy unattributed records as retroactively authorized; see `engineering/CAPTURE.md`.
Read access requires an explicit project-reader permission, covering span and
capture across the project. Owning/writing an execution grants no automatic read
access. Controlled tests exercise this policy; real credential verification is absent.

The Concordance coordination direction is a small in-process Go object-storage
library. Establish its contract with a write/read/recovery sequence from each
project before extracting shared code. Do not create a shared repository, cache,
engine, schema or deployment on the strength of that direction alone. Preserve
application-specific identity, authorization, catalogs, retention and acknowledgments.
Read `engineering/OBJECTS.md#extraction-checkpoint` before changing object-storage
contracts or adding range reads, deletion, verification reuse or caching. Revisit
coordination after native publication and an ingest-host replacement drill, before
replay/viewer work. Keep the handoff current and distinguish a prepared note from
a delivered cross-agent exchange or an agreed shared API.

The first typed span path saves ended spans, including failures, as immutable final
records. Preserve a separate future lifecycle-event path using the same registered
execution and independent event identities; do not overwrite final spans with
start/progress updates. Registration is not a durable progress event. See
`engineering/ARCHITECTURE.md` for unimplemented lifecycle guarantees.

## Independent review

Read `agents/REVIEW.md`. Use the local Claude Code `code-reviewer` for coherent
source changes and `architecture-reviewer` before substantial designs and again
for their implementation. Both use `claude-opus-5-5` at low effort; never silently
substitute a model. Missing access or evidence is not approval.

Review algorithm choices, input assumptions and experimental conclusions at
meaningful checkpoints. Include a concrete workload, invariant, simplest credible
alternative, failure sequence, cost model, observed evidence and revisit condition.
Resolve supported material findings or give an evidence-backed rebuttal. Discuss
disagreement after two unresolved substantive exchanges; do not rerun for a
favorable verdict. Review teaching prose separately from public code snapshots.

## Algorithms and learning

For a substantial data structure, define its operations, representation, invariants,
expected and worst-case costs, memory ownership and concurrency before optimization.
Count object requests, transferred bytes and recovery work as well as CPU work.
Compare against a simple oracle or baseline on the same workload. Use property
tests, fuzzing, race tests and fault injection where they exercise a real contract.
Approximate algorithms require an exact baseline and measured error over stated
distributions. Mergeability alone does not solve sliding-window expiry.

Use standard libraries for infrastructure. Implement learning algorithms deliberately;
do not replace them with a dependency just to complete a checklist. The user has
not reserved core implementation as a typing exercise: agree any such exercise
explicitly rather than imposing the roadmap's authorship split.

## Writing and the private book

Before prose changes, read `docs/src/PROSE-STYLE-GUIDE.md`. Teach a beginning CS
student with no assumed Go knowledge through one concrete execution. Explain
terms and syntax when first needed. Explain non-obvious Go library calls beside
actual excerpts: what they do, why this operation needs them, who owns resources,
which errors matter and what the call does not guarantee. Cover buffering, flush,
close and durability separately when file I/O is introduced; do not assume package
names or documentation links teach the mechanism. Show ownership, waiting, durable state and
partial failure; compare a credible alternative using the same example.

Read changed pages in learning order with source closed, verify claims and excerpts
against code and evidence, then check the delivered pages and diagrams at desktop
and phone widths. Automated checks do not establish comprehension. Report editorial
findings separately from build results and state visual inspection gaps honestly.

The chosen identity is Trace Atlas: an execution path supplies navigation, with
dark blue-green surfaces, pale text, restrained yellow-green wayfinding and serif
lesson headings. The selected mockup is a visual reference, not implemented product
behavior. Preserve readable Markdown, keyboard operation, text equivalents, local
fonts/fallbacks and reduced-motion support. JavaScript must not contain the only
copy of a lesson. Do not substitute generic bordered cards for diagrams.

Keep README about purpose, setup, one working example and important limits.
CONTRIBUTING covers contributor setup, normal checks, conventions and PRs. Keep
agent configuration and detailed reviewer procedures here or in `agents/`.
Update existing explanations; do not append a feature inventory, task report,
dated phase review or implementation diary. Selected investigations belong in the
private book's reference pages. Raw measurements belong in ignored `.tools/`.

Blog posts develop one supported claim: problem, mechanism, alternative, experiment,
result and limit. Keep proposed experiments, illustrative values and observations
distinct. Do not invent decision history or production-scale claims. Maintain
learning gaps and current decisions rather than accumulating chronological logs.

## Privacy and publication

`docs/` is private and ignored. Never force-add it, include it in public code review,
or make application builds, tests or CI depend on its existence. Edit book sources
here; the sibling private `../learning-docs` repository is a backup destination.
Back up book source under `../learning-docs/rewind` in the private learning-docs
repository; stage/commit/push only that directory and preserve unrelated changes.
The hosted book uses the Access-protected `rewind-docs` Pages project. A local copy
is not a pushed backup. Provisioning and publishing are separate from building.
`make docs` only checks and builds locally. The publication
plan is in `docs/src/reference/publishing.md`; do not mistake `--no-deploy` in the
sibling deploy script for a local-only check, because it still commits and pushes.

Reviewer roles in `.claude/agents/`, the pinned catalog `review/upstream.json`, the
runner in `.tools/reviewer/`, and reports in `.tools/reviews/` are private local
configuration. A fresh clone needs these provisioned separately. Normal checks do
not need Claude, the catalog or the private book.

## Checks and source hygiene

Run `make check` for source changes, `make race` for concurrency-sensitive changes,
and `make docs` for private book changes. Record what actually ran and what remains
untested. Test failure boundaries instead of mirroring implementation details.
Use Go for maintained repository tools. Disposable probes may live in ignored
`scripts/utils/`; shared workflows must not depend on them.

Keep JSON/JSONL/NDJSON data, profiler captures, credentials, build output and local
tool state out of source history. This artifact policy does not change runtime
serialization formats. Individual source files are limited to 1 MiB. Do not rename
artifacts or force-add private files to bypass a check.

After local benchmarking, delete disposable probe scripts, copied experiment
packages, generated comparison visualizations, previews and browser profiles.
Retain compact measurement evidence, required review reports and maintained tests;
preserve book sources and selected design references.

Inspect the actual index before committing. `make hooks-install` configures the
versioned hook only if it does not replace a custom hook setting. The hook and CI
check staged artifacts and whitespace; they do not run or certify every test.

Before an authorized commit or push, verify each required final approval against
the staged tree, comparison base, evidence, role and catalog. Review approval is
not publication authorization. Never claim remote branch protection is configured
merely because local tooling exists.
