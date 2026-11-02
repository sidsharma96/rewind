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
test backends; native PostgreSQL registration/publication/lookup now composes with the R2
object adapter in disposable-database/controlled-HTTP tests. A public capture format,
typed acceptance, real credential verification and replay remain unimplemented.
The private `rewind-dev` bucket exists; its scoped live probe passes
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

Research relevant established open-source implementations or documented production
systems before presenting consequential options and recommendations. Prefer primary
sources, pin implementation references, and explain their workload, mechanism and
limits. Bring that evidence into the initial comparison; distinguish observations
from our proposed design rather than treating popularity as justification.

Explain implementation decisions to the user as a newcomer. At each consequential
checkpoint, lead with the question and recommendation, follow one concrete example
with a simple illustration or sequence diagram, compare credible options in a
choice/benefit/risk-or-cost table, and show short illustrative Go snippets with
unfamiliar syntax explained. Include pinned primary research in the initial
comparison: explain each system's workload, observed mechanism, relevance and limits,
then distinguish that evidence from our inference and proposed design. Defend the
recommendation and state what would make us revisit it. Label illustrative code and
unmeasured costs; reviewer approval is not research or experimental evidence.
Present one consequential decision at a time and ask which option the user prefers
in ordinary chat prose. Do not use clickable/tappable question forms unless the
user explicitly requests them.
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
Store requires owner-aware catalog checks and explicit project-reader permissions;
real credential verification is absent. The native publication adapter repeats
current writer/binding/run/expiry checks; do not expose the controlled capabilities
or treat legacy unattributed records as retroactively authorized; see `engineering/CAPTURE.md`.
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

Preserve unknown signed 32-bit numbers in span kind and status as uninterpreted
evidence. Consumers must expose an explicit unknown case, never silently map them
to success, unset or internal. Revisit this policy if a required consumer cannot
safely expose an unknown category; bring that consumer and its concrete failure
case to a decision checkpoint before changing acceptance. See
`engineering/CAPTURE.md#numeric-kind-and-status-values` for the policy and limits.

Preserve all 32 bits of received span and link flags and compare each complete
value for retries. Consumers interpret only documented bits with masks; no unknown
bit supplies meaning or authority. Revisit if a required consumer cannot safely
separate known-bit interpretation from preserved unknown bits; show its concrete
failure first. See `engineering/CAPTURE.md#span-and-link-flags`.

Floating metadata compares by exact numeric value, with both NaNs equal and both
zero signs equal; different numeric types remain distinct. JSON saving does not
preserve NaN diagnostic bits. Preserve original zero signs in saved metadata and
exact capture bytes. Revisit if a required consumer needs zero sign or NaN bits as
retry-significant evidence. See `engineering/CAPTURE.md#floating-metadata-values`.

For retry comparison only, absent and completely default Resource, Scope and Status
messages are equivalent at their three span/context positions. Preserve original
encoded presence; never generalize this to arbitrary nested messages or capture
bodies. Revisit if a required consumer needs empty-shell presence itself to affect
retry-content identity. See `engineering/CAPTURE.md#optional-descriptive-message-comparison`.

Compare prepared span metadata directly, read-only, retaining the copying projection
as an independent test oracle. The metadata codec operation must bound and validate
both complete graphs, refuse unknown wire fields and require ordered attributes
before returning equal or different. Errors are neither equality nor conflicts.
Keep every reachable input stable for the whole call; never normalize borrowed
messages during comparison. Bind special Status handling to its named descriptor
and keep schema coverage and oracle fuzzing. The lab delegates to `record.MetadataCodec` and retains the copying oracle.
Runtime retry integration and the full saved-version/body contract remain separate. See
`engineering/CAPTURE.md#direct-metadata-comparison`.

Preserve every structurally supported received link, including an empty message,
when target IDs are absent or correctly sized but all zero. Derive an explicit
no-usable-target result; never fabricate a target or discard the link. Refuse wrong
nonempty ID widths. Preserve original target bytes, attributes, trace state, full
flags, dropped counts and order for retries; absent and all-zero IDs stay distinct.
The containing span still requires its own valid execution IDs and authorization.
A usable target key proves neither existence nor permission. Revisit if a required
consumer cannot safely represent unavailable targets, using its concrete failure.
See `engineering/CAPTURE.md#links-without-usable-targets`.

The selected saved-object direction is a small common envelope carrying version,
record kind and encoding, with kind-specific payloads. After object integrity
verification, cross-check the declared kind against the authorized catalog's
expected kind. Refuse unsupported versions/kinds and mismatches; never decode a
future kind as a final span. The label grants no authority and cannot reconstruct
acceptance. `record.EnvelopeCodec` now implements the seven-byte `RWND` prefix,
version 1, span/capture tags 1/2 and plain/gzip tags 0/1. The version covers the
whole format revision; kind-specific schemas remain open, and synthetic fixture
payloads do not freeze them. Map catalog kinds explicitly rather than casting
`ingest.Kind`. Decode requires caller-verified size/digest and an authorized
expected kind, checks labels before decompression, and returns owned payloads or
nil on error. Typed admission, complete payload codecs and production limits remain open;
see `engineering/CAPTURE.md#saved-object-kind-discrimination`.
Envelope record kind is separate from descriptive OTLP span kind, whose unknown
numeric values must still be preserved under the policy above.
The first envelope omits an exact expanded-payload length. Enforce separate stored
and actual expanded byte limits; a size label cannot replace either. Revisit for
an actual reader needing size before decoding, block/range access or measured
value from early declared-size refusal. Gzip readers accept a complete sequence of
valid members as one logical payload, including empty members; cap the whole stored
input and combined expanded output. Refuse invalid/truncated later members and
non-member trailing bytes before payload parsing. Our writer emits one member.
Revisit for a required consumer or measured workload that needs a narrower rule.
The envelope codec implements these byte boundaries with explicit limits and the
agreed configurable compression policy; kind-specific payload decoding, typed
admission and production byte limits remain open.


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

Before prose changes, read `docs/src/PROSE-STYLE-GUIDE.md` and `docs/BOOK-RULES.md`
(the rules every learning book shares; they win for guide chapters). Teach a beginning CS
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
guide chapter that teaches their mechanism, or in a reference page when no chapter
does; a recorded mistake is told as rule 3 of `docs/BOOK-RULES.md` describes. Raw measurements belong in ignored `.tools/`.

Blog posts develop one supported claim: problem, mechanism, alternative, experiment,
result and limit. Keep proposed experiments, illustrative values and observations
distinct. Do not invent decision history or production-scale claims. Maintain
learning gaps and current decisions rather than accumulating chronological logs.

## Code excerpts in the book

A reader with only the book, never the repository, should understand 95% of the
code a chapter is about (Rule 7 of `docs/BOOK-RULES.md`). Show the real types and
functions, whole or honestly trimmed, with one concrete input before them, a
line-by-line walk after them, and the real output they produce. Go one level down
(the standard library, the database, the operating system) or up (a trace, a log,
a test run) and come back. A link adds to code on the page; it never replaces it.

- Put `<!-- excerpt: <path>.go#L<a>-L<b> @<commit> current -->` on the line above
  each excerpt of implemented code and fence it as ```` ```go ```` (or
  ```` ```sql ````). Paste output as ```` ```text ````.
- `current` means the excerpt matches the working source. The project changes at
  every checkpoint, so a checkpoint that changes excerpted code also refreshes the
  excerpt, its walk-through and its marker; the check fails until it does. Use
  `historical`, labelled `**Before: bug example.**` or
  `**Before: performance comparison.**` above the marker, only for a recorded wrong
  turn, with the current code on the same page.
- Decision snippets and future examples are not excerpts. Give them no marker and
  label them illustrative or proposed.
- Trim honestly: replace each cut with a comment containing "…" that says what was
  cut. Pin code links to a commit that has been pushed (`/blob/<commit>/`), because
  `tools/checkdocs` does not verify them.
- Standard-library or dependency code cannot carry a marker. Quote it with a link
  pinned to the Go release or module version in `go.mod`, and compare it with the
  copy under `$(go env GOROOT)/src` or `$(go env GOMODCACHE)`.
- Paste only output that really ran. Quote recorded measurements, never timings
  rerun on a busy machine; label numbers worked out from the code as derived.
- Walk each piece of code once in the book; elsewhere give a sentence and a link.
  Reference pages link to chapters rather than repeating their walk-throughs.
- Check with `../learning-docs/check-excerpts.py rewind --require-status
  --scoped-history` as well as `make docs`. The sibling `deploy.sh` runs both.

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
