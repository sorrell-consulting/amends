# Testing and correctness evidence

Use this guide to choose checks, find contract coverage, and reproduce a failure. The [semantic contract](semantics.md) defines correctness; [evaluation](evaluation.md) is the shortest runnable introduction. Test success means the exercised assertions held. A test can correctly assert BLOCKED or INCOMPLETE without producing verifier PASS.

## Commands by purpose

Use the Go version in [go.mod](https://github.com/sorrell-consulting/amends/blob/main/go.mod), GNU Make, and a C compiler for the race detector. Service-backed targets require the healthy local dependencies from the [runbook](runbook.md); they do not silently skip unavailable infrastructure.

| Purpose | Command | Requirements / output |
| --- | --- | --- |
| Ordinary contributor gate | `make check` | Formatting check, vet, fresh race-enabled tests, both binaries; no services |
| Ordinary tests only | `make test` | Includes core/property tests, both production-orchestration memory corpora, CLI and mutation-runner tests |
| Durable SQL boundaries | `make test-postgres` | PostgreSQL; `AMENDS_TEST_DATABASE_URL` selects endpoint |
| Source lifecycle and broker coordinates | `make test-worker` | PostgreSQL and Redpanda; `AMENDS_TEST_BROKERS` selects broker |
| Relay, view, and full pipeline | `make test-delivery` | Both services; includes bounded verifier integration tests |
| Focused verification | `make test-verify` | Selects `TestVerification` cases from delivery suite |
| Replay and fresh rebuild | `make test-admin` | Both services, fresh test-owned resources |
| Owned faults and process cleanup | `make test-showcase` | Both services; builds real child binaries |
| Production delivery schedules with memory I/O | `make test-sim-delivery` | Named schedules and fixed 32-run corpus |
| Production worker schedules with memory I/O | `make test-sim-worker` | Named lifecycle cases and fixed 16-run corpus |
| Isolated semantic guard campaign | `make test-mutations` | Explicit opt-in; local PostgreSQL; mutations run only in temporary copies |
| Save ordinary evidence | `make evidence` | Fresh tests, metadata, corpus manifests, demo and sample traces under `artifacts/evidence/` |
| Save integration evidence | `make evidence-postgres`, `make evidence-worker`, `make evidence-delivery`, `make evidence-verify`, `make evidence-admin`, `make evidence-showcase` | JSON test output and relevant artifacts; same service requirements as corresponding tests |
| Save mutation evidence | `make evidence-mutations` | Alias for the explicit campaign |

`make` defaults to `check`. `make fmt` changes source formatting; `make fmt-check` only checks it. `make help` lists all targets. Choose focused checks for the change and preserve required CI coverage; there is no need to run a service suite just to watch the in-memory example.

Integration tests create unique synthetic namespaces/topics and clean only their own resources. Standalone drills retain resources for inspection. Neither is permission for blanket volume cleanup or stopping another user's stack. The actual SQL lost-connection cases require the documented local non-TLS endpoint.

## Documentation build and publication

Documentation uses Python 3.12, MkDocs, and Material; it does not need Go, Docker, or running application services. From the repository root:

```sh
python3 -m venv .venv-docs
. .venv-docs/bin/activate
python -m pip install -r requirements-docs.txt
make docs-serve   # Local preview; Ctrl-C stops it.
make docs-check   # Strict build, local links, source targets, and chart/fixture values.
```

`make docs-build` runs `mkdocs build --strict` alone. Generated output goes to ignored `site/`. `scripts/check_docs.py` checks the generated HTML's local links, images, and anchors, checks GitHub source targets against repository files, and compares chart data and its accessible table with the hand-derived fixtures. It does not establish remote URL availability or browser rendering. After visual changes, inspect desktop/mobile layouts, both themes, diagram rendering, hover/tap values, and navigation away from and back to the charts. Also check search, the mobile drawer, keyboard focus, code copying, and text/chart contrast against the actual backgrounds. Essential explanations and the inventory table remain readable without JavaScript.

The link checker derives the hosting base path from the built homepage's canonical URL, which MkDocs generates from `site_url`. This supports both custom-domain roots and project subpaths, including absolute links in `404.html`. `make docs-check` also runs regression checks that preserve rejection of missing files, missing anchors, and links outside the project path.

Python dependencies are pinned in [requirements-docs.txt](https://github.com/sorrell-consulting/amends/blob/main/requirements-docs.txt). Chart.js and Mermaid browser bundles are served locally and loaded only when a page needs them. Material's page lifecycle initializes charts after instant navigation. Check cold visits to prose, chart, and diagram pages, navigation between them, and theme changes while a library is still loading. Template overrides install the local Mermaid loader before Material, display the official logos and business-site link, and keep the repository link static, avoiding CDN fallback and API requests for repository statistics. System fonts avoid an external font dependency.

The documentation is published at [amends.sorrellconsulting.com](https://amends.sorrellconsulting.com/). The [documentation workflow](https://github.com/sorrell-consulting/amends/blob/main/.github/workflows/docs.yml) strictly builds on pull requests, pushes to `main`, and manual dispatch. Deployment and Pages artifact upload require the repository variable **`AMENDS_DOCS_PUBLISH` to equal `true`** and a non-pull-request run on `main`. The publication gate remains separate from the build checks.

Deployment uses the official configure/upload/deploy Pages actions, the `github-pages` environment, and Pages/OIDC write permissions limited to the deployment job. Hosting requires GitHub Pages configured to use Actions, the custom domain, and `AMENDS_DOCS_PUBLISH=true`. If a successful build does not publish, check the branch/event, publication gate, Pages source configuration, and any environment approvals. After a deployment, inspect the canonical custom-domain URL as well as the workflow result; a successful build alone does not establish public-site behavior.

## Evidence layers

| Layer | Establishes within its exercised scope | Does not establish |
| --- | --- | --- |
| Hand-derived fixtures | Independently specified meaning of concrete histories | All histories |
| Pure-core scenarios | Authority, daily closes, conflicts, withdrawals, versions | SQL durability or broker behavior |
| Generated histories | Broader correction/arrival-order coverage against an independent oracle | Exhaustive source histories |
| Serial memory model | Explicit atomic outcomes, acknowledgement uncertainty, recovery and boundary rules | Real locks, broker groups, or wall-clock deadlines |
| Production orchestration with controlled memory I/O | Actual worker/relay/view retry and lifecycle decisions under selected schedules | Arbitrary concurrent I/O completion or infrastructure behavior |
| Real PostgreSQL and broker tests | Actual adapter behavior and selected failures at pinned versions/settings | Every Kafka-compatible broker or production-scale reliability |
| Owned process/operator drills | Visible detection, selected interruption, recovery/refusal, retained reports | General availability or exhaustive crash coverage |
| Named guard mutations | A specific assertion detects removal of a specified semantic guard | A general mutation score or formal proof |

## Independent expectations and oracle

The fixture starts with 100 inventory units and a low-stock threshold of 60 units. Expected daily closing balances are 50/30/80 units on Days 1/2/3, then 70/50/100 after the Day 1 amendment. An alert requires a close to fall below 60 from a previous close (or opening balance) at or above 60. The original 100-to-50 fall occurs on Day 1; the corrected 70-to-50 fall occurs on Day 2. Cancellation of the last Day 3 activity removes that day's stored row while retaining its deletion version; the Day 2 balance of 50 carries forward implicitly. [Expected JSON](https://github.com/sorrell-consulting/amends/tree/main/fixtures) is hand-derived; tests compare **both** the separately implemented full-history calculation (the oracle) and incremental/view path with it. Never regenerate expectations from either algorithm to make a test pass.

The [oracle](https://github.com/sorrell-consulting/amends/blob/main/internal/oracle/oracle.go) reads raw source assertions and independently validates business input, resolves revision authority, groups days, sums with `math/big.Int`, and derives closes/crossings. It shares domain types and basic wire decoding, but no incremental resolver, fold, diff, processing state, or rewind snapshots. An import-boundary test enforces that separation. The [technical explanation](technical-guide.md#the-independent-oracle) describes the independent algorithm; [verification](verification.md#how-the-code-establishes-the-boundary) explains how the operational command obtains its own broker history.

Shared decoding/configuration mistakes can still affect both paths. Hand-stated cases therefore include normalization, malformed/missing/null/duplicate fields, cancel-before-new, reactivation, sparse and zero-quantity days, threshold equality, opening below threshold, intraday excursions, equal-version conflicts, and numeric bounds. Compare current business state at the same complete boundary; publication order/count and semantic version history need not match a fresh reconstruction.

## Core properties

### P01: Current-state equivalence

For each generated valid finite history and delivery schedule, drain processing and output application, then compare current business state with the independent oracle. Compare present balances and still-valid crossing facts; do not require identical output versions or publication histories.

### P02: Authority independent of arrival

Reordering distinct source revisions, injecting lower revisions late, and changing informational SourceTime values do not change the final authoritative state. Exact duplicate records create no additional semantic output. The oracle must derive authority independently of the fold.

### P03: Cancellation authority survives

A lower active revision cannot defeat a higher cancellation. A higher complete active revision may reactivate. Replay and crashes must preserve cancellation evidence.

### P04: Sparse output deletion and reappearance

When the last active transaction on a day disappears, its visible balance disappears and its retained version does not. Later legitimate reappearance advances that same output identity's version. Delivering older states after the withdrawal cannot resurrect it.

### P05: Daily alert semantics

Crossings compare successive daily closes with the opening balance as the initial predecessor. Within-day reordering does not create or remove daily crossings. Later inventory recovery does not erase a valid historical crossing. Corrections may retract, move, or restate it.

### P06: Output version protocol

Versions never decrease. Identical duplicates are harmless. Lower versions cannot replace higher versions. Equal-version conflicting content is a protocol error and leaves the view frontier before the offending record. Version increments only occur for output-state changes.

### P07: Conflict visibility and repair

Both arrival orders of conflicting highest-revision records yield a BLOCKED key and prevent unqualified verification success. Additional input is retained while blocked. A higher unique repair refolds all accumulated authoritative state, preserves conflict evidence, and yields an explicitly qualified verification result.

### P08: Isolation between keys and namespaces

A conflict freezes computation for its key while ingestion for that partition and business processing for other keys continue. A fresh rebuild namespace cannot overwrite, resurrect, or compare versions with the original namespace.

### P09: Verification refuses an invalid boundary

A quiet generator plus a stalled relay or materializer is INCOMPLETE. A missing required source prefix, source-topic recreation, or advancing producer is INCOMPLETE. A mismatch on a complete comparable prefix is DRIFT. At a complete boundary, unresolved authority yields BLOCKED even if unaffected keys match.

Exercise SC25 together with SC08: while the boundary is incomplete, assert overall INCOMPLETE, retained known conflict diagnostics, and BLOCKED trust status for the affected key. Resume the stalled relay or materializer without resolving the source conflict; once the full boundary is established, assert overall BLOCKED with that unresolved conflict still reported. Both verifier results are nonzero. A passing test of these expected negative results must not relabel either result as PASS.

### P10: Arithmetic failures are explicit

Exceed the declared numeric domain. The worker does not wrap or advance the affected input frontier; the oracle independently reports a domain error. Include version and revision-range boundary tests.

## Scenario inventory and traceability

The following maps all [SC01–SC28 contract scenarios](semantics.md#s12-named-semantic-scenarios) to their pure/model and durable evidence responsibilities. Named pure cases live in [test/scenarios](https://github.com/sorrell-consulting/amends/tree/main/test/scenarios). Component guides and tagged integration tests identify the actual boundaries exercised. `None` means no additional scenario-specific case at that layer; shared pipeline checks still apply.

| Scenario | Pure checks / controlled model | Real store checks | Broker integration / local drill |
| --- | --- | --- | --- |
| SC01: Backdated NEW | Earlier insertion refolds the affected suffix; compare with independent oracle | Shared worker atomicity, SC18 | None |
| SC02: Quantity amendment | Hand-stated 50/30/80 to 70/50/100; crossing moves | Correction state, intent, and frontier commit together, SC18 | `crash-mid-correction` |
| SC03: Valid time moves earlier or later | Remove old contribution, insert new, withdraw an emptied day | Withdrawal persistence, SC14 | None |
| SC04: Cancellation and cancel-before-new | Higher cancellation defeats lower active input; no cancellation-only day | Cancellation evidence survives restart | Replay/restart path, SC23 |
| SC05: Reactivation | Higher complete active revision restores activity and continues output versions | Retained deletion version survives reappearance | Late-output path, SC16 |
| SC06: Stale amendment arrives last | Lower revision cannot win through newer SourceTime or arrival | Evidence/progress may advance without business-version increments | `stale-amendment-storm` |
| SC07: Exact duplicate | Canonical equality excludes informational/transport fields; distinguish input duplicates from output duplicates | Duplicate decisions and progress are atomic, SC18/SC21 | `duplicate-flood` |
| SC08: Conflict at highest revision | Both arrival orders block the key; frozen values remain uncertified | Candidates, key status, output intent, and progress persist together | `source-conflict`: expected BLOCKED at a complete boundary; combined SC25 case below |
| SC09: Conflict resolution and historical conflict | Higher unique revision repairs the whole key; historical conflict stays diagnostic | Preserve evidence; enqueue repairs before READY | `source-conflict`: qualified agreement after repair |
| SC10: Other input during a block | Retain blocked-key arrivals; other keys proceed; resolution includes accumulated revisions | Frozen business state with advancing evidence/progress | `source-conflict` with another key on the same partition |
| SC11: Daily-close crossings | Same-day excursions and equal-time permutations preserve daily result | None | None |
| SC12: Opening balance and threshold equality | Opening below threshold and close equal to threshold create no artificial crossing | None | None |
| SC13: Sparse days and zero-quantity activity | Restore the earlier activity close across gaps; active zero differs from cancellation | None | None |
| SC14: Balance disappears | Cancel/move last activity; withdraw balance and refold later crossings | Persist output deletion guard atomically with progress | `disappearing-day` |
| SC15: Alert retracted, moved, or restated | Same-day payload change versus moved identity; later receipt preserves historical crossing | Shared output state/version persistence, SC16/SC21 | Correction demonstration; late-output path, SC16 |
| SC16: Late output after withdrawal | Withdrawal before older statement; higher-version reappearance permitted | Deletion version persists across restart | `disappearing-day`, `materializer-crash` with older redelivery |
| SC17: Equal output version with different content | Duplicate equality versus protocol conflict | Persist diagnostics; preserve view/frontier before offending record | Conflicting envelopes stop the affected output partition |
| SC18: Crash before or after worker commit | Model rollback, committed-but-unacknowledged, and aborted outcomes | Actual rollback/commit and lost-connection recovery | `crash-mid-correction`, `ambiguous-commit`; seek durable source frontier |
| SC19: Ownership replacement and zombie commit | Model both legal epoch/processing orders and obsolete callbacks | Two-connection same-row fence in both lock orders | `zombie-worker`, `rebalance-under-load`; pinned-client lifecycle |
| SC20: Relay crash or lost publication acknowledgement | Model accepted-but-unacknowledged send; retry identical envelope | Committed outbox only; durable pending/acknowledged state and order | Lost publication acknowledgement, relay restart; `relay-stall` |
| SC21: Materializer crash | Model crashes around view commit and acknowledgement | View values, deletion guards, and frontier commit together | `materializer-crash`; duplicate reads after committed application |
| SC22: Malformed input and atomic quarantine | Validate rejection reasons; compare exact expected diagnostics | Quarantine and source frontier commit or roll back together | `poison-event`: qualified status with exact diagnostics manifest |
| SC23: Replay into intact state | Reject unsupported ranges; preserve business values and versions | Administrative fence and monotonic authoritative frontier | Park owner; scan retained consumed range; reject missing history |
| SC24: Isolated rebuild | Equal business projection can have different namespace/version history | Separate namespace state, outbox, and view; old state untouched | `full-rebuild`; old-namespace messages cannot affect new view |
| SC25: Invalid verification boundary | Model stalled relay/view, advancing source, missing history; known SC08 conflict stays diagnostic during INCOMPLETE | Stable view snapshot and durable frontier/receipt observations | `relay-stall`, `history-missing`; advancing source/topic recreation; combine stall with `source-conflict` to check INCOMPLETE then BLOCKED after drain |
| SC26: No-op amendment | Higher authority without changed business facts emits no restatement | Evidence/authority/progress advance without output-version change | None |
| SC27: Numeric overflow | Independent exact-domain checks, including revision/version bounds | Failed fold cannot commit wrapped state or advance frontier | None |
| SC28: Routing and configuration incompatibility | Key/schema/configuration validation; distinguish rejection from incompatible resume | Durable rejection and namespace compatibility checks | Actual routing, fixed partition mapping, and topic-incarnation checks |

The combined SC25/SC08 case is checked in the memory model and in `TestVerificationConflictIncompleteAndRepair` in the [real delivery verification suite](https://github.com/sorrell-consulting/amends/blob/main/internal/delivery/verification_test.go): an incomplete boundary retains known conflict diagnostics and BLOCKED key trust; after drain, unresolved authority yields overall BLOCKED. Both verifier results remain nonzero.

## Generated histories and deterministic schedules

Generate logical source history before selecting delivery order. Keep valid histories separate from conflict/rejection histories: difficult rows must not be silently dropped to obtain agreement. Bound quantities so supported intermediate authority stays within the arithmetic domain, with explicit overflow cases tested separately.

| Runner | Fixed corpus | Decisions and limits |
| --- | --- | --- |
| `sim` | [64 seeds × 2 history classes × 2 delivery orders](https://github.com/sorrell-consulting/amends/blob/main/test/property/corpus.json): 256 runs | 80 actor choices and 80 logical ticks, then fair drain; explicit memory worker/relay/view outcomes |
| `sim/transport` | [16 seeds × 2 history classes](https://github.com/sorrell-consulting/amends/blob/main/sim/transport/testdata/corpus.json): 32 runs | 48 downstream choices, restart/fair drain; production `delivery.Stepper` with memory I/O |
| `sim/ownership` | [8 seeds × 2 history classes](https://github.com/sorrell-consulting/amends/blob/main/sim/ownership/testdata/corpus.json): 16 runs | 64 lifecycle steps plus recovery; production `worker.Lifecycle` with controlled atomic effects |

The core generator uses Go's PCG source, up to eight transaction identities with up to four complete revisions each, cancellations, duplicates, and reordered arrivals. Invalid histories may correctly finish BLOCKED or PASS_WITH_EXCLUSIONS. Matching exact diagnostics and no comparison difference remain required; valid histories still require strict PASS.

| Fault boundary | Required modeled outcomes |
| --- | --- |
| Worker before transaction | No durable changes |
| Worker after reads/refold but before commit | Complete rollback on crash |
| Worker commit reply lost | Both actually committed and actually aborted outcomes |
| Epoch installation | Old transaction finishes before new epoch, or new epoch fences later old work |
| Revocation/assignment callbacks | Delayed obsolete work cannot acquire a new epoch |
| Relay before send | Durable outbox remains pending |
| Broker accepted, acknowledgement lost | Same envelope may be published again |
| Relay after acknowledgement, before mark-sent | Restart republishes unchanged envelope safely |
| View before durable commit | Neither view state nor view progress changes |
| View after commit, before acknowledgement | Duplicate input preserves state and deletion guard |
| Transport delay/reordering | Highest version wins per ID; no cross-output atomicity inferred |
| Temporary store/broker unavailability | Safety holds; progress resumes after availability returns |

Safety assertions run after each observable step. Final equivalence runs only after injected failures cease and the scheduler fairly drains enabled work. If an actor never recovers, the run should remain incomplete.

The serial model separates an actual commit/abort from the caller's known/unknown observation. It rereads progress, preserves retry bytes, and uses explicit logical ticks and resume events. Delivery and worker schedulers instead invoke the production orchestration with controlled I/O and 100/200 ms logical retry waits. They retain the production decision paths while replacing storage/broker effects and elapsed time. Real timer expiry, SQL lock waits, Kafka group management, and arbitrary concurrent completions need separate evidence.

Worker schedules include unknown claims and processing outcomes, both epoch serial orders, canceled/obsolete callbacks, late seeks, unresolved reads, atomic quarantine, and offset gaps. Delivery schedules include accepted-but-unacknowledged publication, receipt-only retry, unknown view commits, retained withdrawals, and retirement after unresolved reads. Details and trace schemas belong in [worker simulation](worker-simulation.md) and [delivery simulation](delivery-simulation.md).

### Reproduce and reduce a failure

```sh
make sim SEED=17
./bin/amendsctl sim -replay artifacts/sim-seed-17.json
make sim-delivery SEED=17
make sim-worker SEED=17
```

Use each command's `-h` for its separate trace/replay options. A saved trace carries raw history, configuration/digest, generator/implementation version, recorded toolchain, seed, and explicit schedule. Exact replay consumes saved records/choices rather than regenerating from the seed. Core traces use version 1 and implementation identifier `core-schedule-v1`; replay rejects other implementation identifiers.

Failures save original and reduced traces under `artifacts/failures/`; `AMENDS_ARTIFACT_DIR` selects another directory for direct test execution. `make evidence` retains sample traces and reports under `artifacts/evidence/`. Preserve the original failure and paired delivery-order trace when applicable. Reducers delete schedule/source chunks only while the defined failure predicate persists, preserve original coordinates and complete assertions, and keep required worker-lifetime dependencies. Reduction is local, not guaranteed globally minimal or proof of identical root cause; inspect the retained predicate/error class when diagnosing it.

## Real database and broker coverage

The [PostgreSQL suite](postgres-store.md) exercises actual transactions and both partition-row lock orders, rollback and both lost-COMMIT outcomes, fencing, atomic quarantine/outbox/frontier changes, publication receipts, view state/tombstone/progress atomicity, and consistent snapshots. Its shared memory/store conformance sample is distinct from the 256-run memory corpus. SQL concurrency claims require tests against an actual database.

The [source suite](source-worker.md) checks explicit retained-topic configuration and source incarnation, routing/raw-key rejection, external durable seeks, guarded callback cancellation, eager rebalances, ownership replacement, and offset gaps across restart. Topic recreation, missing required history, incompatible mapping, or an out-of-range durable frontier must fail visibly. Trailing control-only offsets are not invented worker decisions.

The [delivery suite](output-delivery.md) exercises full fixture correction/cancellation, exact publication bytes, controlled lost observations, relay/view restart, retained withdrawals, and equal-version conflicting content. Its [verification cases](verification.md) establish the stopped-writer boundary using actual broker scans and SQL snapshots; they reject advancing source, missing history, active relay, stalled publication/view, incompatible identity, and unavailable infrastructure. Inspect snapshots and zero lag remain observations.

The [administrative suite](replay-rebuild.md) checks dry-run nonmutation, replay preservation and fencing, changed-frontier refusal, refusal to repair corrupt state, missing-history refusal before destination creation, fresh reconstruction, and original namespace isolation. Actual replay lock/unknown-COMMIT cases supplement broker-facing tests. A rebuild must compare the same captured source end after its guard handoff; a source append cannot silently certify a different history.

[Showcase integration tests](showcase.md#evidence-and-limits) check observed SQL barriers, selected process deaths, unchanged rollback snapshots, recovery under new ownership, exact negative/qualified statuses, version/tombstone preservation, cancellation, and all owned children joined. An unclean relay exit cannot count as a successful handoff. The fourteen runnable drills and their expected final verifier statuses are cataloged in [showcase](showcase.md#drill-catalog); that is the canonical drill inventory.

### Verification deadline classification

The [deadline classifier](https://github.com/sorrell-consulting/amends/blob/main/internal/verify/verify.go) (`boundaryWaitError`) reports INCOMPLETE for a network or context timeout after the overall deadline only when a boundary has already been observed as pending. Earlier adapter timeouts, initial-observation failures, lost sessions, and cancellation remain ERROR. This distinguishes failure to reach a known boundary from an infrastructure failure that prevents a reliable observation.

The [regression cases](https://github.com/sorrell-consulting/amends/blob/main/internal/verify/report_test.go) in `TestBoundaryWaitDeadlineClassification` cover socket timeouts, a deadline expiring before the context timer callback, and infrastructure failures that must remain errors:

```sh
GOWORK=off go test -mod=readonly -race -count=1 \
  -run '^TestBoundaryWaitDeadlineClassification$' -v ./internal/verify
```

### Reproduce repair locality

The [optional 2,000-day example](design.md#how-much-history-needs-repair) compares early and late corrections using production fold/diff functions and the memory materializer. Baseline, correction, and last-day cancellation are checked against independently stated business expectations and the full-history oracle. Run it with the race detector:

```sh
make test-locality
```

The `locality` build tag keeps execution outside ordinary tests. `make vet` includes the tagged code, so normal checks catch compilation errors in the example.

It reports suffix records and emitted business changes. SQL/broker behavior and end-to-end speed are covered by different evidence; a reduced suffix count alone establishes neither.

## Mutation campaign

The [campaign guide](mutation-testing.md) and [manifest](https://github.com/sorrell-consulting/amends/blob/main/tools/mutations/campaign.json) specify twelve non-equivalent guard substitutions and one separately declared equivalent ordering change. Each selected baseline must pass before the mutation runs in a fresh standalone temporary copy. A named semantic assertion must fail for CAUGHT; compiler errors, races, panics, timeouts, skipped/missing tests, and unrelated infrastructure failures do not qualify. Survivors cannot be relabeled equivalent after the fact.

Four operators exercise actual PostgreSQL source-progress, outbox, epoch, and view atomicity boundaries. The output-boundary operator targets the memory verifier; real bounded verification has separate integration tests. The outbox operator drops committed publication intent, rather than publishing an uncommitted broker envelope. Reversing equal-time TxnID traversal is equivalent for additive daily-close business projection and must pass the hand-derived scenarios. Do not distort the contract to kill it.

The campaign is opt-in via `make test-mutations` / `make evidence-mutations`, outside the normal application and push/PR path. Its report retains exact source hashes, edits, selected assertions, test JSON, completion flags, and classifications. The [manual workflow](https://github.com/sorrell-consulting/amends/blob/main/.github/workflows/mutations.yml) runs ordinary and SQL baselines first. Normal CI tests the runner/classifier and source anchors while executing the correct application.

## Evidence artifacts and CI

`make evidence` records the actual toolchain, Git revision/worktree (or explicit source-archive metadata absence), fresh race-enabled test JSON, corpus manifests, core transcript, and sample traces/reports. Integration evidence targets retain JSON test output under their named artifact directories. Drills print fresh run directories containing original reports, manifests, logs, diagnostic inspections, and fault-specific observations. Inspect an artifact's command, revision, dirty state, boundary, and status before attributing it to a checkout. A workflow definition is not an observed run result.

The [ordinary workflow](https://github.com/sorrell-consulting/amends/blob/main/.github/workflows/ci.yml) runs on push, pull request, and manual dispatch. Its checks are:

- `check`: ordinary evidence and Compose configuration validation.
- `postgres`: real SQL evidence against PostgreSQL 18.6.
- `pipeline`: healthy PostgreSQL/Redpanda, worker, delivery, and administrative suites, then standalone retained operator drills and diagnostic review.
- `showcase`: the complete showcase integration suite, including process deaths and cancellation cleanup, against its own PostgreSQL/Redpanda stack on a separate runner.
- `worker`: an aggregate check that succeeds only when both `pipeline` and `showcase` succeed. Failed, canceled, or skipped dependencies cannot pass this gate.

Artifact upload is attempted even on failures. Service jobs stop their own dependencies afterward. `worker-evidence` retains pipeline and standalone drill artifacts; `postgres-evidence` retains database test evidence; `showcase-evidence` retains showcase integration artifacts and its dependency logs. The [mutation workflow](https://github.com/sorrell-consulting/amends/blob/main/.github/workflows/mutations.yml) runs only on manual dispatch, separately from normal application execution.

The pipeline and showcase jobs exercise integration tests and public command wrappers, covering flags, environment wiring, retained artifacts, and cancellation. Each job uses its own runner and dependency stack.

## Limits

Finite corpora sample supported histories and schedules. Memory I/O does not establish real locking, broker durability, or all callback interleavings; real tests cover selected environments and fault boundaries. Race detection finds exercised memory races, not distributed-system correctness. Shared schema/decoding remains a possible common defect.
