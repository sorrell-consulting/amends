# Advanced drill catalog

Use [evaluation](evaluation.md) for the first correction and worker-crash walkthrough. This reference describes deliberate faults, assertions, retained evidence, and recovery limits. Each drill owns fresh synthetic resources and joins its children. A successful wrapper means its assertions passed; the original verifier status remains visible, including negative or qualified outcomes.

Here, **owned** means processes and resources created by that invocation; **join** means wait for a child process to exit. Each **namespace** isolates its configuration and state. A **SQL barrier** is a held database lock that lets the drill observe a worker blocked at a chosen transaction step before injecting a fault. This selects a specific tested boundary rather than guessing when to crash.

## Drill catalog

Every invocation below builds, starts/checks local dependencies, and runs a fresh owned pipeline. Direct CLI usage needs already-running dependencies and `AMENDS_DATABASE_URL`. `crash-mid-correction` dispatches **`amendsctl demo -mode crash-mid-correction`**; the other thirteen use **`amendsctl drill -name NAME`**.

| Drill | Property / fault | Invocation | Expected final verifier | Details |
| --- | --- | --- | --- | --- |
| `crash-mid-correction` | Worker death inside an observed transaction; atomic rollback and recovery | `make drill DRILL=crash-mid-correction` | PASS | [SQL barrier](#worker-crash-barrier) |
| `replay-intact` | Retained consumed input preserves intact state and versions | `make drill DRILL=replay-intact` | PASS | [Replay/rebuild](#replay-and-rebuild-drills) |
| `full-rebuild` | Fresh namespace reconstructs business values and isolates the original | `make drill DRILL=full-rebuild` | PASS | [Replay/rebuild](#replay-and-rebuild-drills) |
| `poison-event` | Atomic quarantine and exact rejection qualification | `make drill DRILL=poison-event` | PASS_WITH_EXCLUSIONS | [Diagnostics](#diagnostic-drills) |
| `source-conflict` | BLOCKED highest authority, full repair, retained conflict evidence | `make drill DRILL=source-conflict` | PASS_WITH_EXCLUSIONS | [Diagnostics](#diagnostic-drills) |
| `relay-stall` | Committed pending output forces INCOMPLETE until publication resumes | `make drill DRILL=relay-stall` | PASS | [Delivery](#delivery-drill-extensions) |
| `disappearing-day` | Withdrawal survives exact older redelivery | `make drill DRILL=disappearing-day` | PASS | [Delivery](#delivery-drill-extensions) |
| `materializer-crash` | View death before withdrawal commit and after durable completion | `make drill DRILL=materializer-crash` | PASS | [View crash](#materializer-crash-extension) |
| `duplicate-flood` | More deliveries/evidence without changed business versions | `make drill DRILL=duplicate-flood` | PASS | [Input](#input-drill-extensions) |
| `stale-amendment-storm` | Lower revisions cannot replace authority or resurrect cancellation | `make drill DRILL=stale-amendment-storm` | PASS | [Input](#input-drill-extensions) |
| `history-missing` | Missing owned source prefix forces verification/replay/rebuild refusal | `make drill DRILL=history-missing` | INCOMPLETE | [Missing history](#missing-history-extension) |
| `zombie-worker` | Obsolete database token cannot commit after replacement | `make drill DRILL=zombie-worker` | PASS | [Fencing](#zombie-worker-extension) |
| `ambiguous-commit` | Real COMMIT request/reply loss; durable read selects retry or skip | `make drill DRILL=ambiguous-commit` | PASS in both runs | [Commit uncertainty](#ambiguous-commit-extension) |
| `rebalance-under-load` | Real eager reassignment with queued corrections on two partitions | `make drill DRILL=rebalance-under-load` | PASS | [Rebalance](#rebalance-under-load-extension) |

Qualified diagnostics require exact named expectations; no wrapper turns the printed qualification into strict PASS. `history-missing` leaves its retained source incomplete, and its printed re-verification command must still fail with INCOMPLETE.

## Running it

Use the Go/Make/C compiler and local Docker Compose prerequisites in the [README](https://github.com/sorrell-consulting/amends/blob/main/README.md) and [runbook](runbook.md). The Make wrappers build both binaries, start the pinned local dependencies if needed, and probe readiness:

```sh
make demo
make demo MODE=correction-only
make drill DRILL=crash-mid-correction
```

The default runs the no-fault story followed by the crash story in separate fresh namespaces. Each has one synthetic stock key, four source records, two source partitions, two assigned worker processes, one relay, and one durable view process. The fixture is on partition zero; partition one is assigned but has no fixture records. The existing worker/delivery integration suites separately exercise populated partitions on both workers.

The runner stops and joins its own application children. It leaves the database and broker running and retains each demo's namespace, topics, and artifacts for inspection. It does not discover or signal unrelated processes. `make down` stops local dependencies while retaining volumes. There is no new reset or volume-deletion command.

The direct CLI supports already-running dependencies and works from any directory after building; `amends` must be beside `amendsctl`:

```sh
export AMENDS_DATABASE_URL='postgres://amends:amends-local-only@127.0.0.1:15432/amends?sslmode=disable'
./bin/amendsctl demo -mode all -brokers 127.0.0.1:19092 \
  -artifacts artifacts/showcase -timeout 3m
```

The timeout is per scenario. Child shutdown has a separate ten-second grace period per live child. SIGINT/SIGTERM cancels the runner and initiates cleanup. An unclean relay exit, expired deadline, failed observation, or unexpected child exit fails the run. Do not treat the last stage's PASS as overall success if cleanup subsequently fails. Killing the controller itself with SIGKILL or losing its host is outside this cleanup guarantee; inspect retained artifacts and owned processes before attempting a manual handoff.

## Business sequence and verification handoff

[`showcase.Run`](https://github.com/sorrell-consulting/amends/blob/main/internal/showcase/showcase.go) invokes the public provisioning and loading commands. It saves the embedded fixture bytes and hashes, obtains fresh source/output topic incarnations, and launches the normal `amends` roles. `DescribeGroups` must show a stable classic group with two members, each assigned one distinct partition.

For code changes, start with `Options.Scenario` and `correctionScenario` in that package; the rebalance schedule has its own function. Scenario code explicitly waits for source/outbox progress and stops/joins the relay before calling a verification stage. Replay/rebuild demonstrations live in `administrative.go`, while the underlying operations remain in `internal/admin`. Integration tests cancel at named, synchronous controller checkpoints, independently of console wording. Each test keeps its expected boundaries, diagnostics, versions, and child exits beside the scenario assertions.

For each of the two stages:

1. Join the finite `source-load` command. Its Kafka client has closed before releasing the producer guard.
2. Observe every source partition at the expected frontier, with no pending outbox intent, quarantine, or view protocol error.
3. Send SIGTERM to the owned relay and join it successfully. A forced kill is a failed handoff. An empty outbox by itself is insufficient.
4. Invoke `verify --quiesce`, which acquires writer guards, scans retained source independently, captures H/O, waits for the view, and compares against the independent oracle.
5. Require strict PASS and the exact expected source boundary, then compare the durable view against the original hand-derived fixture. Save live inspection separately.

Opening inventory is 100 units and the low-stock threshold is 60. Baseline daily closing balances are 50, 30, and 80 units on Days 1–3. Day 1 falls from at least 60 to below it, creating a downward-crossing alert; staying below 60 on Day 2 does not create another. After ISSUE-001 revision 2 replaces the 50-unit issue with a 30-unit issue, closes are 70, 50, and 100 units. The Day 1 alert is withdrawn and Day 2's fall from 70 to 50 creates the newly valid alert. On this fixed history, the observed boundaries are H=[3,0], O=[5,0] before and H=[4,0], O=[10,0] after. These counts are fixture-specific; they are not a general correspondence between offsets and business changes.

The verifier's contract and negative/qualified statuses are unchanged. Automatic orchestration applies only to this newly created, fully owned finite pipeline. Manually operated pipelines still require an operator to drain and stop/join their relay. Session guards do not establish broker fencing or safe automatic takeover from an uncertain relay.

## Replay and rebuild drills

`replay-intact` and `full-rebuild` first run the real correction showcase and join its application processes. They retain the baseline manifest, reports, and exact durable state/view before executing the public administrative commands. Replay must preserve business values, output versions/withdrawals, source progress, outbox, quarantine, and view while installing its administrative fence. Rebuild uses a fresh namespace and output topic and must leave the original state/view unchanged. Both compare hand-derived projections and require strict bounded verification afterward.

The [replay/rebuild reference](replay-rebuild.md) owns the preflight, source-history requirements, dry-run/execution protocol, and reports. `DRY_RUN` or `REPLAYED` alone is not a verifier result. Evidence is retained under the printed `artifacts/operations/run-*/` directory, including `replay.json` or `rebuild.json` and the fresh destination manifest for rebuild. Never use replay to repair corrupted state or rebuild into an existing namespace; uncertain outcomes leave resources for inspection.

## Diagnostic drills

The diagnostic drills use this owned-process runner through `amendsctl drill -name poison-event` and `-name source-conflict` (or `make drill DRILL=NAME`). The [runbook](runbook.md#highest-revision-conflict-or-malformed-input) gives operator recovery steps; [fixtures](https://github.com/sorrell-consulting/amends/blob/main/fixtures/README.md#diagnostic-drills) states the arithmetic and qualifications. These are not new `demo -mode` values. Their evidence defaults to `artifacts/operations/`; integration evidence stays in `artifacts/showcase/`.

After source-topic creation and before output/processing setup, `source.json` records the effective fresh configuration. The conflict variant includes the declared second same-partition key; `source.log` remains the unmodified provisioning transcript. No existing database configuration is changed. Fixture bytes and hashes are retained alongside the pipeline manifest.

Both diagnostic stages use exact declared quarantine counts while draining, then stop/join the relay before invoking the public verifier. Expected diagnostic files derive from the specified source bytes and coordinates, never from observed answers. The runner checks plain and explicitly qualified verifier exits, complete boundary flags, no differences, hand-derived projections, and unchanged frozen business envelopes. `run.json` identifies its status as an assertion outcome; `final_verification_status` and nested `stages` retain `BLOCKED` or `PASS_WITH_EXCLUSIONS` rather than converting them to verifier PASS.

Each ordinary or diagnostic stage also saves `dashboard-STAGE.html`, `quarantine-STAGE.log`, and `conflicts-STAGE.log`. The dashboard is a static local rendering of a new live inspection, not a visualization of the earlier verifier's certified boundary. It always says NOT VERIFIED. Review logs come from the public read-only diagnostic commands; the conflict drill shows unresolved highest authority before repair and preserved superseded candidates afterward. These observations never replace the stage's original verification report.

## Delivery-drill extensions

Run `make drill DRILL=relay-stall` or `make drill DRILL=disappearing-day` (also available as `amendsctl drill -name NAME`). Each creates a fresh owned pipeline, retains evidence under `artifacts/operations/run-*/`, and joins every child on success, failure, or cancellation. These are not `demo -mode` values. No normal processing, oracle, relay, or view decisions change.

`relay-stall` leaves the baseline relay cleanly stopped/joined before loading the correction. The worker reaches H=[4,0] and commits five pending envelopes, but output and view remain at [5,0] with the original business values. Two SQL progress observations retain the same oldest pending timestamp while its age grows; no arbitrary outage-duration threshold is asserted. The public verifier must exit 1 with INCOMPLETE, independent source scan complete, publication/view/boundary checks incomplete, no certified key comparison, and no captured complete O. `verify-stalled.log` preserves that original result; `stall.json` and `dashboard-stalled.html` are separate observations. A new owned relay drains unchanged committed intent, stops/joins cleanly, and the corrected fixture must reach strict PASS at H=[4,0], O=[10,0]. Canceling after the stalled observation retains final INCOMPLETE with overall run ERROR.

`disappearing-day` starts from the corrected fixture and loads the existing `cancel-last-day.jsonl`. The last active Day 3 contribution disappears: balances remain 70/50 on Days 1/2, only the Day 2 crossing remains, and Day 3 retains `BalanceRetracted` version 3 in both processing output state and the view. The withdrawn stage verifies H=[5,0], O=[11,0] against `expected-after-cancel.json` and the independent oracle. With the relay stopped/joined, a finite publisher holds the relay service guard and sends the exact committed bytes of Day 3 `BalanceStated` version 2 from the acknowledged outbox. It neither creates new intent nor changes receipts/versions. Its client closes before guard release; an uncertain publication fails the run rather than being treated as success.

After this older envelope is consumed, verification must again reach strict PASS, now at H=[5,0], O=[12,0]. Processing state and all view rows must remain identical; only view progress advances. `redelivery.json` retains the exact bytes, original sequence, new output coordinate, deletion envelope, and preservation assertions. `versions-withdrawn.log` and `versions-redelivered.log` retain public `view-show` evidence; each stage has its own verifier, fixture projection, inspection, dashboard, and diagnostic-review files. The integration test independently reads the broker records and SQL bytes. This is controlled late delivery, not an arbitrary network reorder or a materializer-crash drill.

## Input-drill extensions

Run `make drill DRILL=duplicate-flood` or `make drill DRILL=stale-amendment-storm` (also `amendsctl drill -name NAME`). These extend the same fresh owned pipeline, with normal workers and public verification; no authority/oracle change, service, schema, failpoint, or aggregate counter is added. Evidence stays under the printed `artifacts/operations/run-*/` directory. The names describe finite semantic exercises, not stress or throughput benchmarks.

`duplicate-flood` starts at the corrected H=[4,0], O=[10,0] boundary. It repeats the two assertions in `duplicate-deliveries.jsonl` 64 times: 64 byte-identical repeats of the correction and 64 canonical duplicates whose informational SourceTime is in 2050. Source progress and retained references reach 132, while authority, balances 70/50/100, the Day 2 alert, all versions, output intent/receipts, and the entire durable view remain unchanged. The relay stays stopped/joined throughout these no-op deliveries. Final strict PASS is at H=[132,0], O=[10,0]. The latest committed sample must have `duplicate=true`, `stale=false`, and zero refold and output work.

`stale-amendment-storm` first loads `stale-protection.jsonl`: ISSUE-001 revision 4 preserves the corrected -30, while RECEIPT-003 revision 3 cancels Day 3. The `protected` stage verifies H=[6,0], O=[11,0], balances 70/50, the Day 2 alert, and Day 3 deletion version 3. With the relay now stopped/joined, `stale-deliveries.jsonl` adds previously unseen lower amendments: ISSUE-001 revision 3 (-40) and RECEIPT-003 revision 2 (+50), both with newer SourceTime in 2050. Neither may take authority or reactivate the receipt. `stale-first` requires strict PASS at H=[8,0], O=[11,0]; the latest sample is stale but not duplicate. Another 64 repetitions of the two-line file bring `stale-storm` to H=[136,0], O=[11,0]. The latest sample is now both duplicate and stale, still with zero refold/output work. There are no expected exclusions or suppressed conflicts.

`input-state-baseline.json` and `input-state-STAGE.json` retain SQL snapshots of the decision/output/view tables, including exact bytea envelopes/state, receipts, and view diagnostics. Processing ownership epochs are omitted from these raw snapshots and checked separately through the store snapshot. Output state, outbox, quarantine, view rows, and view progress must match byte-for-byte; source progress, evidence, and the committed sample advance. Highest revisions and representative authoritative payloads must remain unchanged. Every fixture offset must remain represented exactly once in canonical candidate source references. `input-checks-STAGE.json` records these assertions, finite source-reference counts, complete O, and the latest measurement. These counts are fixture evidence, not historical monitoring metrics.

Each stage also retains its original verifier report, hand-derived projection, inspection, dashboard, and diagnostic-review files. `input-burst.jsonl` and its hash in `run.json.sha256` preserve the exact expanded finite input. Tests separately compare the broker's full source bytes/coordinates against declared fixtures and resolve every stored reference to its canonical candidate. Cancellation before the duplicate burst or after `stale-first` must stop/join all owned children, never launch the remaining burst, and retain overall ERROR even though the last completed verifier stage was PASS.

## Missing-history extension

Run `make drill DRILL=history-missing` (or `amendsctl drill -name history-missing`). This **negative drill** checks refusal to proceed when required history is missing; recovery remains an operator responsibility. It creates its own fresh synthetic namespace and source/output topics; it accepts no existing manifest or topic to truncate. After strict baseline and corrected verification at H=[4,0], O=[10,0], it stops and joins every owned service, acquires the producer guard, and removes source partition zero's offsets [0,2) through the broker's DeleteRecords API. No volumes, existing namespaces, or output records are removed. The topic incarnation, partition count, source end [4,0], and output end [10,0] must remain unchanged; the retained source start becomes [2,0] instead of the required [0,0].

The public verifier must exit 1 with **INCOMPLETE**, explicitly citing unavailable required history. It must not scan a surviving suffix as if it were the complete ledger or certify any key. Its certified H/O fields remain unset; the separately observed vectors in `history.json` are not a verification boundary. JSON/HTML inspection retains the source-retention error, unknown source end/input distance, and the intact READY view with zero output distance. The dashboard remains NOT VERIFIED even though balances still read 70/50/100 and the Day 2 crossing is present.

The runner invokes the normal administrative operations with execution requested: replay of the surviving already-consumed range [2,4), then rebuild into an unused destination. Both must return INCOMPLETE before changing processing state or creating a destination. Bounded retries cover only a still-busy producer guard during session release; other errors fail the drill. Actual SQL and broker observations require the destination namespace and output topic to remain absent. Complete original processing/view snapshots, ownership epochs, exact outbox bytes/receipts, progress, measurements, versions, and tombstones must be unchanged. Integration tests also exercise both dry-run and executing public replay/rebuild commands and require exit 1 with the original INCOMPLETE reports.

`history-request.json` records the exact owned truncation request. `history.json` retains required/observed source starts, source identity/end, output end, partition snapshots including epochs, administrative reports, and preservation/absence assertions. `history-state-before.json` and `history-state-after.json` retain byte-identical SQL state; epochs omitted by that raw snapshot are checked separately. `verify-history-missing.log`, `inspect-history-missing.log`, and `dashboard-history-missing.html` retain the original negative result and uncertified inspection. A successful wrapper reports **assertions PASS** while preserving **final verifier INCOMPLETE**. Re-running its printed verifier command must still exit 1.

Cancellation before removal leaves complete source history intact; cancellation after the incomplete observation must not proceed with administrative operations. Both paths join every owned child and retain overall ERROR with the last stage's original status. An uncertain deletion result fails the run and leaves resources for inspection; the runner does not infer that history survived. Restoring history, checkpoints, broker compaction, topic recreation, and recovery from arbitrary data loss are outside this drill. Do not republish a suffix, reset frontiers, or invent opening movements to obtain PASS. Obtain complete authoritative history and establish a separately validated fresh lineage before attempting reconstruction; no restore/cutover command is implemented here.

## Zombie-worker extension

Run `make drill DRILL=zombie-worker` (or `amendsctl drill -name zombie-worker`). Here “zombie” refers specifically to a live source worker that continues using an obsolete database token. The runner owns a fresh synthetic pipeline and uses normal worker binaries, real PostgreSQL fencing, and public source loading/verification. No worker failpoint, schema change, new service, or altered oracle is introduced.

After strict baseline verification at H=[3,0], O=[5,0], the runner stops/joins both original source workers and starts one `worker-stale`. It loads the three baseline records again as canonical duplicates. The `reowned` stage must reach strict PASS at H=[6,0], O=[5,0], with the original hand-derived values and versions. Broker metadata must show this sole group member assigned both partitions, and its normal attempt log must acknowledge offset 5 with the independently observed database epoch. This establishes that the real child retains the token; no timing sleep or process pause selects the boundary.

The controller commits one normal `Store.Claim` on partition zero and independently observes epoch e+1 with the same frontier 6 and output sequence 5. This **controlled database ownership replacement** advances the database epoch while the broker assignment remains unchanged. An unknown claim outcome fails without assuming rollback. The correction is then appended at source offset 6. The still-assigned obsolete worker must make exactly one attempt with epoch e, report `outcome=fenced`, and exit 1 with the database fence error. It must not retry under a new token or acknowledge processing.

`zombie-before.json` and `zombie-after-fence.json` must match across processing decisions, measurements, quarantine, output versions/envelopes, outbox intent/receipts, view rows, and progress. Separate store snapshots require exactly the installed epoch change. Output/view remain at [5,0], processing at [6,0], and source at [7,0]. `verify-fenced.log` must retain **INCOMPLETE**, with source scanning complete but workers not at H; it must not certify an output boundary or any key. `dashboard-fenced.html` is separate live inspection and remains NOT VERIFIED.

Two normal replacement workers then acquire higher epochs, seek the durable frontier, and process the correction. The normal relay resumes; the `after` stage must match the original corrected fixture and reach strict PASS at H=[7,0], O=[10,0]. Tests require the fixed six output guards: three balance versions 2, withdrawn Day 1 alert version 2, Day 2 alert version 1, and READY version 1. No source offset, output version, or deletion guard is reset. `run.json.zombie_worker` retains worker PID/member identity, old/installed/recovered tokens, frontiers, attempt count, exit outcome, and preservation checks. All original verifier stages remain visible.

Cancellation at `zombie-ready` must not install the replacement epoch or load the correction. Cancellation at `zombie-fenced` must not launch recovery workers or relay. Both paths join all owned children and retain overall ERROR with the original last verifier status. This selected SC19 observation exercises obsolete processing **after** replacement epoch installation; the PostgreSQL suite separately checks both legal transaction/claim lock orders. It does not interrupt an already-running transaction, exercise rebalance-under-load, simulate host/database/broker loss, or establish arbitrary failover safety. Finite duplicate deliveries establish ownership without changing business values; they are not a load benchmark.

## Ambiguous-commit extension

Run `make drill DRILL=ambiguous-commit` (or `amendsctl drill -name ambiguous-commit`). The command runs **two fresh owned pipelines**, with separate manifests/artifacts and strict final verification. It requires one loopback TCP PostgreSQL endpoint with `sslmode=disable`, no TLS or host fallbacks; unsupported configurations fail before setup. This is a narrowly scoped local socket-fault drill, not a general database proxy or a reason to disable TLS elsewhere.

Each pipeline first verifies the original baseline at H=[3,0], O=[5,0], stops/joins both source workers, and keeps its already stopped/joined relay off. A controlled `worker.Lifecycle` in the controller claims partition zero and seeks frontier 3 through the production PostgreSQL adapter. The public loader appends the original correction; a real retained broker scan supplies offset 3. Only this lifecycle's database connection can receive the fault. The ordinary source/relay/view binaries have no fault flag, and no extra listener or service is started. During this selected decision, the controller drives assignment and runs the worker lifecycle in its own process; Kafka group membership remains unchanged.

The socket wrapper uses the pinned pgx v5.11.0 argument-free simple-query COMMIT boundary. It arms only inside the selected `Process` call, after claim/seek, and consumes the fault once. In the **request-loss** run, it closes the connection without transmitting COMMIT: PostgreSQL aborts the decision. In the **reply-loss** run, it transmits COMMIT, consumes complete server frames through `CommandComplete(COMMIT)` and idle `ReadyForQuery`, then closes without delivering those bytes to pgx. Split reads cannot masquerade as successful confirmation; malformed, truncated, error, or rollback responses fail the drill. Neither socket action fabricates a storage success or returns a synthetic unknown after a successful adapter call: the actual adapter must return `ErrCommitUnknown`.

The unmodified production lifecycle logs `outcome=unknown` and invokes its normal durable recovery read over a newly opened connection. The controller temporarily holds that read's result before returning it to the lifecycle, **without holding a SQL transaction**. Backend IDs and connection counts establish reconnection; the faulted backend must have ended. An independent controller read checks the same epoch/frontier, and exact snapshots establish the actual decision outcome:

| Loss | Observed source frontier / emission sequence | Expected preserved attempt sequence after recovery |
| --- | --- | --- |
| Request | 3 / 5; baseline state, sample, intent, and view unchanged | `unknown`, then one `acknowledged` retry |
| Successful reply | 4 / 10; one committed correction and five pending envelopes, old view unchanged | `unknown` only; no second processing call |

Both retain `verify-commit-unknown.log` with **INCOMPLETE** at source H=[4,0], no certified O, and no key comparison. Request loss leaves workers short of H; reply loss leaves publication undrained. Output/view still end at [5,0]. The recovery-read gate deliberately pauses the test at an inspectable point; real outages can occur at other points and durations. `dashboard-commit-unknown.html` remains separate NOT VERIFIED inspection.

After the read result is released, production recovery must complete successfully: it retries only the actually aborted correction, preserving the original unknown log in both cases. The controller joins that lifecycle work before resuming the normal relay. Strict final verification must reach H=[4,0], O=[10,0], the original corrected 70/50/100 balances and Day 2 crossing. Fixed guards require balance versions 2, withdrawn Day 1 alert version 2, Day 2 alert version 1, and READY version 1. The committed correction's SQL snapshot must be identical before and after its recovery read; no new version, compensation, or lowered frontier is allowed.

`run.json.ambiguous_commit` retains the loss mode, original storage error, server confirmation evidence, backend IDs, connection count, recovery token/frontier, exact attempt outcomes, and joined/recovered flags. `commit-before.json`, `commit-observed.json`, and `commit-recovered.json` retain exact durable SQL snapshots (ownership epochs are checked separately). `worker-ambiguous.log` is the production lifecycle's structured attempt log. Usual stage verifier, dashboard, inspection, and fixture artifacts remain available in each run directory. The CLI prints a separate retained-state re-verification command for each pipeline.

Cancellation at `commit-ready` must not append the correction or fire the fault. Cancellation at `commit-unknown` must not release recovery or resume the relay. Both paths join lifecycle work and owned children and retain overall ERROR with the original last verifier status. A canceled reply-loss run still has its committed correction pending; cancellation does **not** imply rollback. These selected SC18 observations do not exercise process/host/server death, power-loss durability, TLS intermediaries, arbitrary network segmentation of requests, broker acknowledgement loss, or concurrent ownership replacement. Separate PostgreSQL and worker suites cover other declared boundaries.

## Rebalance-under-load extension

Run `make drill DRILL=rebalance-under-load` (or `amendsctl drill -name rebalance-under-load`). This drill uses **two populated source partitions**, each with a declared synthetic key and independently stated business expectations. It creates fresh owned source/output topics and a namespace; the second key is configured before namespace creation. Unlike the original showcase's two worker processes, this controller hosts two real production `worker.Run` actors with normal broker clients, eager group callbacks, PostgreSQL claims, external seeks, and processing. Relay, view, loader, and verifier remain normal child processes. No processing/oracle/schema change or ordinary-worker failpoint is added.

The selected schedule is:

1. Start one source actor. Actual broker group metadata must show one stable member assigned both partitions. Load the six-record baseline and require strict PASS at H=[3,3], O=[5,5]. Both keys close at 50/30/80 with the Day 1 crossing. Stop/join the relay and capture durable state and epochs.
2. Arm pre-storage gates at the existing worker state interface, then append 32 repetitions of the two-key correction. Join the finite producer; H is [35,35]. Hold the first polled correction before `Store.Process`, retaining the actual baseline token/frontier. **No SQL transaction or row lock is held by this gate.**
3. Start the second actor. The broker must report PreparingRebalance while the old polled decision is held. Durable SQL state and ownership epochs must remain identical to baseline. Public verification must report INCOMPLETE after scanning all 70 source records: workers have not reached H, no complete O is captured, and no key is certified.
4. Release that polled decision. Normal processing and rebalance callbacks install replacement claims and seek persisted frontiers. Hold the first post-baseline decision under a higher epoch on each partition, again before storage. Actual broker metadata must show two stable members with one partition each. Both actors must have polled queued input at their own durable frontiers, still below 35; otherwise the drill cannot claim reassignment under backlog. The old owner's valid committed prefix can vary by schedule. Verification remains INCOMPLETE here.
5. Release both successor decisions, resume the relay, drain both partitions, and stop/join the relay before strict verification. Final H=[35,35], O=[10,10]; both keys close at 70/50/100 with the Day 2 crossing. Cancel both source actors before joining either; join all remaining children. Check exactly 70 acknowledged source decisions, 20 committed envelopes, and fixed output/view guards per key: balance versions 2, Day 1 alert deletion version 2, Day 2 alert version 1, and READY version 1.

`run.json.rebalance` retains the selected schedule's broker states/assignments, baseline epochs/frontiers, held decision, successor tokens/frontiers, acknowledged claims/decisions, and joined/completed flags. `rebalance-before.json` and `rebalance-preparing.json` must be byte-identical SQL snapshots; the separate partition snapshots check epochs omitted by those raw files. `rebalance-burst.jsonl` and its SHA-256 retain the exact expanded input. `rebalance-worker-1.log` and `rebalance-worker-2.log` contain production attempt logs; these actors do not have separate PIDs. `verify-preparing.log`, `verify-reassigned.log`, and their NOT VERIFIED inspection dashboards preserve both negative stages. The before/after view files contain both hand-checked key projections. The runner prints a retained-state re-verification command after successful cleanup.

Cancellation at `rebalance-ready` must not load the burst or start the second actor. Cancellation at `rebalance-preparing` must not release the old polled decision. Cancellation at `rebalance-reassigned` must not release the successor decisions or start the recovery relay; any earlier valid prefix remains committed. All paths cancel/join every actor and child and retain overall ERROR, even if the last completed verifier stage was PASS. Tests independently check retained broker bytes/coordinates, SQL state, source references, per-partition measurements, and these cancellation boundaries.

“Under load” means a **finite retained backlog on both partitions**, not a continuously running producer, measured throughput, or stress test. The producer has already stopped before the group changes. The selected one-to-two eager-group schedule does not exercise departures, rebalance storms, cooperative assignment, process/host/database/broker death, or arbitrary failover. Existing worker operation deadlines remain in effect; a missed gate or timeout fails rather than being counted as successful evidence. The pre-storage gates demonstrate polling/rebalance ordering. Separate real PostgreSQL tests establish database lock exclusion under both transaction/claim lock orders.

## Materializer-crash extension

`make drill DRILL=materializer-crash` (or `amendsctl drill -name materializer-crash`) extends the disappearing-day story with two actual SIGKILLs. It owns a fresh namespace and normal view processes; no application failpoint, database trigger, new service, or oracle change is introduced. Artifacts default to `artifacts/operations/run-*/`, and the ordinary showcase integration suite exercises the same path.

Before loading cancellation, the controller locks only the existing Day 3 row in `amends_view_rows`, leaving the view-progress row available to the materializer. The normal materializer obtains its progress lock, computes the withdrawal, and waits on its ordinary row upsert. `pg_stat_activity`, `pg_blocking_pids`, the transaction ID, and the owned child's unique application name locate that wait. The controller kills and joins that exact child, requires the same server transaction to remain blocked after SIGKILL, and keeps the barrier until the transaction has ended. An inconclusive kill near the lock deadline fails rather than claiming a precommit crash.

`view-precommit-before.json` and `view-precommit-after.json` must be byte-identical SQL snapshots of every view row and progress/diagnostic row. The Day 3 balance is still version 2, present, with view frontier [10,0]; the earlier Day 1 alert deletion guard also survives. The interruption occurs while the withdrawal upsert is waiting for its row lock, before the withdrawal is written or committed. Worker state and outbox advance independently. Once the cancellation producer has joined and the relay has drained/stopped/joined, the public verifier must exit 1 with INCOMPLETE at H=[5,0], O=[11,0], publication complete but view incomplete, and no certified key comparison. Its five-second bounded wait is not a processing-latency target. `verify-materializer-crashed.log` retains the original report; `dashboard-materializer-crashed.html` remains a separate NOT VERIFIED inspection.

After the old view transaction ends and its service guard is released, a fresh materializer seeks the persisted frontier and applies the withdrawal. The `withdrawn` stage requires strict PASS and the hand-derived cancellation fixture. The controller then kills/joins that replacement **after the independently verified commit**. `view-postcommit-before.json` and `view-postcommit-after.json` must match, retaining Day 3 deletion version 3 and frontier 11. While the view is down, the finite guarded publisher redelivers the exact committed version-2 envelope; another fresh materializer must consume it without resurrecting Day 3 or changing processing/output state. The `redelivered` stage requires strict PASS at H=[5,0], O=[12,0].

`run.json.materializer_crash` retains both signals, owned process PIDs, SQL barrier/backend/transaction identity, rollback observations, committed tombstone/frontier, and replacement PIDs. The normal redelivery/version artifacts also remain available. Cancellation after the incomplete observation retains overall ERROR with final INCOMPLETE; cancellation after the second kill retains overall ERROR with the last verified PASS, never an overall successful recovery. All owned children are joined on these paths.

The second kill occurs after confirmed durable completion, **not** precisely between COMMIT and its response or a broker acknowledgement. The view uses database positions rather than broker group commits. Actual lost COMMIT request/reply tests remain in the PostgreSQL suite. This drill does not exercise database-server death, power loss, or arbitrary concurrent failures, and never kills an existing unowned process or a relay.

## Worker-crash barrier

[`crashCorrection`](https://github.com/sorrell-consulting/amends/blob/main/internal/showcase/crash.go) takes a consistent durable snapshot after baseline verification. A separate PostgreSQL transaction locks the existing Day 2 balance row in `amends_output_state` with `FOR UPDATE`. It does not lock the processing partition row.

The amendment travels through the real broker and normal worker. In `ProcessingTx.Apply`, the worker locks its partition, resolves authority, computes the correction, and updates `amends_key_state`. The diff orders business output IDs before key status; the alert changes and Day 1 balance/output intent are written before the Day 2 balance upsert reaches the locked row. All remain uncommitted.

The controller polls `pg_stat_activity` and `pg_blocking_pids()` for the worker blocked by its specific barrier connection. A unique PostgreSQL `application_name` maps that backend to an owned OS child. The query must be the normal `INSERT INTO amends_output_state` upsert. The controller immediately sends SIGKILL to that child and joins it, checking the actual terminating signal. It then requires the same server transaction to still be blocked after the child has died; a timing observation too close to the SQL lock deadline is inconclusive and fails the drill.

The barrier stays held until the killed worker's transaction has ended. PostgreSQL may notice the closed client only when its three-second lock timeout fires. A second consistent snapshot must exactly match the baseline across key/revision state, output versions/envelopes, outbox rows and receipts, quarantine, input progress/emission sequence, view rows, and view progress. Ownership epochs are the sole omission: another worker may claim a partition while the row barrier prevents the amendment from committing. The artifacts preserve the database byte values as hexadecimal `bytea` strings.

The broker output end vector must also still equal the verified baseline O before the barrier is released. The controller then releases the barrier and starts a replacement worker. Ordinary group assignment and PostgreSQL claims recover the source frontier; no offset is edited. The group must again have two assigned members. The correction then drains and verifies, and partition zero must have a higher epoch than before the crash. Fixed test expectations also require exactly one committed correction transition: balance versions 2, withdrawn Day 1 alert version 2, Day 2 alert version 1, and unchanged READY version 1.

This exercises an OS worker death before COMMIT. The separate PostgreSQL suite covers both outcomes behind lost commit replies; the delivery suite covers selected publication/view recovery cases. It does not exercise database-server death, broker death, power loss, or arbitrary relay failover.

## Artifacts and failures

Every run prints its fresh `artifacts/showcase/run-*/` directory and namespace. Files include:

| File | Meaning |
| --- | --- |
| `manifest.json` | Immutable source configuration and output binding; no connection credentials |
| `run.json` | Overall status/reason, durations, platform/toolchain, binary and fixture hashes, child PIDs/join results, crash observations, and all original stage verification reports |
| `verify-before.log`, `verify-after.log` | Public verifier JSON reports, including H/O, topic incarnations, checks, differences, and verifier binary hash |
| `view-before.json`, `view-after.json` | Readable business values checked against hand-derived fixtures |
| `inspect-*.log` | Separate live progress/diagnostic observations |
| `dashboard-before.html`, `dashboard-after.html` | Static NOT VERIFIED inspections, including the last committed refold/output counts and qualified client timings |
| `rollback-before.json`, `rollback-after.json` | Exact durable snapshots around the aborted correction; worker-crash mode only |
| `view-precommit-*.json`, `view-postcommit-*.json` | Exact view-row/progress/diagnostic snapshots around the two materializer SIGKILLs; materializer-crash only |
| `input-burst.jsonl`, `input-state-*.json`, `input-checks-*.json` | Finite repeated source bytes, SQL snapshots, and no-op authority/output/view assertions; input drills only |
| `history-request.json`, `history.json`, `history-state-*.json` | Owned prefix-removal request, incomplete-history/refusal evidence, original state/epoch preservation, and absent rebuild destination; missing-history only |
| `zombie-before.json`, `zombie-after-fence.json` | Exact durable SQL snapshots around one real stale-worker rejection; token/frontier/exit evidence is in `run.json.zombie_worker` |
| `commit-before.json`, `commit-observed.json`, `commit-recovered.json` | Durable state around actual COMMIT request/reply loss and production lifecycle recovery; fault/connection/attempt evidence is in `run.json.ambiguous_commit` |
| `rebalance-before.json`, `rebalance-preparing.json`, `rebalance-burst.jsonl` | Unchanged durable state during held rebalance and exact finite input; actual assignments, tokens/frontiers, decisions, and joins are in `run.json.rebalance` |
| Worker, relay, view, loader, and setup logs | Process diagnostics; owned worker/relay/view processes use JSON logs. Drills read stable worker event codes and fields; source-worker attempt logs retain storage-call duration and acknowledged/unknown/fenced/error observations; a crash can leave no completed attempt log |

Inputs and expected fixture files are also copied into the run directory. Artifacts are ignored by Git. Record release context with `make evidence-showcase`: it additionally captures the source revision/dirty state (or labels a source archive), Go modules, Compose images, JSON test results, and test transcripts. The controller never reports success after an unknown setup/publication result; inspect failed-run artifacts before any cleanup.

Each successful run prints a shell-quoted verification command for its exact manifest. With the same database environment and broker endpoints, it can be rerun after application children have stopped; diagnostic qualifications remain, and `history-missing` must still return INCOMPLETE rather than PASS. Never pick an unrelated namespace implicitly. Test runs remove only their own synthetic namespaces/topics after retaining evidence; their printed commands cannot reverify deleted test data.

## Evidence and limits

```sh
make test-showcase
make evidence-showcase
```

These targets require both services and build the real child executables. The Go test runner uses the race detector; child executables use the normal build. Tests cover the no-fault path, the observed SIGKILL and rollback/recovery path, fixed versions/tombstones, all children joined, and cancellation after a verified baseline producing an overall ERROR with retained evidence. The separate process lifecycle test refuses an unclean exit as a successful handoff.

CI runs `evidence-showcase` on a separate runner alongside the pipeline suites and preserves artifacts on failure. [Testing](testing.md) explains evidence interpretation and optional historical reproduction. Inspection artifacts retain last committed work, cumulative counts, and the latest 32 samples per partition, while long-term charts and arbitrary fault schedules remain outside scope. The opt-in [mutation campaign](mutation-testing.md) checks selected semantic assertions separately.
