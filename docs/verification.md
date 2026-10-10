# Bounded verification and live inspection

To check a correction, both calculations must describe the same history. Stop new input, let processing and publication finish, then stop the relay and wait for it to exit before comparing stored results. This stable operating state is **quiescence**. **Bounded verification** captures fixed source and output positions and checks agreement within a time budget. Seeing zero lag or an empty outbox alone does not establish correctness.

The separately implemented full-history calculation, or **independent oracle**, chooses the applicable source revisions and computes balances and alerts without reusing the incremental repair algorithm. It shares decoding and data types, so common defects remain possible. The result being checked is the consumer's stored current state, the **materialized view**. Each worker's **frontier** is its durable next source offset; the view has a distinct next output offset. These exclusive coordinates can contain gaps and are not counts of business records.

`amendsctl verify --quiesce` checks a complete comparison boundary; `amendsctl inspect` reports live observations. Verification independently scans retained source records and compares their reconstructed business values and trust status with the durable downstream view. Inspection reports live progress and diagnostics. The [showcase runner](showcase.md) automates drain and the clean stopped-relay handoff for a fresh owned pipeline and exercises a real worker crash. Manually operated pipelines retain the handoff requirements below.

## Operating procedure

Start the pipeline using the [delivery guide](output-delivery.md). Use the same saved pipeline manifest and export the local database URL in each terminal:

```sh
export AMENDS_DATABASE_URL='postgres://amends:amends-local-only@127.0.0.1:15432/amends?sslmode=disable'
./bin/amendsctl source-load -manifest artifacts/pipeline-demo-1.json -file fixtures/before.jsonl
./bin/amendsctl source-load -manifest artifacts/pipeline-demo-1.json -file fixtures/correction.jsonl
./bin/amendsctl source-load -manifest artifacts/pipeline-demo-1.json -file fixtures/cancel-last-day.jsonl
./bin/amendsctl inspect -manifest artifacts/pipeline-demo-1.json
```

Finish source loading. Check that every partition has zero input offset distance and zero pending outbox rows. Stop the relay with Ctrl-C or SIGTERM and wait for its process to exit. Keep source workers and the view running if they still need to drain. Then run:

```sh
./bin/amendsctl verify --quiesce -manifest artifacts/pipeline-demo-1.json -timeout 30s \
  > artifacts/verification-demo-1.json
```

The command exits zero for strict PASS. A clean fixture has source boundary `[5,0]` and normally output boundary `[11,0]`; duplicate publication can increase output offsets without changing business results. The verifier observes actual ends instead of assuming those fixture counts. After verification exits, restart the relay with its original manifest before loading more source input.

`--quiesce` declares the controlled local operating procedure: owned synthetic source loaders only, a stopped and joined relay, and no outstanding orphan publication or concurrent destructive administration. The command awaits an already active finite `source-load` and holds the source-incarnation producer guard and namespace relay guard throughout the comparison. A new source load through any namespace reading the same source, or a relay start in this namespace, is refused while these guards are held. **`source-load` requires `AMENDS_DATABASE_URL`** and holds its producer guard until its broker client closes. Stop older executables before upgrading from namespace-scoped producer guards; see [replay and rebuild](replay-rebuild.md).

The verifier does not stop arbitrary OS processes, drain pending outbox rows itself, or launch replacement runners. An active relay produces INCOMPLETE. Pending outbox rows after the handoff also produce INCOMPLETE: resume the relay, let it drain, stop/join it, and retry. This is the manual handoff for independently operated pipelines; `amendsctl demo` automates it only for its owned finite pipeline. It keeps in-flight publication separate from a racing empty-outbox observation.

Session guards cannot revoke a broker request after a lost database connection. After an uncertain failure, stop and join the old writer and resolve outstanding publication before invoking verification. These guards are not Kafka ACLs, broker fencing, or proof that an uncooperative writer has stopped. The command repeatedly checks broker boundaries and refuses observed changes, but it cannot certify a hostile or independently changing environment. No destructive reset is part of verification.

## How the code establishes the boundary

[`internal/verify`](https://github.com/sorrell-consulting/amends/tree/main/internal/verify) coordinates concrete broker and PostgreSQL adapters. The pure [oracle](https://github.com/sorrell-consulting/amends/blob/main/internal/oracle/oracle.go) shares only schema and decoding with the incremental path.

1. Validate database configuration and output binding. Retain observed conflict and view status diagnostics even if a later boundary check fails. Acquire the producer guard after any active loader finishes, then acquire the stopped relay's guard.
2. Validate the source UUID, partition count, non-compacting unlimited retention, and required starts of zero. Capture the exclusive read-committed source end vector H.
3. Use [`broker.Scan`](https://github.com/sorrell-consulting/amends/blob/main/internal/broker/scan.go) to read the source prefix independently from offset zero to H. It directly assigns partitions, uses read-committed visibility and no automatic reset, checks supplied topic UUIDs and monotonic coordinates, and preserves exact key/value bytes. No processing table supplies oracle input.
4. Independently compute authority, balances, alerts, conflicts, and rejections. Wait for every durable worker frontier to equal H. Require no pending outbox rows while the relay guard remains held, then capture validated output end vector O. Publication receipts must lie below O.
5. Wait for every durable view frontier to equal O. Protocol errors return ERROR. Read the view from a repeatable-read transaction, including retained tombstones, and observe processing diagnostics for exact evidence comparison.
6. Revalidate source/output identities, retention, and unchanged H/O, and check that both writer sessions remain alive. Only then compare business projections and trust state and assign a complete result.

The source scan happens before waiting for downstream completion so that a stopped relay can still yield independently derived conflict diagnostics. If source scanning cannot run, `authority_basis` identifies diagnostics as `worker-observation`. At a successful scan it becomes `independent-source-scan`. Neither an incomplete boundary nor retained last-known business values certifies a blocked key.

In the code, `verify.Run` owns the deadline, writer guards, source scan, and final broker rechecks. The worker/view waits and durable snapshot checks are named stages in [boundary.go](https://github.com/sorrell-consulting/amends/blob/main/internal/verify/boundary.go). A wait returns its last observed pending boundary with any error, so the caller can distinguish budget expiry from an unrelated infrastructure failure. These helpers preserve observations and validate progress; the independent oracle and report comparison remain separate.

Internal read-committed offset gaps are supported when a later visible record establishes the scanned position. A prefix ending only in aborted/control offsets cannot complete this adapter's scan or durable worker frontier; verification returns INCOMPLETE. It never substitutes a high watermark for evidence of consumed records. The owned `source-load` producer is nontransactional. No generic trailing-control advancement or arbitrary online prefix reconstruction is implemented.

`-timeout` bounds the operation, with cancellable waits between observations. Errors and failed preconditions remain visible; the command neither mutates processing/view state nor repairs drift. It only holds transient session guards. It does not install schema or initialize a namespace. Broker and database observations remain separate operations; their comparability depends on the guarded, stopped-writer procedure.

After observing an active producer or lagging worker/view frontier, expiration of the overall verification budget returns INCOMPLETE with that pending boundary. This applies whether the deadline interrupts the retry pause or a subsequent observation. An observation timeout while the overall budget is still live, a lost writer session, or another infrastructure failure remains ERROR. An initial failed observation does not assert that a specific boundary was observed as pending.

Database socket deadlines can expire before the context's cancellation callback runs, leaving an I/O timeout instead of a wrapped `context.DeadlineExceeded`. The verifier checks the absolute overall deadline as well as context state for these timeout errors. Cancellation and non-timeout connection failures remain ERROR, even near that deadline; the original observation error is retained in the report.

If a refresh fails partway through its partition reads, the report retains the last complete progress and view observations rather than replacing them with partial results. Those retained observations are not a new successful read or a certified boundary. In particular, an already observed BLOCKED view status must not disappear merely because a later read failed before reaching that partition. Failed reads still follow the ERROR/INCOMPLETE rules above.

## Results and exact diagnostics

| Status | Meaning | Exit |
| --- | --- | --- |
| PASS | Complete unqualified business, trust, and evidence agreement | 0 |
| PASS_WITH_EXCLUSIONS | Complete agreement with rejected records or superseded conflicts | 1, unless an exact named fixture matches |
| BLOCKED | Complete boundary with unresolved highest-revision authority | 1 |
| DRIFT | Complete boundary with a business, trust, or required-evidence difference | 1 |
| INCOMPLETE | Required boundary, history, or stopped-writer precondition unavailable | 1 |
| ERROR | Infrastructure, protocol, arithmetic, schema, or verifier failure | 1 |

Invalid CLI usage exits 2. File/configuration errors before verification starts go to stderr and exit nonzero. Once verification starts it produces a structured status report. JSON write failure also exits nonzero. Successful inspection is not a verifier result.

The JSON report includes namespace/configuration identity, ledger origin, schema version, implementation label, Go version, the CLI executable's SHA-256, topic identities, retained starts, H/O, completion checks, observed progress, record counts, conflicts, exact rejection evidence, per-key comparison status, differences, and elapsed milliseconds. Builds disable VCS stamping; the binary hash identifies the executable without inventing a commit ID for local edits.

A difference identifies its field, key and first affected day where relevant, expected/observed values, and source coordinates for that key. A blocked key's numeric rows are not certified. Unaffected keys can have their own comparison result, but overall BLOCKED remains even if another key differs. INCOMPLETE takes precedence over known source conflicts, which remain visible with BLOCKED authority status. Durable protocol failure is ERROR.

`--expected-diagnostics FILE` accepts a named JSON fixture with these fields:

- `name`, `namespace`, `config_digest`, and the exact `source_end` vector.
- `diagnostics.rejections`: full `ledger.Rejection` values, including reason, coordinates, decoded key, and exact raw key/value bytes (base64 in JSON).
- `diagnostics.conflicts`: each conflict identity/revision plus `records`, the complete accepted source records that asserted that conflicting revision, including exact bytes and coordinates.

Prepare these expectations from the specified synthetic input and its published coordinates. Do not generate expected evidence by copying the verifier's observed answer. A different reason, raw key/value, record coordinate, extra rejection/conflict, or source boundary fails the match. The report always retains PASS_WITH_EXCLUSIONS; the manifest only permits its exit code to be zero. Supplying a mismatching fixture causes a nonzero exit even when the underlying unqualified comparison is PASS. It cannot waive DRIFT, BLOCKED, INCOMPLETE, or ERROR. There is no blanket quarantine-suppression option. Evidence order is normalized; duplicate extra entries still fail the exact match.

## Inspection and point-in-time metrics

`inspect` reads without taking publication guards. [`Store.Progress`](https://github.com/sorrell-consulting/amends/blob/main/internal/store/postgres/progress.go) reads epochs, worker/view frontiers, emission sequence, pending outbox count, oldest pending timestamp, maximum acknowledged output offset, quarantine count, and protocol error text in one repeatable-read transaction. This is actual SQL observation, not an in-process queue metric.

The CLI adds independently observed broker ends, source/view offset distances, oldest pending age, worker and view projections, historical conflicts, and full quarantine records. `conflict_details` adds canonical candidates, retained source references, highest observed revision, and current whole-key trust status. Topic/incarnation identifiers and completion times for the separate worker/view snapshot reads accompany the SQL progress observation time. Offset distance is not a record count because broker offsets can have gaps. Unknown ends produce null distance fields and explicit `observation_errors`; they never become fabricated zeros. Its progress, key snapshots, and broker metadata are separately timed observations, identified by a non-verification `scope`. A zero inspection exit means the report was read and written, not that the pipeline is healthy.

`inspect -manifest FILE -format html` renders those observations as a self-contained static dashboard on stdout. Save it to an HTML file and open it locally. It needs no HTTP server, browser scripts, network-loaded assets, or new runtime dependencies. Go's `html/template` escapes displayed input; raw quarantine bytes remain base64 in JSON details. The page prominently labels all observations NOT VERIFIED and preserves unknown ends, protocol errors, and uncertified BLOCKED values. It does not join a saved PASS from a different boundary to current observations. A saved page becomes stale; rerun inspection to capture new observations.

Inspection includes durable cumulative counters and a bounded recent history, described below. There is no HTTP metrics endpoint, automatic refresh, long-term chart store, or persisted last-verification status; save verification reports as ordinary artifacts.

### Committed processing measurements

Each partition in `inspect` JSON includes nullable `last_processing`, also rendered in the dashboard's **Latest processing work** panel. Migration 003 adds one optional BYTEA measurement to the partition row. Existing history and untouched partitions remain unmeasured (`null`), not zero-cost. New source decisions replace this sample atomically with state, quarantine, output intent, and source progress under the existing fence. Rollback or a rejected epoch cannot overwrite it. The sample's source coordinates and decision epoch identify what was measured; ownership may subsequently change. Administrative replay preserves the sample; a fresh rebuild measures its own new decisions.

| Field | Meaning |
| --- | --- |
| `work.duplicate`, `work.stale` | Canonical payload already known; revision lower than previously observed highest. Both can be true. |
| `work.refold`, `work.full_refold`, `work.from_day` | Whether computation ran, full-key conflict repair versus suffix repair, and first affected UTC day. A full empty-key refold may have no day. |
| `work.rewind_days` | Nonnegative UTC calendar-day distance from the previous last balance day back to the first affected day; zero for a forward append or same-day repair. This is not the number of stored rows. |
| `work.refolded_records` | Active authoritative transactions included in this fold's suffix (whole key on resolution), including zero-quantity activity. Cancelled and earlier-prefix transactions are excluded. |
| `business_outputs`, `status_outputs` | Newly committed semantic envelopes, including withdrawals; READY/BLOCKED control envelopes are counted separately. These are not broker publication counts. |
| `quarantine_reason` | Rejection reason when applicable; rejected input performs no refold and emits no outputs. |
| `lock_acquire_ns` | Client elapsed time around the partition row-lock acquisition and configuration check, including SQL/network/scheduling overhead. It is not an isolated server lock-wait measurement. |
| `elapsed_before_commit_ns` | Client elapsed time from entering `Begin` through computation and business/quarantine writes. Sampled before the final progress/measurement UPDATE in `Apply`; excludes that write, any caller pause before `Commit`, and the commit response. Includes connection acquisition, setup, and lock acquisition. |
| `sampled_at` | UTC client sampling time before commit, not a database commit timestamp or verification boundary. |

The normal correction rewinds two calendar days, refolds three active transactions, and emits five business envelopes (three balances, one alert withdrawal, one new alert), with no new status envelope. Cancelling the last activity on a day can refold zero remaining records while still emitting a withdrawal. A higher no-op revision can refold records yet emit nothing. The adapter still copies/scans retained key state and reads partition output state; suffix counts must not be presented as total CPU work or database rows visited.

Source-worker `processing attempt` logs include namespace, partition, offset, epoch, `storage_call_elapsed_ns`, and an observation label: `acknowledged`, `unknown`, `fenced`, or `error`. The duration spans the complete storage call, including commit response or failed cleanup, but not subsequent unknown-outcome observation or retry backoff. A lost commit reply stays `unknown` in that attempt's log even if later durable inspection recovers success. Logs are not transactional counters; an OS crash can leave no completed log. The persisted sample is available after a genuinely committed unknown outcome and absent after an aborted one. Neither timings nor sample presence changes recovery decisions or certifies the view.

Stop old application binaries before upgrading and rebuild/restart through the normal commands, which run checksum-checked migrations. Read-only inspection never runs migrations. No historical measurements are backfilled and no percentiles are inferred, and these local observations establish no latency or capacity guarantee.

### Committed aggregates and bounded history

Migration 004 adds nullable `processing_statistics` to the partition row. `inspect` JSON exposes it as `partitions[].processing_statistics`; the HTML **Committed aggregates & recent history** panel renders the same data. Stop old binaries before upgrading through normal checksum-checked startup, then rebuild/restart. Earlier processing is not backfilled. Untouched and upgraded partitions report `null` until their first new committed decision; this means unknown prior history, not zero work.

`first_source` and `started_at` identify the first collected decision and its client sampling time. `decisions` counts committed source decisions, including rejected input; gaps in source offsets do not increase it. `duplicates`, `stale`, `quarantined`, and `refolds` count their respective flags, which can overlap. `refolded_records`, `business_outputs`, and `status_outputs` sum the per-decision values defined above; `max_rewind_days` retains the maximum observed rewind. Timing fields sum qualified client acquisition and precommit durations, excluding the final progress write and COMMIT. These are neither server lock-wait totals nor latency guarantees. If a sum exceeds the signed 64-bit limit, it saturates and `counters_saturated` marks the aggregates as lower bounds; business processing continues.

`recent` retains at most 32 committed samples per partition in source-processing order, oldest first. Eviction reduces neither counters nor the maximum rewind. These observations survive restarts and ownership changes. They update in the same fenced SQL transaction as state, output intent, quarantine, and progress: aborted attempts and stale owners contribute nothing. A genuinely committed unknown outcome contributes once; recovery does not count it again. Administrative replay preserves statistics; a fresh rebuild starts its own collection. This is a bounded monitoring history, not a full source archive, rate estimate, percentile distribution, or verified boundary.

## Read-only diagnostic review

`quarantine list|show` and `conflicts list|show` require an explicit compatible manifest and `AMENDS_DATABASE_URL`, but no running broker. They read one repeatable-read worker snapshot, including its durable input frontiers/epochs, and never run migrations, create a namespace, claim ownership, or change evidence. `snapshot_read_at` is the client-side completion time, not a verified source boundary. Lists return a count, including zero; `show` returns one exact requested record/revision or exits 1 when absent. Bad usage exits 2. Read or JSON-output failure exits 1. Zero means inspection succeeded, not that the pipeline is correct.

Quarantine `show` requires `-partition P -offset O` and retains exact raw key/value bytes and reason. Conflict `show` requires `-item I -location L -txn-id T -revision R`; it never picks one candidate. `at_highest_revision` distinguishes unresolved highest-revision evidence from a superseded conflict. Whole-key status/reasons are separate because another transaction can still block the key. Canonical candidate representatives preserve one set of informational fields and all retained source references; they are not a raw accepted-record archive. The oracle still derives its exact verification diagnostics from broker history, not this inspection. There is no diagnostic export-to-expected-manifest shortcut and no release/resolve override. See [runbook](runbook.md#highest-revision-conflict-or-malformed-input) for source-based repair.

## Evidence and limits

With the pinned PostgreSQL and Redpanda services running:

```sh
make test-verify
make evidence-verify
```

The tests are part of the delivery integration package, so `make test-delivery` also runs them. Separate evidence under `artifacts/verification/` records the toolchain, modules, and JSON test events. CI includes the verifier cases in its delivery suite; hosted CI must still be observed separately.

Real tests cover all three fixture boundaries with two source workers; SC08 combined with SC25 (INCOMPLETE with BLOCKED diagnostics, then BLOCKED after drain, then qualified agreement after repair); exact poison/conflict expectations; incorrect durable quarantine evidence; stalled materialization; deleted view rows producing explained DRIFT; protocol ERROR; active writer guards, expiry inside a controlled retry of an actual PostgreSQL guard query, clean producer handoff, and refusal after actual writer-session termination; source recreation and missing history; internal read-committed gaps and incomplete trailing controls; and an advancing uncooperative source. Expected fixture values and rejection/conflict records are specified independently of the verifier result. Pure tests preserve blocked-result precedence, exact expected-diagnostics matching, and the distinction between pending-boundary budget expiry and infrastructure errors.

The tests own and clean only their synthetic namespaces/topics. They do not establish a real process crash during correction, automatic relay takeover, exhaustive failure coverage, performance, or production readiness. The separate [showcase suite](showcase.md#evidence-and-limits) exercises actual worker-process death and recovery.
