# Deterministic delivery schedules

The delivery scheduler runs the actual relay and materializer retry/recovery decisions against controlled memory I/O. It exercises SC16, SC20, and SC21. The separate `sim` command models serial worker, relay, and view transactions.

## Run and replay

No services are required. From the repository root:

```sh
make test-sim-delivery
make sim-delivery SEED=17
./bin/amendsctl sim-delivery -replay artifacts/delivery-seed-17.json
make evidence
```

The CLI prints a JSON report. Only strict PASS exits zero; INCOMPLETE, BLOCKED, PASS_WITH_EXCLUSIONS, and execution errors exit one. Invalid command usage exits two. Trace files describe exact raw history and choices, so replay does not regenerate either from the seed. Unknown JSON fields, trailing content, incompatible provenance, and invalid choices are rejected. Traces contain their producing Go version; replay records that provenance without requiring the same compiler to be installed.

`make evidence` includes the 32-run delivery corpus in its normal race-enabled tests, copies its manifest to `artifacts/evidence/delivery-corpus.json`, and saves `delivery-seed-17.json` and `delivery-seed-17-report.json`. CI uploads these with core evidence. The corpus test logs counts of fault calls actually reached, rather than counting choices attached to idle or parked attempts.

## Shared code and controlled boundaries

[`delivery.Stepper`](https://github.com/sorrell-consulting/amends/blob/main/internal/delivery/runtime.go) accepts a retry wait function. Its `RelayStep` and `ViewStep` methods contain the production orchestration. The service runners call top-level functions that supply a cancellable timer and a 20-second operation deadline. Each retry loop allows at most three calls, with 100 ms and 200 ms waits, and immediate retirement for fencing or protocol errors. Recovery reads have their own bounded loop inside the write loop.

[`sim/transport`](https://github.com/sorrell-consulting/amends/tree/main/sim/transport) implements the state/publication interfaces and a wait function that advances logical milliseconds. It does not implement a second copy of the retry decisions. Execution has three stages:

1. Process the complete supplied source history with the memory worker, checking its state against the independent source oracle after each record. Freeze every committed outbox envelope and its serialized bytes.
2. Execute the explicit delivery attempts through `Stepper`. Apply each selected memory effect separately from the observation returned to the caller. Check transport invariants after every I/O call, retry wait, and attempt.
3. If `drain` is true, record explicit restarts of retired roles and fairly finish pending relay and view work without further faults. Require progress on each recovery attempt and compare the complete final boundary with the independent oracle.

Source processing is complete before the delivery schedule starts. The harness can therefore check each publication against immutable committed intent, while the view may still lag or see older versions. It does not use incremental fold or diff decisions to construct oracle expectations.

| Code | Responsibility |
| --- | --- |
| [`trace.go`](https://github.com/sorrell-consulting/amends/blob/main/sim/transport/trace.go) | Versioned trace validation, deterministic generation, schedule reduction |
| [`run.go`](https://github.com/sorrell-consulting/amends/blob/main/sim/transport/run.go) | Attempt scheduling, logical retry time, retirement/restart, fair drain, reports |
| [`io.go`](https://github.com/sorrell-consulting/amends/blob/main/sim/transport/io.go) | Chosen effects and observations, immutable publications, atomic memory view commits, safety checks |
| [`run_test.go`](https://github.com/sorrell-consulting/amends/blob/main/sim/transport/run_test.go) | Hand-stated fault schedules and fixed fixture expectations |
| [`corpus_test.go`](https://github.com/sorrell-consulting/amends/blob/main/sim/transport/corpus_test.go) | Fixed seed budget, qualified outcomes, reached-fault counts, failure preservation |

## Trace model

The top-level trace includes `version: 1`, `implementation: "delivery-schedule-v1"`, `toolchain`, `seed`, `config_digest`, the complete immutable `config`, raw `source` records, `steps`, and `drain`. This is a separate format from the core memory scheduler trace.

Each step has an `actor` and `partition`. `relay` attempts the earliest pending envelope. `view` attempts the next published record after its retained frontier. `redeliver` appends an exact copy of an earlier published record selected by `offset`; an unavailable offset is idle. This can place an old version after newer output. `resume-relay` and `resume-view` restart the whole modeled service role, preserving memory durable state. Partition is still validated for resume steps but does not narrow their service-wide effect.

A step may select faults by operation and call number within that attempt. Call numbering resets for each attempt. Unselected calls succeed normally.

| Operation | Actor | Effect or observation |
| --- | --- | --- |
| `pending` | relay | Read earliest committed unacknowledged intent |
| `publish` | relay | Append unchanged envelope bytes to the modeled output partition |
| `receipt-write` | relay | Record the acknowledged publication offset and mark intent delivered |
| `receipt-read` | relay | Resolve an unknown receipt commit |
| `view-write` | view | Atomically apply the version/tombstone decision and advance progress |
| `view-read` | view | Resolve an unknown view commit from retained progress |

For example, this step accepts publication, loses its reply, and delays that unknown observation by 250 logical milliseconds:

```json
{
  "actor": "relay",
  "partition": 0,
  "faults": [
    {"operation": "publish", "call": 1, "outcome": "unknown-committed", "delay_ms": 250}
  ]
}
```

The shared retry code then waits another 100 logical milliseconds and republishes exactly the same envelope. A lost *receipt commit* reply follows a different path: reread the receipt, then retry only the receipt if it was absent. The named tests assert those different call sequences.

| Outcome | Memory effect | Caller observation |
| --- | --- | --- |
| `commit` | Applied | Success |
| `abort` | None | Temporary unavailability |
| `unknown-committed` | Applied | Unknown result |
| `unknown-aborted` | None | Unknown result |
| `crash-before` | None | Canceled attempt, role retires |
| `crash-after` | Applied | Canceled attempt, role retires |

Reads accept only `commit` and `abort`. Writes accept all six choices. Fault selectors are unique per operation/call; calls are numbered 1–9 to include nested recovery retries. `delay_ms` is 0–60,000 and advances logical observation time after the chosen effect. No wall-clock sleeping occurs.

An exhausted retry, unresolved recovery read, or modeled crash retires the role across partitions. Later attempts are visibly parked until an explicit resume or fair recovery restart. A fair drain requires restored availability and actual completion of pending work. With `drain: false`, incomplete progress remains INCOMPLETE, including known conflict diagnostics.

## Checks and reports

At every I/O boundary the harness checks that publication identity and bytes match committed intent, receipts correspond to accepted output, outbox acknowledgements agree with receipts, and view progress stays within published output. An independent maximum-version calculation over the consumed output prefix checks every stored view row, including deletion versions. The checks do not call the production retry or version-selection logic to decide what should have happened. At the final complete boundary, the independent source oracle checks business state and diagnostics.

Named schedules also check the hand-derived fixtures. They cover publication reply loss and delay, both unknown receipt outcomes, retirement after unreadable commit results, retry exhaustion, both sides of a view commit, explicit restart, and a version-1 Day 3 balance arriving after the retained version-3 withdrawal. They assert incomplete intermediate boundaries and the operation sequences used for recovery, so eventual convergence alone cannot hide an incorrect recovery decision.

The report labels its scope `production-delivery-orchestration; memory I/O and logical time`. `before_drain` exposes the scheduled boundary, and `boundary` describes the final modeled boundary with source/output ends and exact diagnostics. `attempts` includes original choices, observed result, count of used fault selectors, and whether the attempt belongs to fair recovery. `events` identifies the attempt index, operation/call, selected outcome, whether its effect was applied, caller observation, and logical completion time. Retry-wait events represent elapsed logical time, with no I/O effect. These are model observations, not a running product verifier report.

The [manifest](https://github.com/sorrell-consulting/amends/blob/main/sim/transport/testdata/corpus.json) declares seeds 1–16, valid and conflict/rejection histories, and 48 scheduled attempts before fair recovery: **32 runs**. Generation reuses the bounded source-history generator, while selecting downstream choices independently. The corpus keeps BLOCKED and qualified results, requiring strict PASS only for valid histories. Named tests supplement the random choices.

On a corpus failure, the test writes the original trace before reducing it. The reducer removes attempts and individual fault selectors only while the same failure string remains reproducible. It preserves source records and configuration. The reduced trace need not be the smallest counterexample. Both traces go under `artifacts/failures/` or `AMENDS_ARTIFACT_DIR`. Replay them with `sim-delivery -replay PATH`. The reducer's own test uses a transparent synthetic predicate, without adding broken production branches.

## Limits

This scheduler is serial within each attempt. Delay advances observation time; another actor does not run between an operation's effect and its return. All modeled effects are resolved before an attempt ends, including a modeled crash. It does not exercise real process death, orphaned broker requests, concurrent in-flight I/O, wall-clock deadline expiry, broker key decoding, or actual SQL transactions. The production deadline remains in its real wrapper.

The separate [worker scheduler](worker-simulation.md) covers selected assignment/seek callbacks, ownership installation, stale tokens, and worker unknown outcomes through shared production lifecycle code. The [named mutation campaign](mutation-testing.md) checks selected semantic guards. Database lock/fence claims require the [PostgreSQL suite](postgres-store.md); real broker and materializer recovery are checked in the [delivery suite](output-delivery.md). The [showcase](showcase.md) exercises selected actual process crashes.
