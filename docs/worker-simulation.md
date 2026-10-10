# Deterministic worker lifecycle schedules

The worker scheduler drives the production assignment, external seek, processing, and retirement logic through controlled memory I/O. It exercises schedules for SC18, SC19, and SC22, with SC08/SC25 status precedence and correction/withdrawal regressions. The [delivery scheduler](delivery-simulation.md) runs the production relay/materializer path.

## Run and replay

No PostgreSQL or broker service is needed for these commands:

```sh
make test-sim-worker
make sim-worker SEED=17
./bin/amendsctl sim-worker -replay artifacts/worker-seed-17.json
make evidence
```

The CLI prints a JSON report. Strict PASS exits zero; incomplete, blocked, qualified, or erroneous results exit one. Bad command usage exits two. Replay reads the saved source bytes, configuration, and explicit choices rather than regenerating a schedule from its seed. Unknown fields, trailing content, incompatible provenance, invalid choices, and reuse of worker lifetime IDs are rejected.

`make evidence` includes the worker schedules in its ordinary race-enabled tests. It copies the [16-run corpus manifest](https://github.com/sorrell-consulting/amends/blob/main/sim/ownership/testdata/corpus.json) to `artifacts/evidence/worker-corpus.json` and retains `worker-seed-17.json` and `worker-seed-17-report.json`. CI uploads these alongside the core and delivery simulation evidence.

## Shared production lifecycle

[`worker.Lifecycle`](https://github.com/sorrell-consulting/amends/blob/main/internal/worker/lifecycle.go) holds the assignment mutex, saved ownership tokens and local frontiers, independent cancellation context, source validation function, state adapter, and retry wait function. [`worker.Run`](https://github.com/sorrell-consulting/amends/blob/main/internal/worker/worker.go) adapts the actual franz-go callbacks and polled records to this component:

| Method | Production use and rule |
| --- | --- |
| `Assign` | Validate the source, claim synchronously, read back epoch/frontier, retain only a compatible token |
| `Seek` | Reobserve under the retained token; use the durable source coordinate without installing another epoch |
| `Process` | Submit one record with the saved epoch/frontier; resolve unknown commits before updating the local frontier |
| `Revoke` | Wait for guarded work and clear the complete eager assignment |
| `Stop` | Cancel independently of the mutex so blocked I/O can finish; a stopped lifetime cannot resume |

The mutex covers validation and the synchronous I/O call. No detached installer can outlive that guarded call. Failure retirement occurs before releasing the mutex. Operation contexts derive from the lifecycle context; production wrappers retain the 20-second budget, classic eager assignment, poll/rebalance gating, and client shutdown outside callbacks. The controlled runner uses the same methods and supplies logical waits and memory I/O.

The production budget starts before entering the guarded lifecycle call, so waiting for the mutex consumes that budget. Cancellation still interrupts I/O independently; it does not bypass the mutex or abandon a transaction while releasing the processing slot.

Retries have a three-call limit with 100 ms and 200 ms waits. Cancellation is checked again after a retry wakeup, before another I/O call. Unresolved recovery reads retire the lifetime rather than allowing another write.

Unknown claim and processing results require different decisions. A successful read of an epoch does not prove that this caller installed it. After an unknown claim, the lifecycle reobserves and then makes a fresh claim only while its context remains live. After an unknown processing commit, matching epoch and advanced frontier prove completion; matching epoch and the original frontier permit retry of the same record. An unexpected frontier or different epoch fences the caller. Named tests assert these call sequences, independently of eventual final agreement.

## Trace and lifetime model

[`sim/ownership`](https://github.com/sorrell-consulting/amends/tree/main/sim/ownership) uses a separate version-1 trace identified by `implementation: "worker-schedule-v1"`. It records the toolchain, seed, configuration digest, complete immutable configuration, raw ordered source records, `steps`, and `drain`. The producing toolchain is provenance, not a requirement that replay install a different compiler automatically.

Each step has `actor`, `worker`, `partition`, and optional `faults`. `worker` identifies one lifetime, not a reusable slot. A new lifetime gets a new ID. The modeled store begins at epoch zero; the core memory simulations claim all partitions at construction.

| Actor | Action |
| --- | --- |
| `start` | Create a fresh lifecycle with no assignment or fetch cursor |
| `assign` | Invoke guarded production assignment for the partition |
| `seek` | Invoke guarded external offset adjustment and set the modeled fetch cursor |
| `process` | Fetch the next visible source record from that cursor and invoke production processing |
| `revoke` | Invalidate local ownership and clear fetch cursors after guarded work |
| `stop` | Cancel and revoke the lifetime; later callbacks cannot revive it |

The fetch cursor advances when a record is delivered to processing, even when processing fails. It is not a durable receipt. A replacement must seek again; it cannot continue from another lifetime's fetch cursor. A visible record at offset 4 advances durable progress to 5, regardless of the number of earlier records. Processing before a seek is reported as `unseeked`; a cursor with no remaining visible record is `idle`.

Normal eager revoke followed by a new assignment may reuse a still-live lifecycle. A late seek after revocation has no saved owner and is fenced. Once stopped or retired on error, that lifecycle rejects subsequent assignment/seek work without further state I/O. This models callbacks reaching an invalidated lifetime; it does not invent a broker generation token or simulate arbitrary reordering of franz-go's serialized assignment callbacks.

## Fault boundaries

Faults select `validate`, `claim`, `observe`, or `process`, and a numbered `call` within one attempt. Call numbering resets for each attempt. Unselected calls succeed. Selectors for unused calls remain in the attempt report and do not count as exercised faults.

| Outcome | Modeled effect | Observation |
| --- | --- | --- |
| `commit` | Applied | Success |
| `abort` | None | Temporary unavailability |
| `unknown-committed` | Applied | Unknown commit result |
| `unknown-aborted` | None | Unknown commit result |

Only `claim` and `process` accept unknown results. `observe` can read epoch zero after a failed first claim, just as PostgreSQL can. Optional `delay_ms` advances logical observation time; retry waits add their own logical milliseconds. No real sleeping or deadline expiry is modeled.

Fault choices cannot override cancellation or a storage fence. A selected committed outcome may therefore produce no effect and a fenced/canceled observation. `effect_applied` and `observation` record what actually occurred, separately from the requested choice.

An optional `interleave` selects a boundary `before` or `after` the atomic modeled I/O effect. `stop: true` cancels the original lifecycle. `replacement: N` creates a fresh lifetime N, assigns the same partition, and seeks its durable frontier before returning to the original call. Both can be selected together. Replacement alone does not cancel the original member; this permits testing an old token still attempting work. Explicit stop supplies local invalidation when that is part of the schedule.

For example, after an initial `start`, `assign`, and `seek`, this step commits a decision with an unknown reply, then installs a competing epoch before the old worker can resolve the result:

```json
{
  "actor": "process",
  "worker": 0,
  "partition": 0,
  "faults": [{
    "operation": "process",
    "call": 1,
    "outcome": "unknown-committed",
    "interleave": {"at": "after", "replacement": 1}
  }]
}
```

The original worker must retire when it rereads the different epoch. It cannot adopt the new owner's token or repeat the already committed decision. Moving replacement to `before` makes the old token fail without committing. These are the two serial epoch/processing orders; the scheduler does not install a new epoch inside a transaction holding the real PostgreSQL partition lock.

Replacement IDs are reserved by the trace even if their selectors are never reached. A later callback to such an ID is reported `unstarted`. A replacement starts without injected faults, so nested intervention cannot recursively create an unbounded schedule. Stopping during a guarded call only cancels it; the model resolves that call before later top-level revoke/stop cleanup.

## Safety, completion, and evidence

Every modeled I/O return, explicit interleave, retry wait, and top-level attempt checks committed state with the independent full-history oracle at the *processed* source prefix. The model also checks outbox sequence/version consistency, immutable committed intent, and quarantine/progress agreement. A stale token that unexpectedly commits is an error. The oracle does not import worker authority, fold, or diff decisions.

With `drain: true`, the runner explicitly stops old lifetimes, starts a fresh worker, assigns/seeks every partition, and finishes source processing through the production lifecycle without further faults. Each recovery processing attempt must advance durable progress. The established no-fault **memory delivery model** then drains relay/view work before the complete source/output boundary is compared. Production relay/view fault testing belongs to `sim-delivery`; this command is not a combined simulation of all production services.

With `drain: false`, partial boundaries remain INCOMPLETE with known diagnostics. Actual state can already be complete despite a canceled caller: a single committed rejection has no downstream output to drain and can be PASS_WITH_EXCLUSIONS while its worker is retired. The report preserves both facts. It never converts that qualification into strict PASS.

The report is labeled `production-worker-lifecycle; memory I/O and logical time`. It contains `before_drain`, the final `boundary`, `attempts`, and `events`. Attempts retain choices, result, error, used-selector count, and a fair-recovery flag. Events identify worker lifetime, attempt index, operation/call, selected outcome, applied effect, observation, logical time, and the partition's installed epoch/frontier at that observation. Replacement I/O events appear inside the original attempt. Counts include only events marked `selected_fault`, excluding normal replacement calls and unused choices.

The fixed corpus runs seeds 1–8 in valid and conflict/rejection classes, with 64 explicit steps per trace before fair recovery: **16 runs**. Named tests additionally check both unknown outcomes, both epoch orders, canceled assignment and seek callbacks, unresolved reads, exact retry exhaustion, gaps across restart, incomplete-then-BLOCKED precedence, atomic quarantine, and the hand-derived correction/withdrawal fixtures. A bounded concurrent unit test checks cancellation while the real lifecycle mutex protects a claim. These checks supplement the real SQL/broker tests rather than replacing them.

Corpus failures save the original trace before attempting reduction. The reducer deletes steps and fault selectors only when the result remains structurally valid and reproduces the same failure string. It preserves source history, configuration, and lifetime dependencies. The reduced trace need not be the smallest counterexample. Both versions are saved under `artifacts/failures/` or `AMENDS_ARTIFACT_DIR`; the test prints exact `sim-worker -replay` commands. Its regression uses a transparent synthetic predicate rather than broken production code.

## Limits

This is a serial, bounded scheduler around observable I/O effects. It does not run Kafka group management, SQL row locks, an actual process crash, arbitrary concurrent completions, detached in-flight transactions, network packet loss, or wall-clock deadline expiry. Topic validation is a controlled source-end observation; recreation and retention rejection remain real-adapter tests.

`make evidence-worker` runs the actual client callback/rebalance/cancellation tests against PostgreSQL and Redpanda. `make evidence-postgres` exercises real fence lock orders and lost COMMIT replies. `make evidence-delivery` and `make evidence-showcase` check downstream integration and the selected actual worker-process crash. The pinned callback API and observed serialization are documented in the [source worker guide](source-worker.md).

The [named guard mutation campaign](mutation-testing.md) checks selected assertions separately. Broader concurrent-event and deadline schedules remain outside this scheduler's supported failure model.
