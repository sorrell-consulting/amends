# amends

**Correction-aware incremental processing, with independent reconstruction to check the result.**

Read the [documentation site](https://amends.sorrellconsulting.com/) for the visual example, architecture, and evaluation walkthrough.

Batch systems often get a useful property almost for free: every run starts again from the full source history. A late transaction, correction, or cancellation changes the next recomputation. An incremental system publishes answers sooner, but must also repair answers it has already published when that history changes.

We use a synthetic inventory ledger to demonstrate the problem. A **receipt** adds stock; an **issue** removes stock. Their signed quantities determine the inventory remaining at the end of each activity day: its **daily closing balance**. Simple arithmetic lets us focus on which revision wins, how far to recompute, which outputs to withdraw, and how to recover after a crash without losing a correction.

The same questions arise in amended financial transactions, claims, orders, and maintenance records.

## A correction that moves an alert

For the invented item/location pair `DEMO-PART / DEPOT-A`, opening inventory is **100 units**. The **reorder threshold of 60 units** marks the low-stock boundary. An alert records a **downward crossing**: the previous closing balance is at or above 60 and the next close is below 60. The opening balance supplies the first day's predecessor.

The initial history issues 50 units on Day 1, issues another 20 on Day 2, and receives 50 on Day 3. A later revision corrects the Day 1 issue to 30 units.

| End-of-day inventory | Original history (units) | After correction (units) |
| --- | ---: | ---: |
| Day 1 | 50 (alert raised) | 70 (alert withdrawn) |
| Day 2 | 30 | 50 (alert raised) |
| Day 3 | 80 | 100 |

The amendment is a **complete replacement**, not an additional movement. The original Day 1 fall from 100 to 50 generated an alert; staying below 60 on Day 2 generated no second alert. After correction, Day 1 ends at 70 and Day 2 falls to 50, so the system withdraws the Day 1 alert and publishes a Day 2 alert. The Day 3 receipt alone never erases a valid historical crossing; the correction changes the history that justified it.

The highest source revision determines which transaction value applies, independently of arrival order. Conflicting values at that revision block the affected item/location's results until resolved. The system publishes numbered updates and withdrawals so consumers can repair their stored results. Retracting an alert does not undo an external action someone already took because of it.

The [fixtures](fixtures/README.md) contain the inputs and hand-derived expected results. The [semantic contract](docs/semantics.md) specifies the rules, including ambiguous source revisions, cancellations, sparse days, and retained deletion versions.

## Run the example

From the repository root, with GNU Make and Go **1.27.2** available (or a Go launcher able to download that toolchain):

```sh
make core-demo
```

This builds the binaries and runs entirely in memory. It shows the original history, the correction, and cancellation of the last activity on Day 3. Each stage prints `CORE_AGREEMENT` after checking the incremental result against a separately implemented full-history calculation (the **independent oracle**) and fixed expected values. No Docker services are needed.

For a real PostgreSQL/Redpanda correction run, with a working local Docker engine and Compose:

```sh
make demo MODE=correction-only
```

To repeat the correction with a demonstration worker killed inside a database-observed transaction:

```sh
make drill DRILL=crash-mid-correction
```

The two service-backed commands start Docker dependencies, create isolated synthetic runs, retain reports and logs, and wait for their application processes to exit. The crash drill checks that the interrupted transaction left no partial durable decision, then resumes from stored progress under new database ownership. Both paths should finish with strict verifier `PASS` before and after the correction. `make down` stops the dependencies while retaining data; use it when you are finished with this stack.

The [evaluation guide](docs/evaluation.md) covers prerequisites, expected output, artifact checks, troubleshooting, and shutdown for all three paths. More failure cases are in the [drill catalog](docs/showcase.md).

## What is implemented

The core selects the applicable source revisions, recalculates affected historical balances and alerts, and identifies results to update or withdraw. PostgreSQL commits processing state, rejected-input evidence, progress, and intended publications together. A separate relay delivers those committed publications; this is the **transactional outbox** pattern. It prevents publication failure from losing a committed correction, without making PostgreSQL and Kafka one atomic transaction.

Consumers maintain the latest stored results, the **materialized view**. Delivery can repeat messages. The view retains each withdrawal's version (a **tombstone**) so an older delayed message cannot recreate a withdrawn fact. If workers overlap during reassignment, a database ownership generation prevents an obsolete worker from committing after its replacement takes ownership.

Operators can inspect progress and diagnostics, verify a stable portion of history, replay already-consumed input against intact compatible state, or rebuild from sufficient retained history into a fresh isolated namespace. A static HTML inspection dashboard always says **NOT VERIFIED** because progress observations do not establish agreement.

Tests combine hand-derived examples and independent calculations with generated histories, controlled failure schedules, and real database, broker, and process-crash checks. The [testing guide](docs/testing.md) maps those layers to the contract and explains their limits. `make check` runs formatting checks, vet, race-enabled ordinary tests, and builds; `make help` lists the optional service-backed checks.

We use [suffix recomputation](docs/design.md#why-refold-the-suffix) so one repair rule handles quantity changes, moved transactions, cancellations, and revised alerts. This costs more for deep corrections. The design compares [established processing frameworks and alternatives](docs/design.md#12-when-this-pattern-does-and-does-not-fit) and includes a [2,000-day example](docs/design.md#how-much-history-needs-repair) measuring how correction depth affects the amount of history recalculated.

## How the result is checked

The incremental path recalculates history from the earliest affected day forward. The oracle independently selects the applicable revisions from the full history and derives daily balances and crossings, without reusing the incremental calculation or its output comparison. We check both paths against hand-calculated fixtures; shared decoding and data types can still introduce shared defects.

Operational verification waits for processing and delivery to finish at a stable, complete source/output boundary. It then compares current business state with the oracle's reconstruction of that same history. See the [verification procedure](docs/verification.md#operating-procedure) for the required shutdown and boundary checks.

`PASS` means complete unqualified agreement. Rejected or historically conflicting input can qualify agreement; unresolved authority is `BLOCKED`, missing boundaries/history are `INCOMPLETE`, and a complete comparable mismatch is `DRIFT`. These distinctions remain visible in reports and exit behavior. See [verification](docs/verification.md) for the procedure and independent-check boundary.

## Scope and further reading

This is a local reference implementation using synthetic data. Before adapting it for production, read the [limitations assessment](docs/assessment.md) for gaps in security, availability, recovery, and capacity evidence.

- **Evaluate it:** [walkthrough](docs/evaluation.md), then [advanced drills](docs/showcase.md).
- **Operate it:** [runbook](docs/runbook.md), with component procedures linked where needed.
- **Understand correctness:** [semantic contract](docs/semantics.md) and [design](docs/design.md).
- **Change the code:** [technical guide](docs/technical-guide.md) and [testing strategy](docs/testing.md).

Licensed under [Apache-2.0](LICENSE).
