---
title: 'amends: Correction-Aware Incremental Processing'
description: See how amends repairs published results after a historical correction, then run the example and check its recovery behavior.
---

# When yesterday's input changes today's answer

A periodic batch job can start again from the full source history. An incremental system publishes results sooner, but a correction to an earlier fact can invalidate results it has already published. Updating the transaction alone is not enough: its consequences must be repaired too.

**amends** demonstrates **correction-aware incremental processing**: repairing published results when authoritative business records are amended or cancelled.

We use a small, synthetic inventory ledger to explain how corrections propagate.

Established processing platforms already provide much of this machinery. The [architecture discussion](design.md#12-when-this-pattern-does-and-does-not-fit) explains why we implement it directly here.

## See the correction

A **receipt** adds inventory; an **issue** removes it. The **daily closing balance** is the inventory remaining after that day's active movements. Start with **100 units** and a **reorder threshold of 60 units**, the example's low-stock boundary.

Issue 50 units on Day 1, issue 20 on Day 2, and receive 50 on Day 3. Now correct the Day 1 issue to **30 units**. This higher source revision completely replaces the original issue. The charts show how the daily closes and alert change:

<div class="inventory-charts" aria-label="Original and corrected inventory comparison">
  <figure>
    <h3>Original history</h3>
    <div class="inventory-canvas"><canvas id="inventory-original" role="img" aria-label="Original inventory: opening 100, Day 1 50, Day 2 30, Day 3 80 units. Downward crossing on Day 1." aria-describedby="inventory-caption">The numerical table below gives every value.</canvas></div>
    <figcaption>Day 1: 100 → 50 units. <strong>Original alert.</strong></figcaption>
  </figure>
  <figure>
    <h3>After the correction</h3>
    <div class="inventory-canvas"><canvas id="inventory-corrected" role="img" aria-label="Corrected inventory: opening 100, Day 1 70, Day 2 50, Day 3 100 units. Day 1 alert withdrawn; downward crossing on Day 2." aria-describedby="inventory-caption">The numerical table below gives every value.</canvas></div>
    <figcaption>Day 1 alert withdrawn. Day 2: 70 → 50 units. <strong>New alert.</strong></figcaption>
  </figure>
</div>

<p id="inventory-caption">Triangles mark crossing days; the dashed line marks the 60-unit threshold. Hover or tap for values.</p>

An alert records a **downward crossing**: the previous close is at or above 60 and the next close is below 60, using opening inventory as the first day's predecessor. Originally, Day 1 qualifies; remaining below 60 on Day 2 does not generate another alert. After correction, Day 1 closes at 70 and Day 2 at 50. Withdraw the original Day 1 alert and publish the newly valid Day 2 alert.

| Observation | Original inventory (units) | Corrected inventory (units) | Alert consequence |
| --- | ---: | ---: | --- |
| Opening | 100 | 100 | Supplies the first day's predecessor; no activity row or alert |
| Day 1 close | 50 | 70 | Original alert must be withdrawn |
| Day 2 close | 30 | 50 | Corrected history requires a new alert |
| Day 3 close | 80 | 100 | No new downward crossing in either history |

The Day 3 receipt does not erase a valid earlier crossing. The correction changes the history that justified it. An alert describes a historical event.

## Follow the repair

The highest source revision determines which transaction value applies. If that revision contains conflicting values, the affected item/location's results remain blocked until the ambiguity is resolved. For an unambiguous change, the processor recalculates balances and alerts from the earliest affected day forward. Earlier unaffected results stay intact. The [design](design.md#5-incremental-state-and-output-repair) explains this repair rule and the full recalculation required when resolving conflicts.

A recent correction can leave most earlier results intact: in a 2,000-day history with activity every day, correcting Day 1,990 requires deriving only the final 11 days. The [repair-locality example](design.md#how-much-history-needs-repair) measures that work and explains the state inspection and other costs that remain.

Changed results and intended publications commit together in PostgreSQL. This **transactional outbox** lets a separate relay retry delivery after a publication failure. Consumers keep the latest stored results, the **materialized view**, and retain each deletion's version (a **tombstone**) so an older delayed message cannot recreate a withdrawn fact. Transport can repeat messages; PostgreSQL and the broker do not share one atomic transaction.

If a worker crashes before its transaction commits, partial work rolls back. The [crash walkthrough](evaluation.md#path-3-crash-during-the-correction) demonstrates recovery from this point under new database ownership. A lost commit reply is different: recovery must reread durable state to find out whether the transaction committed.

Only days with active transactions have stored closing-balance rows. Cancelling the last movement on a day withdraws its row; the preceding balance carries forward implicitly. A missing row does not mean zero inventory.

## Check the answer independently

We check the incremental result against a separately implemented full-history calculation, the **independent oracle**. It reconstructs applicable source revisions and derives balances and alerts without reusing incremental repair decisions. We also check both paths against hand-calculated fixtures.

Once processing and delivery have finished at a stable, complete boundary, verification compares the consumer's results with an independent reconstruction of the same source history. A strict **PASS** means complete, unqualified agreement. Missing history, unresolved conflicts, and other qualifications remain visible in the result. See the [verification procedure](verification.md#operating-procedure) for the required boundary checks.

## Where this problem appears

Similar corrections appear in other domains:

| Domain | Historical change | Possible downstream consequence |
| --- | --- | --- |
| Fund accounting | A prior transaction is amended | Balances, allocations, or P&L need restatement |
| Logistics | A shipment quantity is corrected | Inventory positions and replenishment conclusions change |
| Maintenance | A service record is amended | Maintenance histories or readiness reports change |
| Claims | A claim is reopened or revised | Reserves or reports need recalculation |

**Correcting information does not automatically reverse a business action.** A withdrawn low-stock alert may already have triggered a replenishment order. Cancelling or amending that order requires a separate business process with its own authority and recovery rules.

## Choose a path

[View the in-memory demonstration output](media/core-demo.txt) showing the original history, correction, and withdrawal of a cancelled day's balance.

- **Run it:** the [evaluation guide](evaluation.md) starts with `make core-demo` in memory, then adds PostgreSQL/Redpanda and a worker crash.
- **Understand the solution:** [architecture and design](design.md), followed by the [technical guide](technical-guide.md) and its worked code path.
- **Inspect the rules and evidence:** [semantic contract](semantics.md), [testing strategy](testing.md), and [advanced drills](showcase.md).
- **Operate an existing run:** [runbook](runbook.md), [verification](verification.md), and [replay/rebuild](replay-rebuild.md).

### Three failures to inspect

- **[Crash during a correction](showcase.md#worker-crash-barrier):** kill a worker inside a database-observed transaction, check that partial changes rolled back, then recover and independently verify the result.
- **[Ambiguous database commit](showcase.md#ambiguous-commit-extension):** lose a COMMIT request or its successful reply and show why recovery must reread durable state before deciding whether to retry.
- **[Withdrawal followed by stale delivery](showcase.md#delivery-drill-extensions):** withdraw a day's balance, deliver its older version again, and check that the retained deletion version prevents resurrection.

These three drills expect strict PASS after recovery. The [full catalog](showcase.md#drill-catalog) also includes expected negative and qualified outcomes, which remain distinct from successful verification.

This is a local reference implementation using synthetic data. Before adapting it for production, read the [limitations](assessment.md) for gaps in security, availability, recovery, and capacity evidence.

[Source repository](https://github.com/sorrell-consulting/amends) · [Apache-2.0 license](https://github.com/sorrell-consulting/amends/blob/main/LICENSE)
