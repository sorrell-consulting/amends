# Semantics

**Normative design contract · Revision 2**

This document specifies the intended behavior of amends v1. Public summaries and diagrams must agree with this contract; [testing](testing.md) describes the implementation evidence.

## S1. What is compared

The comparison asks whether the consumer's stored business results agree with a separate calculation over the same source history. The notation below distinguishes that history, the revisions that determine its meaning, and the configuration used by both calculations.

For a finite input prefix H, let A(H) be the unambiguous highest authoritative revision of each transaction identity. Let C be the immutable configuration for the namespace. Let B(A(H), C) be an independent full-history computation of sparse daily closing balances and historical downward-crossing alert facts.

At a complete verification boundary, the target is:

```text
business_projection(current_materialized_view(H, C)) = B(A(H), C)
```

The projection contains present balance rows and present alert facts, with their business values. It excludes publication order, version counts, transport metadata, timestamps of processing, and tombstones retained only for delivery safety. Those have separate invariants. Key trust status is checked separately and must agree with independently determined conflict state.

“Active alert” means a historical downward crossing that remains valid under the currently authoritative history. It does not mean a purchase order is open, stock is still low today, or a previous warning has not been acted on. A later receipt above the threshold does not retract an earlier valid crossing. A correction that removes that crossing does.

The target requires a retained source prefix sufficient to reconstruct the ledger, stable namespace/configuration, unambiguous highest revisions, and a pipeline that has finished processing and materializing the compared prefix. Eventual convergence assumes faults cease or become recoverable and delivery makes progress. Permanent outages, destroyed durable storage, arbitrary corruption, and hostile writers are outside the guarantee.

## S2. Configuration and identity

A namespace identifies one isolated run with immutable schema version, ledger origin, stock-key definitions, opening balances, reorder thresholds, source-topic incarnation, and partition mapping. It also identifies distinct processing state, outbox, output topic, and view. Recreating a topic or changing its partition count requires a new namespace in v1.

A stock key is `(item, location)`. A transaction identity is `(stock key, TxnID)`; TxnID is stable and unique within that stock key. A source must never use a changed StockKey as an amendment to an existing identity. A movement between keys is represented as a cancellation plus a new transaction, with no atomic cross-key invariant promised.

Opening balance is the balance immediately before the configured ledger origin. No accepted transaction may precede that origin. Opening balance and threshold are configuration, not synthetic NEW events, and do not themselves create balance rows or alerts. Unknown stock keys and records outside the configured schema are rejected with durable evidence.

The day boundary is midnight UTC. Wall-clock time, SourceTime, and ingestion time do not control authority or finality. “Daily close” means the cumulative result through a UTC day under the available authoritative history, including a provisional result for any incomplete day. Historical days remain revisable; v1 has no irreversible closing or finalization operation.

## S3. Input envelope and authority

The intended input fields are:

```go
type Transaction struct {
    TxnID      string    `json:"txn_id"`
    Revision   int64     `json:"revision"`    // 1..MaxInt64
    Kind       Kind      `json:"kind"`        // NEW, AMEND, CANCEL
    StockKey   StockKey  `json:"stock_key"`   // also the broker record key
    ValidTime  time.Time `json:"valid_time"`  // normalize to UTC
    Qty        int64     `json:"qty"`         // signed movement
    SourceTime time.Time `json:"source_time"` // informational only
}
```

Positive quantities are receipts; negative quantities are issues; adjustments may have either sign. Zero-quantity active transactions are allowed and still count as activity on their day. Cancellation records must use quantity zero. Revision is a positive signed 64-bit integer to fit a Postgres BIGINT without an unsigned-range mismatch; exhaustion is an error, never a wraparound.

NEW and AMEND are complete replacement records with the same upsert behavior. A higher AMEND may arrive before NEW or skip any number of intermediate revisions. Its interpretation must not depend on a prior patch. NEW and AMEND both identify an active transaction at their revision. Reusing an identity for an unrelated real-world transaction is outside the source contract.

CANCEL is a complete assertion that the identity is inactive at its revision. Its valid time is retained as source metadata but contributes no activity day. Cancelling a previously active transaction removes the old active contribution, regardless of the cancellation record's own valid time. Cancel-before-new creates an authority tombstone without creating a business output. A higher complete NEW or AMEND can reactivate the identity; lower revisions cannot.

The broker key must match the envelope stock key. The producer must use the fixed partition mapping in the namespace manifest. A mismatched or misrouted record is rejected, not treated as a cross-key update. The namespace configuration and retained source records are inputs to verification, not trusted results supplied by the worker.

### Canonical equality

Canonical business payload comprises identity, revision, kind, normalized valid time, and quantity. Equivalent timestamp offsets normalize to the same instant. SourceTime, ingestion time, retry metadata, and broker coordinates are excluded from authority equality. Different informative timestamps therefore do not create a business conflict.

Exact canonical duplicates do not change derived state or output versions. Use canonical bytes or field equality to confirm equality; a digest is an index or diagnostic aid, not the sole unquestioned correctness test. Distinct business payloads at the same revision are conflicting source assertions, even when their daily arithmetic happens to coincide.

### Arithmetic boundary

Daily sums use exact integer semantics. The implementation must check overflow in stored balances, quantities, counters, and version increments. The default generator must produce histories whose intermediate accepted states are within the declared bounds; a safe generator bound is the absolute opening value plus the sum of absolute candidate quantities below MaxInt64.

An unexpected fold overflow stops that partition with an explicit error and leaves its input frontier unchanged. Do not silently wrap, saturate, quarantine a valid record as if it were malformed, or publish an approximate balance. The independent oracle should use a separate exact accumulation strategy, such as arbitrary-precision integers, and report an out-of-domain result explicitly.

## S4. Conflicts and quarantine

Maintain durable canonical evidence for revisions that have been observed. Authority is the highest revision number observed for an identity, not the last message received. A unique payload at that revision is authoritative. Two or more distinct payloads at that highest revision leave authority unresolved.

When highest-revision authority is unresolved, mark the entire affected stock key BLOCKED. Persist the conflict and emit the versioned `KeyStatusStated(BLOCKED)` control output. Retain the last computed business outputs without certifying them; do not choose a numeric winner based on arrival time or a hash. The last displayed values can differ between invalid intermediate histories and must be visibly qualified.

Continue ingesting and recording revisions for the blocked key, but freeze its business-output computation. Other stock keys, including those in the same partition, may continue. This permits source repair to arrive without blocking the entire stream.

A unique higher revision resolves a lower conflict for that identity. Once all current highest-revision conflicts on a key have been resolved, recompute that key completely from its authoritative transactions and opening configuration. Diff against its last computed output state; enqueue any business repairs before `KeyStatusStated(READY)`. Preserve historical conflict evidence. A later distinct payload at the new highest revision blocks the key again.

A conflict discovered only at an already-superseded revision is an audit diagnostic, not an ambiguity in the current authoritative value. It is still reported. Verification of repaired histories is qualified; it must not erase the history of exclusions or source-contract violations.

Malformed input is durably quarantined with raw record or retained raw-record reference, reason code, namespace, topic incarnation, partition, and offset. Its processing decision and next input position commit atomically. A standalone quarantine topic is not required in v1. Any later quarantine notification must use a transactional outbox, not an uncoordinated second write.

There is no generic “release this conflicting payload as authoritative” command. A correction must arrive as a valid source assertion at a higher revision. Malformed records may be corrected and re-published as new source records under the same authority rules; the original rejection is retained. A rejected malformed record does not establish a canonical revision payload.

Database unavailability, lock failure, serialization failure, and unknown commit outcomes are infrastructure conditions, not quarantine reasons.

## S5. Daily balances and crossing alerts

For a key, select the unique highest revision of each identity and discard the cancelled ones. Let D be the set of UTC days containing at least one remaining active transaction. An active zero-quantity transaction belongs to D.

For each d in D:

```text
before(d) = opening_balance + sum(qty for active transactions with day < d)
close(d)  = before(d) + sum(qty for active transactions with day = d)
cross(d)  = before(d) >= reorder_threshold AND close(d) < reorder_threshold
```

Publish one balance fact for each d in D and one alert fact for each d where cross(d) is true. Equality with the reorder threshold is not below threshold. Multiple intraday rises and falls do not produce extra alerts. Transaction order within a day does not change this additive daily result.

If a day has no active transactions, it has no balance row. Its balance is implicitly the most recent earlier close, or the opening balance. It cannot create a daily crossing. If the last active transaction moves away or is cancelled, remove that day's balance fact. Recompute later days and any crossing relationships that depend on the removed day.

The active alert set may contain more than one historical crossing on different days, even if the latest inventory balance has recovered. This is a history of currently valid crossing facts, not a current-stock status API.

## S6. Incremental repair

For an unblocked key, compare the previous active revision with the new authoritative active revision or cancellation. The earliest affected day is the minimum of the days of the old and new active contributions, ignoring absent contributions. If neither contributes, no balance refold is required.

Restore the last stored closing balance strictly before the earliest affected day; when no such day exists, use the opening balance. Do not assume a stored row exists for the preceding calendar day. Recompute the suffix through the latest affected or remaining active day. If that suffix becomes empty, withdraw its obsolete outputs.

A forward-only fast path is permitted only when it produces exactly the same business state as full reconstruction. Resolve blocked keys by a full per-key refold in v1; do not try to maintain a delicate incremental rewind position across an unresolved interval.

For reproducible traversal, sort active transactions by `(ValidTime, TxnID)`. Under these additive daily semantics, removing the TxnID tiebreak can be an equivalent mutation. Stable diagnostics are useful, but do not describe that order as the source of daily-balance correctness.

Diff newly computed business state against the previous committed derived-output state. “Previous” here means state for which publication intent was committed, not necessarily already delivered to the view. No semantic difference means no business output and no version increment. A change only to the informational cause does not by itself restate an unchanged business fact.

## S7. Output protocol and consumer behavior

There are two business output families and one small trust-status family:

| Message | Output identity within namespace | Meaning |
| --- | --- | --- |
| BalanceStated | `(balance, stock key, day)` | Upsert the current daily close |
| BalanceRetracted | Same identity as BalanceStated | Withdraw a day that no longer has active transactions |
| AlertRaised | `(alert, stock key, crossing day)` | Upsert a still-valid historical crossing, including threshold and close |
| AlertRetracted | Same identity as AlertRaised | Withdraw a crossing that no longer exists |
| KeyStatusStated | `(key-status, stock key)` | Upsert READY or BLOCKED with stable reason references |

A configured key with no status envelope has the initial logical status READY; this says nothing about processing progress. On the first accepted input, the worker emits an explicit READY or BLOCKED status as appropriate. Later status messages are emitted only when the status or its stable reason set changes. An untouched key has no balance or alert rows.

Use a canonical, unambiguous encoding of these tuples. Namespace is part of every output ID. Output version is a durable monotonically increasing positive integer per output ID. It starts at 1 on first emission and increments on a business change, withdrawal, reappearance, or status change. Output versions and deletion state are retained indefinitely for this bounded local example; v1 has no garbage-collection protocol.

A changed alert payload for an existing crossing day is a higher-version AlertRaised on the same identity. Moving the crossing to another day is a retraction at the old identity plus a new raised fact at the new identity. A reinstated balance or alert continues that identity's previous version sequence.

Every envelope includes schema version, namespace, output ID, output version, message type, business payload or withdrawal marker, triggering source identity/revision where available, and source coordinates. This is **change attribution**: it identifies why an output revision was emitted. It is not a complete list of all dependencies of a cumulative balance.

Reconstruction requires the retained source history, configuration, and declared code/schema version. Inspection may display contributing transactions, but v1 does not build a full dependency graph or guarantee arbitrary as-of reporting.

### Materialization rule

Apply an output only when its version is greater than the stored version for the same namespaced ID. An equal-version, equal-content message is a duplicate. An equal-version, different-content message is a protocol violation: stop the affected output partition, persist diagnostics, and do not advance its frontier or arbitrarily select a payload. A lower version is ignored.

The comparison, business upsert or withdrawal, retained version, and output-consumer position must commit in one durable transaction. An ignored duplicate still advances the output-consumer position atomically. Withdrawals hide a business row but retain the version and deletion marker; physical deletion of that guard is forbidden in v1.

Version 5 arriving before version 4 may cause version 4 never to be applied. That is correct current-state materialization, not “each version applied once.” At-least-once publication and version-aware consumption do not imply atomic repair of multiple rows. A reader can see a partly propagated correction until the verification boundary is reached.

## S8. Atomic input progress and fencing

Processing state, revision/conflict evidence, balances, derived-output state, version increments, outbox rows, durable quarantine, and input progress for a decision belong to one Postgres transaction. Duplicate and stale inputs may change only evidence, counters where durable, and progress.

`next_offset` is an exclusive source frontier: the next source position to read. Broker offsets are positions, not necessarily a contiguous count of user records. Advance through records in partition order using broker-provided coordinates and the adapter's validated seek behavior; do not confuse a fetch cursor or auto-committed group offset with durable processing progress. A new owner seeks to the database's authoritative position.

The partition-state row contains an ownership epoch. Both epoch installation and state-changing processing transactions serialize on that row. The implementation locks it before reading mutable processing state, verifies the expected epoch and frontier, and retains the lock until commit or rollback. A failed epoch/frontier check aborts the whole transaction.

The guarantee is: **once epoch e+1 has durably replaced e, no processing transaction using e may commit.** A transaction holding the row lock may finish before the new epoch can install; that is allowed. Kafka revocation notification is not the database linearization point. Epochs do not grant security authorization.

Lifecycle code must stop obsolete assignment work, discard revoked callbacks, and prevent a delayed stale assignment from installing a newer epoch after its assignment has been invalidated. Test this behavior with the pinned Kafka client; a pure store test is insufficient.

If the commit response is lost, the outcome is unknown. Re-read durable epoch and progress and recover from what committed. Do not lower progress, emit compensation, or allocate a new output version merely because a caller did not see the successful acknowledgement.

## S9. Outbox, delivery, and liveness

A single relay service publishes committed rows in order for each source partition. Publication uses the stock key as the output-topic key. An outbox row is marked sent only after a successful broker acknowledgement. A lost acknowledgement can lead to redelivery of exactly the same output identity, version, and content.

Retain committed outbox envelopes unchanged. Retries must not synthesize fresh versions. Relay progress and durable publish receipts support verification, but “sent” means acknowledged publication, not durable application by the view. The view's own frontier must also be observed.

Transient failures use bounded-attempt or bounded-time retry behavior with explicit backoff state; exhausted attempts make the stalled component visible and resumable. Simulation must control time and acknowledgement outcomes. No finite backoff policy establishes a latency guarantee during an outage.

## S10. Verification and quiescence

The first implementation uses a stopped-input boundary, not online snapshot reconstruction. `amendsctl verify --quiesce` is the orchestration interface for the local stack:

1. Stop and await the owned synthetic producer. Confirm no other writer is permitted for the namespace. Capture source topic incarnation, configuration digest, ledger origin, retained starts, and exclusive end-offset vector H. Recheck that the source does not advance during the run.
2. Wait until durable worker frontiers have reached H, without skipping unhandled records. Require supported source retention and a complete history beginning at the declared ledger origin. Missing or compacted required history is not repaired by assuming zero opening movements.
3. Drain publication for the processed prefix; wait for relevant outbox acknowledgements and resolve or safely drain outstanding relay attempts. Capture output topic incarnation and exclusive end-offset vector O after that drain.
4. Wait until the durable view-consumer frontiers reach O. Read a stable view snapshot. Do not take worker-derived tables as a substitute for the downstream view.
5. Independently scan source records from the namespace's required start through H, using the same declared visibility rules. Independently derive authority, diagnostics, balances, and alerts. Compare business state and trust status, and verify that excluded records match durable quarantine evidence.
6. Report H, O, namespace, configuration digest, code/schema version, completion checks, counts, diagnostics, elapsed time, and differences. If the producer or configuration changed, do not report a clean comparison of unlike histories.

The source topic must preserve the complete raw history required by the example. Compaction keyed by stock key would discard distinct transactions and cannot supply that history. A topic's retained start alone does not prove absence of destructive compaction or topic recreation; the manifest, broker configuration, and fixture provenance must also agree.

### Statuses and exit behavior

| Status | Meaning | Default exit behavior |
| --- | --- | --- |
| PASS | Complete boundary, no unresolved conflicts or rejected-input qualifications, business and trust state agree | Zero |
| PASS_WITH_EXCLUSIONS | Complete boundary and agreement for accepted/repaired input, with explicitly reported rejections or superseded conflicts | Nonzero unless exact expected-diagnostics manifest is supplied |
| BLOCKED | Complete boundary, but a highest revision is ambiguous, so some business results cannot be certified | Nonzero |
| DRIFT | A complete valid comparison found a business/trust-state or required-evidence mismatch | Nonzero |
| INCOMPLETE | Boundary not reached, source changed, or required history unavailable | Nonzero |
| ERROR | Infrastructure, protocol, arithmetic, schema, or verifier failure prevents a reliable result | Nonzero |

A diagnostics manifest is a named fixture listing exact record references/canonical fingerprints and reason codes, not a blanket “ignore quarantine” option. Even when it allows a zero exit for an expected poison-input drill, the printed status remains PASS_WITH_EXCLUSIONS. Negative tests may succeed by asserting an expected BLOCKED or INCOMPLETE result; this does not convert the underlying verification into PASS.

When the boundary is incomplete and unresolved highest-revision conflicts are already known, the overall verification status is INCOMPLETE. Include the known conflicts in the report and retain each affected key's BLOCKED trust status; neither the incomplete report nor the retained business values certify that key. Once the complete boundary is established, report BLOCKED if authority remains unresolved at that boundary.

At a complete boundary, a scoped report may show agreement for unaffected keys, but the overall report must retain BLOCKED if another key has unresolved authority. No absence of observed differences should override an incomplete comparison.

## S11. Replay, rebuild, and retention

Replay is a controlled re-read of a retained range that has already been consumed into intact, compatible state. It runs under a valid fence, uses a separate administrative scan cursor, and preserves the monotonic authoritative frontier. It must not skip ahead of unprocessed input, clear tombstones, reset versions, or reinterpret old records under changed configuration.

A normal owner is parked while the administrative replay owns the partition. After replay, normal processing resumes at the unchanged or safely advanced authoritative frontier. Previously rejected input remains recorded; replay is not an instruction to release it. Unsupported ranges or missing history fail before any mutation.

Rebuild starts from empty state in a fresh namespace and reads the full required source history under the same declared configuration, unless a deliberate new-config experiment is being run separately. It produces a new outbox, output topic, and view. Its restarted output versions cannot collide with the old namespace. The oracle comparison ignores transport namespace when comparing equivalent business results, but consumers never mix namespaces.

An empty rebuild from an arbitrary middle offset is invalid. A compatible checkpoint could make a later start sufficient, but checkpoint support is not part of v1. Live replacement of the old namespace, aliases, and consumer cutover are likewise outside v1.

Input-history retention, output-state deletion guards, configuration manifests, and schema/code identity are correctness dependencies. A reset of local volumes is destructive. The default example retains small histories until explicit reset and documents the disk-growth tradeoff.

## S12. Named semantic scenarios

These paragraphs are the required specification counterparts for `test/scenarios`. [Testing](testing.md#scenario-inventory-and-traceability) maps pure/model, PostgreSQL, source-worker, delivery, verifier, and operator-drill evidence to these IDs. Requirements and evidence remain distinct; a named scenario does not establish exhaustive failure coverage.

### SC01: Backdated NEW

A newly seen active identity whose valid day precedes existing activity changes that day and every affected later close. A new crossing may appear and a later crossing may disappear. The final business projection equals the independent full-history result.

### SC02: Quantity amendment

A higher full revision replaces the old quantity rather than adding to it. For the demo's Day 1 issue, replacing -50 with -30 changes closes from 50/30/80 to 70/50/100 and moves the crossing from Day 1 to Day 2.

### SC03: Valid time moves earlier or later

Replace the old contribution at its old day with the new contribution at its new day. The rewind begins at the earlier affected active day. Moving the only transaction off a day withdraws that day even if the total final balance is unchanged.

### SC04: Cancellation and cancel-before-new

A higher CANCEL removes an active transaction and persists its authority. If CANCEL arrives first, lower NEW and AMEND inputs remain inactive when they later arrive. No cancellation-only activity day is created.

### SC05: Reactivation

After cancellation, a higher complete NEW or AMEND makes the identity active again. Existing output IDs that reappear use versions greater than their withdrawal versions, not a restarted counter.

### SC06: Stale amendment arrives last

A lower revision with the newest SourceTime, ingestion time, or offset does not replace the higher revision. Evidence and input progress may advance; unchanged business outputs do not acquire new versions.

### SC07: Exact duplicate

Repeated canonical payloads, including ones with different transport or informational timestamps, preserve authority and business outputs. Output redelivery is handled independently by the materializer's version rules.

### SC08: Conflict at highest revision

Different canonical payloads at the same highest revision block the key. Run both arrival orders: neither may report unqualified verification success or silently designate first-arrival authority. Retained stale business values are explicitly uncertified.

### SC09: Conflict resolution and historical conflict

A unique higher full revision resolves the ambiguity. Recompute the whole blocked key, repair outputs, and publish READY after the repairs. Historical conflicting evidence remains and verification reports the qualification. A newly observed conflict only below a unique higher revision does not re-block the key.

### SC10: Other input during a block

While one key is BLOCKED, its further revisions are retained without business computation; another key continues normally. Resolution of the blocked key incorporates all accepted revisions accumulated during the blocked interval.

### SC11: Daily, not intraday, crossings

Two same-day movements may take an intraday running balance below and then back above the threshold. When the daily close is not below, there is no crossing alert for that day. Permuting equal-time records does not change the daily business result.

### SC12: Opening balance and threshold equality

A first close below threshold creates a crossing only if the opening balance was at or above it. Starting below threshold is not a new crossing. A close exactly at threshold is not below it; a subsequent close strictly below can create a crossing.

### SC13: Sparse days and zero-quantity activity

Across a calendar gap, restore the last prior activity-day close rather than requiring yesterday's row. A zero-quantity active transaction creates a daily balance row but cannot itself change the balance or create a new downward crossing.

### SC14: Balance disappears

Cancel or move the last active transaction on a day. Emit BalanceRetracted for the old day, remove it from the visible business projection, retain its version, and refold subsequent activity days and crossings.

### SC15: Alert retracted, moved, or restated

A removed crossing emits AlertRetracted; a crossing on a new day emits AlertRaised there. If the crossing remains on the same day but its close changes, upsert the alert payload at a higher version. A later recovery receipt alone leaves the prior historical crossing valid.

### SC16: Late output after withdrawal

Deliver a withdrawal before an older statement or raised alert for the same output ID. The older version cannot resurrect the row. Later legitimate reappearance uses a still-higher version and is allowed.

### SC17: Equal output version with different content

Supply two different envelopes for the same output ID and version. The view stops that output partition and reports a protocol failure without advancing beyond the conflicting record. Arbitrary winner selection is forbidden.

### SC18: Crash before or after worker commit

A pre-commit crash leaves no partial decision or advanced input frontier. A post-commit crash may cause re-reading, but durable position and authority prevent a duplicate semantic transition. Test ambiguous acknowledgement as both actually-committed and actually-aborted outcomes.

### SC19: Ownership replacement and zombie commit

Once a newer database epoch is installed, the older worker cannot commit state, versions, quarantine, outbox, or progress. Also exercise the allowed schedule in which an old transaction commits before the new epoch can acquire the lock.

### SC20: Relay crash or lost publication acknowledgement

A relay crash before acknowledgement leaves publication pending. A publish that succeeded before a lost acknowledgement can be repeated with identical content and version. The current view still converges when publication and materialization drain.

### SC21: Materializer crash

Crashing before its durable commit leaves both view state and view progress unchanged. Crashing after commit but before broker acknowledgement may cause a duplicate read that is harmless. The stored deletion version survives both cases.

### SC22: Malformed input and atomic quarantine

A malformed or misrouted input is rejected with durable evidence and progress in one transaction. A crash cannot leave the source position advanced without that evidence. Verification reports the exclusion and checks the expected rejection rather than silently ignoring the row.

### SC23: Replay into intact state

Re-read a retained processed range under the fence. The business projection, authoritative frontier, and existing output versions do not regress or reset. Reject a range that tries to skip unprocessed records or relies on lost retained history.

### SC24: Isolated rebuild

Reconstruct a fresh namespace from the full required history. Its business values match the original verified result while its versions and message sequence may differ. It neither writes into the old view nor accepts late messages from the old namespace.

### SC25: Invalid verification boundary

Stop the generator while the relay or view remains stalled, or let the source advance during comparison. Verification returns INCOMPLETE rather than a clean mismatch count. Missing required history and topic recreation also prevent a complete comparison.

Combine the stalled-boundary case with a known SC08 highest-revision conflict. The overall result is INCOMPLETE and includes the conflict diagnostics; the affected key remains BLOCKED. After the pipeline drains to a complete boundary, verification returns BLOCKED if that authority is still unresolved. Establishing the boundary does not resolve the conflict.

### SC26: No-op amendment

A higher complete revision can change an informational field or move within the same activity day without changing any business output. Authority and evidence advance, but unchanged balances and alerts are not restated merely to update their cause fields.

### SC27: Numeric overflow

Force a would-be fold outside the supported integer range. The worker reports an arithmetic failure and does not commit a wrapped balance or advance the affected input frontier. The oracle independently reports the out-of-domain condition.

### SC28: Routing and configuration incompatibility

A broker key differing from the envelope is rejected with evidence. A changed partition mapping, namespace configuration, schema, or source-topic incarnation prevents ordinary resume/replay; use a new namespace rather than accepting an incompatible state lineage.

## S13. Compact guarantee table

| Property | Contract |
| --- | --- |
| Batch-equivalent current business state | Conditional target at a complete valid boundary; evidence must be supplied by implementation tests |
| Message delivery exactly once | Not promised |
| Repeated/reordered valid output converges | Highest-version durable materialization with retained withdrawals |
| Stale owner cannot commit | After the newer epoch is durably installed |
| Replay safe | Only the supported retained range into intact compatible state |
| Empty rebuild from arbitrary offset | Not supported |
| Every historical output version applied | Not promised or required |
| Atomic downstream multi-row repair | Not promised |
| Correction undoes an external action | Not promised |
| No latency or capacity limit | Not promised; measure the example and report limits |
| Full dependency lineage or exhaustive proof | Not claimed |
