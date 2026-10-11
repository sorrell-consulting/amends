# Operability and limitations assessment

See [testing](testing.md) for the checks behind these claims and the [runbook](runbook.md) for recovery procedures.

## 1. Intended operating envelope

We built this example to make correction and recovery logic easy to inspect. It runs locally with synthetic complete-revision input, small retained histories, fixed configuration, two workers, a single Postgres server, one relay, and one materializer.

Workers commit processing state and intended publications together in an **outbox**; the relay delivers those records, and the materializer stores current results. Each service's durable next input position is its **frontier**. Database ownership generations (**epochs**) reject obsolete workers after replacement (**fencing**). Retained deletion versions (**tombstones**) prevent older output from restoring withdrawn facts. The failure table uses these distinctions; [design](design.md) explains their transaction boundaries.

Production use would require workload, availability, and security decisions beyond this local demonstration.

## 2. Failure-mode assessment

| Failure or limitation | Detection | Recovery | Residual risk |
| --- | --- | --- | --- |
| Worker crashes before commit | Lag, missing durable frontier advance | Reassignment/restart from database position | Recovery depends on intact durable storage |
| Commit acknowledgement lost | Timeout plus reconnect inspection | Read durable epoch/progress before retry | Both outcomes are tested at selected boundaries; arbitrary failure schedules remain outside that evidence |
| Old owner attempts a write | Fence rejection and trace | Stop obsolete actor; current owner resumes | Obsolete callback must not install a new epoch |
| Relay or broker stalls | Oldest pending outbox age | Resume unchanged envelope publication | No latency bound during outage |
| View crashes or falls behind | Durable view lag | Restart and re-deliver, preserving tombstones | Per-output application is not an atomic whole-correction snapshot |
| Highest-revision source conflict | BLOCKED key and conflict evidence | Higher unique full source revision, full key refold | Source must supply authority; implementation cannot invent it |
| Malformed input | Durable quarantine with progress | Correct new source record and qualified verification | Excluded input can matter operationally even when accepted input matches |
| Day or crossing disappears | Withdrawals and verification | Apply versioned retraction | Deleting tombstone versions reintroduces resurrection risk |
| Equal-version conflicting output | Materializer protocol error | Preserve evidence; repair cause or isolate a rebuild | No automatic safe payload selection |
| Incomplete/missing source history | Manifest and retention checks; verifier status | Restore a sufficient source or fail explicitly | Ordinary replay cannot reconstruct lost facts |
| Deep historical correction | Refold count, duration, lock wait, lag | Drain and inspect; adjust optional load | Other keys in that partition wait; no capacity claim |
| Arithmetic overflow | Explicit domain error before commit | Correct bounded-domain decision | A valid final sum may still have unsafe intermediate histories |
| Drift after complete drain | Independent oracle comparison | Preserve evidence, correct cause, rebuild in isolation | Oracle can also contain defects; hand-stated fixtures remain important |
| Disk growth | Local disk and table/topic size observations | Explicit cleanup of disposable namespaces | No safe automatic evidence/tombstone GC protocol in v1 |
| Lost Postgres or broker volume | Startup/history checks and missing manifest state | Fresh rebuild only with sufficient surviving source | No high availability or disaster-recovery objective |

## 3. Correctness evidence still has limits

Generated histories and seeded schedules sample defined behavior; they do not exhaust all possible executions. The memory store may model the SQL adapter incompletely. Real integration tests exercise actual storage behavior but only for the tested environment and failure boundaries. Shared decoding or domain-type errors can affect both the incremental path and oracle.

The explicit contract, independent authority and aggregation code, hand-calculated fixtures, real database concurrency tests, mutation results, and reproducible traces provide complementary checks. The verifier also refuses incomplete or ambiguous comparisons. Together they support the declared behavior within the tested histories and failure boundaries.

## 4. Security and public exposure

The development stack is local-only. Minimum containment requirements are loopback-bound host ports, an isolated container network, synthetic fixtures, no external secrets, no production credentials, and no automatic cloud deployment. Document test credentials as local development values; do not reuse them elsewhere.

Production authentication, authorization, TLS, key/secrets management, trusted identity, patching, backup/restore, incident response, multi-user access, and supply-chain assurance are not provided by this repository. Basic local containment is not evidence of compliance with CMMC, NIST, or another framework. The development stack is not suitable for public hosting.

## 5. Domain limits

The inventory ledger is a teaching domain with immutable opening balances and reorder thresholds. It does not validate actual stock ownership, purchase orders, inventory reservations, transfers, substitutions, backorders, or equipment readiness. Cross-key invariants and integration with actual logistics systems are outside its scope.

Historical crossing facts are revisable decision-support outputs. Retracting one does not recall an email, cancel an order, reverse a payment, or compensate an external action. Such actions need separate business workflows with their own authority, idempotency, and [compensation rules](https://learn.microsoft.com/en-us/azure/architecture/patterns/compensating-transaction).

The example records daily-close crossings. Intraday crossings require different identities and ordering rules. Daily output is sparse: an absent day carries the preceding balance implicitly and does not mean zero inventory.

## 6. Recovery and change limits

Ordinary restart and controlled replay depend on intact compatible state. Empty reconstruction requires complete sufficient history. Configuration changes, topic recreation, repartitioning, output tombstone cleanup, and new semantics can make replay unsafe.

V1 rebuilds in isolation. It does not define live consumer cutover, backup/checkpoint compatibility, schema migration of existing materialized views, or bounded recovery time.

## 7. Capacity statements

We use a one-key, four-message fixture so its arithmetic can be checked by hand. The opt-in [2,000-day example](design.md#how-much-history-needs-repair) measures suffix records and changed outputs in memory. It demonstrates repair locality without measuring throughput or end-to-end latency. Capacity measurements would also need a representative workload, hardware, dependencies, correction depths, concurrency, and output volume.

A long partition transaction can dominate latency regardless of average throughput. The [implemented per-decision measurements](verification.md#committed-processing-measurements) report rewind depth, suffix records, changed envelopes, and client acquisition/precommit timings; worker logs include completed storage-call duration and unknown outcomes. Real SQL tests confirm acquisition measurements include an observed partition-lock wait. Inspection also retains cumulative committed counts/timing sums and the latest 32 samples per partition. These do not isolate server wait, count all copy/scan work, provide long-term percentiles, or establish a latency bound. Collection starts with the first instrumented decision; earlier history is unknown. Measure the workload and resulting lock/lag behavior before selecting a more complex architecture.
