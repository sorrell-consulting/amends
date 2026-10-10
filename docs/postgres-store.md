# PostgreSQL storage implementation

The PostgreSQL adapter persists processing state, source progress, output intent, quarantine, publication receipts, and the materialized view. Processing decisions, publication receipts, and view application each have their own transaction. It is exercised against the local PostgreSQL 18.6 instance using pgx v5.11.0.

The [broker source worker](source-worker.md) and [relay/view transport](output-delivery.md) call this adapter. The [bounded live verifier and inspection metrics](verification.md) use these APIs; the [process-crash showcase](showcase.md) observes rollback and recovery across actual worker death. The storage integration tests deliver envelopes through a database test harness; the delivery suite also consumes actual broker output. A passing integration test is not itself a product verifier PASS.

## Running the database checks

From the repository root:

```sh
make up
make test-postgres
make evidence-postgres
```

`make test-postgres` runs fresh race-enabled tests with the `integration` build tag. It uses `AMENDS_TEST_DATABASE_URL`, defaulting to the synthetic local endpoint already configured in Compose:

```text
postgres://amends:amends-local-only@127.0.0.1:15432/amends?sslmode=disable
```

The tests fail if the selected database is unavailable. Direct tagged test execution also fails if the variable is missing; it does not silently skip the suite. `make check` requires no services and runs the core/model tests while compiling the database adapter.

Each test creates a unique namespace and removes only its own rows on cleanup. The schema and migration ledger remain. No existing namespace, broker topic, or Docker volume is reset. An interrupted test can leave its synthetic namespace behind; there is no automatic sweep of unrelated state.

`make evidence-postgres` writes selected toolchain and module versions plus JSON test events under `artifacts/postgres/`. The environment test records the server version and requires `fsync=on`. The adapter sets `synchronous_commit=on` for its transactions. The CI workflow has a separate PostgreSQL service job running the same evidence target; hosted results must be observed separately.

## Schema and representation

Migrations [001](https://github.com/sorrell-consulting/amends/blob/main/migrations/001_initial.sql), [002](https://github.com/sorrell-consulting/amends/blob/main/migrations/002_output_transport.sql), [003](https://github.com/sorrell-consulting/amends/blob/main/migrations/003_processing_measurements.sql), and [004](https://github.com/sorrell-consulting/amends/blob/main/migrations/004_processing_statistics.sql) are embedded in order by [migrations/embed.go](https://github.com/sorrell-consulting/amends/blob/main/migrations/embed.go). `postgres.Migrate` checks the contiguous installed history and SHA-256 checksums, then applies missing migrations transactionally under an advisory transaction lock. Repeated installation is idempotent. An unknown version, gap, or changed applied migration fails. Migration 002 preserves the original migration and existing processing data while adding output identity and raw protocol diagnostics.

| Table | Stored content |
| --- | --- |
| `amends_schema_migrations` | Applied migration version, checksum, installation time |
| `amends_namespaces` | Configuration bytes, configuration digest, storage format version |
| `amends_partitions` | Ownership epoch, exclusive source frontier, emission sequence, optional last committed processing measurement, cumulative statistics, bounded recent history |
| `amends_key_state` | Complete per-key revision evidence and computed projection |
| `amends_output_state` | Latest committed envelope and version for each output ID |
| `amends_outbox` | Exact committed envelope bytes, partition sequence, creation time, acknowledgement time, and observed output offset |
| `amends_quarantine` | Raw rejected record, source position, and rejection reason |
| `amends_output_binding` | Immutable output topic name, broker incarnation, and partition count |
| `amends_view_progress` | Exclusive output frontier, stopped-partition reason, and exact offending offset/key/value |
| `amends_view_rows` | Highest retained envelope, version, and presence/deletion marker |

Namespace IDs are JSON-encoded strings, and stock/output IDs are JSON-encoded tuples stored as TEXT. Configuration, state, records, and envelopes are JSON bytes stored as BYTEA. This preserves the exact outbox bytes and handles identifiers containing escaped control characters, including NUL, without relying on PostgreSQL JSONB's representation limits.

We serialize revision candidates, conflicts, sparse balances, and alerts together in one row per stock key to keep the adapter easy to inspect. Each key update replaces that serialized state. Epochs, frontiers, emission sequences, output versions, presence, and publication receipts have explicit SQL columns and constraints.

Key-state encoding and decoding are centralized in `internal/store/postgres/state_codec.go`. Explicit JSON tags in `fold.State`, `Identity`, and `Candidate` specify the capitalized field names required by storage format version 1; ledger fields use their wire names. A Go-field rename must preserve these tags. The hand-stated `testdata/key-state-v1.json` fixture checks the complete representation, retained cancellation/conflict evidence, and continued processing after decoding. It must not be regenerated from the fold implementation.

The serialized Go state format is version 1; the SQL schema version is 4. These are separate version spaces. Migration 003 adds a nullable measurement without rewriting historical state or backfilling fabricated costs. Migration 004 adds nullable cumulative statistics and the latest 32 samples; earlier history is unknown. Upgrades from populated schemas 1, 2, and 3 are tested. Decide how to version or migrate persisted state before making incompatible changes. Evidence cleanup, deletion-version cleanup, and live namespace cutover are not implemented.

## Adapter entry points

The adapter is [internal/store/postgres](https://github.com/sorrell-consulting/amends/tree/main/internal/store/postgres). Shared snapshots, tokens, committed deliveries, and view input/position values live in [internal/store/types.go](https://github.com/sorrell-consulting/amends/blob/main/internal/store/types.go). Worker and delivery protocols use `store.ErrFence`, `store.ErrCommitUnknown`, and `store.ErrProtocol` from [errors.go](https://github.com/sorrell-consulting/amends/blob/main/internal/store/errors.go); PostgreSQL session guards and `ErrServiceBusy` remain adapter-specific.

| Entry point | Responsibility |
| --- | --- |
| `Migrate(ctx, pool)` | Install or validate the schema |
| `New(pool, config)` | Validate and copy configuration without database mutation |
| `Store.Ensure(ctx)` | Initialize a namespace or validate its existing immutable configuration |
| `BindOutput` / `OutputBinding` | Install or read immutable output identity; reject adoption of previously delivered unbound state |
| `AcquireService` | Hold a dedicated session lock for a producer, relay, or view; no automatic failover |
| `Progress` | Observe epochs, source/view positions, pending count/age, receipts, quarantine count, protocol status, the last processing measurement, cumulative statistics, and bounded history in one snapshot |
| `Receipt` | Read publication outcome after an unknown acknowledgement commit |
| `ApplyViewInput` / `ViewPosition` | Apply decoded output or retain raw protocol failure; observe durable recovery position |
| `Claim(ctx, partition)` | Install a new epoch under the partition row lock |
| `Observe(ctx, partition)` | Read installed epoch and source progress |
| `Begin(ctx, token, expected)` | Lock the partition and validate the processing guard |
| `ProcessingTx.Apply(record)` | Prepare all database writes for one input decision |
| `ProcessingTx.Commit()` | Commit the entire decision |
| `ProcessingTx.Rollback()` | Abort and release the transaction |
| `Process(ctx, token, expected, record)` | Convenience wrapper around begin/apply/commit |
| `Pending(ctx, partition)` | Read the earliest unacknowledged envelope and its exact bytes |
| `Acknowledge(ctx, partition, sequence, outputOffset)` | Record an ordered publication receipt |
| `ApplyView(ctx, partition, expected, offset, envelope)` | Atomically materialize one output and its progress |
| `Snapshot(ctx)` and `View(ctx)` | Read consistent processing and view snapshots separately |

`Ensure` never overwrites an existing manifest or fills in missing state for an existing namespace. Configuration disagreement fails. Migration and initialization are explicit operations; constructing a Store does not mutate the database.

## Processing and ownership

`Begin` obtains `SELECT ... FOR UPDATE` on the namespace/partition row before reading mutable key or output state. It checks the token's namespace, configuration digest, positive epoch, and expected source frontier against the installed values.

`Apply` uses the pure `fold.Validate`, `fold.ApplyMeasured`, and `diff.Reconcile` functions. `fold.Apply` delegates to the same measured implementation; the independent oracle does not. Accepted input writes key evidence/projection, output state, and outbox envelopes. Rejected input writes its exact raw record and reason. Both persist the processing measurement, cumulative statistics, and bounded recent history in the same final UPDATE as the source frontier and emission sequence. Statistics are read while the partition row lock is held. Rollback/fencing contribute nothing, and replay preserves the observations. Failure of that write poisons the whole transaction. `Commit` classifies outcomes without further precommit I/O. See [measurement definitions and timing limits](verification.md#committed-processing-measurements).

`ProcessingTx.recordProgress` owns the final measurement/statistics read and frontier UPDATE inside that same transaction. An `Apply` error, including a failure in this helper, poisons the transaction: `Commit` rolls back rather than permitting partial writes. Only one record is accepted per processing transaction. Callers must always defer `Rollback`; a successful commit makes that cleanup harmless.

`Claim` locks the same partition row before incrementing the epoch. If processing already owns the lock, it can finish before the replacement claim. If the replacement wins first, old processing sees the newer epoch after waiting and fails its guard. The integration test checks database-observed blocking through `pg_blocking_pids`, rather than assuming that a sleep proves one connection blocked another.

The adapter bounds operations with a 15-second context, a 3-second lock timeout, a 10-second statement timeout, and a 15-second idle-in-transaction timeout. These are limits for the small local implementation, not a latency promise. A deep correction can fail its budget without advancing progress. Cleanup uses a separate bounded context because pgx does not automatically roll back when the operation context is cancelled.

The store has no knowledge of Kafka assignment validity. `Observe` reports installed state; it does not authorize a revoked worker to resume. The source worker implements that separate lifecycle guard; the database fence remains necessary.

## Unknown commit outcomes

A successful commit call returns nil. An unclassified commit failure returns an error matching `ErrCommitUnknown`; it never reports optimistic success or assumes rollback from a lost connection. Explicit pgx closed/rolled-back transaction results remain distinguishable.

After an unknown processing result, read durable epoch/progress and resume from the installed frontier. After an unknown claim, reobserve under a still-valid local assignment. After an unknown view result, use its durable output frontier. Do not compensate, lower a frontier, or allocate another output version merely because a reply was lost.

The test socket wrapper exercises two actual database outcomes:

1. Close the connection before transmitting COMMIT, leaving PostgreSQL to roll back.
2. Send COMMIT, read a successful server CommandComplete and ReadyForQuery, then close the socket before pgx receives those reply bytes.

Both paths return unknown to the adapter caller. Separate connections inspect what actually committed. These tests cover processing, quarantine, ownership claims, publication receipts, and view application. The fault wrapper is compiled only into integration tests.

A separate case terminates the test's PostgreSQL backend after a correction's writes but before commit. It checks that the old projection, outbox, and frontier remain together, then installs a replacement owner and successfully applies the correction once.

These are connection and transaction failure tests. They do not kill a deployed worker process, crash the PostgreSQL server, simulate power loss, or exercise broker acknowledgement loss.

## Outbox and publication receipts

Output versions are allocated from committed output state while holding the processing lock. Each emitted envelope is serialized once; the same bytes enter the latest output row and the outbox row in the transaction.

`Pending` returns those bytes and the earliest pending sequence. The [relay](output-delivery.md#relay-decisions-and-publication-receipts) publishes `Delivery.Bytes` unchanged. A later attempt may obtain another output offset, but it must not replace the output ID, version, payload, or cause.

`Acknowledge` records an observed output offset only for the earliest pending row. Receipts must advance within the partition. Repeating an identical receipt is idempotent, which supports recovery after its database commit reply is lost. A conflicting receipt or an attempt to acknowledge later pending work fails.

The acknowledgement transaction also locks the processing partition row. This is a small, explicit serialization cost in the adapter. It does not hold that lock during broker publication. The runner preserves the single-relay ordering assumption with serial steps and a dedicated session guard. That guard does not fence broker requests already in flight; see the [operating limits](output-delivery.md#singleton-operation-and-failure-limits).

Creation and acknowledgement timestamps support the outbox-age observations in [inspection](verification.md#inspection-and-point-in-time-metrics). The application does not expose a metrics endpoint.

## Durable view

`ApplyView` locks the view-progress row before reading an output's stored envelope. It applies the version-aware view decision, then commits the retained envelope, version, presence marker, and next output offset together.

A higher version replaces a lower one. An equal identical envelope or a lower valid version leaves the current fact intact while advancing consumption progress. Withdrawals retain their version and envelope with `present=false`.

An invalid envelope or conflicting equal version commits a stopped-partition diagnostic while leaving rows and frontier unchanged. A stale caller frontier is an operation error and does not poison the output partition. The diagnostic survives reconnection and prevents further application through the adapter.

`View` reads rows, deletion guards, diagnostics, and frontiers from one repeatable-read snapshot. `Snapshot` does the same for processing state. These separate reads do not establish a complete common source/output boundary; the [bounded verifier](verification.md) coordinates that boundary using writer guards, a clean relay handoff, and repeated broker/database observations before comparing the downstream view with the independent source-history oracle.

## Evidence and limits

The integration suite covers:

- All three hand-stated fixture stages, checked independently against the oracle and durable view.
- Cancellation authority, higher-revision reactivation, historical conflicts, blocked-key accumulation, and repair ordering after reconnection.
- Rollback and atomic quarantine, emission/epoch/output-version exhaustion, and arithmetic failures.
- Actual partition lock ordering and rejection of stale ownership.
- Exact outbox retries, ordered acknowledgements, and idempotent receipt recovery.
- Durable duplicate handling, conflicting equal-version diagnostics, and deletion guards after reconnect.
- Both actual outcomes behind a lost commit reply for each database mutation boundary listed above.
- Per-record conformance with the memory store for the selected valid and conflict/rejection histories from seed 7.
- Upgrade from populated schemas 1, 2, and 3 to schema 4, idempotent reruns, checksum rejection, and no fabricated historical measurements.
- Committed refold/output measurements and aggregates, bounded history across restarts/ownership changes and offset gaps, unchanged observations on rollback/fencing/replay, both unknown-commit outcomes without double counting, counter saturation, and lock acquisition measured across an actual database-observed wait.
- Immutable output binding, refusal of previously delivered unbound state, and dedicated relay/view session guards including actual backend termination.

The database conformance sample is separate from the 256-run memory corpus. It is not 256 executions against PostgreSQL.

The [source worker](source-worker.md), [output transport](output-delivery.md), and [bounded verifier](verification.md) exercise this store through actual broker adapters. The [showcase](showcase.md) observes selected process deaths, rollback, recovery, and automatic relay handoff for its owned pipeline. Committed measurements, cumulative counts, and the latest 32 samples per partition are exposed through inspection; long-term charting is outside this adapter.

For the API and locking basis, see [pgx v5.11.0](https://pkg.go.dev/github.com/jackc/pgx/v5@v5.11.0), its [transaction implementation](https://github.com/jackc/pgx/blob/v5.11.0/tx.go), and [PostgreSQL 18 explicit locking](https://www.postgresql.org/docs/18/explicit-locking.html). The repository's tests supply implementation evidence for the limited cases above.
