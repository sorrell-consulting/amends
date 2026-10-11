# Broker source ingestion and worker lifecycle

The `amends` worker connects the retained Redpanda source topic to the PostgreSQL processing store. The source commands provision topics, save a manifest, and publish fixtures. The pinned adapter uses franz-go v1.22.1, Redpanda v26.2.3, and PostgreSQL 18.6.

A **manifest** records the source topic's identity and immutable run configuration. Its topic **incarnation** distinguishes the actual broker-created topic from a later topic with the same name. For each partition, PostgreSQL stores the next input offset (**frontier**) and an ownership generation (**epoch**). After a replacement epoch installs, rejecting older-worker commits (**fencing**) protects state during overlapping ownership beliefs; a Kafka callback alone does not establish that database boundary.

`source-load` requires `AMENDS_DATABASE_URL`: it holds a source-incarnation producer session guard through broker-client shutdown. Verification and rebuild namespaces reading that source share the guard to await an active finite load and refuse new publication during comparison. Stop older executables before upgrading from namespace-scoped producer guards. See [verification](verification.md#operating-procedure) for the stopped-writer preconditions.

The worker commits revision evidence, derived output intent, quarantine, and source progress. It leaves committed envelopes in the outbox for the separately implemented [relay and view transport](output-delivery.md). The transport provides output binding, publication, consumption, and live view inspection. The [bounded verifier and inspection report](verification.md) expose separate comparison and observation paths. The [showcase](showcase.md) automates owned fault injection, recovery, drain, and verification. Inspection exposes last committed work, cumulative counts, and bounded recent history. A passing source integration test is not a product verifier PASS.

## Run source ingestion

From the repository root, start the local dependencies and build:

```sh
make up
make build
mkdir -p artifacts
```

Choose a new namespace and topic for this run. The example names below must not already exist. Save the generated manifest; ordinary restarts reuse this same file and the existing database state.

```sh
./bin/amendsctl source-create -namespace source-demo-1 -topic amends-source-demo-1 \
  > artifacts/source-demo-1.json
export AMENDS_DATABASE_URL='postgres://amends:amends-local-only@127.0.0.1:15432/amends?sslmode=disable'
./bin/amends -manifest artifacts/source-demo-1.json
```

Run another `amends` process in a second terminal with the same environment and manifest to exercise group assignment. The default source has two partitions; the default fixture's only stock key maps to partition zero, so one worker may have no business input. The integration suite uses two configured keys on separate partitions to exercise both workers actively.

In another terminal, export the same `AMENDS_DATABASE_URL` and publish the supplied stages:

```sh
./bin/amendsctl source-load -manifest artifacts/source-demo-1.json -file fixtures/before.jsonl
./bin/amendsctl source-load -manifest artifacts/source-demo-1.json -file fixtures/correction.jsonl
./bin/amendsctl source-load -manifest artifacts/source-demo-1.json -file fixtures/cancel-last-day.jsonl
```

The loader validates JSON shape and configured routing before publishing the finite file. It publishes nontransactionally, in file order, using the manifest's explicit partition mapping. Successful lines report broker coordinates. It enables idempotent-producer cancellation and a delivery deadline so an in-flight request can terminate with an unknown result. A failed/unknown publication exits nonzero; rerunning can redeliver identical revisions. It does not bypass source authority or delete earlier assertions.

`source-create` refuses to adopt an existing topic. If creation or manifest output fails, inspect the named topic before choosing another namespace/topic; a failed response does not prove creation failed. Do not overwrite a saved manifest during ordinary restart. The CLI does not yet provide a manifest recovery/adoption command.

Both source commands and the worker accept `-brokers`, defaulting to `127.0.0.1:19092`. The worker and `source-load` require `AMENDS_DATABASE_URL`; credentials are absent from the manifest. SIGINT/SIGTERM cancels and joins worker activity before closing its broker client. A stopped worker can be restarted with the same command. A fatal/exhausted operation exits 1 with a diagnostic; resolve the reported condition before restarting. `make down` stops dependencies and preserves their volumes.

## Manifest and raw records

The generated JSON contains `source_topic` and `config`. Configuration includes namespace, schema version, ledger origin, stock keys, opening balances, thresholds, partition mapping, and `source_incarnation`. The incarnation is `kafka:` followed by the broker's 16-byte topic UUID in lowercase hexadecimal. The PostgreSQL namespace stores and checks this configuration and its canonical digest.

Topic creation explicitly sets `cleanup.policy=delete`, `retention.ms=-1`, and `retention.bytes=-1`, with one replica for the local single-node example. Before assignment, external seeking, and after every poll, the worker reads current metadata, effective retention configuration, retained starts, and read-committed end positions. It rejects changed identity/partition count, unsafe retention, missing history before zero, and an external frontier beyond the broker end. End positions only bound valid seeks; they never authorize a processing commit.

Metadata is checked after polling so buffered records cannot silently bridge a detected topic recreation. A nonzero Fetch topic UUID is also checked when supplied by the negotiated protocol. These separate observations are not an atomic lock against administrative changes. The example assumes no concurrent destructive administration; a complete verifier must revalidate identity, retained history, and provenance at its boundary. Current settings and a retained start of zero cannot prove that compaction was never enabled in the past.

Broker keys are UTF-8 JSON objects with exactly `item` and `location` string members. Whitespace and field order may differ; duplicate names, unknown/case-alias names, nulls, invalid UTF-8, and trailing JSON are rejected as key encodings. An invalid key decodes to an empty stock key, which cannot match an accepted transaction. The normal input validator then commits the appropriate rejection and progress together. For an otherwise valid envelope this is `KEY_MISMATCH`; malformed envelopes follow the normal validation precedence.

`ledger.Record.RawKey` preserves the original key bytes, while `Value` preserves the original value bytes. Both serialize as base64 in evidence JSON, allowing NUL and invalid UTF-8. Existing memory fixtures omit `raw_key`; it does not affect canonical source authority. A syntactically valid wrong key and a correct key sent to the wrong partition remain distinct `KEY_MISMATCH` and `ROUTING` cases.

## Assignment, processing, and retirement

[`worker.Run`](https://github.com/sorrell-consulting/amends/blob/main/internal/worker/worker.go) owns one consumer client and a separate metadata client. Its shared [`worker.Lifecycle`](https://github.com/sorrell-consulting/amends/blob/main/internal/worker/lifecycle.go) owns the guarded map of assigned partitions and the independent retirement context. The consumer uses a deterministic group ID derived from the namespace, classic eager range assignment, disabled auto-commit, read-committed fetches, no automatic offset reset, and `PollRecords(ctx, 1)` with `BlockRebalanceOnPoll`.

Each worker process handles its partitions serially. Two processes can own different partitions concurrently.

1. `OnPartitionsAssigned` validates the source and synchronously claims each partition under the lifecycle mutex. PostgreSQL installs the epoch under the same partition-row lock used for processing. The callback reads back epoch and progress and retains only compatible results.
2. `AdjustFetchOffsetsFn` reobserves PostgreSQL state and validates the retained broker boundary. It verifies the saved ownership token and seeks to `next_offset` with `WithEpoch(-1)`. It does not install epochs. A Kafka leader epoch is unrelated to a PostgreSQL ownership epoch.
3. A poll returns at most one record. The worker checks the source again, decodes transport data, and calls the store with its saved epoch and expected frontier. Only a successful commit or a durably resolved committed outcome updates the local next position.
4. Every poll path releases `AllowRebalance`, including timeout, cancellation, and processing failure. Empty timeout fetches contain no business records and create no source decisions.
5. Revoke/loss clears the guarded assignment. Shutdown first cancels the independent runner context, waits for guarded work, invalidates ownership locally, and closes the client outside callbacks. The pinned client can serialize offset adjustment ahead of revocation, so its callback context alone is never used as proof of assignment validity.

The mutex spans token validation and synchronous claiming/processing. No delayed goroutine can install an epoch after invalidation. Independent context cancellation interrupts a blocked SQL claim or offset load; callback and poll work share a 20-second budget, below the one-minute rebalance timeout. PostgreSQL supplies its own shorter statement/lock bounds. A database transaction already holding the partition row may commit before a replacement claim acquires that lock; the store's fence governs that ordering.

[`State`](https://github.com/sorrell-consulting/amends/blob/main/internal/worker/worker.go) exposes the store operations used by the worker: `Claim`, `Observe`, and `Process`. The production implementation is the PostgreSQL store. Tests wrap that store to control delays and caller observations while still using actual broker assignments and database decisions.

The CLI defaults to text logs; `-log-format json` emits JSON objects for machine readers. Each completed `Process` attempt emits a `processing_attempt` event with namespace, partition, source offset, epoch, `storage_call_elapsed_ns`, and an `acknowledged`, `unknown`, `fenced`, or `error` observation. Unknown-recovery reads and retry waits are outside that attempt's elapsed time. A terminal processing failure also emits `processing_stopped` with the same attribution and its final outcome. The `event` codes and named attributes are the drill contract; message wording and field order are presentation. Logs do not change retry/fence decisions, prove commit after an unknown reply, or survive every process crash. The durable store separately retains the [last committed work measurement](verification.md#committed-processing-measurements) with the source frontier; it is visible through inspection even when a successful commit reply was lost. Neither channel is a cumulative or lossless metrics service.

## Unknown outcomes and retry limits

Store calls receive at most three attempts, with cancellable 100 ms and 200 ms waits and a shared operation deadline. A fence is terminal. Exhaustion stops the worker visibly; an explicit restart resumes from durable state. Broker requests also use bounded retry/time settings. This does not establish progress during permanent failure or a fixed latency guarantee.

After an unknown processing commit, the worker rereads epoch and progress before deciding anything else:

| Durable observation | Action |
| --- | --- |
| Same epoch, `next_offset == record.offset + 1` | The decision committed; continue without reapplying it |
| Same epoch, original expected frontier | The decision did not commit; retry the same record |
| Different epoch or unexpected frontier | Retire; do not adopt another owner's state |
| Observation cannot complete | Stop with unresolved-outcome diagnostics; restart must reread |

After an unknown claim, the worker reobserves durable state but does not adopt the observed epoch as proof of its own claim. Under a still-valid assignment it attempts a fresh claim. This may consume another ownership epoch; it cannot allocate duplicate business output versions. Cancellation can leave an outcome unresolved for the next process, which must seek from PostgreSQL.

The [worker scheduler](worker-simulation.md) invokes the same guarded lifecycle methods with memory state, controlled source validation, and logical retry waits. Production callbacks still supply their real 20-second contexts and the normal timer. Cancellation is rechecked after a retry wakeup, before another I/O call. The scheduler exercises lifecycle decisions; the real client callback ordering and SQL claims below remain separate integration evidence.

## Offset visibility and incomplete tails

The source consumer uses read-committed visibility. An aborted transaction's data does not establish revision authority. Offsets remain broker coordinates: a visible record at offset 4 advances the durable frontier to 5 even if the previous visible record was at 0.

Trailing control or filtered records do not produce worker decisions. The runner leaves its frontier after the last visible record, potentially below the broker end. A later visible record can advance across the gap. The integration suite checks this behavior across restart. The bounded verifier returns INCOMPLETE for such a trailing gap because no cursor-only completion rule is implemented. The supplied fixture loader produces nontransactionally.

## Tests and limits of the evidence

With both dependencies running:

```sh
make test-worker
make evidence-worker
```

The tagged suite fails when its configured services are unavailable. `AMENDS_TEST_DATABASE_URL` and `AMENDS_TEST_BROKERS` override the local test endpoints. Tests create random `amends-test-*` source topics and isolated database namespaces, then delete only their own resources. Migrations and unrelated namespaces/topics/volumes remain. An interrupted test may leave its own synthetic data behind.

The suite exercises:

- Two real group members, two active source partitions, correction/cancellation, stale and duplicate input, ownership replacement, and ordinary restart. Hand-derived results and an independent oracle over the published source records check processing output intent; the oracle never reads worker authority state.
- PostgreSQL frontier 1 overriding an actual group commit of 5, with first processing at offset 1.
- Both committed and aborted outcomes behind controlled unknown claim/processing observations. The separate PostgreSQL suite drops actual COMMIT replies on the wire; these lifecycle wrappers specifically check orchestration recovery.
- Cancellation while a real SQL claim is blocked, located using `pg_blocking_pids`; cancellation during a delayed real offset-adjustment callback; and broker-observed rebalance waiting for a polled decision.
- Exact raw-key quarantine, misrouting, recreated topic UUID, increased partition count, deleted retained history, finite retention, and a database frontier beyond the broker end.
- Actual aborted Kafka transactions, visible coordinate gaps, and an incomplete trailing control-record tail across restart.

`artifacts/worker/` receives toolchain/module versions and JSON test events. The CI workflow runs the same suite with the pinned Compose dependencies; hosted execution remains a separate observation. These checks cover the source side of SC18, SC19, SC22, SC25, and SC28.

Broker/server failure, power loss, cooperative/server-side group protocols, and complete source/output/view boundary verification are not established by the source suite. Abrupt process death during a correction is exercised separately by the [showcase](showcase.md). The separate [verification suite](verification.md#evidence-and-limits) checks a guarded, manually handed-off boundary. Controlled lost publication observations after real broker acceptance are covered separately in [delivery tests](output-delivery.md#evidence).

The callback/offset API basis is the pinned [franz-go v1.22.1 documentation](https://pkg.go.dev/github.com/twmb/franz-go@v1.22.1/pkg/kgo), alongside the earlier [client findings](design.md#pinned-client-findings). The implementation tests above supply the narrower local evidence.
