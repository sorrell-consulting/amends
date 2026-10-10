# Relay and durable view transport

A correction can commit just before publication fails. To avoid losing it, processing stores the intended publications with its state in a PostgreSQL **transactional outbox**. A separate relay publishes those records, and a consumer (the **materializer**) maintains the latest stored results, or **view**. A withdrawn result retains a deletion version (**tombstone**) so an older delayed message cannot restore it. Retries can duplicate publication; there is no atomic transaction spanning PostgreSQL and the broker.

The output delivery path connects committed PostgreSQL outbox rows to a retained Redpanda output topic and a durable view consumer. The running path includes source workers, relay, output broker, and view. Integration tests exercise the correction and cancellation through that whole path, compare the materialized result with hand-derived fixtures and the independent source oracle, and check selected retry/restart cases.

The [bounded operational verifier and inspection metrics](verification.md) use an explicit stopped-relay handoff. The [showcase](showcase.md) performs a real worker crash and automates drain/clean relay handoff for its own pipeline. `view-show` reports a live database snapshot; it does not establish a complete source/output boundary or print verifier PASS.

## Run the pipeline

Use the pinned local dependencies and build from this repository:

```sh
make up
make build
mkdir -p artifacts
export AMENDS_DATABASE_URL='postgres://amends:amends-local-only@127.0.0.1:15432/amends?sslmode=disable'
```

For a fresh run, choose unused namespace and source/output topic names. Existing source manifests can also be used if that namespace has not already published or materialized output without a binding.

```sh
./bin/amendsctl source-create -namespace pipeline-demo-1 -topic amends-pipeline-demo-1-source \
  > artifacts/source-demo-1.json
./bin/amendsctl output-create -manifest artifacts/source-demo-1.json \
  -topic amends-pipeline-demo-1-output > artifacts/pipeline-demo-1.json
```

In separate terminals, export the same database URL and run two source workers, one relay, and one view consumer:

```sh
./bin/amends -role source -manifest artifacts/pipeline-demo-1.json
./bin/amends -role source -manifest artifacts/pipeline-demo-1.json
./bin/amends -role relay -manifest artifacts/pipeline-demo-1.json
./bin/amends -role view -manifest artifacts/pipeline-demo-1.json
```

Each command runs in the foreground. `-role source` is the default, preserving the previous worker command. The fixture has one key on partition zero; the real integration test uses two configured keys on different partitions to exercise both source workers actively. `-brokers` defaults to `127.0.0.1:19092` on all broker-facing commands.

Publish and inspect each stage:

```sh
./bin/amendsctl source-load -manifest artifacts/pipeline-demo-1.json -file fixtures/before.jsonl
./bin/amendsctl view-show -manifest artifacts/pipeline-demo-1.json
./bin/amendsctl source-load -manifest artifacts/pipeline-demo-1.json -file fixtures/correction.jsonl
./bin/amendsctl view-show -manifest artifacts/pipeline-demo-1.json
./bin/amendsctl source-load -manifest artifacts/pipeline-demo-1.json -file fixtures/cancel-last-day.jsonl
./bin/amendsctl view-show -manifest artifacts/pipeline-demo-1.json
```

Publication and materialization are asynchronous. A snapshot may show an incomplete repair until the pipeline catches up. The clean default fixture normally reaches source frontier 5 and output/view frontier 11 in partition zero. Retries can add duplicate output positions, so 11 is not a general completion rule. After cancellation, balances are 70 on January 1 and 50 on January 2, the valid alert is on January 2, and January 3's balance remains as a version-3 tombstone.

`view-show` prints projections, all retained envelopes including tombstones, durable view positions, and protocol-error messages from one repeatable-read snapshot. It is read-only and creates neither schemas nor namespaces. Its `scope` explicitly says it is not verification. Known protocol errors remain visible in its JSON even though the inspection command itself can successfully read the snapshot.

SIGINT/SIGTERM stops each runner. Restart with the saved pipeline manifest. Stop the runners before `make down`; dependency volumes remain intact. Do not recreate topics, clear protocol diagnostics, or reset counters to force a restart to pass.

## Output identity and schema upgrade

The source configuration and its digest stay unchanged. The optional `output` manifest object records output topic name, broker UUID incarnation, and partition count. PostgreSQL stores the same immutable tuple in `amends_output_binding`. An incarnation is unique across namespaces in this database, preventing accidental shared output lineage.

`output-create` creates an empty retained topic with the same partition count as the source. It uses the same explicit non-compacting, unlimited-retention properties as source creation. Binding an already processed namespace is allowed when its output remains pending and the view is untouched. Existing receipts, view rows/progress, or view errors without a binding prevent adoption.

Binding serializes on the namespace, processing-partition, and view-progress rows. Repeating the exact binding is idempotent; changing any field fails. A lost binding commit reply is resolved through a database reread. If the binding exists but the manifest output was lost, repeat `output-create` with the same source manifest and output topic name: it validates the existing binding and prints it again. An unbound topic left after failed/unknown creation is not automatically adopted.

Migration [002_output_transport.sql](https://github.com/sorrell-consulting/amends/blob/main/migrations/002_output_transport.sql) adds the binding table and `error_offset`, `error_key`, and `error_value` columns to view progress. The migration runner checks an ordered, contiguous checksum history and applies missing migrations atomically under its advisory transaction lock. Real tests upgrade populated version-1 state, rerun migration idempotently, and reject a changed checksum.

Both downstream runners compare their manifest to the database binding and validate the output topic's UUID, partition count, retention, and retained starts. The view also refuses a stored frontier beyond the current broker end. Topic recreation requires a fresh namespace; a new broker UUID cannot be substituted into the existing binding. These metadata observations are separate from SQL commits, so concurrent destructive administration remains outside the supported operating procedure.

## Relay decisions and publication receipts

[`RelayStep`](https://github.com/sorrell-consulting/amends/blob/main/internal/delivery/relay.go) performs one pending delivery for one partition:

1. Read the earliest unacknowledged outbox sequence and its exact stored envelope bytes.
2. Publish those bytes with the envelope's stock key and the source partition's corresponding output partition. The producer uses manual partition selection and all-in-sync-replica acknowledgements.
3. After a successful broker acknowledgement, persist that output offset against the outbox sequence.
4. Only then proceed to a later sequence in that partition.

The runner services partitions serially and never calls the business diff. Retrying publication cannot allocate a new output ID/version/cause. A lost publication observation leaves the row pending and can cause an exact duplicate. The producer allows cancellation of uncertain in-flight sends and has a delivery deadline; it does not claim exactly-once transport.

Once an output offset is known, the current attempt retries only the database receipt. On an unknown SQL commit it reads `Receipt`: a matching offset resolves success, no receipt permits retrying the same receipt, and a different receipt or unavailable observation stops the runner. It does not blindly republish after a possibly committed receipt. A process restart after accepted publication but before any stored receipt may still publish the same envelope again.

Retries have three attempts, 100/200 ms cancellable waits, and a shared 20-second operation budget. Exhaustion stops the service visibly. Restart is explicit resumption from durable pending state. Broker metadata validation and idle polling also have bounded contexts. The small I/O interfaces retain publication, acknowledgement, and observation as distinct operations; the [delivery scheduler](delivery-simulation.md) drives the same orchestration with controlled memory I/O and logical waits, while the production loop retains real timers and deadlines.

## View decisions and protocol failures

The single materializer directly assigns all healthy output partitions, seeking to PostgreSQL's `next_offset` with no automatic reset and no group commits. It uses read-committed visibility and checks topic identity after each poll, including buffered records. A supplied Fetch UUID is also checked. Like the source adapter, it does not advance a durable frontier using a high watermark or trailing control records alone.

[`DecodeOutput`](https://github.com/sorrell-consulting/amends/blob/main/internal/broker/output.go) validates UTF-8, complete required objects, exact field names, and the stock-key match. Unknown members, duplicates, case aliases, null fields, missing required fields, and trailing JSON are protocol failures. Business envelope/namespace/routing/cause checks remain in the pure view implementation. Its common-envelope, business-output, and key-status validators report the failed rule without changing rows or advancing progress. Wire decoding does not supply revision authority or modify the independent oracle.

[`ApplyViewInput`](https://github.com/sorrell-consulting/amends/blob/main/internal/store/postgres/view.go) locks the view-progress row and checks its expected frontier. Greater output versions replace current rows, equal identical envelopes are duplicates, and lower versions leave current state unchanged. Equal-version different content, including a changed cause, is a protocol violation. Business state, retained version/tombstone, and progress commit together; ignored duplicates also advance progress atomically.

A protocol failure commits its reason and exact broker offset/key/value without changing rows or advancing the frontier. It is not source quarantine. The runner pauses that partition and continues healthy partitions; if all partitions are stopped, it exits with a protocol error. Restart retains the stop and excludes the affected partition. There is no automatic clear/skip command. Raw diagnostic bytes remain available in the view-progress row for investigation.

After an unknown view commit, the runner rereads position and error state. A durable protocol error remains an error. The expected next coordinate resolves an applied decision; unchanged progress permits retry; unexpected progress or failed observation retires the runner. A delayed statement cannot resurrect a row whose higher deletion version remains stored.

## Singleton operation and failure limits

Each relay/view runner holds a dedicated PostgreSQL session advisory lock for its namespace and role. A second healthy instance fails visibly. The connection is removed from the pool and closed on shutdown, so its session lock cannot leak into pooled application transactions. The runner checks the session and never silently reconnects it after loss. Actual database tests exercise duplicate starts, independent roles, backend termination, and a replacement session.

Local connection close does not acknowledge server-side lock release: PostgreSQL's [termination protocol](https://www.postgresql.org/docs/18/protocol-flow.html#PROTOCOL-FLOW-TERMINATION) lets the client send Terminate and close immediately, while the backend cleans up when it receives the disconnect. After a known clean stop/join, a brief `ErrServiceBusy` can therefore precede successful replacement admission. Observe release or retry admission within a bounded wait; never bypass the guard or infer that a still-running or uncertain old writer has stopped. The producer-handoff integration test forces this ordering at a test-only socket boundary against the actual database.

This is an operational singleton guard, not a broker fencing or automatic failover protocol. A request already in flight cannot be revoked by a PostgreSQL lock. Stop and join the old runner before starting a replacement; after an uncertain failure, preserve the pending envelopes and diagnostic evidence. Duplicate output is safe for the versioned view, and the [bounded verifier](verification.md) requires the operator to stop and join the relay, then holds its service guard before capturing the output boundary. Concurrent orphan relay attempts are outside the tested failure model.

## Evidence

With both pinned services running:

```sh
make test-delivery
make evidence-delivery
make evidence-postgres
```

The delivery suite uses actual PostgreSQL and Redpanda. It creates isolated synthetic namespaces/topics and removes only those resources. Evidence under `artifacts/delivery/` records the toolchain, module versions, and JSON test events. CI runs this target after source-worker integration; hosted results must still be observed separately.

Tests cover the complete transport path through all three hand-derived fixture stages with two source workers and two active keys; comparison to an independent oracle over the owned published source records; cancellation followed by restart and older output; exact duplicate publication after controlled acknowledgement loss; relay restart after broker acceptance but before a receipt; committed and unchanged database state behind controlled unknown observations; durable equal-version conflict evidence; healthy-partition progress beside a stopped partition; malformed-output rejection including NUL-containing diagnostic names; and output-topic recreation refusal.

The delivery wrappers return unknown observations either before a database write or after a real successful write/publication. Focused tests also require retirement when observations are unavailable or contradict the expected receipt/frontier. They do not drop Kafka wire packets or kill an OS process mid-transaction. The PostgreSQL suite drops actual COMMIT requests/replies and tests atomic view recovery. These distinctions limit what the evidence establishes for SC16–SC21, SC25, and SC28.

The [bounded verifier](verification.md) implements operational source scanning and complete-boundary comparison. The separate [owned showcase](showcase.md) includes worker and [materializer process-crash drills](showcase.md#materializer-crash-extension). The latter kills an actual view process at a pre-upsert SQL barrier, verifies unchanged durable view state/progress, and exercises restart plus older delivery after a second, confirmed-postcommit process death. These drills do not cover lost wire packets or database-server failure. The API basis is the pinned [franz-go documentation](https://pkg.go.dev/github.com/twmb/franz-go@v1.22.1/pkg/kgo) and [PostgreSQL session advisory locks](https://www.postgresql.org/docs/18/explicit-locking.html#ADVISORY-LOCKS); the tests establish the narrower local behavior described above.
