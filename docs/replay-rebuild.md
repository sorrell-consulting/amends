# Controlled replay and isolated rebuild

`amendsctl replay` checks already-consumed input against intact compatible durable state. `amendsctl rebuild` reconstructs processing state and the consumer's stored results (the **materialized view**) from sufficient retained source history in a fresh **namespace**, an isolated configuration/state identity. Replay does not reconstruct missing state; rebuild does not modify the original namespace. These commands implement S11, SC23, and SC24; neither provides live consumer cutover or repair of an existing view in place.

Use the pinned toolchain, PostgreSQL, and Redpanda described in the README. Set `AMENDS_DATABASE_URL` to the local database and supply an explicit saved manifest. Both commands default to a checked dry run. Execution requires `--execute --quiesce`, confirming that ordinary source workers and source loaders have been stopped and joined. Drain and stop/join the original relay and view before comparing an unchanged original pipeline in a drill. The commands do not kill or adopt independently launched services.

## Replay

For a manifest whose partition zero has already consumed offsets `[0, 4)`:

```sh
./bin/amendsctl replay -manifest pipeline.json -partition 0 -from-offset 0 -until-offset 4
./bin/amendsctl replay -manifest pipeline.json -partition 0 -from-offset 0 -until-offset 4 --execute --quiesce
```

The command validates source incarnation, partition mapping, retention from the declared beginning, configuration, and the requested consumed range. It scans the complete stopped source boundary and selects the requested interval. Read-committed offset gaps are permitted; an interval ending in invisible/control offsets cannot be certified by this scanner and fails explicitly.

The PostgreSQL replay transaction locks the partition row and checks the observed epoch and frontier again. Accepted input must already have matching canonical revision evidence and source coordinates. Rejected input must retain the exact raw record, raw key, and reason. Refolding/reconciling accepted records must require no new envelopes. Failure rolls back without installing ownership. Successful execution commits a higher epoch and preserves all business state, output versions, withdrawals, quarantine, outbox rows, and source progress. Restart normal workers afterward so they acquire fresh ownership and seek the unchanged frontier.

`DRY_RUN` and `REPLAYED` describe administrative completion, not a verifier PASS. Run the usual bounded verifier after resuming/draining the pipeline. A lost replay COMMIT reply remains an error with an unknown outcome; inspect the durable epoch before retrying. Replay cannot reconstruct missing authority evidence, release quarantine, or repair drift.

## Rebuild

Choose an unused namespace and unused output topic:

```sh
./bin/amendsctl rebuild -manifest pipeline.json -new-namespace rebuilt-demo -output-topic rebuilt-demo-output
./bin/amendsctl rebuild -manifest pipeline.json -new-namespace rebuilt-demo -output-topic rebuilt-demo-output --execute --quiesce
```

Rebuild reads the original retained source topic under the same schema, ledger origin, keys, partition mapping, opening balances, and thresholds. Only the namespace and output lineage change. Unlike replay, rebuild does not require the original processing tables or namespace to survive: a sufficient source and its saved configuration are the reconstruction inputs. If original state exists, its configuration must match the manifest. Missing database schema is installed only during execution, after source validation. It refuses existing destination state or an existing output topic and offers no middle-offset option. Validation and the full source scan precede destination creation.

All cooperating loaders, verifiers, and rebuilds lock by source incarnation for the producer role. This prevents a loader using either namespace from changing their shared source while a guard is held. Relay and view guards remain namespace-specific. Every cooperating process must use the same source-incarnation guard; namespace-scoped producer guards cannot protect a source shared by multiple namespaces. Session loss fails the operation; it does not establish broker fencing against an orphan request.

The destination uses the production worker lifecycle for assignment, external-frontier seeking, and processing. Its owned relay publishes the committed envelopes and drains; its owned view materializes them and joins. The command then uses the independent broker-history verifier, requiring the same source end vector captured before reconstruction. An append during the guard handoff cannot certify a different history. The original namespace is never written.

Each execution retains `manifest.json` and `report.json` in a fresh `artifacts/rebuild/run-*` directory, printed on stderr. The manifest is saved as destination resources become known so failed attempts remain inspectable. Failure can leave a partial new namespace or topic; there is no automatic destructive cleanup or adoption of that destination on retry. An unknown creation or commit outcome is an error requiring inspection. Choose a fresh destination for another full rebuild.

The nested verification report preserves `PASS`, `PASS_WITH_EXCLUSIONS`, `BLOCKED`, `DRIFT`, `INCOMPLETE`, and `ERROR`. Only strict PASS exits zero by default. `-expected-diagnostics FILE` permits a qualified result only for an exact named fixture matching the new namespace, digest, source boundary, and diagnostics. It never changes the printed qualification or suppresses an unresolved conflict.

## Evidence and drills

```sh
make test-postgres
make test-admin
make evidence-admin
make drill DRILL=replay-intact
make drill DRILL=full-rebuild
make drill DRILL=history-missing
```

The PostgreSQL suite exercises replay preservation, rejected ranges/evidence, actual row-lock waiting, changed-frontier refusal, stale-worker fencing, and both database outcomes behind lost replay commit replies. It also checks fresh-namespace refusal and the shared-source writer guard. The broker suite exercises dry runs, real consumed-input replay, isolated reconstruction, original state/view preservation, hand-derived correction/withdrawal values, old-namespace output rejection, qualified/conflicted histories, and cancellation.

The replay/rebuild operator drills first run the real correction showcase and join its processes. They then run the selected operation, compare durable original state and view (allowing only replay's new epoch), check the hand-derived 70/50/100 balances and Day 2 crossing, and require strict bounded verification. Reports and manifests remain under `artifacts/operations/`. The existing `crash-mid-correction` drill remains available. CI includes the suite and retained drills; [testing](testing.md#evidence-artifacts-and-ci) explains evidence collection.

The [missing-history drill](showcase.md#missing-history-extension) instead removes a required prefix only from its own fresh source topic after stopping/joining its services. Verification, replay of the surviving range, and rebuild must all retain INCOMPLETE. The original namespace, epochs, version/tombstone state, and output/view progress remain unchanged; actual SQL/broker checks require no new destination. Its wrapper succeeds by checking refusal, not by recovering history or converting INCOMPLETE to verifier PASS.
