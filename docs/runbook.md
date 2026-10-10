# Runbook

Use this guide to operate an existing local synthetic pipeline. For your first run, use the [evaluation walkthrough](evaluation.md). For deliberate faults in fresh owned namespaces, use the [drill catalog](showcase.md).

## Operating boundary

The reference stack is local, with synthetic data and loopback-only host ports. A **namespace** isolates one run's immutable configuration and state. Normal restarts preserve its source history, revision evidence, and output versions, including retained deletion versions (**tombstones**) that prevent older messages from restoring withdrawn results. They also preserve database **frontiers**: the next source offset for workers and next output offset for the view service. Offsets are positions, not business-record counts. Named volumes provide persistence, not backups or high availability. Never remove volumes as part of routine recovery.

### Prerequisites and connections

Use a local Linux Docker engine with the Compose plugin. On Windows, start Docker Desktop and enable its integration for the WSL distribution containing this checkout. `docker version` must report both client and server; a Compose version alone does not establish daemon availability. Initial startup downloads the pinned images from their registries. Go is needed for application checks, not these dependency targets.

Allow at least 4 GiB of available Docker memory and space for the images and retained data. This is a starting allocation, not a measured minimum: Redpanda has a 3 GiB container limit and a 2 GiB heap with one processing core; Postgres has a 512 MiB limit. Disk use grows with retained history. Use a single checkout at a time for this fixed `amends-local` project; change neither its project name nor ports casually when investigating existing data.

| Service | Image version | Host connection | Persistent volume |
| --- | --- | --- | --- |
| `postgres` | `postgres:18.6-bookworm` | `127.0.0.1:15432`, database/user `amends`, password `amends-local-only` | `amends-local_postgres-data` |
| `redpanda` | `docker.redpanda.com/redpandadata/redpanda:v26.2.3` | Kafka `127.0.0.1:19092` | `amends-local_redpanda-data` |

These credentials are synthetic local development values. Only these two ports are published, both on IPv4 loopback. Containers use the project bridge network and the broker's internal address `redpanda:9092`; its advertised host address is `127.0.0.1:19092`. The broker's admin API is available inside its container and is not published. There is no Console, cloud service, or application container.

### Start, inspect, and stop

Run from the repository root:

```sh
make deps-config  # Parse the configuration; no daemon needed.
make up           # Start both services; wait up to 120 seconds for health.
make ready        # SQL query, broker health, and Kafka metadata probes.
make status       # Include stopped containers and health state.
make down         # Graceful stop; retain containers, network, and volumes.
```

The Makefile always names the Compose file, project, and services explicitly. `make up` returns nonzero if startup or health checking fails; it may leave one or both containers running for inspection. `make ready` does not repair or restart anything and returns nonzero when a required probe fails. A running process can still be unhealthy: Postgres must execute an authenticated TCP `SELECT 1`, and Redpanda must report cluster health. Its health probe uses the pinned `rpk --exit-when-healthy` inside a 30-second container-side timeout, allowing periodically collected health reports to settle after test-owned topic deletion; persistent unhealthy/unavailable state still fails. The additional Kafka metadata probe checks its internal listener. These are dependency checks, not inventory or end-to-end verification.

For bounded diagnostics:

```sh
docker compose --project-name amends-local --file docker-compose.yml logs --tail 100 postgres redpanda
```

If the daemon is unavailable, fix the local Docker/WSL prerequisite. If a port is occupied, identify its owner before changing anything. After a failed startup, `make down` stops any containers this project created. It does not remove their evidence. The same `make up` resumes the retained volumes.

### Persistence and retention

The Postgres 18 image stores its versioned data directory beneath the mounted `/var/lib/postgresql`. Redpanda stores broker state beneath `/var/lib/redpanda/data`. Both use named volumes. No host source directory is used for database or broker data. Image tags pin release versions; they are not immutable digest pins.

[Broker bootstrap settings](https://github.com/sorrell-consulting/amends/blob/main/deploy/redpanda/bootstrap.yaml) disable topic auto-creation and write caching, and select the non-compacting `delete` cleanup policy. The startup command keeps fsync enabled. **Retained topics must explicitly set both `retention.ms=-1` and `retention.bytes=-1` at creation.** The cluster's time-retention default remains finite; do not use it for complete source history.

For example, with the dependencies running, create a small infrastructure-only topic and inspect its effective configuration:

```sh
docker compose --project-name amends-local --file docker-compose.yml exec -T redpanda \
  rpk topic create amends-retention-example --partitions 1 --replicas 1 \
  --topic-config cleanup.policy=delete --topic-config retention.ms=-1 --topic-config retention.bytes=-1
docker compose --project-name amends-local --file docker-compose.yml exec -T redpanda \
  rpk topic describe amends-retention-example --print-configs
```

Expect `cleanup.policy=delete`, both retention values `-1`, and `write.caching=disabled`. `amendsctl source-create` applies and verifies the three retention properties and records the broker UUID in a source manifest; disabled write caching comes from the local broker configuration. `make up` starts dependencies only. Health/readiness probes do not certify topic retention or source completeness. The meaning of the topic settings is documented in [Redpanda's topic properties](https://docs.redpanda.com/streaming/current/reference/properties/topic-properties/).

We retain the full source history for verification, so retention is unbounded. Monitor disk use. Changing a topic's settings can invalidate the full-history verification precondition.

Bootstrap configuration applies only to a fresh broker volume. Editing that file does not reconfigure an existing cluster. Inspect the current settings before applying a configuration change; do not reset a volume to make a configuration change appear to work. Named volumes are persistence, not backups or high availability. A single broker cannot survive destruction of its only data volume.

No reset target is supplied. Removing either named volume destroys its stored history/state; recreating source history invalidates ordinary replay assumptions. Destructive reset requires separate approval naming the affected resources and data. Routine stop/start must never use volume removal or blanket Docker cleanup.

## Observe the pipeline

Workers save processing decisions and intended publications together in a PostgreSQL **outbox**. The relay publishes those committed records; the materializer, or view service, stores the latest version of each result. Inspect each stage separately to find where progress stopped. An ownership **epoch** is a database generation; **fencing** refuses an older worker's commit after a newer generation is installed.

Set `AMENDS_DATABASE_URL` for your stack and `MANIFEST` to the exact saved pipeline manifest. Nothing implicitly selects the last run.

```sh
./bin/amendsctl inspect -manifest "$MANIFEST"
./bin/amendsctl inspect -manifest "$MANIFEST" -format html > inspection.html
./bin/amendsctl view-show -manifest "$MANIFEST"
./bin/amendsctl quarantine list -manifest "$MANIFEST"
./bin/amendsctl conflicts list -manifest "$MANIFEST"
```

`inspect` reports durable source/view frontiers, observed broker ends, offset distances, pending outbox count/age, projections, and diagnostics. Frontiers and captured ends are exclusive positions; an individual record offset names that record's position. Offset distances need not count processed records. Unknown broker ends remain unknown. Worker and view reads have separate completion times; this is not an atomic whole-pipeline snapshot. The static HTML page has no server, JavaScript, external assets, or automatic refresh and always says **NOT VERIFIED**. READY and zero lag do not certify agreement.

`last_processing` contains the last committed decision per partition: source coordinates and epoch, duplicate/stale classification, rewind depth, suffix records, changed envelopes, and qualified client timings. `processing_statistics` contains cumulative committed counts since collection began and the latest 32 samples. Missing history is unknown, not zero or backfilled. Offset gaps, aborted attempts, stale owners, and administrative replay do not add committed decisions. Duplicate/stale flags can overlap; saturated counters are lower bounds.

For slow processing, compare `lock_acquire_ns` with `elapsed_before_commit_ns` and the worker log's complete `storage_call_elapsed_ns`. Acquisition includes network/query/scheduling time, not just server lock wait. Precommit timing excludes the final write and commit response. Failed or unknown attempts remain log observations; the previous committed sample may remain visible. See [measurement definitions](verification.md#committed-processing-measurements) and [aggregates/history](verification.md#committed-aggregates-and-bounded-history). These samples are not long-term percentiles or a latency guarantee.

## Routine verification

Finish source loading, inspect progress, drain pending output, and **stop and join the relay** before comparing:

```sh
./bin/amendsctl verify --quiesce -manifest "$MANIFEST"
```

Follow the full [operating procedure](verification.md#operating-procedure). The command holds producer/relay guards, independently scans the retained source, waits for workers and view, and rechecks the complete source and output boundaries (`H` and `O`, respectively). It does not stop independently launched processes or publish pending rows. An active relay or remaining outbox is INCOMPLETE. After verification, restart the relay before loading more input.

After an uncertain writer/session failure, stop and join the old writer and resolve outstanding publication first. A session lock cannot revoke an in-flight broker request. The owned showcase automates this handoff only for its own finite pipeline.

Keep the original structured report, including namespace/configuration, H/O, completeness checks, diagnostics, and differences. It remains separate from later inspection snapshots.

| Result | Interpretation | Action |
| --- | --- | --- |
| PASS | Complete unqualified agreement | Preserve report for the run |
| PASS_WITH_EXCLUSIONS | Agreement for accepted/repaired input with explicit qualifications | Review exact rejection/conflict evidence; expected fixture manifest required for zero exit |
| BLOCKED | Complete boundary with unresolved highest-revision authority | Request or inject the authorized synthetic higher-revision repair |
| DRIFT | Complete comparable state differs | Stop new input, preserve evidence, investigate before changing state |
| INCOMPLETE | Not drained, source changed, or history unavailable | Restore the missing precondition; do not read as zero drift |
| ERROR | Reliable comparison could not be made | Diagnose infrastructure, schema, protocol, or arithmetic cause |

If known conflicts coexist with an incomplete boundary, the overall report is INCOMPLETE and includes the known conflict diagnostics; affected keys remain BLOCKED. Restore the missing boundary precondition and verify again. At a complete boundary, report BLOCKED if authority remains unresolved. A drained pipeline does not itself resolve source authority.

For negative fault tests, the drill may assert an expected BLOCKED or INCOMPLETE condition. That is successful testing of a refusal to certify, not a passing verifier result.

## Worker stopped or input lag rising

**Symptoms:** source end advances but durable worker progress does not, or a processing attempt reports unknown/fenced/error.

**Confirm:** inspect the partition epoch/frontier, worker logs, SQL connectivity, row-lock waits, and transaction duration. A BLOCKED stock key freezes its business outputs while ingestion and other keys may continue. A deep correction can hold the partition lock and delay both other keys and ownership replacement. Trailing control/filtered broker records cannot be certified as consumed by this adapter; see [source offset limits](source-worker.md).

**Recover:** restart the failed worker through the normal assignment lifecycle so it reads durable progress and seeks that coordinate. Never manually advance or lower an offset. A missing COMMIT reply means **unknown**, not aborted: reread durable state before retrying. Do not allocate replacement versions for work that may already have committed. Duplicate/stale no-op input needs no business repair; preserve evidence and let it drain.

**Verify:** stop input, drain, perform the clean relay handoff, then verify the expected complete result. The [ambiguous-commit](showcase.md#ambiguous-commit-extension) and [input drills](showcase.md#input-drill-extensions) retain precise assertions for these cases. The socket-fault drill requires its documented local non-TLS endpoint; do not disable TLS on another deployment to run it.

## Relay stalled or outbox age rising

**Symptoms:** worker progress advances while pending output and its oldest age grow.

**Confirm:** inspect relay logs, broker connectivity, pending committed envelopes, and durable publication receipts. Check the view separately; an empty process queue is not an empty durable outbox.

**Recover:** restore connectivity and resume the relay from pending state. Stop and join an old relay before replacement. Retry the exact stored bytes and version; never mark an outbox row sent manually or generate new semantic versions to clear lag.

**Verify:** a stalled pipeline is INCOMPLETE. After publication and view application drain, stop/join the relay and verify again. The [relay-stall drill](showcase.md#delivery-drill-extensions) demonstrates both states. Permanent unavailability has no eventual-progress deadline.

## Materializer stopped or view lag rising

**Symptoms:** output publication is complete but view progress lags, or a partition reports a protocol error.

**Confirm:** inspect view logs, its durable frontier, versions/tombstones, and exact offending envelope. Equal version with different content is a protocol error; older or identical delivery is different.

**Recover:** for infrastructure failure, stop/join the old view and restart with its saved manifest and durable progress. Preserve all deletion versions. For conflicting equal-version content, investigate the cause while the affected partition remains stopped; never choose a payload or skip the offending record manually. If correction requires reconstruction, use an isolated fresh namespace after fixing the cause.

**Verify:** recovered ordinary delivery should reach the expected complete status. A protocol failure remains ERROR. [Disappearing-day](showcase.md#delivery-drill-extensions) and [materializer-crash](showcase.md#materializer-crash-extension) demonstrate retained withdrawals across older delivery and selected process deaths. An absent sparse activity day is not a zero balance to insert.

## Highest-revision conflict or malformed input

**Symptoms:** a key is BLOCKED or quarantine grows.

**Confirm:** use `conflicts list` and `quarantine list`, then their `show` commands (`-h` lists selectors). Rejections retain exact raw key/value bytes and source coordinates. Conflict review retains canonical candidates and their source references; candidate order is not authority. Superseded evidence can remain after repair. BLOCKED numeric values are uncertified even if unchanged and other keys continue.

**Recover:** obtain a unique higher complete source revision for conflicting authority. For malformed data, publish a new valid source record and preserve the rejected original. There is no quarantine release or operator flag that chooses source truth. Do not erase historical conflict evidence.

**Verify:** an incomplete boundary stays INCOMPLETE with known conflicts included. A complete ambiguous boundary is BLOCKED. Repair refolds the whole key and preserves qualifications; `PASS_WITH_EXCLUSIONS` exits nonzero unless the exact named diagnostics fixture matches. Never use blanket exclusions. See the [diagnostic drills](showcase.md#diagnostic-drills) and [exact diagnostics contract](verification.md#results-and-exact-diagnostics).

## Ownership replacement and stale workers

**Symptoms:** fenced attempts, repeated ownership changes, or processing paused during reassignment.

**Confirm:** correlate database epochs/frontiers with group assignments and lifecycle logs. Broker leader epochs are not database ownership epochs. A transaction holding the partition row may finish before a replacement claim obtains its lock; once a new epoch is installed, the old token cannot commit later work.

**Recover:** retire obsolete lifetimes and let normal group assignment claim and seek durable state. Never install an epoch from an obsolete callback or bypass the fence. Relay/view singleton session guards prevent healthy duplicate starts but do not establish automatic failover; retain the stop/join requirement.

**Verify:** require unchanged state after a rejected stale write, then complete-boundary agreement after recovery. The [zombie-worker](showcase.md#zombie-worker-extension) drill is controlled database replacement; [rebalance-under-load](showcase.md#rebalance-under-load-extension) exercises actual eager group reassignment. Neither covers arbitrary concurrent failures.

## Drift, replay, and isolated rebuild

**Symptoms:** a complete comparison reports DRIFT, or retained history/configuration checks fail.

**Confirm:** stop new input and retain manifests, exact verifier reports, source coordinates, logs, SQL state, and output evidence. Check source incarnation, configuration, retention, and the reported first difference. Missing history is INCOMPLETE, not a successful comparison of the surviving suffix.

**Recover:** correct the cause before selecting an administrative operation. [Replay and rebuild](replay-rebuild.md) supplies exact commands and reports:

| Need | Safe operation and boundary |
| --- | --- |
| Check consumed retained input against intact compatible state | `replay` defaults to dry run; execution installs an administrative fence and preserves business state, versions, quarantine, outbox, and source frontier |
| Reconstruct from complete sufficient history | `rebuild` defaults to dry run; execution uses a fresh namespace and output lineage, leaving the original untouched |
| Missing required prefix, corrupt intact state, or incompatible configuration | Refuse ordinary replay; establish sufficient authoritative history and compatible configuration before fresh reconstruction |

Execution requires `--execute --quiesce` and the documented stopped/joined application handoff. Replay does not repair corruption or release quarantine. Rebuild does not repair in place, adopt an uncertain partial destination, or implement live consumer cutover. Never reset frontiers, invent opening movements, or scan an arbitrary suffix to obtain PASS. An unknown administrative commit/creation outcome requires inspection; it is not permission to repeat blindly.

**Verify:** administrative completion such as REPLAYED is not verifier PASS. Compare the complete boundary afterward. A rebuild preserves its nested qualified/negative status. The [history-missing drill](showcase.md#missing-history-extension) removes only its own fresh topic's prefix and requires refusal before administrative mutation or destination creation.

## Capacity and recovery limits

Unbounded synthetic topic retention and durable evidence grow disk usage. Inspect local disk, topic, and table sizes; there is no automatic safe evidence/tombstone garbage collector. Arithmetic overflow fails explicitly before progress advances; repair the source/domain decision rather than skipping input. The system has no backup/restore or checkpoint protocol, availability objective, throughput claim, or recovery-time bound. See the [assessment](assessment.md) for the remaining operating limits and [testing](testing.md) for appropriate checks.
