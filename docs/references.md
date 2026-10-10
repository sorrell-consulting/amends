# Implementation references

Primary sources for storage, broker, and client behavior. Read them alongside the pinned versions in [go.mod](https://github.com/sorrell-consulting/amends/blob/main/go.mod) and [docker-compose.yml](https://github.com/sorrell-consulting/amends/blob/main/docker-compose.yml); documentation alone does not establish application correctness.

**R1: PostgreSQL documentation, Explicit Locking.** Row-lock behavior and transaction lifetime underpin the same-row epoch/processing serialization. [Official documentation](https://www.postgresql.org/docs/current/explicit-locking.html).

**R2: Apache Kafka documentation, Design.** Consumer-position coordination, delivery semantics, and retained-history considerations inform the explicit state/progress and delivery boundaries. [Official design documentation](https://kafka.apache.org/43/design/design/).

**R3: franz-go v1.22.1, kgo.** [Pinned API documentation](https://pkg.go.dev/github.com/twmb/franz-go@v1.22.1/pkg/kgo), [callback/options source](https://github.com/twmb/franz-go/blob/v1.22.1/pkg/kgo/config.go), and [group lifecycle source](https://github.com/twmb/franz-go/blob/v1.22.1/pkg/kgo/consumer_group.go). Relevant symbols include `AdjustFetchOffsetsFn`, `OnPartitionsAssigned`, `OnPartitionsRevoked`, `OnPartitionsLost`, `BlockRebalanceOnPoll`, `NoResetOffset`, `setupAssignedAndHeartbeat`, and `manageFailWait`. See [observed scope and limits](design.md#pinned-client-findings); library documentation does not establish the adapter's correctness.

**R4: Apache Kafka documentation, Topic Configs.** Review deletion/compaction and retention behavior when specifying the source history's preservation. Verify equivalent settings on the selected local broker; Kafka documentation alone does not establish local-broker parity. [Official topic configuration documentation](https://kafka.apache.org/43/configuration/topic-configs/).

**R5: pgx v5.11.0 and PostgreSQL 18.** The database adapter pins pgx and records module checksums. Its transaction cleanup and unknown-result handling follow the [pinned transaction source](https://github.com/jackc/pgx/blob/v5.11.0/tx.go) and [API documentation](https://pkg.go.dev/github.com/jackc/pgx/v5@v5.11.0). Actual row serialization is tested against PostgreSQL 18.6; [PostgreSQL 18 locking documentation](https://www.postgresql.org/docs/18/explicit-locking.html) explains the underlying lock behavior. See the [implemented evidence and limits](postgres-store.md).

The semantic contract and fixture arithmetic are repository specifications, not findings reported by these references. The source/SQL integration tests establish the exercised behavior of the pinned adapters.
