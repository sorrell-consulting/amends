# References

## Implementation references

Primary sources for storage, broker, and client behavior. Read them alongside the pinned versions in [go.mod](https://github.com/sorrell-consulting/amends/blob/main/go.mod) and [docker-compose.yml](https://github.com/sorrell-consulting/amends/blob/main/docker-compose.yml); documentation alone does not establish application correctness.

**R1: PostgreSQL documentation, Explicit Locking.** Row-lock behavior and transaction lifetime underpin the same-row epoch/processing serialization. [Official documentation](https://www.postgresql.org/docs/current/explicit-locking.html).

**R2: Apache Kafka documentation, Design.** Consumer-position coordination, delivery semantics, and retained-history considerations inform the explicit state/progress and delivery boundaries. [Official design documentation](https://kafka.apache.org/43/design/design/).

**R3: franz-go v1.22.1, kgo.** [Pinned API documentation](https://pkg.go.dev/github.com/twmb/franz-go@v1.22.1/pkg/kgo), [callback/options source](https://github.com/twmb/franz-go/blob/v1.22.1/pkg/kgo/config.go), and [group lifecycle source](https://github.com/twmb/franz-go/blob/v1.22.1/pkg/kgo/consumer_group.go). Relevant symbols include `AdjustFetchOffsetsFn`, `OnPartitionsAssigned`, `OnPartitionsRevoked`, `OnPartitionsLost`, `BlockRebalanceOnPoll`, `NoResetOffset`, `setupAssignedAndHeartbeat`, and `manageFailWait`. See [observed scope and limits](design.md#pinned-client-findings); library documentation does not establish the adapter's correctness.

**R4: Apache Kafka documentation, Topic Configs.** Review deletion/compaction and retention behavior when specifying the source history's preservation. Verify equivalent settings on the selected local broker; Kafka documentation alone does not establish local-broker parity. [Official topic configuration documentation](https://kafka.apache.org/43/configuration/topic-configs/).

**R5: pgx v5.11.0 and PostgreSQL 18.** The database adapter pins pgx and records module checksums. Its transaction cleanup and unknown-result handling follow the [pinned transaction source](https://github.com/jackc/pgx/blob/v5.11.0/tx.go) and [API documentation](https://pkg.go.dev/github.com/jackc/pgx/v5@v5.11.0). Actual row serialization is tested against PostgreSQL 18.6; [PostgreSQL 18 locking documentation](https://www.postgresql.org/docs/18/explicit-locking.html) explains the underlying lock behavior. See the [implemented evidence and limits](postgres-store.md).

The semantic contract and fixture arithmetic are repository specifications, not findings reported by these references. The source/SQL integration tests establish the exercised behavior of the pinned adapters.

## Further reading: concepts and design patterns

These readings provide conceptual background. The [semantic contract](semantics.md) and [testing guide](testing.md) define amends' implemented scope and evidence.

| Reading | Connection to amends |
| --- | --- |
| [Transactional Outbox](https://microservices.io/patterns/data/transactional-outbox.html) — Chris Richardson | Explains committing publication intent with business state and delivering it asynchronously, as in amends' outbox and relay. |
| [How to do distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html) — Martin Kleppmann | Explains why storage must reject stale owners; amends enforces PostgreSQL ownership epochs independently of Kafka producer/leader epochs, without a distributed lease service. |
| [Idempotent Receiver](https://www.enterpriseintegrationpatterns.com/patterns/messaging/IdempotentReceiver.html) — Enterprise Integration Patterns | Introduces safe handling of duplicate messages; amends applies version checks to repeated updates and withdrawals. |
| [Kafka Log Compaction](https://docs.confluent.io/kafka/design/log_compaction.html) — Confluent | Explains compaction and deletion-marker retention, useful when distinguishing broker tombstones from amends' retained view deletion versions. |
| [Kafka Message Delivery Guarantees](https://docs.confluent.io/kafka/design/delivery-semantics.html) — Confluent | Explains delivery guarantees and coordination with external storage, the boundary addressed by amends' database transactions and asynchronous relay. |
| [Bitemporal History](https://martinfowler.com/articles/bitemporal-history.html) — Martin Fowler | Explains retroactive corrections and changing knowledge of history; amends repairs current derived results without offering arbitrary queries over past system-time states. |
| [Arrangements](https://materialize.com/docs/fundamentals/concepts/arrangements/) — Materialize | Explores indexed state for incremental queries and joins, a broader design than amends' independent stock-key refolds. |
| [Compensating Transaction](https://learn.microsoft.com/en-us/azure/architecture/patterns/compensating-transaction) — Microsoft | Explains business-specific recovery workflows for external actions, which amends leaves to downstream applications. |
