# ADR 0001: Kafka and franz-go for work transport

Status: accepted for proposed design; implementation validation pending. Date: 2026-10-25.

## Context

FluxRail is an infrastructure learning project requiring Apache Kafka. It should demonstrate partitioning, consumer groups, durable delivery, replay/retention, producer acknowledgements, and lag-based scaling. These requirements do not make Kafka a native delayed task queue.

## Decision

Use Kafka with single-node combined KRaft broker/controller locally, explicit ready and DLQ topics, three partitions, replication factor one, job UUID keys, and `fluxrail.workers.v1` consumers. PostgreSQL remains authoritative for eligibility and outcome. Delay/retry timing is a database concern.

Choose **franz-go**. Its explicit offset/rebalance APIs fit the planned partition runners, and its pure-Go implementation avoids a native Kafka library requirement on ARM64 development machines. Producer idempotence, delivery acknowledgements, and manual commit behavior must be set/reviewed explicitly in the pinned version. This is an engineering fit decision, not a performance superiority claim. [franz-go guide](https://github.com/twmb/franz-go/blob/master/docs/producing-and-consuming.md)

## Alternatives

**IBM Sarama** is a viable Go Kafka client with producers and consumer groups. Its consumer-group session/claim API could implement the same design. Prefer franz-go's control surface for this project's explicit ownership and offset handling; do not claim Sarama is unmaintained or incapable. Reconsider if pinned-client correctness tests expose a material problem. [IBM Sarama](https://github.com/IBM/sarama)

Task-oriented queues can simplify per-task acknowledgements, delayed delivery, and priorities. PostgreSQL-only polling would reduce infrastructure and may be sufficient for many actual applications. Neither meets this project's explicit Kafka learning requirement. Other brokers are not substituted.

## Consequences and limitations

- Kafka adds broker operation, advertised-listener networking, retention, and rebalance complexity.
- Initial sequential per-partition execution makes commits safe but allows head-of-line blocking. Useful parallelism is bounded by partitions and per-worker slots.
- Kafka ordering is partition-local; FluxRail does not promise global or business-effect ordering.
- Producer idempotence does not remove application-level outbox resend duplicates.
- Replication factor one and all-ISR acknowledgements provide no redundant broker copy or high availability.
- Exact client/broker versions are pinned and tested in Phase 1. No throughput advantage is asserted.

Details: [HLD](../HLD.md), [Kafka contract](../LLD.md#kafka-contract).
