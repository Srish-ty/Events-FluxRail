# ADR 0003: At-least-once delivery, idempotency, and fenced ownership

Status: accepted for proposed design; implementation validation pending. Date: 2026-10-25.

## Context

Broker acknowledgements, database commits, worker crashes, and external side effects do not share one transaction. A restarted consumer may see a completed job again; a disconnected worker may keep computing after its lease expires.

## Decision

Adopt at-least-once transport with database duplicate suppression. Submission keys map normalized requests to one job. Each ready event carries a dispatch generation. Workers claim only a current `QUEUED` generation, increment the attempt number and fencing token atomically, and create one live attempt. Completion and renewal require matching owner/token and an unexpired lease.

Offsets advance only after durable disposition, sequentially per partition. Recovery closes expired attempts and retries within the attempt budget. Cancellation fences ownership in its transaction. Manual retry creates a new child job rather than reopening terminal history. [LLD lifecycle](../LLD.md#lifecycle)

Automatic retries use stable job IDs as external idempotency keys. Targets supporting fencing should reject older tokens as well. This target-side contract is required for effects beyond FluxRail's database. Kafka-level guarantees do not imply external exactly-once behavior. [Kafka design](https://kafka.apache.org/41/design/design/)

## Alternatives

- At-most-once dispatch/early unconditional commits reduce duplicates but can abandon accepted work.
- End-to-end exactly-once execution would require transactional participation or idempotent/fenced behavior from every effect target; it cannot be promised by Kafka configuration alone.
- Worker process heartbeats without per-attempt leases cannot establish which attempt owns a particular job.
- Deduplicating only by Kafka event ID cannot distinguish a valid new retry generation from an obsolete execution request.

## Consequences and limitations

The database rejects stale results; it cannot stop a disconnected process from writing to another system. Cancellation means FluxRail's terminal state has changed, not that every external effect ceased. Leases introduce renewal load and require careful handling of uncertain database outcomes. Consumer revocation and shutdown need integration tests with the chosen client.

No exact recovery latency, no end-to-end exactly-once execution, and no recovery from permanent authoritative-data loss are promised. The reliability contract becomes a demonstrated capability only after Phase 4 failure tests pass.

Details: [failure analysis](../LLD.md#failure-analysis), [delivery and consistency](../HLD.md#delivery-and-consistency).
