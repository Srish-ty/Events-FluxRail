# ADR 0002: PostgreSQL source of truth and transactional outbox

Status: accepted for proposed design; implementation validation pending. Date: 2026-10-25.

## Context

Accepting a job requires durable metadata and eventual Kafka delivery. Writing the database and broker independently can lose dispatch after a successful acceptance or publish work whose database transaction later fails. Schedules, attempts, cancellation, and execution ownership also need consistent state.

## Decision

PostgreSQL owns jobs, attempts, schedules, leases, and publication intents. Commit a ready outbox row in the same transaction as immediate acceptance or due promotion. Commit terminal failure and DLQ intent together. Use pgx and reviewable SQL migrations through golang-migrate. [golang-migrate](https://github.com/golang-migrate/migrate)

The dispatcher takes expiring publication claims in short transactions, publishes outside the transaction, and marks success only after acknowledgement and with its matching claim token. Failed/ambiguous publications remain retryable. The scheduler and recovery loops lock bounded batches using `FOR UPDATE SKIP LOCKED`; they never hold a job lock during handler execution or network publication. [PostgreSQL SELECT](https://www.postgresql.org/docs/current/sql-select.html)

## Alternatives

- Direct database/broker dual write: less code, but an unsafe crash gap.
- Kafka as the only system of record: possible with a substantially different event-sourced architecture; complicates query, scheduling, cancellation, and claim semantics for this scope.
- Distributed transactions across PostgreSQL and Kafka: operational complexity and no general solution for arbitrary handler side effects.
- Change-data-capture outbox relay: valid later option, but introduces another service and operational surface before a polling relay has been measured.

## Consequences and limitations

Acceptance survives broker downtime while database storage and admission limits permit. There is no atomic transaction between PostgreSQL and Kafka; a crash after publication but before acknowledgement persistence duplicates the event. Workers must tolerate duplicates.

Polling and leases add write volume, indexes, and maintenance work. Published outbox cleanup must preserve unpublished rows. Queued reconciliation republishes the same dispatch generation after a cooldown so Kafka retention does not become the only recovery mechanism. Permanent database volume loss still requires backups and restore; outbox is not a backup.

Details: [schema](../LLD.md#database-schema), [execution algorithms](../LLD.md#reliable-execution-algorithms).
