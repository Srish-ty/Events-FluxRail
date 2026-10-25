# FluxRail implementation roadmap

[README](../README.md) · [HLD](HLD.md) · [LLD](LLD.md)

Phase 0 is documentation only. Each later phase requires an explicit implementation request. Commands below are **future validation targets**; referenced files, packages, test cases, and scripts must be created in that phase before the commands are usable. No dependencies, deployments, tests, or benchmark results currently exist.

The architecture specifies the final core MVP contract. Intermediate milestones may expose only a subset, but must not claim reliability that is introduced later. Tests accompany each behavior as it is built. Phase 7 broadens automation and experiments rather than delaying all testing until the end.

## Phase 0 — Documentation and architecture

**Dependencies:** project brief and engineering review.

**Tasks/artifacts:** README, HLD, LLD, this roadmap, and four ADRs. Define states, schema, envelopes, HTTP behavior, transaction boundaries, deployments, risks, and guarantees. Research official platform behavior and distinguish design from implementation.

**Acceptance:** relative documentation links resolve; Mermaid diagrams are syntactically checked; component names, API examples, lifecycle, generation/fencing semantics, outbox, cancellation, and recovery agree. No application or infrastructure files are generated. Diagram syntax validation does not prove the design or deployment works.

**Validation:** inspect all Markdown, check links, run a Mermaid parser when available, then review `git diff --check` and the staged file list. Markdown-only scope can be reviewed with:

```sh
git diff --cached --check
git diff --cached --stat
git diff --cached --name-only
```

**Exit:** architecture is ready for human review. Published design does not mean runtime features are implemented.

## Phase 1 — Local development foundation

**Dependencies:** approved Phase 0 design; supported Go, Docker/Compose; native ARM64 or AMD64 environment.

**Tasks:** initialize the Go module and only useful directories; implement typed configuration, JSON slog, minimal entry points, a Gin router hosted by a configured HTTP server, database/broker connectivity, health/readiness, and versioned migrations from the LLD. Pin Gin alongside the supported Go toolchain. Compose supplies PostgreSQL, single-node KRaft Kafka, persistent volumes, explicit topic creation/listeners, and separate host/container addresses. Verify image manifests and pin exact versions/digests; capture module requirements and image compatibility in a development runbook. Provide a smoke script and example environment file with placeholders only.

**Artifacts:** `go.mod`, `go.sum`, entry points, config/logging/storage foundation, `migrations/`, `deployments/docker/compose.yaml`, `.env.example`, `.gitignore`, and smoke tests/runbook.

**Acceptance:** clean Compose startup; migrations apply to an empty database and report current version; Go connects through the documented host and container routes; probes behave correctly; restart preserves data. Native ARM64 startup is verified before declaring M1 support. No job execution claim yet.

**Planned commands:**

```sh
docker compose -f deployments/docker/compose.yaml config
docker compose -f deployments/docker/compose.yaml up -d --wait
go test ./internal/config/... ./internal/storage/...
go run ./cmd/api
curl -f http://127.0.0.1:9090/readyz
```

The migration runner and smoke script must have exact documented invocations when created. Do not hide migrations behind each application's startup.

## Phase 2 — API and persistence

**Dependencies:** Phase 1 healthy dependencies and migrations.

**Tasks:** implement Gin route groups and request-ID/slog/recovery/error middleware, immediate submission, GET detail/list, typed validation, request limits, keyset pagination, normalized request hashing, idempotency races, transactional job/outbox creation, outbox leasing, publication/backoff, and controller entry point. Future scheduling/cancel/retry inputs must be explicitly rejected until Phase 4, not accepted and stranded. Publish ready messages without starting execution services.

**Artifacts:** API/domain/storage/outbox/Kafka packages, Gin/httptest API contract tests, real database/broker integration tests, sample requests, and publication-failure runbook.

**Acceptance:** matching concurrent idempotency requests create one job; mismatched reuse returns `409`; rollback produces neither job nor ready intent; accepted jobs eventually reach Kafka after broker recovery. Killing the dispatcher after acknowledgement but before marking publication produces duplicate delivery without losing the job. List filters/cursors remain consistent and bounded.

**Planned commands:**

```sh
go test ./internal/api/... ./internal/jobs/... ./internal/outbox/...
go test -tags=integration ./tests/integration/... -run 'TestSubmission|TestOutbox'
go run ./cmd/controller
```

## Phase 3 — Distributed execution

**Dependencies:** Phase 2 ready events and database state.

**Tasks:** handler registry, three bounded safe handlers, frozen checksum examples, atomic claims and attempts, sequential partition runners with process concurrency cap, explicit offset commits, outcome persistence, and graceful drain. Include generation/fence fields, live-lease completion predicates and baseline renewal from the start; recovery becomes complete in Phase 4. Until then, use disposable test jobs and document that interrupted RUNNING jobs do not yet recover automatically. Restrict execution to max_attempts=1 (default one in this development slice); reject higher values until Phase 4. Permanent failures close as FAILED and retryable failures exhaust immediately as DEAD_LETTERED, each with its terminal DLQ outbox intent, preserving the final state semantics. This minimal exhaustion/publication path is a Phase 3 prerequisite; configurable retries and full DLQ operations activate in Phase 4. Existing Phase 2 jobs with larger budgets stay unexecuted until Phase 4 or must be replaced with explicit disposable one-attempt test jobs. Defer RUNNING duplicate records without committing until their outcome is durable; the recovery-based shortcut is unavailable yet.

**Artifacts:** worker executable, handler implementations, claim/outcome queries, partition ownership tests, multi-worker scenario, and shutdown instructions.

**Acceptance:** multiple workers in one group process jobs; only a valid atomic claim creates an attempt; results persist; duplicate terminal messages do not re-execute. Commits never pass unfinished partition work. Revoke/shutdown tests prove no false successes. Phase 3 is not the crash-safe core MVP.

**Planned commands:**

```sh
go test -race ./internal/worker/... ./internal/handlers/...
go test -tags=integration ./tests/integration/... -run 'TestWorkers|TestDuplicate|TestOffsets'
go run ./cmd/worker
```

Run multiple worker processes in separate terminals; three partitions cap useful active partition consumers.

## Phase 4 — Reliability and scheduling: core MVP gate

**Dependencies:** Phase 3 execution and ownership tests.

**Tasks:** future `run_at`, scheduler promotion, persisted jittered retries, full renewal failure handling, expired-attempt recovery, rate-limited same-generation queued repair, terminal DLQ intents, poison quarantine, cancellation, and idempotent child-job manual retry. Adopt the full LLD state/HTTP contracts and remove intermediate limitations.

**Artifacts:** scheduler/recovery packages, additional migrations where needed, complete failure tests, DLQ inspection/retry instructions, and failure matrix with observed outcomes.

**Acceptance:** all [LLD failure scenarios](LLD.md#failure-analysis) pass. Kill a worker during a handler; observe abandonment and bounded retry or exhaustion. Resume a stale worker and reject completion. Cancel against a racing result; exactly one database transition wins. Outbox recovery works across broker outages; no offset advances on uncertain database state. Retention/offset-reset repair republishes eligible queued work. External effects remain subject to the target's idempotency contract.

**Planned commands:**

```sh
go test -race ./internal/...
go test -tags=integration ./tests/integration/...
go test -tags=failure ./tests/failure/...
```

Tests must use isolated containers/data and bounded timeouts; destructive container kills target only the test environment. This phase completes the functional MVP, not production readiness.

## Phase 5 — Observability

**Dependencies:** core MVP state semantics stable.

**Tasks:** instrument the LLD metrics; add database gauges and a single lag collector; expose internal metrics; build optional Compose monitoring profile, Grafana dashboards, and alert rules. Preserve structured logs introduced in Phase 1. Document counter crash windows and missing-metric semantics. Validate health/readiness during outages.

**Artifacts:** instrumentation, Prometheus configuration/rules, Grafana provisioned dashboards, troubleshooting guide, and metrics tests.

**Acceptance:** locally inspect accepted/completed jobs, p50/p95 duration, failures, eligible backlog, in-progress work, outbox age, consumer lag, and worker resources. No job UUIDs or arbitrary errors in metric labels. Collector failure is visible rather than reported as zero lag. Alerts distinguish scheduled future jobs from stuck eligible work.

**Planned commands:**

```sh
go test ./internal/observability/...
docker compose -f deployments/docker/compose.yaml --profile monitoring up -d --wait
curl -f http://127.0.0.1:9090/metrics
promtool check config deployments/docker/prometheus.yml
```

Exact probe/scrape port mappings and the `promtool` container invocation must be documented with the new configuration.

## Phase 6 — Kubernetes and autoscaling

**Dependencies:** core MVP and observability; kind, kubectl, Helm; verified multi-architecture images.

**Tasks:** nonroot Docker builds, ARM64/AMD64 images, Helm chart, kind runbook, single-instance in-cluster Kafka/PostgreSQL, topic init/migration jobs, probes, requests/limits, Secrets references, persistence, manual worker scale, then optional KEDA. Keep operator installation separate. Demonstrate canonical all-in-kind topology first; hybrid networking is optional follow-up work.

**Artifacts:** Dockerfiles, chart, kind configuration, demo values, image-loading/run scripts, KEDA integration, and screenshots backed by repeatable commands.

**Acceptance:** host API port-forward accepts work; pods resolve Kafka's advertised address and PostgreSQL DNS; jobs complete after pod restarts. Manual scale and KEDA lag growth/drain work with a one-to-three replica range. Scale-down cancels/drains safely and abandoned work recovers. No high-availability claim for single-instance infrastructure or cluster-local PVCs.

**Planned commands:**

```sh
docker buildx build --platform linux/arm64,linux/amd64 -f deployments/docker/Dockerfile .
kind create cluster --name fluxrail --config deployments/kind/config.yaml
helm lint deployments/helm/fluxrail
helm template fluxrail deployments/helm/fluxrail
helm upgrade --install fluxrail deployments/helm/fluxrail --namespace fluxrail --create-namespace
kubectl -n fluxrail rollout status deployment/fluxrail-worker
kubectl -n fluxrail scale deployment/fluxrail-worker --replicas=3
```

Multi-platform build validation and loading a single native image into kind are separate operations. The runbook must provide the native `--load` build and `kind load docker-image` commands. KEDA validation requires its compatible operator installed before enabling chart autoscaling. Use `helm template` review before applying resources.

## Phase 7 — Testing, performance, and showcase

**Dependencies:** reproducible local modes and complete feature tests.

**Tasks:** automate Go unit/race/integration tests in GitHub Actions; expand failure scenarios; implement k6 submission workload and completion observation; run controlled comparisons across worker counts/partition counts. Update design/runbooks to match implementation. Produce a benchmark report with raw data and exact reproduction commands.

**Artifacts:** `.github/workflows/`, load scripts, sample data, benchmark report, demo guide, and updated architecture/status documentation. Add a CI badge only once a real workflow exists.

**Acceptance:** clean-checkout CI works; correctness tests pass before load runs. Report accepted, completed, failed, cancelled, and still-pending jobs separately; include throughput, p50/p95 end-to-end latency, consumer lag, resources, duplicate dispositions, recovery time, and error rates. Record laptop/VM budget, CPU architecture, dependency digests, workload distribution, timeouts, warm-up, run duration, repetitions, and raw results. Explain failures and saturation; do not infer capacity from API acceptance alone.

**Planned commands:**

```sh
go test ./...
go test -race ./internal/...
go test -tags=integration ./tests/integration/...
go test -tags=failure ./tests/failure/...
k6 run tests/load/submit.js
```

Performance numbers remain unknown until actual runs complete. Do not write expected throughput improvements into acceptance criteria.

## Optional work after the core

Evaluate recurring/cron schedules, priority queues, rate limiting, multi-tenancy, gRPC, web dashboard, OpenTelemetry, and dependency-aware workflows only after their value and operational cost are clear. Each needs a new design/ADR and tests, especially changes to scheduling fairness, authorization boundaries, and execution semantics.
