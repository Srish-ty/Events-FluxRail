# ADR 0004: Compose first, canonical all-in-kind demonstration later

Status: accepted for proposed design; implementation validation pending. Date: 2026-10-25.

## Context

The system must run without mandatory cloud spend and fit a developer's Apple Silicon M1 environment. Kubernetes is valuable for deployment/scaling education, but it should not slow the first correctness iterations. Kafka advertised addresses make mixed host/container/cluster topologies particularly sensitive to networking assumptions.

## Decision

Mode A uses Docker Compose for Kafka/PostgreSQL with Go processes on the host or optionally in containers. Prometheus/Grafana are opt-in. Document distinct reachable Kafka listeners for host and container clients.

Mode B uses kind and Helm with single-instance Kafka/PostgreSQL inside the cluster alongside Go deployments. Use stable in-cluster service DNS, explicit topic/migration jobs, PVCs, and API port-forward. Load native architecture images into kind. KEDA operator and observability are optional additions; KEDA starts with at least one worker and a maximum matching ready-topic partitions. [kind documentation](https://kind.sigs.k8s.io/docs/user/quick-start/)

Use native Linux ARM64 on M1 and AMD64 on compatible hosts. Verify exact manifest support and pin image digests in implementation, rather than assuming a repository name guarantees multi-architecture support. Laptop resource figures are starting budgets to measure, not validated minima.

## Alternatives

All-in-Kubernetes from the beginning increases startup time and resource use before core behavior exists. Compose-only is simpler but omits the requested deployment/scaling showcase. Cloud-managed services introduce spend and credentials without being necessary for local learning.

A hybrid kind/Compose mode can reuse dependencies but requires explicit Docker networking, broker advertised DNS reachable from pods, and a reproducible pod connectivity test. It is optional, not the default demonstration path. Pod `localhost` is never the host database or broker.

## Consequences and limitations

Compose and kind need separate tested topology documentation. Stateful single instances are demo infrastructure, not production HA. PVCs support pod restarts but cluster deletion loses local data. Monitoring/KEDA may need to be disabled on smaller machines. Production deployment would additionally require secrets governance, TLS/authentication, broker replication, database backups, failure-domain design, and restore evidence; these are outside Phase 0 implementation scope.

Details: [local deployment](../HLD.md#local-deployment), [Phase 6](../ROADMAP.md#phase-6--kubernetes-and-autoscaling).
