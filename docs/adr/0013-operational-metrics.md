# ADR 0013 — Derived low-cardinality operational metrics

Status: accepted

## Decision

SciRust Hub exposes Prometheus text-format metrics at `GET /metrics`. The
endpoint uses the Prometheus text exposition format `0.0.4` with content type
`text/plain; version=0.0.4; charset=utf-8`, deterministic ordering and a final
newline.

Metrics are **derived on every scrape** from authoritative persisted Hub state:
registered component manifests, run records, workflow records, artifact
metadata and the lifecycle-event high-water sequence. No mutable metric counter
is updated on Hub write paths, so metrics cannot diverge from the control-plane
records after a restart or partial failure.

The initial series are intentionally low-cardinality:

- `scirust_hub_build_info{version=...}`;
- `scirust_hub_executor_info{backend=...}`;
- `scirust_hub_component_manifests`;
- `scirust_hub_run_records{state=...}`;
- `scirust_hub_workflow_records{state=...}`;
- `scirust_hub_workflow_attempts`;
- `scirust_hub_artifacts`;
- `scirust_hub_artifact_bytes`;
- `scirust_hub_lifecycle_event_high_water_sequence`.

No component, run, workflow, artifact or attempt UUID is exported as a metric
label. This avoids unbounded time-series cardinality and unnecessary exposure
of per-execution identifiers.

All snapshot series are gauges. Even quantities that currently tend to grow are
named as current persisted-record counts rather than counters, because future
retention/compaction could legitimately decrease them.

## Security boundary

`/metrics` is placed behind the same optional bearer middleware as `/api/v1`.
When `SCIRUST_HUB_TOKEN` is configured, scraping requires that bearer token.
Loopback deployments without a token retain the existing local-development
behavior. `/health` and `/ready` remain the only intentionally unauthenticated
operational probes.

Metrics contain aggregate counts, artifact byte totals, build version and the
configured executor backend. They do not contain bearer credentials,
environment values, process output, artifact contents or entity UUID labels.

## Cost model

A scrape lists current component/run/workflow/artifact metadata and therefore
costs O(number of retained records). The lifecycle high-water mark has a direct
repository operation and does not scan the event stream. This favors a small,
truthful implementation over a second mutable aggregation database. If future
retention volume makes scrape-time aggregation expensive, a separately proven
snapshot/cache design can be introduced without changing metric semantics.
