# Changelog

All notable changes to SciRust Hub are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning is
pre-1.0 (`0.x`), so minor bumps may contain breaking changes.

## [Unreleased]

### Added

- Opt-in native Rustls HTTPS for both `scirust-hubd` and
  `scirust-hub-worker`. PEM certificate and private-key paths must be configured
  together and malformed/missing material fails startup closed. HTTP remains
  the local-compatible default; bearer authentication remains independent and
  mTLS/client-certificate authentication is not claimed.

- Configured multi-worker remote placement: repeating `--remote-worker-url`
  (or comma-separating `SCIRUST_HUB_REMOTE_WORKER_URL`) discovers compatible
  worker descriptors before dispatch and deterministically selects the lowest
  Hub-local in-flight target. Duplicate worker identities fail closed; once a
  target is selected there is no ambiguous post-dispatch failover. Per-run
  provenance records the concrete selected worker target while single-worker
  remote mode remains compatible.

- Durable append-only lifecycle event log (ADR-0012): SQLite migration v3
  records component, artifact, run, workflow and workflow-attempt lifecycle
  changes in the same transaction as authoritative metadata writes. Cursor
  reads are exposed as `GET /api/v1/events?after=&limit=`, `scirust-hub event
  list`, and read-only MCP tool `hub.list_events`; ephemeral memory mode uses
  an equivalent composite store.
- Derived low-cardinality operational metrics (ADR-0013): authenticated
  `GET /metrics` emits Prometheus text format 0.0.4 from authoritative
  component/run/workflow/artifact state plus the lifecycle event high-water
  mark. No mutable metrics store or entity-UUID labels are introduced.
- Control-plane bearer authentication (ADR-0011): `SCIRUST_HUB_TOKEN` protects
  `/api/v1/*`; `/health` and `/ready` remain supervisor probes. Non-loopback
  Hub binds fail closed without a token. CLI/MCP clients attach the token from
  the environment. Native TLS/mTLS and fine-grained authorization remain
  explicitly out of scope for this change.
- Authenticated distributed executor substrate (ADR-0010): standalone
  `scirust-hub-worker`, versioned worker protocol, `RemoteExecutor`, workdir
  directory/file transport without a shared filesystem, lease identity,
  heartbeat/liveness, cancellation and idempotent duplicate attempt/result
  handling. The daemon can select local `process` or configured `remote`
  execution; this is not yet a multi-worker placement scheduler.
- Real SciCapsule Hub v1 execution path: fail-closed validation of the
  published `capsule.execute@1.0.0` process contract, bounded immutable
  external artifact ingress, raw `POST /api/v1/artifacts`, CLI `artifact put`,
  and an end-to-end regression using real SciCapsule pack/sign/policy/request
  tooling with both successful execution and corrupted-capsule rejection.
  Hub continues to delegate `.scicap` parsing, trust evaluation and bounded
  capsule execution to SciCapsule.
- Bounded parallel DAG workflow scheduling: explicit `max_concurrency`
  (bounded to 1..=64), deterministic ready-set admission, dependency barriers,
  fail-fast cancellation of active siblings, and restart-safe fail-closed
  handling of interrupted workflows. Omitted concurrency preserves the
  one-at-a-time default.
- Workflow cancellation and retry attempts: persisted monotonic cancellation
  intent propagates to active runs and prevents downstream admission; retry
  policy supports bounded attempt count, optional fixed backoff and explicit
  retryable failure categories. Every retry receives a fresh `AttemptId` and
  `RunId`, with ordered attempt provenance persisted in workflow records.
- Verified SciCapsule component-contract fixture and integration documentation,
  establishing the ownership boundary before the real execution E2E landed.
- Read-only MCP adapter (`hub-mcp`, binary `scirust-hub-mcp`, ADR-0007):
  JSON-RPC 2.0 over NDJSON stdio following the established SciRust MCP
  discipline. It exposes Hub status/components/runs/workflows/artifacts and now
  lifecycle-event introspection, while submission/execution remains outside
  MCP pending an explicit authorization model.
- Run reproduction closing the provenance loop:
  `POST /api/v1/runs/{id}/reproduce` and `scirust-hub run reproduce <id>
  [--wait]` re-submit a recorded run's exact stored spec as a new queued run
  linked via `reproduced_from`. Component-version drift or missing input
  artifacts fail closed before execution.
- Capability discovery over HTTP: `GET /api/v1/components?capability=<name>`
  filters to latest manifests declaring that capability; malformed names
  return structured validation errors.
- Sequential workflow orchestration foundation (ADR-0006): multi-step specs
  with unique step keys, explicit `after` dependencies and cross-step artifact
  references; deterministic DAG execution, per-step run provenance, SQLite
  migration v2, HTTP endpoints and CLI verbs. Later bullets above extend this
  foundation with cancellation, retries and bounded parallel scheduling.
- Durable persistence (`hub-store-sqlite`): component, run, artifact and
  workflow repository ports over embedded SQLite using WAL,
  `synchronous=FULL`, forward-only migrations, canonical JSON snapshots and
  parameterized SQL. `sqlite` is the daemon default; `memory` remains an
  explicit ephemeral mode. Lifecycle events extend the same store in v3.
- Declared output-file ingestion: process bindings declare named
  workdir-relative outputs; `{output:<name>}` placeholders resolve directly in
  argv, required outputs are enforced, and cleanly produced files are ingested
  as immutable artifacts.

### Changed

- Public documentation is reconciled through merged PR #14 so it no longer
  describes workflows as sequential-only, remote execution as absent, or the
  control-plane HTTP surface as wholly unauthenticated.

### Fixed

- Store-level semantics are pinned by tests to match the in-memory backend for
  component ordering/replay/conflicts, deterministic record ordering and
  immutable artifact metadata.
- Remote workdir transport explicitly preserves empty/pre-created directories,
  discovered by the distributed end-to-end regression.

## [0.1.0] - 2026-08-25

Foundation: first tested vertical slice.

### Added

- Domain model (`hub-core`): typed identifiers (`ComponentId`, `RunId`,
  `ArtifactId`), domain-separated SHA-256 content digests with hex wire form,
  validated semver-shaped versions, capability model with validated open
  names, component manifests with process execution bindings, run specs with
  centralized limits, a controlled run state machine with recorded
  transitions, provenance-bearing run records, and a generic DAG with cycle
  detection and deterministic topological ordering.
- Registry and orchestration: idempotent manifest registration with content
  digests and conflict detection, capability discovery, run submission
  validation (declared capability, input port coverage, artifact existence,
  placeholder resolution), input materialization into per-run working
  directories, stdout/stderr capture as content-addressed artifacts, full run
  records as provenance.
- Execution (`hub-executor`): `ProcessExecutor` — structured argv, no shell,
  constructed environment, per-stream output caps with truncation flags,
  wall-clock timeout via kill+reap, cooperative cancellation; and a scripted
  `MockExecutor` for deterministic tests.
- Persistence ports plus in-memory repositories and an atomic file-backed
  content-addressed blob store.
- Wire protocol (`hub-protocol`): versioned DTOs, tolerant reader for additive
  evolution, structured error envelope, explicit schema-version gate.
- HTTP API (`hub-api`, axum): `/health`, `/ready`, `/api/v1/{components,
  capabilities,runs,executions,artifacts}` with precise status-code mapping and
  error envelopes.
- Binaries: `scirust-hubd` daemon and `scirust-hub` CLI client.
- End-to-end proofs over real TCP: registration, discovery, submission,
  execution, provenance transitions and byte-exact artifact propagation.
- CI running fmt/clippy/build/test/doc with pinned actions.

### Not included at 0.1.0

The initial release did not yet include durable SQLite state, workflow
scheduling, SciCapsule execution, remote workers, MCP, control-plane
authentication or lifecycle events. Those capabilities are described under
`Unreleased` above as they landed after the foundation.
