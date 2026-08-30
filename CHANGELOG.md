# Changelog

All notable changes to SciRust Hub are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning is
pre-1.0 (`0.x`), so minor bumps may contain breaking changes.

## [Unreleased]

### Added

- Durable append-only lifecycle event log (ADR-0012): SQLite migration v3
  records component, artifact, run, workflow and workflow-attempt lifecycle
  changes in the same transaction as authoritative metadata writes. Cursor
  reads are exposed as `GET /api/v1/events?after=&limit=`, `scirust-hub event
  list`, and read-only MCP tool `hub.list_events`; ephemeral memory mode uses
  an equivalent composite store.

- Read-only MCP adapter (`hub-mcp`, binary `scirust-hub-mcp`, ADR-0007):
  JSON-RPC 2.0 over NDJSON stdio mirroring scirust-mcp's protocol shape
  (version `2025-06-18`). Tools: hub.status, hub.list_components (with
  capability filter), hub.get_component, hub.list_runs/get_run,
  hub.list_workflows/get_workflow, hub.list_artifacts/get_artifact.
  Introspection only — no execution entry points; the adapter reaches a
  running daemon over HTTP.

### Added

- Run reproduction closing the provenance loop:
  `POST /api/v1/runs/{id}/reproduce` and `scirust-hub run reproduce <id>
  [--wait]` re-submit a recorded run's exact stored spec as a new queued run
  linked via `reproduced_from`. Guards: the component must still be
  registered at the same version (drift is a validation error) and every
  input artifact must still exist. `RunRecord.reproduced_from` is additive
  and backward-compatible with stored records.
- Capability discovery over HTTP: `GET /api/v1/components?capability=<name>`
  filters to latest manifests declaring that capability; malformed names
  return structured validation errors.

### Added

- Sequential workflow orchestration (ADR-0006): multi-step specs with unique
  step keys, explicit `after` dependencies and cross-step input references
  (`from_step`); deterministic topological execution via the DAG primitive,
  fail-fast on step failure; per-step run provenance recorded in
  `WorkflowRecord`s; SQLite migration v2 stores them durably; HTTP endpoints
  (`POST/GET /api/v1/workflows[/{id}|/executions]`) and CLI
  (`workflow submit/run/list/inspect`).

### Added

- Durable persistence (`hub-store-sqlite`): all three repository ports over
  one embedded SQLite database — WAL journaling with `synchronous=FULL`,
  forward-only migrations recorded in `schema_migrations`, canonical JSON
  storage with projected columns for ordering/lookup, parameterized SQL only.
- Daemon `--store sqlite|memory` switch; `sqlite` is the default so daemon
  state survives restarts (proven by a kill -9 restart e2e test).

- Declared output-file ingestion: process bindings may declare `outputs`
  (name + workdir-relative path + media type + required flag); argv gains a
  `{output:<name>}` placeholder; the orchestrator pre-creates output parent
  directories, ingests produced files as artifacts after clean exits, and
  fails runs whose required outputs were not produced.

### Fixed

- Store-level semantics pinned by tests to match the in-memory backend
  exactly (lexicographic component-version ordering, idempotent replay,
  digest-carrying conflicts, immutable artifact metadata rows).

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
  directories, stdout/stderr capture as content-addressed artifacts, full
  run records as provenance.
- Execution (`hub-executor`): `ProcessExecutor` — structured argv, no shell,
  constructed environment, per-stream output caps with truncation flags,
  wall-clock timeout via kill+reap, cooperative cancellation; and a scripted
  `MockExecutor` for deterministic tests.
- Persistence ports plus in-memory repositories and an atomic file-backed
  content-addressed blob store.
- Wire protocol (`hub-protocol`): versioned DTOs, tolerant reader for
  additive evolution, structured error envelope, explicit schema-version gate.
- HTTP API (`hub-api`, axum): `/health`, `/ready`, `/api/v1/{components,
  capabilities,runs,executions,artifacts}` with precise status-code mapping
  and error envelopes.
- Binaries: `scirust-hubd` daemon (config → tracing → stores → serve →
  graceful shutdown) and `scirust-hub` CLI client with human/JSON output.
- End-to-end proofs over real TCP: walking skeleton through the daemon and a
  CLI-driven two-component pipeline with byte-exact artifact propagation.
- CI running fmt/clippy/build/test/doc with pinned actions.

### Not included (deliberately)

- Durable registries (SQLite backend deferred behind repository traits).
- Workflow/DAG execution scheduling; remote/container/capsule executors;
  authentication on the HTTP surface. See README limitations and ADRs.
