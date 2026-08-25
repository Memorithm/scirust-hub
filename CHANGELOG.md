# Changelog

All notable changes to SciRust Hub are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning is
pre-1.0 (`0.x`), so minor bumps may contain breaking changes.

## [Unreleased]

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
