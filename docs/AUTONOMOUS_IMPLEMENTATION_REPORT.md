# Autonomous implementation report — SciRust Hub foundation

Date: 2026-08-25
Reconciled through: `main` @ `4461e8701ea38d70781e76b93d814c2c988584e6`
Prepared for independent human review. Nothing here is claimed without a
reproducible command or GitHub evidence for the referenced revision.

## Scope completed

A single-node control plane with tested vertical slices:

1. daemon starts, serves `/health`, `/ready`, `/api/v1/*`;
2. component manifests register with content digests; identical replays are
   accepted (`already_registered`), divergent content is `409`;
3. capabilities are discoverable by exact name;
4. run specs validate against the registry (capability declared, every input
   port bound to an existing artifact, limits enforced);
5. runs execute through the real process executor in per-run working
   directories with materialized immutable input copies;
6. stdout/stderr become content-addressed artifacts (capped, truncation
   flagged);
7. provenance-bearing run records persist (component identity/version,
   contract version, params digest, input/output artifact digests, backend,
   env var names, exit code, duration, full transition history);
8. everything is queryable through HTTP and the CLI;
9. registries and run records are durable across daemon restarts via
   `hub-store-sqlite` (embedded SQLite, WAL + FULL sync, versioned migrations),
   proven by a kill -9/restart e2e test;
10. components can declare output files (`outputs:` with `{output:<name>}` argv
    placeholders); the Hub pre-creates their parent directories, ingests them
    byte-exactly on clean exits and fails runs whose required outputs are
    missing;
11. sequential workflow orchestration (ADR-0006) supports multi-step specs
    with cross-step artifact references, deterministic topological execution,
    fail-fast semantics, per-step provenance, SQLite migration v2, HTTP
    endpoints and CLI verbs.

## PR history

| PR | Scope | Status |
|---|---|---|
| #1 `feat/hub-foundation` | domain, registry, runs, executor, API, CLI, e2e | merged |
| #2 `feat/durable-sqlite-store` | SQLite persistence behind existing ports | merged |
| #3 `feat/artifact-file-ingestion` | declared output-file ingestion | merged |
| #4 `feat/workflow-orchestration` | sequential workflows over the DAG | merged |

## Architecture

- 5 crates + 2 binaries: `hub-core` (sync domain + ports + in-memory
  backends), `hub-protocol` (versioned DTOs), `hub-executor` (process/mock),
  `hub-api` (axum), `hub-store-sqlite` (durable repository adapter),
  `scirust-hubd`, `scirust-hub`.
- Executor port is synchronous by design; the async HTTP layer offloads via
  `spawn_blocking` (ADR-0004). Run cancellation uses a cooperative token
  checked by the executor between supervision polls.
- Digests mirror SciRust's length-framed SHA-256 discipline but use hub-
  namespaced domains (`scirust-hub:*`) so values are never confused with
  monorepo digests (ADR-0004).
- Persistence is accessed through repository traits. The daemon defaults to
  SQLite via `hub-store-sqlite`; in-memory implementations remain available
  for deterministic tests and explicit ephemeral operation.
- Workflow execution is deliberately single-node, sequential and fail-fast;
  parallel scheduling and distribution are not implied by the DAG model.

## Repository reconnaissance (real commits inspected)

| Repository | Branch @ HEAD | Findings used |
|---|---|---|
| `Memorithm/scirust` | master @ 9301799 | MSRV 1.89, PolyForm Noncommercial; `Digest32` construction; `.scicap` manifest v1 schema (validated relative paths, sorted sha256-bound payloads); provenance = Merkle signing of emitted code (NOT run records); discovery = OT protocols; events-* = anomaly detection; MCP server with hash-chained audit |
| `Memorithm/SciCapsule` | main | product bootstrap only (71 lines); format primitives intentionally in monorepo |
| `Memorithm/forge` | main | evolutionary search driven by execution; forge-bridge = typed facade, "aucun service HTTP"; forge-worker = own TCP protocol |
| `Memorithm/scirust-hub` | main | bootstrapped control-plane implementation; PRs #1-#4 merged |

Notably rejected assumptions: `scirust-ids` is intrusion detection (not typed
IDs), `scirust-discovery` is OT protocol scanning (not a registry) — neither
is reusable for Hub purposes.

## Tests executed (exact commands and results)

The PR #4 head (`2948c60998aa18caeabe6d0fe51acdd016afc0ab`) recorded:

```text
cargo fmt --all -- --check                                   → PASS
cargo clippy --workspace --all-targets --all-features --locked \
    -- -D warnings                                           → PASS (0 warnings)
cargo build --workspace --all-targets --locked               → PASS
cargo test --workspace --all-features --locked               → PASS, 96 passed / 0 failed / 0 ignored
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps   → PASS
git diff --check                                             → clean
```

The suite covers hub-core domain/workflow units, process-executor behavior
(timeout/cancel/truncation/env isolation/argv verbatim), protocol
round-trips, API router tests, SQLite migrations/durability and end-to-end
flows through the real daemon/CLI.

The demo pipeline was also executed during foundation validation:
`cargo run -p scirust-hubd --example demo_pipeline` → two-stage pipeline
succeeded, final output `{"TEXT":"SCIRUST-HUB"}` with both provenance lines.

## Dependency changes

Dependencies introduced through PRs #1-#4 include `serde`, `serde_json`,
`thiserror`, `uuid` (v4 ids), `sha2` (digests), `clap` (CLI/daemon flags),
`axum`+`tokio` (HTTP adapter only), `tower`/`http-body-util` (dev/test),
`tracing`/`tracing-subscriber`, `ureq` (blocking CLI HTTP client) and
`rusqlite` with bundled SQLite for the durable store. Their responsibilities
remain isolated behind the relevant adapter boundaries.

## Integration findings

- The natural future Hub↔SciCapsule seam is `scirust-capsule-schema`'s
  validated manifest v1; execution remains uncontracted ("does not implement
  … execution"), so the Hub registers nothing capsule-shaped yet rather than
  faking it.
- Forge offers no service contract today (bridge-only facade); any Hub
  integration must wait for one or wrap forge-worker's TCP protocol
  deliberately.
- SciRust's MCP server suggests a future MCP adapter exposing Hub
  capabilities; domain independence is preserved so that stays additive.

## Known limitations

- Artifact tracking covers captured streams and explicitly declared output
  files; undeclared files written into a run working directory are ignored.
- Capability queries are exact-name matches; no predicate/constraint search.
- Workflow execution is sequential and fail-fast. Parallel scheduling,
  retries and distributed execution are not implemented.
- `WorkflowState::Cancelled` exists, but workflow cancellation is not yet
  wired to a currently running step/run.
- No authentication/TLS; bind to localhost.

## Deferred work (deliberate)

Workflow cancellation propagation; parallel scheduling; retry policy;
remote/container/SciCapsule executors; optional output-file glob ingestion;
metrics; lifecycle event log; authentication/TLS; and shared ecosystem
protocol extraction if/when a cross-repository consumer justifies it.

## Security limitations

Subprocesses are resource-controlled, not sandboxed (see SECURITY.md).
HTTP surface is unauthenticated. Unknown-field tolerance on read means new
fields can appear before validation rules exist for them; strictness is at
domain construction time instead.

## Potential review hotspots

- `hub-core/src/orchestrator.rs`: placeholder substitution, run claims,
  workflow artifact resolution and fail-fast transitions.
- `hub-executor/src/lib.rs`: reader-thread capping semantics and kill/reap
  ordering.
- Protocol tolerance policy: unknown fields allowed on read (documented in
  CHANGELOG/protocol docs) — confirm this matches ecosystem expectations.
- Idempotency scope: registration replays are canonical-content equivalent;
  whitespace differences in incoming JSON normalize away through protocol
  deserialization/re-serialization before domain storage.

## CI status

PR #4 head `2948c60998aa18caeabe6d0fe51acdd016afc0ab` has GitHub Actions CI run
`32853488193` with conclusion **success**. The workflow runs the repository
fmt / clippy (`-D warnings`) / build / test / rustdoc gates. This supersedes
the earlier foundation-era billing-blocker note.

## Recommended next PR

Wire workflow cancellation through the active step/run and expose it through
the existing HTTP/CLI workflow surface, while preserving the current
sequential scheduling semantics. The cancellation path should be race-tested
and must leave both workflow and active run provenance in coherent terminal
states.
