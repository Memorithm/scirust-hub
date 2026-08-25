# Autonomous implementation report — SciRust Hub foundation

Date: 2026-08-25
Branch: `feat/hub-foundation` (PR target: `main`)
Prepared for independent human review. Nothing here is claimed without a
reproducible command in this repository.

## Scope completed

A single-node control plane with one tested vertical slice:

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
8. everything is queryable through HTTP and the CLI.

## Architecture

- 4 crates + 2 binaries (`docs/adr/0002-workspace-architecture.md`):
  `hub-core` (sync domain + ports + in-memory backends), `hub-protocol`
  (versioned DTOs), `hub-executor` (process/mock), `hub-api` (axum),
  `scirust-hubd`, `scirust-hub`.
- Executor port is synchronous by design; the async HTTP layer offloads via
  `spawn_blocking` (ADR-0004). Cancellation is a cooperative token checked
  between polls.
- Digests mirror SciRust's length-framed SHA-256 discipline but use hub-
  namespaced domains (`scirust-hub:*`) so values are never confused with
  monorepo digests (ADR-0004).
- Persistence is in-memory behind repository traits; SQLite deferred
  (ADR-0005).

## Repository reconnaissance (real commits inspected)

| Repository | Branch @ HEAD | Findings used |
|---|---|---|
| `Memorithm/scirust` | master @ 9301799 | MSRV 1.89, PolyForm Noncommercial; `Digest32` construction; `.scicap` manifest v1 schema (validated relative paths, sorted sha256-bound payloads); provenance = Merkle signing of emitted code (NOT run records); discovery = OT protocols; events-* = anomaly detection; MCP server with hash-chained audit |
| `Memorithm/SciCapsule` | main | product bootstrap only (71 lines); format primitives intentionally in monorepo |
| `Memorithm/forge` | main | evolutionary search driven by execution; forge-bridge = typed facade, "aucun service HTTP"; forge-worker = own TCP protocol |
| `Memorithm/scirust-hub` | empty | bootstrap performed |

Notably rejected assumptions: `scirust-ids` is intrusion detection (not typed
IDs), `scirust-discovery` is OT protocol scanning (not a registry) — neither
is reusable for Hub purposes.

## Tests executed (exact commands and results)

```text
cargo fmt --all -- --check                                   → PASS
cargo clippy --workspace --all-targets --all-features --locked \
    -- -D warnings                                           → PASS (0 warnings)
cargo build --workspace --all-targets --locked               → PASS
cargo test --workspace --all-features --locked               → PASS, 83 passed / 0 failed / 0 ignored
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps   → PASS
git diff --check                                             → clean
```

Breakdown of the 83: 57 hub-core units, 11 hub-executor process-behavior
units (timeout/cancel/truncation/env isolation/argv verbatim), 5 protocol
round-trips, 7 API router tests over the real axum stack, 2 TCP e2e suites
(daemon walking skeleton + malformed-request hardening), 1 CLI e2e driving
two components and verifying byte-exact artifact propagation plus transition
provenance.

The demo pipeline was also executed manually:
`cargo run -p scirust-hubd --example demo_pipeline` → two-stage pipeline
succeeded, final output `{"TEXT":"SCIRUST-HUB"}` with both provenance lines.

## Dependency changes

New (all justified in-repo): `serde`, `serde_json`, `thiserror`, `uuid`
(v4 ids), `sha2` (digests), `clap` (CLI/daemon flags), `axum`+`tokio`
(HTTP adapter only), `tower` (dev: router oneshot tests), `http-body-util`
(dev: reading test bodies), `tracing`/`tracing-subscriber` (structured logs),
`ureq` (blocking CLI HTTP client — chosen over reqwest to avoid an async
runtime in the CLI). No dependency duplicates anything available from the
SciRust monorepo as a usable external crate.

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

- In-memory registries: daemon restart loses component/run state (blobs
  persist on disk unused across restarts).
- Only stream outputs are tracked as artifacts; files written to the working
  directory are not auto-ingested.
- Capability queries are exact-name matches; no predicate/constraint search.
- No authentication/TLS; bind to localhost.
- DAG ships as a primitive; no multi-node scheduling.

## Deferred work (deliberate)

SQLite store implementing existing traits; artifact auto-ingestion globs;
workflow orchestration over `Dag`; remote/container/SciCapsule executors;
metrics; event log for lifecycle history; shared ecosystem protocol crate
extraction (would live outside this repo).

## Security limitations

Subprocesses are resource-controlled, not sandboxed (see SECURITY.md).
HTTP surface is unauthenticated. Unknown-field tolerance on read means new
fields can appear before validation rules exist for them; strictness is at
domain construction time instead.

## Potential review hotspots

- `hub-core/src/orchestrator.rs`: placeholder substitution and claim/duplicate
  execution logic.
- `hub-executor/src/lib.rs`: reader-thread capping semantics and kill/reap
  ordering.
- Protocol tolerance policy: unknown fields allowed on read (documented in
  CHANGELOG/protocol docs) — confirm this matches ecosystem expectations.
- Idempotency scope: registration replays are byte-exact canonical JSON;
  whitespace differences in incoming JSON normalize away via re-serialization.

## CI status

PR checks: **blocked by GitHub infrastructure, not by this code.** The
workflow run fails in ~2 s before executing any step with the annotation:

```text
The job was not started because recent account payments have failed or your
spending limit needs to be increased. Please check the 'Billing & plans'
section in your settings.
```

Verified persistent across a manual re-run. Resolving it requires an account
billing action outside this repository's scope. The workflow file runs the
exact same six gates documented above, which all pass locally on the same
commit; once billing is resolved, `gh run rerun` should reproduce these
results remotely.

## Recommended next PR

SQLite persistence behind `RunRepository`/`ComponentRepository`/
`ArtifactMetadataRepository` (migrations + durability), then artifact file
ingestion and workflow execution over `Dag`.
