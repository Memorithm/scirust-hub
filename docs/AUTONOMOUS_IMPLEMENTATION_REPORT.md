# Autonomous implementation report — SciRust Hub

Date: 2026-08-30  
Reconciled through: `main` @ `f7de2829af40f8f54bfc17cf5dfec573e4c3cbcf`  
Prepared for independent human review. Claims below are limited to merged
repository behavior and the validation evidence recorded by the corresponding
pull requests and GitHub Actions runs.

## Current implemented scope

SciRust Hub is now a durable control plane with local and remote execution
rather than the single-node foundation described by the original report.
Merged functionality includes:

1. component registration with content-digest identity, idempotent replay and
   conflict rejection;
2. capability discovery, including capability-filtered component lookup;
3. immutable external artifact ingestion plus captured stdout/stderr and
   declared output-file ingestion;
4. provenance-bearing run records and run reproduction with component-version
   and input-artifact drift guards;
5. durable SQLite storage for components, runs, artifacts and workflows;
6. workflow orchestration over the DAG with cross-step artifact references,
   bounded parallelism, deterministic ready-set selection and fail-fast
   semantics;
7. persisted workflow cancellation propagated to active runs, plus opt-in
   bounded retries with fresh attempt/run identities and attempt provenance;
8. verified SciCapsule `capsule.execute@1.0.0` process-contract integration,
   including a real SciCapsule end-to-end success/corruption regression;
9. read-only MCP introspection over NDJSON stdio;
10. an authenticated remote worker backend behind the existing synchronous
    `Executor` port, with workdir transport, lease identity, liveness,
    cancellation and duplicate attempt/result handling;
11. optional control-plane bearer authentication for `/api/v1/*`, with
    non-loopback binds refused when authentication is absent;
12. a durable append-only lifecycle chronology, committed transactionally with
    authoritative SQLite metadata mutations and exposed through HTTP, CLI and
    MCP cursor reads.

## Merged PR history

| PR | Scope | Status |
|---|---|---|
| #1 | Hub foundation: registry, runs, executor, artifacts, provenance, API/CLI | merged |
| #2 | durable SQLite component/run/artifact persistence | merged |
| #3 | declared output-file ingestion | merged |
| #4 | sequential workflow/DAG orchestration foundation | merged |
| #5 | documentation/DAG semantics reconciliation | merged |
| #6 | run reproduction + capability-filtered discovery | merged |
| #7 | read-only MCP adapter | merged |
| #8 | verified SciCapsule Hub v1 contract fixture | merged |
| #9 | workflow cancellation + retry attempts | merged |
| #10 | bounded parallel DAG scheduling | merged |
| #11 | real SciCapsule Hub v1 execution path + artifact ingress | merged |
| #12 | authenticated distributed executor substrate | merged |
| #13 | control-plane bearer authentication | merged |
| #14 | durable lifecycle event log | merged |

## Architecture at PR #14

Workspace roles remain separated:

- `hub-core`: domain types, validation, digests, run/workflow state, DAG
  scheduler, repository ports, provenance and in-memory backends;
- `hub-protocol`: versioned Hub wire DTOs plus the versioned worker protocol;
- `hub-executor`: local `ProcessExecutor`, test executor, remote executor and
  remote worker service;
- `hub-store-sqlite`: durable repositories and lifecycle-event chronology;
- `hub-api`: thin axum adapter over domain/repository ports;
- `hub-mcp`: read-only MCP adapter reaching a running Hub;
- `scirust-hubd`: daemon composition root;
- `scirust-hub`: CLI client;
- `scirust-hub-worker`: standalone authenticated remote process worker.

The synchronous `Executor` port remains the execution abstraction. The async
HTTP layer does not own scheduling semantics. Local and remote backends feed
observed outcomes back through the same Hub run/provenance machinery.

## Workflow execution semantics

The original sequential workflow model has been extended, not replaced.
`WorkflowSpec.max_concurrency` bounds parallel admission of independent DAG
nodes. Omission retains the conservative single-concurrency behavior. The
scheduler uses deterministic dependency/ready ordering, stops admitting new
work after failure/cancellation and signals active sibling runs.

Retries are opt-in. A retry policy bounds attempt count, optional fixed backoff
and retryable observed failure categories. Every retry has a fresh `AttemptId`
and fresh `RunId`; workflow provenance retains ordered attempt history.
Cancellation is not retried.

Restart behavior remains fail-closed for potentially side-effecting execution:
persisted cancellation intent is reconciled, while a non-cancelled workflow
left `running` across daemon lifetime boundaries is not silently replayed.

## SciCapsule boundary

Hub does not parse or redefine `.scicap`. The verified integration contract is
`capsule.execute@1.0.0` and uses ordinary Hub process-component semantics:
immutable `capsule`, `policy` and `request` artifacts are materialized; the
configured SciCapsule executable receives direct argv placeholders and must
produce the required machine-readable result output.

SciCapsule/SciRust retain ownership of canonical capsule validation, detached
signature/trust-policy evaluation, safe materialization/extraction and bounded
entrypoint execution. Hub owns outer orchestration and provenance. The real E2E
regression demonstrates both successful execution and corrupted-capsule
rejection; it does not turn Hub into a capsule parser or OS sandbox.

## Distributed executor boundary

PR #12 introduced `scirust-hub-worker` and `RemoteExecutor` without changing
the orchestrator's execution authority. The transport carries workdir-relative
directory layout and immutable file bytes, so no shared filesystem is assumed.
A worker advertises identity/protocol/capabilities/payload constraints and
heartbeat interval. Each invocation has an attempt identity; payload drift
under one attempt identity and conflicting duplicate results are rejected.
Worker loss, stale heartbeat, protocol/identity drift and authorization failure
fail closed as execution failures; cancellation is forwarded to the active
lease.

This is a real remote execution substrate but **not yet a multi-worker
capability-aware placement scheduler**. One daemon remote backend is configured
against one worker endpoint.

## Authentication and transport security

PR #13 protects `/api/v1/*` when `SCIRUST_HUB_TOKEN` is configured and refuses
unauthenticated non-loopback daemon binds. CLI and MCP clients use the same
environment token automatically. `/health` and `/ready` remain unauthenticated
supervisor probes.

The API retains only a SHA-256 verifier in shared HTTP state. This is
shared-secret authentication, not per-principal authorization. It also does not
provide transport confidentiality: Hub and worker still require a trusted TLS
reverse proxy, service mesh or tunnel when crossing an untrusted network.
Native TLS/mTLS and role-based authorization are not claimed.

The remote worker uses its own worker bearer credential and trust boundary.
Control-plane and worker credentials should not be conflated.

## Durable lifecycle chronology

PR #14 adds SQLite migration v3 with an append-only `lifecycle_events` table.
Events cover component registration, artifact recording, run creation/state
changes, workflow creation/state changes/cancellation and workflow attempt
creation/state changes.

For SQLite, the event append occurs in the same transaction as the metadata
mutation that produced it. Identical registration/artifact replays do not add
new events. Initial run snapshots expand the already-recorded transition list
once; later run writes append only newly observed transitions.

Readers use `sequence > after` with bounded pages ordered by sequence. Sequence
is local to one Hub database and establishes append order; timestamps are not a
global distributed ordering mechanism. HTTP, CLI and the read-only MCP adapter
expose the cursor stream. The authoritative component/run/workflow/artifact
records remain the source of truth; the event log is operational evidence, not
event sourcing.

## Validation evidence

Every functional PR above passed the repository's standard gates before merge:

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo build --workspace --all-targets --locked
cargo test --workspace --all-features --locked
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps --locked
```

For the latest merged functional increment (#14), the exact cleaned PR head
`97e70391026232ee2d6b70be390ec8dcba584823` passed final CI run
`33303575051`, including format, Clippy, build, tests and rustdoc. Its separate
pre-PR validation run `33303404912` also passed focused regressions for
in-memory event cursoring, SQLite event idempotency/reopen durability and HTTP
cursor pagination.

Earlier high-value focused evidence recorded in the merged PRs includes:

- #9: cancellation/retry identity, exhaustion, active cancellation and restart
  reconciliation tests;
- #10: independent-node parallelism, dependency barriers, bounded concurrency,
  queued-work cancellation and interrupted-workflow restart handling;
- #11: real SciCapsule pack/sign/policy/request/execute flow plus corrupted
  capsule rejection;
- #12: remote transport/materialization, bad-auth, unavailable-worker,
  duplicate-attempt and conflicting-result regressions;
- #13: API bearer-auth and unauthenticated non-loopback refusal regressions.

No performance, security-isolation or scientific claims are inferred from
these functional tests beyond what they directly exercise.

## Current security limitations

- Local and remote process execution is **not an OS sandbox**. Children inherit
  resources reachable by the daemon/worker OS identity.
- Control-plane authentication is one shared bearer secret, not users/roles or
  fine-grained authorization.
- Hub and worker do not terminate native TLS/mTLS.
- Remote execution currently targets one configured worker endpoint; there is
  no trusted multi-worker scheduler/placement policy.
- Lifecycle attributes intentionally exclude token values, environment values
  and captured process payloads, but anyone authorized for `/api/v1` can read
  the lifecycle endpoint under the current coarse-grained model.

## Current functional limitations

- A multi-worker registry/placement scheduler is not implemented.
- A container/sandbox executor is not implemented.
- Only declared output files and captured streams are tracked; undeclared
  workdir files are ignored.
- MCP remains read-only; execution tools await a deliberate authorization
  model.
- An exported metrics surface is still missing even though the durable event
  chronology now provides a strong source for operational counters.
- Native TLS/mTLS and principal/role authorization remain future work.
- Repository licensing remains an explicit unresolved decision; no license
  file is present.

## Recommended next implementation

Build a small operational metrics surface from authoritative Hub state and/or
the durable lifecycle chronology without introducing a second mutable counter
store. Prefer low-cardinality metrics (run/workflow terminal states, artifact
counts/bytes, event high-water mark, executor/backend identity where safe) and
avoid component/run/workflow UUID labels. The endpoint's authentication and
information-disclosure boundary must be explicit, and metric derivation must
remain reproducible from persisted Hub data.
