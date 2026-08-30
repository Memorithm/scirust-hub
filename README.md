# SciRust Hub

**SciRust Hub is the control plane of the SciRust ecosystem**: it registers
ecosystem components, describes what they can do, orchestrates executions,
captures immutable artifacts and records execution provenance — without
absorbing the implementation of the products it coordinates.

Status: `0.2.0`. The tested control-plane slice now covers registry and
capability discovery, durable SQLite metadata, local and authenticated remote
process execution, bounded-parallel workflows with retries/cancellation,
SciCapsule execution through its published process contract, run reproduction,
read-only MCP introspection, control-plane bearer authentication and a durable
append-only lifecycle event stream.

## Why it exists

The Memorithm ecosystem spans independent products such as SciRust,
SciCapsule and Forge. The Hub provides the shared control-plane concerns that
should not be reimplemented inside those products: component registration,
capability discovery, execution orchestration, immutable artifact flow and
provenance. Ownership boundaries are documented in
[`docs/architecture/BOUNDARIES.md`](docs/architecture/BOUNDARIES.md).

## What it does not do

- It does **not** reimplement SciRust's scientific stack, tensor kernels or
  autodiff.
- It does **not** define or parse the `.scicap` capsule format. Canonical
  capsule validation, trust-policy evaluation and capsule execution remain
  owned by SciCapsule/SciRust.
- It does **not** turn Forge into a Hub service or absorb Forge's evolutionary
  search semantics.
- Registering a component never executes its code.
- Neither the local process executor nor the remote worker is an OS sandbox.
  Child processes run with the privileges of the daemon/worker OS identity.
- Bearer authentication is not TLS/mTLS and is not fine-grained authorization.

## Build

```bash
cargo build --workspace --all-targets --locked
```

MSRV is Rust **1.89** (matching the SciRust ecosystem); any compatible recent
stable toolchain works. SQLite is bundled by the Rust dependency, so no system
SQLite package is required for the normal build.

## Run locally

```bash
# terminal 1: durable SQLite store by default
cargo run -p scirust-hubd -- \
  --listen 127.0.0.1:8477 \
  --data-dir ./hub-data

# terminal 2: register a component
cargo run -p scirust-hub -- component register examples/component.json

# discover capabilities
cargo run -p scirust-hub -- capabilities

# submit + execute a run, then inspect provenance
cargo run -p scirust-hub -- --output json run submit \
  --component <component-uuid> \
  --capability demo.echo \
  --params '{"msg":"hello"}' \
  --wait
cargo run -p scirust-hub -- run list
cargo run -p scirust-hub -- artifact inspect <artifact-uuid> --content

# inspect the append-only lifecycle stream
cargo run -p scirust-hub -- event list --after 0 --limit 100
```

Loopback operation may remain unauthenticated for local compatibility. To
protect `/api/v1/*`, configure the same control-plane token for daemon and
clients through the environment:

```bash
export SCIRUST_HUB_TOKEN='replace-with-a-secret'
cargo run -p scirust-hubd -- --listen 127.0.0.1:8477 --data-dir ./hub-data
cargo run -p scirust-hub -- run list
```

A non-loopback Hub bind is refused unless `SCIRUST_HUB_TOKEN` is configured.
Native HTTPS is opt-in: provide both `SCIRUST_HUB_TLS_CERT` and
`SCIRUST_HUB_TLS_KEY` (PEM certificate chain + private key), or the matching
`--tls-cert`/`--tls-key` flags. Supplying only one fails closed. HTTP remains the
loopback-compatible default. TLS protects transport but does not replace bearer
authentication. `/health` and `/ready` intentionally remain unauthenticated
supervisor probes.

## Remote execution

The daemon can use the existing synchronous `Executor` boundary through an
authenticated remote worker rather than the local `ProcessExecutor`. The
worker transports workdir-relative directory structure and immutable file
bytes; a shared filesystem is not assumed. Lease identity, heartbeat/liveness,
cancellation and duplicate-attempt/result handling fail closed.

Use environment variables for worker credentials rather than putting them in
routine command history:

```bash
# worker host
export SCIRUST_HUB_WORKER_TOKEN='replace-with-a-worker-secret'
# Optional native HTTPS on the worker:
# export SCIRUST_HUB_WORKER_TLS_CERT='/etc/scirust/worker-cert.pem'
# export SCIRUST_HUB_WORKER_TLS_KEY='/etc/scirust/worker-key.pem'
cargo run -p scirust-hub-worker -- \
  --listen 127.0.0.1:8488 \
  --data-dir ./worker-data

# Hub host (use https:// when worker TLS is enabled)
export SCIRUST_HUB_REMOTE_WORKER_URL='http://127.0.0.1:8488'
export SCIRUST_HUB_REMOTE_WORKER_TOKEN="$SCIRUST_HUB_WORKER_TOKEN"
cargo run -p scirust-hubd -- --executor remote --data-dir ./hub-data
```

The worker bearer token is a separate trust boundary from
`SCIRUST_HUB_TOKEN`. The worker can serve native HTTPS when both
`SCIRUST_HUB_WORKER_TLS_CERT` and `SCIRUST_HUB_WORKER_TLS_KEY` are configured.
`RemoteExecutor` already accepts `https://` endpoints and validates them through
its TLS client stack/system trust roots; self-signed/private CAs must therefore
be trusted by the Hub host rather than bypassed. Plain HTTP remains suitable
only for loopback or a trusted private/tunneled boundary. A single configured
URL retains the original direct `RemoteExecutor`.
Repeating `--remote-worker-url` (or comma-separating
`SCIRUST_HUB_REMOTE_WORKER_URL`) enables a configured worker pool. The pool
queries every worker descriptor before dispatch, skips unavailable or
incompatible endpoints, rejects duplicate worker identities, and selects the
lowest local in-flight count with worker-id/endpoint tie-breaking. Once selected,
a run stays pinned to that worker; an ambiguous post-dispatch transport failure
is never replayed elsewhere. All configured workers currently share the same
environment-only worker bearer token.

## Workflows

Multi-step workflows chain exact Hub artifacts between runs. The scheduler
supports bounded parallelism for independent DAG nodes, deterministic ready-set
selection, fail-fast behavior, persisted cancellation intent and opt-in retry
policies. Omitted concurrency/retry settings preserve the conservative defaults
(one-at-a-time scheduling and one attempt).

```bash
cargo run -p scirust-hub -- workflow submit examples/workflow.json
cargo run -p scirust-hub -- workflow run <workflow-uuid>
cargo run -p scirust-hub -- workflow inspect <workflow-uuid>
cargo run -p scirust-hub -- workflow cancel <workflow-uuid>
```

Every retry receives a fresh attempt identity and a fresh run identity; attempt
history remains in workflow provenance. Active sibling runs are cancelled when
a parallel workflow fails fast or is cancelled. Workflows found `running`
after a daemon restart fail closed rather than silently replaying potentially
side-effecting work.

## Manifest format

See [`examples/component.json`](examples/component.json). Key points:

- `schema_version` must be `1`; unsupported versions are rejected explicitly.
- Capabilities use validated open names (`namespace.action`) with typed ports.
- `execution.type: "process"` declares fixed structured argv. Placeholders
  `{params}`, `{input:<name>}` and `{output:<name>}` occupy whole arguments;
  argv goes directly to the OS, not through an implicit shell.
- Declared output parent directories are pre-created. Required outputs missing
  after a clean exit fail the run; oversized files fail rather than being
  silently truncated.
- Registration is idempotent for identical manifest content and rejects
  divergent content under the same `(id, version)`.

## SciCapsule integration

SciCapsule is registered as an ordinary Hub process component with capability
`capsule.execute@1.0.0`. Hub materializes immutable input artifacts and owns the
outer orchestration/provenance; SciCapsule validates canonical capsule bytes,
evaluates its explicit trust policy and performs bounded entrypoint execution.

The v1 contract uses three inputs:

- `capsule` — canonical `.scicap` bytes;
- `policy` — SciCapsule trust-policy v1;
- `request` — deterministic Hub request with detached signatures and bounded
  execution options.

It produces one required machine-readable `result` artifact. Hub has a
fail-closed guard for the published v1 contract and rejects drifted/future
adapter shapes it does not understand. The end-to-end regression builds and
runs the real SciCapsule flow, including a corrupted-capsule rejection path;
Hub still does not parse `.scicap` itself.

See [`docs/integrations/SCICAPSULE.md`](docs/integrations/SCICAPSULE.md).

## Run reproduction

A recorded run can be reproduced as a **new** run using the exact stored spec:

```bash
cargo run -p scirust-hub -- run reproduce <run-uuid> --wait
```

Reproduction is rejected if the referenced component version has drifted or an
input artifact is no longer present. The new record links back through
`reproduced_from`; it never rewrites the original provenance.

## Lifecycle events

SQLite schema v3 records an append-only operational chronology derived from
successful metadata mutations. Component, run, artifact and workflow event
rows are committed in the same SQLite transaction as the authoritative
metadata mutation that caused them. The event stream is therefore useful for
incremental observers and future metrics, but it is **not** a replacement for
the authoritative records.

```text
GET /api/v1/events?after=<sequence>&limit=<1..1000>
```

Results are ordered by monotonically increasing store-local `sequence` and
return `next_after` for the next page. The read-only MCP adapter exposes the
same chronology as `hub.list_events`.

## Metrics

Hub exposes low-cardinality Prometheus text metrics at `GET /metrics`. The
snapshot is recomputed from authoritative persisted records on every scrape;
there is no independent mutable counter store to drift after restart.

```bash
curl -H "Authorization: Bearer $SCIRUST_HUB_TOKEN" \
  http://127.0.0.1:8477/metrics
```

The initial surface reports build/executor info, component-manifest count,
run/workflow counts by state, retained workflow-attempt count, artifact count
and logical bytes, plus the lifecycle-event high-water sequence. Entity UUIDs
are deliberately not used as labels. `/metrics` follows the same bearer policy
as `/api/v1`.

## MCP introspection

`scirust-hub-mcp` is a read-only MCP server over NDJSON stdio. It reaches the
running Hub over HTTP and exposes status, component, run, workflow, artifact
and lifecycle-event introspection. It automatically uses `SCIRUST_HUB_TOKEN`
when configured. Execution/submission tools remain outside MCP until an
explicit authorization model is introduced.

## Architecture

```text
apps/scirust-hub          CLI client
apps/scirust-hubd         control-plane daemon
apps/scirust-hub-worker   authenticated remote process worker
        │
crates/hub-api             axum /api/v1 adapter
crates/hub-mcp             read-only MCP adapter
crates/hub-protocol        versioned Hub/worker DTOs
crates/hub-executor        local + remote Executor backends
crates/hub-store-sqlite    durable metadata + lifecycle-event store
crates/hub-core            domain, DAG/workflow scheduler, provenance,
                           repository ports and in-memory backends
```

Design decisions live in [`docs/adr/`](docs/adr/). The ecosystem ownership
model is documented under [`docs/architecture/`](docs/architecture/).

## Test

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo build --workspace --all-targets --locked
cargo test --workspace --all-features --locked
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps --locked
```

CI additionally contains focused integration/regression coverage for daemon and
CLI flows, crash/restart durability, SciCapsule contract execution, workflow
cancellation/retries/parallelism, remote worker transport/idempotency/liveness,
control-plane authentication, lifecycle-event cursor durability,
configured multi-worker placement/identity safety and TLS configuration gates.

## Current limitations

- Process execution remains **not sandboxed**. Local and remote worker children
  inherit the privileges available to their daemon/worker OS identity.
- Multi-worker execution is a configured pool with descriptor discovery
  and deterministic local-load placement; there is not yet a dynamic worker
  registration/expiry service or resource-aware global scheduler.
- Bearer authentication is shared-secret authentication, not fine-grained
  authorization. There are no principals/roles yet.
- Hub and worker provide opt-in native server TLS with PEM certificate/key
  files, but not mTLS/client-certificate authentication or certificate hot reload.
- Only declared outputs and captured streams are tracked; arbitrary undeclared
  files written into a workdir are ignored.
- SciCapsule is integrated through its verified versioned process contract,
  not by moving capsule parsing/extraction into Hub.
- Licensing remains an explicit open repository decision; no license file is
  currently present.
