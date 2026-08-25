# SciRust Hub

**SciRust Hub is the control plane of the SciRust ecosystem**: it registers
ecosystem components, describes what they can do, orchestrates executions,
captures output artifacts and records execution provenance — without
absorbing anyone's code.

Status: `0.2.0`. One tested vertical slice (registry → discovery → run →
process executor → artifacts → provenance → HTTP API → CLI) with durable
SQLite-backed registries.

## Why it exists

The Memorithm ecosystem spans several independent products (SciRust scientific
computing, SciCapsule capsules, Forge evolutionary search, …). Nothing tied
them together: no central registry, no shared run semantics, no provenance.
The Hub fills exactly that role. Ownership boundaries are documented in
[`docs/architecture/BOUNDARIES.md`](docs/architecture/BOUNDARIES.md).

## What it does not do

- It does **not** reimplement SciRust's scientific stack, tensor kernels or
  autodiff.
- It does **not** define the `.scicap` capsule format (owned by the SciRust
  monorepo) and does **not** execute capsules yet (no verified contract).
- It does **not** act as a package manager or build service for Forge or
  anything else; Forge is an evolutionary search engine and has no HTTP
  service to integrate with today.
- Registering a component never executes its code.
- The local process executor is **resource control, not a security sandbox**:
  children run with the daemon's OS privileges under hard caps (output bytes,
  wall-clock timeout, constructed environment).

## Build

```bash
cargo build --workspace --all-targets --locked
```

MSRV is Rust **1.89** (matching the SciRust ecosystem); any recent stable
toolchain works. No system dependencies required for build or tests.

## Run

```bash
# terminal 1: start the daemon (durable SQLite store by default)
cargo run -p scirust-hubd -- --listen 127.0.0.1:8477 --data-dir ./hub-data
# or explicitly: --store sqlite | --store memory

# terminal 2: register a component from a manifest file
cargo run -p scirust-hub -- component register examples/component.json

# discover by capability
cargo run -p scirust-hub -- capabilities

# submit + execute a run, then inspect provenance
cargo run -p scirust-hub -- --output json run submit \
    --component <uuid-from-registration> \
    --capability demo.echo \
    --params '{"msg":"hello"}' \
    --wait
cargo run -p scirust-hub -- run list
cargo run -p scirust-hub -- artifact inspect <artifact-uuid> --content
```

An in-process demo pipeline (no daemon needed):

```bash
cargo run -p scirust-hubd --example demo_pipeline
```

## Manifest format

See [`examples/component.json`](examples/component.json). Key points:

- `schema_version` must be `1`; unknown versions are rejected explicitly.
- Capabilities are validated open names (`namespace.action`) with typed ports;
  the Hub indexes whatever is truthfully declared.
- `execution.type: "process"` declares a fixed argv binding. Placeholders
  `{params}` (canonical JSON), `{input:<name>}` (materialized artifact path)
  and `{output:<name>}` (declared output file path, parent directories
  pre-created by the Hub) may occupy a whole argument each. argv goes
  straight to the OS — there is no shell anywhere.
- Declared `outputs` are ingested as artifacts after a clean exit
  (`required: true` fails the run when the file was not produced; oversized
  files fail rather than being truncated).
- Registration is idempotent: byte-identical manifests are accepted replays;
  different content under the same `(id, version)` is a `409 conflict`.

## Architecture

```text
apps/scirust-hub   CLI client        ┐
apps/scirust-hubd  daemon            │ thin transport layer
crates/hub-api     axum /api/v1      ┘
crates/hub-protocol  versioned wire DTOs
crates/hub-executor  process executor (+ mock for tests)
crates/hub-core      domain: ids, digests, capabilities, components,
                     runs/state machine, DAG, repository ports,
                     in-memory stores, orchestrator
```

Design decisions live in [`docs/adr/`](docs/adr/) (role and boundaries,
workspace split, capability model, execution model, persistence). The
integration model diagram is in
[`docs/architecture/INTEGRATION_MODEL.md`](docs/architecture/INTEGRATION_MODEL.md).

## Test

```bash
cargo test --workspace --all-features --locked
```

83 tests: domain units (state machine, digests, validation, DAG, registry),
executor behavior (timeout, cancellation, truncation, env isolation), router
tests over the real axum stack, and two end-to-end suites that spawn the real
binaries over TCP — including a full register → discover → run → artifact →
provenance walk driven through the CLI.

## Current limitations

- Only declared outputs and captured streams are tracked; undeclared files
  written into the working directory are ignored.
- Multi-node workflow orchestration is not implemented; the DAG primitive
  ships unit-tested but nothing schedules it yet.
- No authentication/TLS: bind to localhost until that lands.

Licensing: this repository currently has **no license file**; licensing is an
open decision and intentionally not copied from other ecosystem repositories.
