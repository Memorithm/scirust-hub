# ADR-0001: Hub role and boundaries

Status: accepted
Date: 2026-08-25

## Context

The Memorithm organization hosts several products: the SciRust scientific
computing monorepo (`Memorithm/scirust`), SciCapsule (portable execution
capsules), Forge (an execution-driven evolutionary search engine for
algorithms), NVIDIA-Native-Inference-Stack, ElasticXxx, and CCOS. There is no
central system that registers these components, describes what they can do,
orchestrates executions across them, or records what actually happened.

Reconnaissance performed before this decision (2026-08-25):

- `Memorithm/scirust` (master): ~130-crate workspace, MSRV 1.89, PolyForm
  Noncommercial license. Relevant contracts inspected:
  - `scirust-digest`: `Digest32`, domain-separated SHA-256 with length-framed
    domain prefix; hex interchange.
  - `scirust-capsule-schema`: validated `.scicap` manifest v1 (schema version,
    name, relative entrypoint path, payloads sorted by path bound to SHA-256
    + exact byte length). Schema only: no archive I/O, no signing, no
    execution.
  - `scirust-provenance`: Lamport/Merkle **signing of emitted code artifacts**
    (leak attribution). It is not a run-provenance record store.
  - `scirust-discovery`: OT/industrial protocol discovery (Modbus, BACnet,
    SNMP, OPC-UA, mDNS). Not a component registry.
  - `scirust-events-*`: time-series event/anomaly detection. Not a lifecycle
    event bus.
  - `scirust-mcp`: MCP server exposing SciRust tools with JSON schemas and a
    hash-chained audit log.
- `Memorithm/SciCapsule` (main): product bootstrap (71 lines); format
  primitives intentionally live in the monorepo.
- `Memorithm/forge` (main): forge-core/forge-worker/forge-bridge; bridge is a
  typed Rust facade, "aucun service HTTP n'est actuellement fourni". Forge is
  a search engine driven by real executions, not a registry or builder.

## Decision

SciRust Hub is built as the ecosystem control plane. It owns:

1. A registry of components and their declared capabilities.
2. Run specification, validation and lifecycle (state machine).
3. Execution through pluggable executor backends.
4. Artifact metadata plus content-addressed artifact storage it manages.
5. Execution provenance records (what ran, with what inputs, producing what).
6. Versioned API and CLI access to all of the above.

The Hub does not own scientific algorithms, tensor kernels, capsule formats,
or search strategies. Those stay in their own repositories.

## Consequences

- The Hub may reference, validate against, orchestrate and record, but never
  reimplements other products' functionality.
- Where SciRust already provides an abstraction (e.g. digest construction),
  the Hub mirrors the *construction discipline* rather than linking the
  private monorepo as a dependency (see ADR-0004), keeping cross-repository
  coupling zero until a shared protocol crate is deliberately extracted.
- Capabilities are named openly (`namespace.action` strings) because the
  ecosystem nomenclature is not yet stable.

## Alternatives considered

- Embedding orchestration inside `Memorithm/scirust`: rejected, the mission
  keeps the Hub as a separate repository and process boundary.
- Making the Hub a thin wrapper over `scirust-mcp`: MCP is one possible
  adapter surface, not the control plane itself.
