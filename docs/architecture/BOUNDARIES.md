# SciRust Hub — Ownership boundaries

Last verified against real repositories: 2026-08-25
(see `docs/AUTONOMOUS_IMPLEMENTATION_REPORT.md` for the exact commits inspected).

## SciRust Hub owns

- The registry of ecosystem components and their declared capabilities.
- Component manifests, their validation and content digests.
- Run specifications, run lifecycle (state machine) and validation.
- Execution control through executor backends it defines.
- Artifact metadata and the artifact blob store it manages.
- Execution provenance records for runs it orchestrates.
- Its versioned HTTP API and CLI.

## SciRust Hub does not own

- Scientific/numerical algorithms, tensor kernels, autodiff, SIMD or GPU
  runtimes — owned by `Memorithm/scirust`.
- The `.scicap` capsule format and its verification primitives — defined in
  the SciRust monorepo (`scirust-capsule-schema`); the SciCapsule product
  layer lives in `Memorithm/SciCapsule`. The Hub may register/validate
  capsule manifests as components; it does not define the format.
- Evolutionary search strategy, candidate generation or measurement harnesses
  — owned by Forge.
- CUDA/JIT inference internals — owned by NVIDIA-Native-Inference-Stack.

## Verified descriptions of other products

- **SciRust** (`Memorithm/scirust`, master): scientific computing monorepo;
  provides `Digest32` domain-separated SHA-256 (`scirust-digest`), validated
  `.scicap` manifest v1 schema (`scirust-capsule-schema`), Merkle-signed code
  artifacts (`scirust-provenance`), OT protocol discovery
  (`scirust-discovery`), an MCP tool server (`scirust-mcp`).
- **SciCapsule** (`Memorithm/SciCapsule`, main): product repository for
  portable reproducible execution capsules; currently a bootstrap (format
  primitives intentionally live in the monorepo).
- **Forge** (`Memorithm/forge`, main): execution-driven evolutionary search
  engine for algorithms (forge-core, forge-worker TCP evaluation,
  forge-cli over a Sled registry). Per its README, forge-bridge is a typed
  Rust facade and "aucun service HTTP n'est actuellement fourni". It is not a
  package registry, builder or compilation service.

## Not yet established

- A wire contract between the Hub and SciRust processes: none published yet.
  Integration will require either a shared protocol crate or adapters built
  on verified interfaces; none is assumed by this foundation.
- A SciCapsule execution contract: `scirust-capsule-schema` is explicitly a
  schema-only crate ("does not implement archive I/O, signing, provenance,
  licensing, or execution"). What a Hub-run capsule execution would invoke is
  therefore **not yet established**.
- Any Forge ↔ Hub integration: no HTTP service exists to integrate with;
  forge-worker speaks its own TCP protocol. Deferred until a verified need
  and contract exist.
- NNIS, ElasticXxx, SciAgent, Jetson runtime contracts with the Hub: not yet
  established; the executor port is the future seam for them.
