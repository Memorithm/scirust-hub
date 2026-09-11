# Memorithm catalog — generated 2026-09-12

Source of truth for repository roles. READMEs do not override this file.
Regenerate from workspace manifests; do not invent a 31st repository until this catalog is CI-backed.

## Org snapshot

30 repositories. Sole public maintainer: ZEKRITI Tarek (`CHECKUPAUTO`). One private empty stub: `scirust-automotive`.

| Repo | Default branch | Lang | Size (KB) | Role |
|---|---|---|---:|---|
| scirust | master | Rust | 167139 | Canon scientific platform ≥130 workspace members |
| SoulSystem | main | Rust | 312017 | Agent runtime; vendors CCOS/OctaSoma/SLHA/SciRust/Forge/TQ |
| TurboQuant | main | Rust | 383674 | Canon 3-bit KV codec (`rust/` workspace, 1.0.0-rc1) |
| nonlocal-relativity-v2 | main | Rust | 128564 | **FORK** of SciRust 0.14.0 — not a science bench |
| FLAT-ATTENTION | main | Rust | 53691 | Canon fused linear-memory attention |
| PAPERS-AGENT | master | Rust/Py | 48963 | Literature agent |
| CCOS-Enterprise | main | Rust | 42821 | Enterprise SKU; subtree of Core allowed |
| CCOS-Core | main | Rust | 41038 | Canon causal-memory kernel |
| TDI | main | Rust | 9689 | Canon TDI series 1–11 |
| ADA | main | Rust | 2287 | Attention algorithm discovery (37 crates) |
| SLHAv2 | master | Rust | 2298 | SLHA tiles; nested Elastic extract |
| NNIS | main | Rust | 1487 | CUDA / execution driver |
| RSI | main | Rust | 1564 | RSI product; vendors forge/ccos/cogno |
| itd-simulator | main | Python | 1507 | Canon ITD V29.18 + itd-rs |
| ElasticXxx | main | Rust | 1133 | Canon Elastic resource language |
| octasoma | master | Rust | 865 | Canon 3D semantic memory |
| scirust-hub | main | Rust | 746 | Hub + **this catalog** |
| COGNO-1 | main | Rust | 698 | Canon bounded neuro-symbolic core |
| riemann_ndim_bench | main | Rust | 657 | RH numerical bench (TDI-10 extract) |
| FLAT-ATTENTION | main | Rust | 53691 | Attention kernels + ADA graduation crates |
| Replikans | main | Rust | 504 | Finance vertical, 24 crates |
| orchestrator | main | Rust | 476 | Org lifecycle orchestrator |
| Forge | main | Rust | 415 | Execution-driven search |
| ProofLab | main | Rust | 299 | Lean/formal discovery |
| SciCapsule | main | Rust | 241 | Capsule format |
| ExtremEngine | agent/initial-engine | Rust | 225 | 16-crate engine (non-standard default branch) |
| NoiseLab | main | Rust | 181 | Stochastic controls + FLAT pin |
| KVLab | main | Python | 99 | KV causal lab (C1–C11 preregistered) |
| SciRust-Verify | feat/scirust-verify-foundation | Rust | 576 | Non-standard default branch |
| GOT | main | Python | 55 | Agent adversity bench |
| scirust-automotive | main | — | 0 | Private empty |

## Workspace member counts (measured from Cargo.toml, not README)

| Location | Members (approx) | Notes |
|---|---:|---|
| Memorithm/scirust root | ≥130 + excludes SOS/Studio/hypermemory/fuzz/ccos | Canon |
| SoulSystem root | ~150 + GPU/TQ/AVID excludes | Second monorepo |
| CCOS-Enterprise | ~40 | Includes `core/` subtree |
| ADA | 37 | Discovery + graduation |
| Replikans | 24 | `unsafe forbid`, unwrap deny |
| FLAT-ATTENTION crates/* | 16+ | Semantic + EPG + ADA candidates |
| ExtremEngine | 16 | Default branch is not `main` |
| ElasticXxx | 10 | |
| TurboQuant rust/ | 9 | Authors still CHECKUPAUTO |
| Forge | 8 | |
| COGNO-1 | 6 | MSRV 1.97.1 |
| ProofLab | 4 | PolyForm NC |
| TDI | 4 | tdi-core/bench/ai/operator |
| itd-simulator/itd-rs | 3 | Frozen V29.18 |
| CCOS-Core | 1 + memory-runtime | |
| orchestrator | 1 package, empty `[dependencies]` | |

## Holdouts — orchestrator must not AUTO_MERGE

- TDI freeze 11.2 fields and labeled holdouts
- itd-simulator V29.18 SHA / future 0.2.0 tag
- Replikans custody / secrets
- SoulSystem vendor trees until replaced by git pins
- nonlocal-relativity-v2 except FORK.md / freeze docs
