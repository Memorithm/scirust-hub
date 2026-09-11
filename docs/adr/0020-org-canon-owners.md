# ADR-0020 — Canonical owners for shared concepts

Status: Accepted 2026-09-12
Owner: Memorithm org governance (hub catalog)

## Decision

For each shared concept there is exactly one owner repository. Every other repository consumes that owner via a git dependency pinned to an immutable SHA (or a documented subtree command). Vendoring the owner tree is forbidden except for the single approved exception below.

## Canon table

| Concept | Owner repo | Allowed copy | Forbidden |
|---|---|---|---|
| SciRust platform | Memorithm/scirust | none | SoulSystem/scirust-*, SLHAv2/scirust, nonlocal-relativity-v2 |
| TDI operator / series | Memorithm/TDI | scirust-tdi as thin facade only | new TDI benches |
| ITD field | Memorithm/itd-simulator (`itd-rs`) | scirust-itd as facade | new ITD simulators |
| CCOS kernel | Memorithm/CCOS-Core | CCOS-Enterprise/core via documented subtree | SoulSystem/ccos, RSI/ccos as evolving forks |
| FLAT attention | Memorithm/FLAT-ATTENTION | git pin only | second SHA in flight |
| Elastic language | Memorithm/ElasticXxx | SLHAv2/elastic as extract-to-retire | new Elastic roots |
| TurboQuant codec | Memorithm/TurboQuant | none | SoulSystem/turboquant edits |
| OctaSoma | Memorithm/octasoma | git pin (Enterprise already pins `2e2e0f1`) | SoulSystem/octasoma edits |
| Forge search | Memorithm/Forge | git pin | SoulSystem/forges, RSI/forge as source |
| COGNO authority | Memorithm/COGNO-1 | git pin | RSI/crates/cogno-* as source |
| ADA discovery | Memorithm/ADA | FLAT `flat-ada-*` graduation crates only | new ADA workspaces |
| Relativity experiment | scirust `experiments/nonlocal-relativity-v2` | standalone repo is a frozen fork | treating the standalone repo as platform |
| KV experiments | Memorithm/KVLab | none | new KV labs |
| Agent adversity | Memorithm/GOT | none | rewriting GOT into TDI-11 |
| Formal discovery | Memorithm/ProofLab | none | embedding Lean into SciRust root |

## Approved exception

CCOS-Enterprise may keep `core/` as a subtree of CCOS-Core if and only if the README documents the exact `git subtree` command and the SHA is listed in this catalog.

## Consequences

- Orchestrator allowlist: refuse PRs that enlarge a vendor tree of a canon owner.
- NoiseLab already demonstrates the correct pattern (git rev pin of scirust + FLAT).
- SciRust currently pins FLAT `75d3bd6`; NoiseLab pins FLAT `4529a20` (FLAT default tip at audit). V0-4 retires the older pin after a compile check, not before.
