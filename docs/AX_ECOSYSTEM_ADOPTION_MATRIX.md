# AX-inspired execution adoption matrix — Memorithm

Date: 2026-09-27

This matrix translates useful task/workspace/isolation ideas observed in Google
AX into Memorithm-owned Rust contracts. It does **not** make Google AX a
dependency and does not require Go, Kubernetes or Redis.

The authoritative execution envelope is being built in SciRust Hub. RemoteOps
owns concrete host/runtime enforcement. ElasticXxx owns adaptive resource
policy. Each downstream repository keeps its own scientific, product, safety or
business authority.

## Adoption vocabulary

- **Provider** — implements a shared execution-plane contract.
- **Strong consumer** — runs agents, generated code, mutable experiments or
  privileged automation and should request explicit task capabilities and
  isolation.
- **Bench consumer** — benefits primarily from exact workspaces, fixed resource
  envelopes, isolation between experiments and provenance.
- **Operational consumer** — uses the task model for build/test/deploy/support
  automation, not as product-domain semantics.
- **Reference-only** — no runtime integration until a concrete execution path
  requires it.

| Repository | Adoption class | AX-inspired benefit / required direction |
| --- | --- | --- |
| **scirust-hub** | Provider | Own `TaskSpec`, `WorkspaceSpec`, task identity/capabilities, lifecycle, backend admission and provenance. |
| **RemoteOps** | Provider | Own truthful host/sandbox capability discovery, concrete process/container/gVisor/microVM adapters, egress enforcement and backend checkpoint mechanics. |
| **ElasticXxx** | Provider | Treat task CPU/RAM/GPU/token/concurrency/energy envelopes as adaptive resources; validate/verify/rollback physical actuation. |
| **Forge** | Strong consumer | Generated candidates request container-or-stronger isolation, default-deny egress and explicit budgets; verification remains authoritative. |
| **PAPERS-AGENT** | Strong consumer | Candidate-code/evaluation jobs execute as isolated tasks with exact source/environment identity and external verification boundaries. |
| **RSI** | Strong consumer | Self-improvement and mutation work occurs in child task/workspace envelopes with restricted capabilities and explicit promotion gates. |
| **SoulSystem** | Strong consumer | Agent tool authority becomes task-scoped; child agents/tasks inherit only declared subsets, not ambient host privilege. |
| **Replikans** | Strong consumer | Autonomous actions use task-scoped capability grants, bounded resources and retained action provenance; credentials remain outside task manifests. |
| **COGNO-1** | Strong consumer | Preserve deterministic authority while binding tool/model execution to explicit task capabilities and workspaces. |
| **SML-GENIUS** | Strong/bench consumer | Training/search/evaluation jobs pin code/data revisions, resource envelopes and isolation; model semantics stay repository-owned. |
| **ADA** | Strong/bench consumer | Candidate generation and hardware qualification run as separately budgeted tasks; promotion evidence remains independent. |
| **TDI** | Bench consumer | Every experiment can pin workspace, budget, isolation and trajectory without changing preregistration/evidence authority. |
| **itd-simulator** | Bench consumer | Research runs gain exact environment identity, bounded resources and reproducible isolated execution. |
| **ProofLab** | Strong/bench consumer | Search/falsification tasks may be isolated, while proof status remains solely under the configured trusted proof kernel. |
| **KVLab** | Bench consumer | KV campaigns get pinned workspaces, matched resource envelopes, isolated runs and retained evidence identity. |
| **NoiseLab** | Bench consumer | Stochastic campaigns gain explicit seeds/workspaces/resources and isolated intervention runs. |
| **FieldLab** | Bench consumer | Field-dynamics campaigns gain matched resource envelopes, exact revisions and independent task provenance. |
| **BooleanLab** | Bench consumer | Boolean search/qualification tasks gain explicit budget/isolation and reproducible environment identity. |
| **riemann_ndim_bench** | Bench consumer | Long numerical runs use exact workspaces, bounded resources and checkpoints without promoting numerical evidence to proof. |
| **NeuralOperator** | Bench consumer | Training/solver comparisons use pinned datasets/code, explicit accelerator budgets and isolated reproducible tasks. |
| **nonlocal-relativity-v2** | Bench consumer | Numerical/research campaigns gain exact source revisions, resource envelopes and retained provenance. |
| **GOT** | Bench/strong consumer | Adversity experiments isolate the agent under test, preserve holdout boundaries and prevent task privilege leakage. |
| **ProspectEngine** | Bench consumer | Prospective scenario/intervention evaluations use explicit task inputs, budgets and immutable evidence outputs. |
| **SLHAv2** | Bench/runtime consumer | KV/model integration benchmarks request explicit RAM/VRAM/compute budgets and exact workspace identity; task control does not own KV semantics. |
| **FLAT-ATTENTION** | Bench/runtime consumer | Kernel/search/qualification runs gain exact workspace and hardware identity plus bounded GPU/CPU resources. |
| **NNIS** | Runtime consumer | GPU inference/kernel tasks advertise and request concrete NVIDIA/GPU capabilities and retain device/toolchain provenance. |
| **TurboQuant** | Bench/runtime consumer | Codec and KV benchmarks use matched task budgets, exact revisions and hardware/environment identity. |
| **scirust** | Shared substrate consumer | Publish deterministic process/library capabilities usable inside tasks; do not absorb task orchestration or sandbox policy. |
| **SciCapsule** | Execution boundary consumer | Capsules remain independently validated; Hub task envelopes provide outer workspace/resource/isolation provenance. |
| **SciRust-Verify** | Verification consumer | Verification jobs consume immutable task artifacts and return scoped evidence; task success never upgrades a verdict by itself. |
| **octasoma** | Runtime/bench consumer | Memory benchmarks/import/export jobs use bounded task resources and exact provenance; memory authority stays local to the product. |
| **orchestrator** | Strong consumer / convergence target | Existing orchestration paths should map to Hub task contracts rather than create a second incompatible task authority. |
| **ExtremEngine** | Operational/runtime consumer | Build, asset-processing and adaptive-quality experiments can be task-bounded; game-engine semantics stay outside Hub. |
| **scirust-automotive** | Operational/bench consumer | Simulation/test/calibration automation gains exact workspace, resources and provenance. |
| **CHECKUPAUTO.FR** | Operational consumer | Deployment, security-supervisor and data-maintenance automation use task-scoped capabilities and exact target/workspace identity; production business authority remains application-owned. |
| **CCOS-Core** | Evidence consumer | Task identity/provenance may be input evidence; execution logs do not become cognitive authority automatically. |
| **CCOS-Enterprise** | Governed evidence consumer | Admit selected trajectory/evidence records through tenant/policy/provenance controls; never replace Hub's authoritative execution journal. |
| **ProspectEngine** | Bench consumer | Scenario/intervention dispatch can carry exact task inputs, budgets and immutable evidence outputs. |

## Ecosystem-wide rules

1. Exact Git object identity is preferred over floating branch names in
   reproducible task workspaces.
2. A supervised process is never labelled a security sandbox.
3. Untrusted/generated code targets default-deny networking and
   container-or-stronger isolation.
4. Task capabilities are least-privilege grants, not aliases for shared host
   credentials.
5. Backends must fail closed when they cannot enforce an advertised
   isolation/network/resource property.
6. Scientific and product repositories retain final semantic authority.
7. Retries, checkpoints and resumes retain fresh attempt identity and explicit
   lineage; ambiguous side effects are not replayed silently.
8. Task telemetry is operational evidence. CCOS admission is explicit and
   governed rather than automatic.
9. ElasticXxx may adapt task resources only within declared invariants and
   after backend capability validation.
10. Google AX remains a studied external reference. Memorithm contracts are
    Rust-native and independently versioned.

## Rollout order

1. Hub domain contract and durable lifecycle.
2. RemoteOps backend capability and isolation adapters.
3. ElasticXxx task-resource bridge.
4. Forge hostile-code qualification as the first demanding security consumer.
5. TDI/ProofLab/KVLab/NoiseLab/FieldLab/BooleanLab matched experimental tasks.
6. Agent products (SoulSystem, RSI, Replikans, COGNO-1, SML-GENIUS).
7. Runtime/GPU products and operational applications.
8. Suspend/resume and workload identity only after side-effect and trust
   semantics are qualified.
