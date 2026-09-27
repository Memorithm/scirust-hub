# AX-inspired Task Runtime Roadmap

The target is a Rust-native, executor-neutral Memorithm task runtime. Google AX is an architectural reference only; it is not a runtime dependency.

| Phase | Deliverable | Exit criterion |
| --- | --- | --- |
| AXH-1 | Domain contracts | Task/workspace/identity/capability/budget/sandbox contracts compile and fail closed under tests |
| AXH-2 | Durable task lifecycle | SQLite persistence and append-only lifecycle events cover create/admit/run/suspend/resume/terminate |
| AXH-3 | Immutable workspace | Repositories materialize from exact object IDs and a verified workspace digest is retained |
| AXH-4 | Backend admission | Dispatch refuses backends that cannot enforce requested isolation/network/budget controls |
| AXH-5 | First real sandbox | A container-class backend demonstrates filesystem, process, network and resource isolation under adversarial tests |
| AXH-6 | Remote execution | RemoteOps/scirust-hub-worker advertises signed/versioned backend capabilities and rejects drift |
| AXH-7 | Workload identity | Task-scoped identity is authenticated independently of broad host credentials |
| AXH-8 | Suspend/resume | Snapshot lifecycle has explicit side-effect rules, replay safety and provenance |
| AXH-9 | Trajectory evidence | Task/model/tool/process transitions can be exported without turning CCOS into a raw log store |
| AXH-10 | Ecosystem qualification | Forge hostile-candidate, TDI isolated-bench and autonomous-PR campaigns pass bounded qualification |

## Cross-repository responsibilities

- RemoteOps: host/sandbox adapters, inventory, transport and deployment.
- ElasticXxx: adaptive CPU/RAM/GPU/token/concurrency policy and verified actuation.
- Forge: generated candidate execution requests container-or-stronger isolation by default.
- TDI/ProofLab/KVLab/NoiseLab/BooleanLab/FieldLab: experiment tasks pin inputs, budgets and evidence boundaries.
- SciRust/SciCapsule/SciRust-Verify: portable primitives, capsules and evidence verification remain independently owned.
- CCOS-Enterprise: retain selected governed trajectory evidence, never act as the authoritative execution journal.
- Agent products (SoulSystem, RSI, Replikans, COGNO-1, SML-GENIUS): use task-scoped capabilities rather than ambient host authority.

## Explicit non-goals

- no mandatory Kubernetes;
- no mandatory Redis;
- no Go control-plane dependency;
- no claim that ProcessExecutor is a sandbox;
- no implicit privilege inheritance from a shared MCP server;
- no automatic promotion of task telemetry into trusted knowledge.


## Implementation status

- **AXH-1 — merged**: Rust domain contracts for task identity, exact-revision workspaces, capabilities, resource budgets and truthful sandbox admission.
- **AXH-2 — active**: authoritative task lifecycle, append-only transition history, in-memory/SQLite persistence and lifecycle events.
- **AXH-3+ — pending qualification**: workspace materialization, real sandbox backends, RemoteOps capability attestation, workload identity, suspend/resume backend mechanics and ecosystem qualification.
