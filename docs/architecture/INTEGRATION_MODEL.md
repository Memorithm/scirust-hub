# SciRust Hub — Integration model

The Hub is a single-node control plane. Components register *descriptions*;
executions happen through executor backends that the Hub supervises but does
not absorb.

```mermaid
flowchart TD
    CLI[scirust-hub CLI] -->|HTTP /api/v1| API
    User([Human / CI]) -->|HTTP /api/v1| API

    subgraph Daemon[scirust-hubd]
        API[Axum router /api/v1] --> SVC[Orchestrator + Registry services]
        SVC --> REG[(ComponentRepository)]
        SVC --> RUNS[(RunRepository)]
        SVC --> AMETA[(ArtifactMetadataRepository)]
        SVC -->|Executor port| EX
        subgraph EX[Executor backends]
            PEX[ProcessExecutor\nlocal subprocess, capped]
            FUT[future: remote / container /\nSciCapsule / NNIS executors]
        end
        SVC --> ASTORE[FileSystemArtifactStore\ncontent-addressed blobs]
    end

    PEX -->|runs declared argv| COMP[Local components\ne.g. SciRust tools]
    EX -.->|seams only, no code yet| OTHER[SciRust services / SciCapsule /\nForge / external runtimes]
```

## Flow of the first vertical slice

1. `POST /api/v1/components` registers a manifest (validated, digested,
   idempotent for identical content). **No code executes at registration.**
2. `POST /api/v1/runs` submits a RunSpec referencing a component and one of
   its capabilities; the orchestrator validates it against the registry.
3. The validated run is queued, then executed by the configured executor in a
   per-run working directory with materialized input artifacts.
4. Captured outputs become content-addressed artifacts; stdout/stderr are
   stored as capped artifacts referenced from provenance.
5. A provenance-bearing `RunRecord` is persisted and served back through the
   API/CLI.

## What integration means here

"Integrating X" = registering a truthful manifest for X (its real identity,
declared capabilities, verified execution binding) and, when a contract is
verified, executing it through a backend that speaks to it. It never means
absorbing X's code into the Hub. Unverified integrations stay listed under
"Not yet established" in `BOUNDARIES.md` instead of being simulated.
