# SciRust Hub Agent Bootstrap Contract

Before autonomous coding, component-contract work, scheduler/worker changes, provenance changes, authentication/authorization work, cross-repository integration, PR creation, or merge decisions, read:

```bash
git fetch origin agent/ecosystem-roadmap && \
git show origin/agent/ecosystem-roadmap:.agent/SCIRUST_HUB_ECOSYSTEM_ROADMAP.yaml
```

For ML component, resource scheduling, distributed-training orchestration, model/data/checkpoint artifact, benchmark-campaign, or cross-repository ML work, also read:

```bash
git fetch origin agent/ecosystem-roadmap && \
git show origin/agent/ecosystem-roadmap:.agent/ML_MATURITY_5_OF_5.yaml
```

The ML maturity overlay makes 5/5 an evidence-backed control-plane exit criterion. Hub must not count static worker URLs, unverified capability declarations, or local process execution as a mature heterogeneous ML scheduler. It must preserve immutable model/data/checkpoint lineage and keep DDP/model execution in their owning runtimes.

If the roadmap or applicable ML overlay cannot be fetched or read, fail closed for major control-plane, component-contract, authorization, scheduler, cross-repository, or merge decisions. Read-only diagnosis is allowed.

## Repository role

SciRust Hub is the ecosystem control plane: registry, capability discovery, orchestration, immutable artifact flow, provenance, run/workflow lifecycle, and executor control.

It must not absorb implementation owned by SciRust, SciCapsule, SciRust-Verify, Forge, ElasticXxx, NNIS, FLAT-ATTENTION, SLHAv2, or research repositories. A conceptual ecosystem relationship is not a wire contract until a versioned contract is published and tested.

Registration never executes component code. Authentication is not equivalent to authorization or sandboxing. Process supervision is not automatically an OS sandbox.

Required CI must be green on the exact PR head before merge. A 5/5 ML orchestration claim additionally requires the applicable capability-schema, resource-aware worker-pool, distributed-workflow, immutable-artifact and qualification-campaign gates in the ML overlay.

Reread the roadmap and applicable ML overlay at every session start, before component-contract changes, before global scheduler/auth/provenance work, after ecosystem-role or ML-priority changes, and before cross-repository or merge decisions.

Do not merge the roadmap or ML maturity overlay itself into `main` unless the user explicitly requests it.
