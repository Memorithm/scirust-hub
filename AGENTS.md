# SciRust Hub Agent Bootstrap Contract

Before autonomous coding, component-contract work, scheduler/worker changes, provenance changes, authentication/authorization work, cross-repository integration, PR creation, or merge decisions, read:

```bash
git fetch origin agent/ecosystem-roadmap && \
git show origin/agent/ecosystem-roadmap:.agent/SCIRUST_HUB_ECOSYSTEM_ROADMAP.yaml
```

If the roadmap cannot be fetched or read, fail closed for major control-plane, component-contract, authorization, scheduler, cross-repository, or merge decisions. Read-only diagnosis is allowed.

## Repository role

SciRust Hub is the ecosystem control plane: registry, capability discovery, orchestration, immutable artifact flow, provenance, run/workflow lifecycle, and executor control.

It must not absorb implementation owned by SciRust, SciCapsule, SciRust-Verify, Forge, ElasticXxx, NNIS, FLAT-ATTENTION, SLHAv2, or research repositories. A conceptual ecosystem relationship is not a wire contract until a versioned contract is published and tested.

Registration never executes component code. Authentication is not equivalent to authorization or sandboxing. Process supervision is not automatically an OS sandbox.

Required CI must be green on the exact PR head before merge.

Reread the roadmap at every session start, before component-contract changes, before global scheduler/auth/provenance work, after ecosystem-role changes, and before cross-repository or merge decisions.

Do not merge the roadmap itself into `main` unless the user explicitly requests it.
