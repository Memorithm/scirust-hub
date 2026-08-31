# SciRust Hub repository agent instructions

Before repository changes, fetch and read the persistent off-main ecosystem roadmap:

```bash
git fetch origin agent/ecosystem-roadmap && \
git show origin/agent/ecosystem-roadmap:.agent/SCIRUST_HUB_ECOSYSTEM_ROADMAP.yaml
```

For ML component, resource scheduling, distributed-training orchestration, model/data/checkpoint artifact, benchmark-campaign, or cross-repository ML work, also read:

```bash
git show origin/agent/ecosystem-roadmap:.agent/ML_MATURITY_5_OF_5.yaml
```

Treat root `AGENTS.md` as mandatory bootstrap policy. If the roadmap or applicable ML overlay is unavailable, fail closed for major control-plane, component-contract, authorization, scheduler, cross-repository, or merge decisions.

Hub coordinates ecosystem components; it does not absorb their domain semantics. Treat conceptual relationships as non-operational until a versioned contract is published and tested. A `5/5` ML orchestration claim requires verified resource capabilities, resource-aware placement, distributed workflow semantics, immutable ML artifact lineage and reproducible qualification evidence.
