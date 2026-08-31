# SciRust Hub repository agent instructions

Before repository changes, fetch and read the persistent off-main ecosystem roadmap:

```bash
git fetch origin agent/ecosystem-roadmap && \
git show origin/agent/ecosystem-roadmap:.agent/SCIRUST_HUB_ECOSYSTEM_ROADMAP.yaml
```

Treat root `AGENTS.md` as mandatory bootstrap policy. If the roadmap is unavailable, fail closed for major control-plane, component-contract, authorization, scheduler, cross-repository, or merge decisions.

Hub coordinates ecosystem components; it does not absorb their domain semantics. Treat conceptual relationships as non-operational until a versioned contract is published and tested.
