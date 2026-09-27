# AX-inspired Task Runtime Bootstrap

Status: active implementation programme.

This programme absorbs useful task/workspace/isolation abstractions observed in Google AX without adding a Go, Kubernetes, Redis, or Google AX runtime dependency.

## Ownership

SciRust Hub owns the ecosystem task envelope:

- `TaskSpec` and stable task identity;
- immutable workspace inputs pinned to exact repository revisions;
- explicit capability grants;
- declared resource budgets;
- isolation and egress requirements;
- backend admission;
- lifecycle/provenance integration.

RemoteOps owns host reachability, host-specific deployment and sandbox/runtime adapters. ElasticXxx owns adaptive resource policy. Forge/TDI/SciRust and other repositories own their domain semantics.

## Mandatory invariants

1. A validated task is metadata, not authorization and not proof of isolation.
2. Floating Git branches/tags are not accepted as immutable workspace identity.
3. Process supervision must never be described as a hostile-code sandbox.
4. Network egress is explicit. Default-deny is the preferred policy.
5. Capability grants are task-scoped; sharing a transport credential must not silently widen task authority.
6. Resource budgets are enforceable requirements only after backend admission proves support.
7. Retries and resumes receive explicit identities and retained provenance.
8. Suspend/resume must not replay ambiguous side effects.
9. A future SPIFFE/SVID identity may authenticate a TaskIdentity; the current principal string alone does not.
10. Hub must preserve product ownership boundaries.

## Initial implementation slices

- AXH-1: pure Rust domain contracts for TaskSpec, WorkspaceSpec, TaskIdentity, CapabilitySet, ResourceBudget and sandbox requirements.
- AXH-2: persist task envelopes and lifecycle state in the authoritative Hub store.
- AXH-3: workspace materializer with exact-SHA verification and content/provenance digests.
- AXH-4: executor admission interface and truthful backend descriptors.
- AXH-5: container backend with resource and network-policy enforcement.
- AXH-6: RemoteOps worker integration and host capability discovery.
- AXH-7: short-lived workload identities and mTLS/SPIFFE evaluation.
- AXH-8: suspend/resume/checkpoint contract with fail-closed side-effect semantics.
- AXH-9: OpenTelemetry-compatible task trajectory export.
- AXH-10: ecosystem qualification campaigns for Forge, TDI and autonomous PR work.

No slice may claim security/isolation/reproducibility beyond evidence from its exact backend and test environment.
