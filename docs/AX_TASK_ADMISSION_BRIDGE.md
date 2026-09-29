# AX task admission bridge

SciRust Hub is the declarative orchestration owner for Memorithm. It adopts the
useful AX distinction between a task and its workspace, while keeping
execution enforcement in RemoteOps.

## Mapping

| AX-inspired concept | Hub record | Required evidence |
|---|---|---|
| Task | immutable run/task identity | goal, capability, resource envelope, policy revision |
| Workspace | materialization request | repository, exact object ID, input digest |
| Model | inference policy reference | provider/model label, limit, credential reference |
| Runtime | executor selection | RemoteOps backend descriptor, bounded qualification evidence reference, and separate default-deny claim |
| Evidence | artifact/event references | append-only lifecycle events and result digests |

## Admission sequence

```text
declare → validate schema → bind workspace objects → request capability proof
        → record admission → dispatch to RemoteOps → reconcile → verify
```

The Hub must refuse dispatch when source identity, capability requirements,
resource bounds or evidence destination are incomplete. A stronger-than-process
isolation claim requires an explicit qualification flag and a bounded reference
to immutable evidence. Process supervision cannot claim qualified isolation.
Declared network, workspace, resource or capability enforcement also requires
a qualification evidence reference. A task requesting default-deny egress is
admitted only when the backend separately claims both network-policy
enforcement and default-deny enforcement. Legacy descriptors default
qualification fields to false or absent and remain fail-closed.

A registered component is metadata only and is never executed during discovery.

## Boundary

The Hub does not become an OS sandbox and does not copy RemoteOps backend
logic. A local or remote process executor is resource supervision unless a
backend qualification record proves stronger containment. A timeout after a
possible side effect is reconciled by task identity before any retry.

## Promotion

A result is promotable only when the exact task, workspace, runtime,
configuration and evidence references are available. AX-like declarative
manifests improve repeatability; they do not replace Rust-level validation or
scientific/product-specific acceptance criteria.
