# RemoteOps host capability snapshot v1

SciRust Hub may ingest `remoteops.host-capability-snapshot/v1` as a bounded diagnostic record. RemoteOps publishes this snapshot as an unsigned observation; it is not a worker identity, a backend qualification, or a placement grant.

## Consumer rules

- Require the exact schema identifier and trust status `unsigned_observation`.
- Reject unknown fields, unsupported embedded observation versions, malformed values, and documents larger than the consumer's fixed byte limit.
- Preserve the observation timestamp and reported values for inspection. Preserve unknown resource-limit states as unknown.
- Do not construct, update, or satisfy a `SandboxBackendDescriptor`, `CapabilitySet`, `ResourceEnforcement`, worker registration, or admission decision from this document.
- Do not infer available capacity from host totals, cgroup limits, detected tools, kernel features, or their presence.
- Do not treat tool presence, version strings, or host kernel settings as evidence that an isolation, network, workspace, or resource control is enforced.
- Keep this diagnostic record separate from the RemoteOps backend qualification v2 record and its immutable evidence reference.

The snapshot currently reports an observation time, sandbox-tool and kernel observations, and CPU/memory inventory. Its schema deliberately contains no worker identifier or signature. A future authenticated discovery contract must add independently verifiable worker identity and replay protection before Hub can bind observations to a registered worker. That still will not qualify a backend or authorize task placement: those require separate enforcement evidence and dimension-specific observed capacity under the existing admission contract.

## Versioning

The consumer accepts only v1. An incompatible producer shape requires a new schema version and explicit Hub support. A v1 record remains diagnostic even if a later version adds identity or attestation; promotion to scheduling input requires a separate reviewed contract and admission change.
