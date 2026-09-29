# AXH6 worker trust provisioning boundary

Status: architecture decision for a future authenticated worker discovery contract. This document assigns authority; it does not implement a trust store, signed envelope, challenge service, worker registration, or task admission.

## Owner and authority

SciRust Hub owns the verifier-side trust registry because Hub owns worker registration, lifecycle, and task placement. A registry entry must bind a stable worker identifier to one or more explicitly approved public-key identifiers, public keys, validity windows, and revocation state. The registry must be durable and auditable. RemoteOps owns measurement collection and the worker-side private signing key, but its own report cannot authorize its key in Hub.

Only an authenticated Hub administrative operation with a dedicated worker-trust permission may provision, rotate, or revoke a registry entry. Existing inspect, control, metrics, legacy full-control, and worker transport permissions must not silently imply that new permission. A future migration must specify how legacy full-control credentials are handled before any write API is enabled. Enrollment cannot happen on first connection or by copying a key from a snapshot. Initial records require an out-of-band authenticated identity check and an explicit administrative approval bound to the worker identifier and key fingerprint.

## Lifecycle requirements

1. **Provision:** validate identifier uniqueness, public-key encoding, algorithm allowlist, key identifier, validity interval, and source approval. Record who authorized the change, when, and the approved fingerprint in an append-only audit trail. Do not store private material.
2. **Challenge:** Hub issues an unpredictable, single-use challenge bound to the expected worker and an expiration deadline. The verifier consumes it atomically, including after signature failure, so retries require a new challenge. Persist or coordinate replay state across verifier instances and restarts before enabling multi-instance verification.
3. **Verify:** resolve the approved key from Hub's registry before verifying a future signed envelope. Reject unknown workers or keys, wrong algorithms, expired or revoked keys, stale/reused challenges, mismatched worker binding, and a digest mismatch against the received exact snapshot bytes. Host timestamps are diagnostics; verifier freshness comes from the challenge deadline.
4. **Rotate:** authorize the replacement key through the same administrative channel. An explicit overlap window may allow both old and new keys; each accepted statement identifies one key. Revoke the old key at the end of the window, with no fallback to an unapproved key.
5. **Revoke:** make revocation effective for new verifications before acknowledging the administrative operation. Define cache invalidation and failure behavior for unreachable registry replicas. A worker with no currently approved key must fail closed.
6. **Recover:** restore trust records and replay state consistently from an audited backup. A restore must never make a previously revoked key or consumed challenge valid again; if that cannot be established, suspend worker verification until an administrator reprovisions trust.

## Data and admission separation

The existing `remoteops.host-capability-snapshot/v1` remains `unsigned_observation`. Future signature verification could bind a report to a provisioned worker key and a challenge; it would not prove honest kernel measurements or enforcement by a sandbox backend. Hub must keep backend qualification, dimension-specific enforcement, available capacity, capability grants, and task admission under their separate evidence and policy checks. No unsigned v1 report is promoted in place.

## Implementation gates

- Review the dedicated administrative permission, approval channel, event audit, persistence, and recovery path before exposing trust mutations.
- Select the signature algorithm and maintained library against Hub and RemoteOps supported targets and MSRV; publish an unambiguous domain-separated signing encoding and exact-byte digest rules.
- Publish a separate versioned signed envelope with positive and adversarial fixtures. Keep v1 readable for diagnostics without interpreting it as identity.
- Test unknown/revoked/expired keys, wrong worker, key rotation overlap, modified payload, stale or reused challenge, concurrent consumption, restart and multi-instance replay, and registry outage. Demonstrate that a valid signature alone cannot admit a task.
- Require the exact-head CI and host qualification evidence appropriate to each implementation PR. This document alone does not establish those properties.

RemoteOps' complementary review gates are in `Memorithm/RemoteOps/docs/AXH6_WORKER_ATTESTATION_DESIGN.md`.
