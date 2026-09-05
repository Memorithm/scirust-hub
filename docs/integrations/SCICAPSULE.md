# SciCapsule integration

SciRust Hub orchestrates SciCapsule through explicit versioned `process`
component bindings. Hub does not decode, redefine or own `.scicap`; the
canonical capsule format remains in the SciRust capsule crates and SciCapsule
remains responsible for integrity, local trust-policy evaluation and bounded
capsule execution.

Two execution contracts are supported additively:

```text
capsule.execute@1.0.0
capsule.execute@2.0.0
```

Version 1 remains backward compatible. Version 2 adds a reproducible execution
evidence envelope and is the required source for the published SciRust-Verify
edge.

## Shared immutable inputs

Both versions declare the same Hub input artifacts:

- `capsule` — `application/vnd.scirust.scicap`;
- `policy` — `application/vnd.scicapsule.trust-policy.v1+json`;
- `request` — `application/vnd.scicapsule.hub-run-request.v1+json`.

The request contains detached signatures and bounded execution options. Hub
materializes these immutable artifacts and passes paths to SciCapsule. Hub does
not evaluate their trust semantics.

## Version 1

The v1 process binding remains:

```text
/opt/scicapsule/bin/scicapsule
  hub-run
  --capsule {input:capsule}
  --policy {input:policy}
  --request {input:request}
  --result {output:result}
```

It emits:

```text
application/vnd.scicapsule.hub-run-result.v1+json
```

The repository fixture is `examples/scicapsule-component.json`.

## Version 2 reproducible execution evidence

The v2 producer was qualified from SciCapsule PR #30:

- final exact head: `bb79eea787f0d9562585b27dd38f5f57fa5b5ea9`;
- merge: `31e4a825c8a45837ce4f8ff69f936b46e53d3b82`;
- required CI: SciCapsule CI #79, success on that exact head.

The Hub fixture `examples/scicapsule-v2-component.json` invokes:

```text
/opt/scicapsule/bin/scicapsule-hub-evidence-v2
  --capsule {input:capsule}
  --policy {input:policy}
  --request {input:request}
  --result {output:result}
  --scicapsule-program /opt/scicapsule/bin/scicapsule
```

It emits:

```text
application/vnd.scicapsule.hub-run-result.v2+json
```

SciCapsule v2 snapshots the three caller-provided inputs, preserves the existing
v1 trust/execution path as authority, and binds the result to exact SHA-256
identities for the capsule, policy, serialized request, launcher, invoked
SciCapsule binary and source v1 result. It also records deterministic signature
envelope value identities and the producer OS/architecture scope.

The Hub contract guard requires `execution_mode=bounded_process_unix`,
`sandbox=none`, `trust_decision_owner=SciCapsule`, and
`trust_is_scientific_verdict=false`. Unknown future versions fail closed at run
submission.

## SciRust-Verify edge

SciRust-Verify PR #32 qualified a separate source-preserving consumer:

- final exact head: `bfca2aa4eca00d9a41a369284d95a07a38841f48`;
- merge: `0d319ef157922635932a7b1591b6c364f46b9106`;
- required CI: Verify CI #106, success including repository dogfood on that exact head.

Hub publishes the separate capability:

```text
capsule.verify.scicapsule@1.0.0
```

The fixture `examples/scicapsule-verify-component.json` accepts the immutable v2
result and invokes:

```text
/opt/scirust-hub/libexec/scirust-verify-scicapsule
  --evidence {input:evidence}
  --output {output:dossier}
```

It emits an integrity-sealed dossier:

```text
application/vnd.scirust-verify.dossier.v1+tar
```

The Verify verdict is limited to structural conformance of the qualified v2
evidence envelope. Verify preserves the producer-reported capsule, policy,
request, signature, runtime and source-result identities, but does not claim to
have independently rehashed referenced bytes that were not supplied as inputs.
Hub stores and routes the dossier; Hub does not recompute or reinterpret Verify
verdict semantics.

## Registration and execution

Register v1, v2 and Verify manifests through the normal component-registration
path using stable component UUIDs and absolute deployment paths. The examples in
this repository pin the qualified deployment contract; operators that relocate
binaries must generate equivalent manifests that are accepted by the Hub
contract guard or update the qualified deployment contract explicitly.

Before execution, create the request artifact with SciCapsule, for example:

```text
scicapsule create-hub-request \
  --output request.json \
  --signature release-a.sig \
  --signature release-b.sig \
  --timeout-seconds 30 \
  --env LANG=C \
  -- --example literal-argument
```

Ingest capsule, policy and request through Hub's bounded artifact ingress, bind
the returned artifact IDs to `capsule.execute`, execute the v2 run, then bind the
result artifact to `capsule.verify.scicapsule` when a Verify dossier is required.
Each stage remains a separate Hub run with immutable artifact lineage.

## Reproducibility and ownership boundary

Hub records component version, exact input artifact digests, attempts, output
artifacts and process provenance. SciCapsule owns capsule format, signatures,
trust policy and bounded entrypoint execution. SciRust-Verify owns dossier,
claim, limitation and verdict semantics. Hub owns orchestration and immutable
artifact flow.

This separation is intentional:

- capsule integrity is not authenticity;
- authenticity under a trust policy is not scientific correctness;
- an integrity-sealed Verify dossier does not strengthen the underlying claim;
- process supervision or `bounded_process_unix` is not an OS sandbox.

## Security boundary

Neither Hub process execution nor the qualified SciCapsule v2 contract claims
filesystem, network, syscall, privilege, CPU, memory or device isolation. The
v2 evidence explicitly records `sandbox=none`. Hostile capsules require an
external qualified isolation boundary; absence of that boundary must not be
silently relabeled as sandboxed execution.

Hub never infers authorization from capsule bytes, signature envelopes,
provenance metadata or a Verify dossier. Local trust authorization remains a
SciCapsule decision over the exact pinned capsule bytes and supplied policy.

## Regression guarantees

`crates/hub-core/tests/scicapsule_contract.rs` parses the v1 and v2 execution
fixtures through Hub's public domain types and invokes the real
`validate_execution_contract` guard. It verifies backward compatibility, the
qualified v2 process shape and fail-closed future-version behavior.

`crates/hub-core/tests/scicapsule_verify_contract.rs` pins the separate Verify
process boundary, qualified source/consumer commits, trust ownership and
`hub.policy_interpretation=forbidden` rule.

## Artifact ingress

Hub accepts external immutable input bytes at `POST /api/v1/artifacts`. The raw
request body is bounded by Hub's configured `max_artifact_bytes`; callers must
send `x-scirust-artifact-name` and `content-type`. The CLI equivalent is:

```text
scirust-hub --output json artifact put demo.scicap \
  --name demo.scicap \
  --media-type application/vnd.scirust.scicap
```

Hub does not inspect capsule bytes on ingress. Contract validation occurs at run
submission; canonical capsule validation remains delegated to SciCapsule during
execution.
