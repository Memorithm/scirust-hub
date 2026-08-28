# SciCapsule integration

SciRust Hub can orchestrate trusted SciCapsule execution through the Hub's
existing version-1 `process` component binding. This integration deliberately
does not teach Hub to parse, redefine or own `.scicap`; the canonical capsule
format remains in the SciRust capsule crates and the SciCapsule product remains
responsible for integrity, trust-policy evaluation and bounded capsule
execution.

## Contract ownership

SciCapsule owns the versioned adapter contract documented in its
`docs/HUB_CONTRACT.md`. Hub validates and preserves the corresponding component
manifest shape as an integration regression.

The v1 capability is:

```text
capsule.execute@1.0.0
```

It declares these immutable Hub input artifacts:

- `capsule` — `application/vnd.scirust.scicap`;
- `policy` — `application/vnd.scicapsule.trust-policy.v1+json`;
- `request` — `application/vnd.scicapsule.hub-run-request.v1+json`.

It produces one required artifact:

- `result` — `application/vnd.scicapsule.hub-run-result.v1+json`.

The request artifact contains detached signatures and the bounded execution
options. Keeping signatures in the request allows SciCapsule trust policies to
require an arbitrary supported threshold without changing the fixed Hub port
shape.

## Registration

Install a compatible SciCapsule binary at a known absolute path, then use
SciCapsule itself to generate the Hub manifest. Example:

```text
scicapsule hub-manifest \
  --component-id 00000000-0000-0000-0000-000000000001 \
  --program /opt/scicapsule/bin/scicapsule \
  --output scicapsule-component.json
```

Register the resulting manifest using the normal Hub component-registration
path. The repository fixture at `examples/scicapsule-component.json` is a
contract test/example; operators should generate a manifest using their actual
absolute SciCapsule installation path and chosen stable component UUID.

## Execution binding

The generated process binding is structurally equivalent to:

```text
/opt/scicapsule/bin/scicapsule
  hub-run
  --capsule {input:capsule}
  --policy {input:policy}
  --request {input:request}
  --result {output:result}
```

Hub resolves the input/output placeholders using its existing artifact
materialization rules and passes the resulting argv directly to the OS. No
shell is introduced by this integration.

Before submission, create the request artifact with SciCapsule, for example:

```text
scicapsule create-hub-request \
  --output request.json \
  --signature release-a.sig \
  --signature release-b.sig \
  --timeout-seconds 30 \
  --env LANG=C \
  -- --example literal-argument
```

Upload/store the capsule, policy and request as Hub artifacts, bind them to the
three capability inputs, and submit the run through the ordinary Hub API/CLI.
The Hub process executor controls the outer process lifecycle and captures its
streams/provenance. SciCapsule performs the inner canonical capsule validation,
local trust authorization, private materialization and bounded entrypoint
execution.

## Reproducibility boundary

Hub records the exact input artifact digests and component version under its
normal run provenance model. The SciCapsule result additionally records the
SHA-256 of the exact capsule bytes, canonical manifest name/entrypoint, matched
trusted signer names and required signature threshold.

The contract deliberately contains no timestamp, random run identifier, host
path or process identifier in the SciCapsule result. Hub remains responsible
for its own run identity/timing/provenance metadata.

## Security boundary

This integration is **not an OS sandbox**. Hub's process executor and
SciCapsule's runner provide explicit environment construction, direct argv,
timeouts and process lifecycle controls, but they do not by themselves isolate
filesystem access, networking, syscalls, privileges, CPU or memory. Run hostile
capsules only behind an appropriate container/VM/sandbox boundary.

Trust also stays local to SciCapsule. Hub does not infer authorization from a
capsule, signature envelope or provenance statement. The explicit policy
artifact is evaluated by SciCapsule against the exact canonical capsule bytes
before payload process creation.

## Regression guarantee

`crates/hub-core/tests/scicapsule_contract.rs` parses
`examples/scicapsule-component.json` through Hub's real public domain types and
validates the process binding and capability contract. A breaking Hub manifest
change or a drifting SciCapsule adapter shape therefore fails Hub CI instead of
silently changing interoperability.
