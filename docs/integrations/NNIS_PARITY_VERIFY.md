# NNIS NNML1 parity validation and SciRust-Verify edge

SciRust Hub publishes two separate versioned process capabilities for already-produced NNIS NNML1 exact-checkpoint parity evidence:

1. `inference.nnis.parity_validate@1.0.0` invokes the qualified NNIS validation process;
2. `inference.nnis.parity_verify@1.0.0` passes the original evidence and the NNIS validation result to SciRust-Verify and retains the resulting sealed dossier as an immutable artifact.

Hub coordinates these processes and artifact identities. It does not implement NNIS parity semantics and does not recompute SciRust-Verify verdict semantics.

## Qualified source identities

NNIS producer contract:

- contract: `nnis.nnml1.parity-validation@1.0.0`
- input: `application/vnd.nnis.nnml1.parity-evidence.v1+json`
- output: `application/vnd.nnis.nnml1.parity-validation.v1+json`
- NNIS PR: `#114`
- exact qualified PR head: `c74b6b04c45e320c86cdd973b31f49f43c720681`
- merge: `0ae4b0d4659c8de9b8a8322ed6ab7f8e110b53f2`
- NNIS CI: run `#334`, success on the exact qualified head

SciRust-Verify consumer contract:

- process: `scirust-verify-nnis-parity`
- dossier contract: `scirust-verify.nnis-parity-dossier@1.0.0`
- output: `application/vnd.scirust-verify.dossier.v1+tar`
- Verify PR: `#33`
- exact qualified PR head: `a120c54ed05b058d9347691d0036eaaaa831df41`
- merge: `593692aea76b90e50899e733344ffa4cf61ba380`
- Verify CI: run `#110`, success on the exact qualified head, including repository dogfood

## Producer capability

`examples/nnis-parity-validation-component.json` binds:

```text
/usr/bin/python3
  /opt/nnis/libexec/nnis_hub_nnml1_parity_validate.py
  --evidence {input:parity_evidence}
  --result {output:validation}
```

The NNIS deployment must install the wrapper together with its NNIS-owned `validate_nnml1_multi_model_parity_evidence.py` dependency. Hub must not replace that validator with a Hub-owned implementation.

Successful validation means that the input artifact conforms to NNIS's qualified NNML1 parity evidence contract. The wrapper creates no new CUDA/model measurements and does not authorize promotion. Its output explicitly keeps these facts false:

- `promotion_authorized`
- `serving_performance_verified`
- `general_model_family_support_verified`

## Verify capability

`examples/nnis-parity-verify-component.json` binds:

```text
/opt/scirust-hub/libexec/scirust-verify-nnis-parity
  --parity-evidence {input:parity_evidence}
  --validation {input:validation}
  --output {output:dossier}
```

SciRust-Verify independently hashes the exact original parity-evidence bytes and requires the NNIS validation result to reference the same SHA-256 and evidence kind. It also validates the qualified result envelope, links the execution Git commit across both artifacts, seals the exact NNIS validation result into the dossier, and preserves NNIS checkpoint/parity/reference/backend fields as source observations.

The Verify `VERIFIED` result is scoped only to exact-byte binding and validation-envelope conformance. It does not independently rerun NNIS checkpoint/tokenizer/greedy/logit/same-head semantics and does not establish model quality, serving performance, general model-family support, cross-host portability, or promotion authorization.

## Workflow composition

A Hub workflow should preserve the same immutable `parity_evidence` artifact across both steps:

```text
parity_evidence
    |
    +--> inference.nnis.parity_validate@1.0.0 --> validation
    |                                                |
    +------------------------------------------------+
                                                     |
                                                     v
                              inference.nnis.parity_verify@1.0.0
                                                     |
                                                     v
                                                  dossier
```

The validation result must be passed as the immutable artifact emitted by the producer step; it must not be reconstructed from stdout or Hub metadata. The dossier is a separate immutable Hub artifact.

## Ownership and limitations

NNIS owns checkpoint identity, tokenizer/reference identity, greedy trajectory, logit tolerance, same-head composition, runtime promotion and model-family admission semantics.

SciRust-Verify owns evidence-dossier structure, integrity sealing, scoped claim evaluation and verdict semantics.

Hub owns registration, orchestration, immutable artifact flow and execution provenance. Hub does not interpret `validated` as a performance result and does not upgrade the scoped Verify verdict into a broader ML claim.

This edge does not establish HML1 resource-aware worker placement, HML2 distributed ML orchestration, HML4 qualification-campaign maturity, CUDA availability on a Hub worker, Jetson/ARM64 support, serving throughput, or general NNIS model-family support.
