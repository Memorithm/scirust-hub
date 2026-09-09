# NNIS local Hugging Face generation

SciRust Hub exposes a typed orchestration edge for the NNIS-owned local Hugging Face/Safetensors generation process.

The Hub capability is:

```text
inference.nnis.hf_generate@1.0.0
```

Its producer contract is owned by NNIS:

```text
nnis.hf-generation@1.0.0
application/vnd.nnis.hf-generation.v1+json
```

The qualified NNIS producer was merged by NNIS PR #128:

- final exact head: `e5bf07b932c3d734bd28d449ddfb6cf48cd2fc61`
- merge commit: `21370baee6f77f2d8538660092cd0ed5630c2180`

Hub does not implement model loading, CUDA execution, tokenization, greedy sampling, decoded-output semantics, numerical equivalence, performance qualification, or promotion policy.

## Input and output

Input:

- `model_bundle`: `application/vnd.scirust-hub.soup-bundle.v1+tar`

Output:

- `generation`: `application/vnd.nnis.hf-generation.v1+json`

The adapter extracts the deterministic model bundle using the existing bounded SOUP bundle machinery, resolves the requested model root, invokes the NNIS process contract, validates only the known versioned result envelope, and copies the producer bytes exactly into the declared Hub output artifact.

The adapter never reconstructs the NNIS JSON document.

## Parameters

The v1 Hub adapter accepts only:

- `prompt`: required UTF-8 string, 1 byte through 1 MiB;
- `device`: optional non-negative CUDA device ordinal, default `0`;
- `max_new_tokens`: optional integer `1..=65536`, default `16`;
- `model_subpath`: optional model root inside a deterministic bundle.

Unknown parameters fail closed.

The process contract itself remains authoritative for native model/session capacity, CUDA availability, tokenizer behavior and generation failures.

## Preflight ordering

`inference.nnis.hf_generate@1.0.0` does not consume or reinterpret an `nnis.hf-preflight@1` report. The preflight and generation results are separate immutable artifacts with different semantics.

For the SOUP interoperability path, the reference workflow establishes ordering in the DAG:

```text
llm.train
   |
   v
llm.merge.nnis
   |\
   | +--------------------+
   v                      |
inference.nnis.hf_preflight
   |                      |
   +---------- after -----+
                          v
              inference.nnis.hf_generate
```

The generation step reads the same `file:merged_model_bundle` emitted by `llm.merge.nnis`, and also declares `after: ["preflight"]`. Therefore CUDA generation is not scheduled until the NNIS CPU-only preflight step has completed successfully, while Hub still preserves the preflight artifact independently.

The checked-in recipe is:

```text
examples/soup-nnis-generation-workflow.json
```

It uses placeholder immutable artifact IDs for the external SOUP config and dataset. Operators must replace those IDs with real Hub artifacts before submitting the workflow.

## Deployment paths

The Hub component executes:

```text
python3 /opt/scirust-hub/libexec/nnis_hf_generate_hub_adapter.py
```

The adapter delegates to the NNIS-owned process at:

```text
/opt/nnis/libexec/nnis_hub_hf_generate.py
```

which in turn delegates native execution to `nnis-hf generate`.

The paths can be overridden for controlled deployment/testing through the adapter CLI/environment, but the published component contract pins the producer identity and semantics above.

## Resource declaration

The capability publishes `hub.ml.resource-requirements@1.0.0` with:

- backend: `nnis`;
- device resolution: `operation_defined`;
- dtype resolution: `operation_defined`;
- accelerator: `required`;
- memory fit: `runtime_preflight`;
- placement enforcement: `component_preflight`.

This is an HML0 declaration only. Hub does not infer worker compatibility, available VRAM, HML1 placement, HML2 distributed inference, or HML4 qualification from these properties.

## Failure semantics

Hub publishes no generation artifact when the NNIS generation process exits unsuccessfully.

The adapter also fails closed when:

- the model bundle is not a regular file;
- a parameter is unknown or outside the v1 bounds;
- the declared output already exists;
- the NNIS producer result is empty, oversized or invalid JSON;
- schema version, contract, media type or status differ from the qualified v1 envelope.

A producer result using a future NNIS contract is not silently accepted.

## Non-claims

Successful orchestration establishes only that the qualified NNIS local-HF generation process returned a valid `nnis.hf-generation@1.0.0` result artifact.

It does not establish:

- numerical equivalence to Transformers or another runtime;
- model quality;
- serving latency, throughput or memory performance;
- runtime/model promotion eligibility;
- general model-family support;
- FP16, quantized or adapter-native SOUP execution;
- Jetson/ARM64 qualification;
- HML1 resource-aware placement;
- HML2 distributed ML orchestration;
- HML4 qualification-campaign maturity.
