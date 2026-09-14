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

The original NNIS producer was merged by NNIS PR #128:

- final exact head: `e5bf07b932c3d734bd28d449ddfb6cf48cd2fc61`
- merge commit: `21370baee6f77f2d8538660092cd0ed5630c2180`

NNIS PR #148 (`5436736002834dd6dd7d5ace8c1c044b47aed18f`) hardens that producer with live per-stream capture limits, a native-process deadline and atomic no-replace JSON publication. Its successful wire contract remains unchanged. Those producer protections require deploying the updated NNIS wrapper; a historical source identity in the Hub manifest does not prove which wrapper is installed on a worker.

Hub does not implement model loading, CUDA execution, tokenization, greedy sampling, decoded-output semantics, numerical equivalence, performance qualification, or promotion policy.

## Input and output

Input:

- `model_bundle`: `application/vnd.scirust-hub.soup-bundle.v1+tar`

Output:

- `generation`: `application/vnd.nnis.hf-generation.v1+json`

The adapter extracts the deterministic model bundle using the existing bounded SOUP bundle machinery, resolves the requested model root, invokes the NNIS process contract, validates only the known versioned result envelope, and publishes the producer bytes exactly into the declared Hub output artifact.

The adapter never reconstructs the NNIS JSON document. Result intake reads a bounded snapshot once and validates that snapshot. Publication uses the same bytes, not a second read of the producer path. A change to the source after validation therefore cannot substitute different bytes at publication.

## Parameters

The v1 Hub adapter accepts only:

- `prompt`: required UTF-8 string without NUL, 1 through 16,000 bytes;
- `device`: optional integer CUDA device ordinal in `0..=2147483647`, default `0`;
- `max_new_tokens`: optional integer `1..=65536`, default `16`;
- `model_subpath`: optional model root inside a deterministic bundle.

Both the raw prompt bound and the 16,384-byte UTF-8 serialized parameter-envelope bound apply. JSON escaping and companion parameters count toward the latter: a prompt fitting 16,000 raw bytes is not necessarily an admissible parameter envelope. Booleans are rejected for integer device/token fields. Unknown parameters fail closed.

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

It uses placeholder immutable artifact IDs for the external SOUP config and dataset. Operators must replace those IDs with real Hub artifacts before submitting the workflow. Its train and merge step timeouts are capped at 3,600,000 ms, matching Hub's current default per-run limit; longer operations require an explicitly configured compatible Hub limit rather than an example that cannot execute under defaults.

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

The paths can be overridden for controlled deployment/testing through the adapter CLI/environment, but the published component contract pins the producer identity and semantics above. NNIS PR #148's wrapper applies a default 3,600-second native deadline. The Hub adapter does not forward a timeout parameter: Hub's outer per-run supervision remains a separate limit. The native child retains the caller's process group for outer cancellation.

## Resource declaration

The capability publishes `hub.ml.resource-requirements@1.0.0` with:

- backend: `nnis`;
- device resolution: `parameter:device`;
- dtype resolution: `operation_defined`;
- accelerator: `required`;
- memory fit: `runtime_preflight`;
- placement enforcement: `component_preflight`.

This is an HML0 declaration only. Hub does not infer worker compatibility, available VRAM, HML1 placement, HML2 distributed inference, or HML4 qualification from these properties.

## Failure and publication semantics

Hub publishes no generation artifact when the NNIS generation process exits unsuccessfully.

The adapter also fails closed when:

- the model bundle or generation result is not a regular file, including symlinks and special files;
- a parameter is unknown or outside the raw or serialized v1 bounds;
- the declared output already exists, including a dangling symlink;
- the NNIS producer result is empty, larger than 40 MiB, invalid UTF-8 or invalid JSON;
- JSON contains duplicate keys or non-finite constants;
- the schema version is not the integer `1`, or contract, media type or status differ from the qualified v1 envelope;
- the source length changes between its descriptor metadata and bounded read;
- staging, synchronization or atomic publication fails.

The serialized 40 MiB result cap is separate from NNIS's 16 MiB raw cap on each output stream. JSON escaping can expand captured text, so successful producer generation does not guarantee that the resulting JSON fits the Hub cap.

The validated bytes are written to a private temporary file in the destination directory, flushed and synced, and published with an atomic no-replace hard link. A failed write/sync/link cannot expose a partial final artifact. A competing publisher's destination is never overwritten or removed. Only the adapter's own temporary is cleaned up, on a best-effort basis. A filesystem without hard-link support fails closed rather than falling back to partial publication.

This protects final-path publication in trusted run directories. It is not a claim of hostile-filesystem isolation, total-process memory limits, directory-entry durability after power loss, or prevention of all concurrent same-size source modifications during the read. The guarantee is that the exact snapshot validated is the snapshot published. A crash may leave private staging files without a final artifact.

A producer result using a future NNIS contract is not silently accepted. Hub checks the transport envelope, not the truth of model-quality, numerical-equivalence or promotion claims.

## Regression checks

Run the existing process adapter tests and the wire/publication fault tests:

```bash
python3 -m unittest \
  scripts/test_nnis_hf_generate_hub_adapter.py \
  scripts/test_nnis_hf_generation_transport.py -v
```

CI runs both suites with all other ML adapter tests and Rust gates. Synthetic JSON, process and filesystem fixtures are not CUDA/model evidence.

## Non-claims

Successful orchestration establishes only that the NNIS local-HF generation process returned an artifact with an accepted `nnis.hf-generation@1.0.0` envelope.

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
