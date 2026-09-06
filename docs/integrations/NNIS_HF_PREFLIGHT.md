# NNIS Hugging Face preflight integration

Status: proposed `inference.nnis.hf_preflight@1.0.0` on this branch.

This integration exposes the NNIS CPU-only local Hugging Face/Safetensors preflight through SciRust Hub while preserving the ownership boundary: Hub materializes immutable artifacts and orchestrates the process; NNIS remains authoritative for model admission and direct-execution readiness semantics.

## Qualified NNIS producer

The component is bound to the NNIS artifact-producing preflight introduced by NNIS pull request 124:

```text
final exact head: 5975b4a01c7c0825ad8ca97ce4c9e384d0cc4b67
merge commit:     b1e2766ace9a2076d4a5721f6dca2d2c7d48d04d
report schema:    nnis.hf-preflight@1
```

That producer supports:

```text
nnis-hf validate --model DIR --json --output FILE
```

and writes the exact producer JSON as an atomic file artifact. NNIS pull-request CI and post-merge CI passed, including its declared Rust 1.77 check. Those facts qualify the process boundary; they do not establish model quality, numerical equivalence, serving performance, or general Hugging Face model support.

## Hub capability

The manifest is:

```text
examples/nnis-hf-preflight-component.json
```

It publishes:

```text
inference.nnis.hf_preflight@1.0.0
```

Input:

```text
model_bundle: application/vnd.scirust-hub.soup-bundle.v1+tar
```

Output:

```text
preflight: application/json, schema nnis.hf-preflight@1
```

The bundle media type is a Hub transport contract. It does not imply that a bundle was produced by a compatible SOUP workflow or that NNIS will admit its contents.

## Materialization boundary

NNIS consumes a directory-shaped local Hugging Face model, while Hub process inputs and outputs are immutable file artifacts. The adapter therefore reuses the existing deterministic Hub bundle implementation from `soup_hub_adapter.py` rather than defining another archive format.

The existing bundle extraction rules reject unsafe paths, symbolic and hard links, devices, FIFOs, duplicate members, overwrite attempts, excessive member counts, and excessive extracted payload. After extraction, the existing model-root resolver either finds one unambiguous model directory or requires the optional `model_subpath` parameter.

The NNIS adapter then invokes, without a shell:

```text
nnis-hf validate --model <resolved-directory> --json --output <Hub-output-path>
```

The adapter does not parse `config.json`, inspect Safetensors tensor semantics, evaluate dtypes, decide whether a decoder graph is complete, or reinterpret `direct_execution_ready`. Those remain NNIS responsibilities.

## Report contract validation

Hub performs only a wire-version check on a successful NNIS result:

- the report must be a regular file;
- it must fit the bounded report-size limit;
- it must parse as a JSON object;
- top-level `schema` must equal `nnis.hf-preflight@1`.

The adapter does not rewrite the report bytes and does not recalculate any NNIS field. Unknown report schema versions fail closed.

Any non-zero `nnis-hf` exit remains a Hub process failure. In particular, a source that NNIS can structurally inspect but that is not ready for the current direct execution path is not converted into orchestration success. This allows workflows to stop before GPU scheduling without teaching Hub NNIS admission policy.

## Parameters

The v1 component accepts only:

- `model_subpath`: optional normalized relative path used when a deterministic model bundle contains more than one plausible model root.

Unknown parameters fail closed. No Hub parameter can override NNIS dtype, architecture, tokenizer, graph-completeness, or direct-readiness rules.

## Resource declaration

The capability publishes `hub.ml.resource-requirements@1.0.0` with:

- backend: `nnis`;
- device: `operation_defined`;
- dtype: `model_defined`;
- accelerator: `none` for this CPU-only preflight operation;
- memory: `runtime_preflight`;
- placement enforcement: `component_preflight`.

This is an HML0 discovery contract. It is not evidence that Hub has HML1 resource-aware worker placement.

## Runtime and deployment

Install the Hub adapters on a worker:

```bash
sudo install -d -m 0755 /opt/scirust-hub/libexec
sudo install -m 0644 scripts/soup_hub_adapter.py \
  /opt/scirust-hub/libexec/soup_hub_adapter.py
sudo install -m 0644 scripts/nnis_hf_preflight_hub_adapter.py \
  /opt/scirust-hub/libexec/nnis_hf_preflight_hub_adapter.py
```

`nnis-hf` must also be available on the worker. By default the adapter resolves it from `PATH`; deployment may set `NNIS_HF_BIN` to an explicit executable path.

Register the component with:

```bash
cargo run -p scirust-hub -- component register examples/nnis-hf-preflight-component.json
```

Registration is metadata-only and never executes NNIS.

## Important SOUP boundary

The current Hub `llm.train` capability emits a SOUP training model/adaptor bundle. That output must **not** be assumed to satisfy NNIS's current direct local-HF execution boundary.

The NNIS direct path currently documented by NNIS requires a dense merged F32 local Hugging Face artifact. Therefore this component can preflight an externally supplied compatible deterministic model bundle, but this change alone does not establish a complete `Hub SOUP train -> NNIS inference` workflow.

A true Hub-owned SOUP-to-NNIS workflow still needs a separately versioned producer step that creates the exact dense merged F32 artifact expected by NNIS, or an equivalent qualified artifact source. That future step must preserve SOUP merge semantics rather than implementing model merging inside Hub.

## Non-claims

This integration does not establish:

- support for every Hugging Face or SOUP model;
- FP16 direct execution admission;
- 4-bit or other quantized representation admission;
- adapter-native NNIS execution;
- numerical or logit equivalence;
- model-quality evidence;
- serving performance evidence;
- physical GPU-memory evidence;
- HML1 resource-aware worker placement;
- HML2 distributed ML orchestration;
- HML4 qualification-campaign maturity.

## Validation

The Python tests use a fake `nnis-hf` producer to verify exact report-byte preservation, fail-closed unknown parameters/schema versions, and propagation of NNIS rejection:

```bash
python3 -m unittest scripts/test_nnis_hf_preflight_hub_adapter.py
```

The Rust contract test parses and validates the shipped manifest through Hub's real component model and pins the NNIS ownership/non-claim properties and execution binding:

```bash
cargo test -p hub-core --test nnis_hf_preflight_component_contract --locked
```

Repository CI additionally runs format, Clippy, locked workspace build/test, rustdoc, and the existing Python adapter contract suite.
