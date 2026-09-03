# SOUP integration

Status: published `llm.ship@1.0.0`; proposed `llm.train@1.0.0`, `llm.eval@1.0.0`, and `llm.export@1.0.0` on this branch.

This integration treats [SOUP](https://github.com/MakazhanAlpamys/Soup) as an independent LLM post-training product. SciRust Hub owns the outer component contracts, immutable artifact flow, execution lifecycle and provenance. It does not reimplement SOUP training, evaluation, model loading, scoring, export or verdict semantics.

## Qualified upstream revisions

The already-published ship component remains tied to the SOUP revision recorded in `examples/soup-ship-component.json`. The train/eval/export v1 contracts were reviewed against SOUP release reference `v0.73.3` and exact upstream commit:

```text
05b646523727925990530667e7012ede50bd30b2
```

The exact commit is provenance. It is not a claim that arbitrary future SOUP revisions are compatible.

## Capabilities

The integration uses four separate Hub components because a Hub v1 component has one process execution binding:

| Capability | Input | Output | Purpose |
| --- | --- | --- | --- |
| `llm.ship@1.0.0` | SOUP evidence JSON | verdict JSON | replay a frozen ship gate |
| `llm.train@1.0.0` | SOUP config template + dataset | deterministic model bundle + report | execute local SOUP training |
| `llm.eval@1.0.0` | deterministic model bundle | result JSON | execute `soup eval benchmark` |
| `llm.export@1.0.0` | deterministic model bundle | deterministic export bundle + report | execute a validated SOUP export |

The manifests are `examples/soup-{ship,train,eval,export}-component.json`.

## Training configuration contract

Hub does not parse or rewrite SOUP's schema. Instead the immutable config input must contain two explicit tokens:

```yaml
base: meta-llama/Llama-3.1-8B-Instruct
task: sft
data:
  train: ${SOUP_HUB_DATASET}
output: ${SOUP_HUB_OUTPUT}
```

Before execution, the adapter replaces `${SOUP_HUB_DATASET}` with the absolute path of Hub's materialized `dataset` artifact and `${SOUP_HUB_OUTPUT}` with a new output directory inside the per-run Hub work directory. Both tokens are required. This keeps dataset identity tied to the Hub input digest without teaching Hub the SOUP configuration schema.

The `llm.train` parameter object currently accepts only:

- `gpus`: `"auto"` or integer `1..64`;
- `trust_remote_code`: boolean, default `false`.

Unknown parameters fail closed. Remote-code execution remains opt-in.

## Deterministic model bundle v1

SOUP models, adapters, tokenizer files and checkpoints are directory-shaped, while Hub process outputs are files. The adapter therefore emits an uncompressed deterministic tar with media type:

```text
application/vnd.scirust-hub.soup-bundle.v1+tar
```

Bundle v1 has these invariants:

- all paths live below one `artifact/` root;
- entries are sorted;
- uid/gid and owner names are normalized;
- modification times are zeroed;
- directory/file modes are normalized;
- symbolic links and non-regular filesystem entries are rejected;
- extraction rejects absolute paths, `..`, duplicate members, links, devices and FIFOs;
- member count and total extracted payload are bounded;
- existing extraction targets are never overwritten.

Hub ingests declared output files through streaming content-addressed storage. The default artifact ceiling is 64 GiB. HTTP request bodies and inline artifact responses keep independent, much smaller limits, so this is not a 64 GiB HTTP upload surface.

## Evaluation contract

`llm.eval` safely extracts the model bundle into the Hub run directory, resolves one model root, and invokes `soup eval benchmark` without a shell.

Accepted parameters are:

- `benchmarks`: safe comma-separated task identifiers, default `mmlu`;
- `fewshot`: integer `0..100`;
- `batch_size`: integer `1..4096`, default `8`;
- `device`: `cpu`, `mps`, `cuda`, or `cuda:<index>`;
- `model_subpath`: explicit relative path when a bundle contains more than one plausible model root.

The result artifact contains the bounded child stdout/stderr and the resolved parameter set. SOUP remains authoritative for benchmark semantics.

## Export contract

`llm.export` accepts `format`, optional `quant`, optional `base`, and optional `model_subpath`. Formats are allowlisted by the adapter and currently include the export formats reviewed in the qualified SOUP revision: `gguf`, `onnx`, `tensorrt`, `awq`, `gptq`, `bitnet`, `tq1_0`, `torchao`, and `gguf-ud`.

SOUP may produce a file or directory; the adapter normalizes either form into the same deterministic bundle v1 before Hub ingests it.

## Ship verdict boundary

SOUP's `ship` command uses verdict-oriented process statuses: `0 = SHIP`, `2 = DON'T SHIP`, `1 = runtime error`, `3 = usage/validation error`. A plain Hub process binding would discard a valid exit `2` as a process failure. `soup_hub_adapter.py` therefore maps exits `0` and `2` to adapter success only when a regular, non-symlink verdict file exists. Runtime errors, usage errors, signals, unknown statuses and missing verdicts remain failures.

## Runtime and deployment

The component manifests expect the adapter at:

```text
/opt/scirust-hub/libexec/soup_hub_adapter.py
```

SOUP itself must be installed on the selected worker with the extras needed by the operation. For example, training needs SOUP's training dependencies and benchmark evaluation needs the eval stack. The Hub integration does not install or upgrade SOUP at execution time.

For a source checkout:

```bash
sudo install -d -m 0755 /opt/scirust-hub/libexec
sudo install -m 0644 scripts/soup_hub_adapter.py \
  /opt/scirust-hub/libexec/soup_hub_adapter.py
```

Register only the capabilities available on that worker, for example:

```bash
cargo run -p scirust-hub -- component register examples/soup-train-component.json
cargo run -p scirust-hub -- component register examples/soup-eval-component.json
cargo run -p scirust-hub -- component register examples/soup-export-component.json
cargo run -p scirust-hub -- component register examples/soup-ship-component.json
```

Registration remains metadata-only.

## Security and provenance

The adapter invokes child processes with structured argv, `shell=False`, and closed stdin. Unknown Hub parameters are rejected. Hub inputs must be regular files. Bundle capture/extraction rejects symlinks and unsafe tar member types. Child stdout/stderr is written to run-local files and only a bounded prefix is embedded in the JSON report.

This is process supervision and provenance, not an OS sandbox. A SOUP worker can download remote model/data dependencies allowed by the SOUP configuration and network policy. `trust_remote_code` remains false unless a run explicitly requests it. The selected worker's OS identity and privileges still define the execution boundary.

No Jetson/aarch64 training compatibility is claimed by this contract. That requires a real hardware qualification run of the relevant SOUP/PyTorch/bitsandbytes stack.

## Validation

The Python suite covers semantic ship exits, deterministic bundle round trips, symlink rejection, tar traversal rejection, unknown parameters, and fake-process end-to-end train/eval/export flows:

```bash
python3 -m unittest scripts/test_soup_hub_adapter.py
```

Rust tests parse every shipped manifest through Hub's real `ComponentManifest` validator:

```bash
cargo test -p hub-core --test soup_component_contract --locked
cargo test -p hub-core --test soup_ml_component_contract --locked
```

Repository CI additionally runs format, Clippy, workspace build/test and rustdoc gates.

## Ecosystem boundary after Hub integration

Hub integration makes SOUP executable and provenance-preserving inside the Memorithm control plane. It does not by itself make SOUP a SciRust compute backend. The next cross-repository contracts are deliberately separate:

1. SciRust: deterministic verifier/reward process usable by SOUP RLVR/GRPO without importing PyTorch into SciRust;
2. Forge: execution-driven search over SOUP recipes, with SOUP evaluation evidence authoritative and `soup sweep` retained as a baseline;
3. ElasticXxx: adaptive model-residency/resource policy mapped to SOUP streaming/batch knobs without moving adaptation policy into Hub.

Those edges must each be versioned and tested before they are treated as operational.
