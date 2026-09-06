# SOUP → NNIS dense F32 artifact bridge

Status: proposed `llm.merge.nnis@1.0.0` on this branch.

This integration connects the existing Hub SOUP artifact transport to the published NNIS CPU-only Hugging Face preflight without moving either product's domain semantics into SciRust Hub.

The pipeline boundary is:

```text
SOUP LoRA adapter bundle
  -> llm.merge.nnis@1.0.0
  -> dense F32 Hub model bundle
  -> inference.nnis.hf_preflight@1.0.0
  -> nnis.hf-preflight@1 artifact
```

Hub owns artifact materialization, deterministic bounded bundling, process invocation, and provenance. SOUP owns base-model loading, PEFT adapter loading, `merge_and_unload`, tokenizer handling, and checkpoint serialization. NNIS owns model admission and direct-execution readiness.

## Reviewed SOUP merge surface

The adapter was designed against SOUP `main` commit:

```text
9f1e48b33a5fcda7e621e612ec9305cb52a38e07
```

At that commit, `soup merge`:

- requires a local `--adapter` directory containing `adapter_config.json`;
- auto-detects the base model from `base_model_name_or_path` when `--base` is absent;
- accepts `float16`, `bfloat16`, and `float32` as `--dtype` values;
- loads the base through Transformers on CPU;
- loads the adapter through PEFT;
- calls `merge_and_unload()`;
- on the ordinary `fp16` save-format path, writes the merged model with `save_pretrained` and saves the tokenizer into the same output directory;
- denies remote code unless `--trust-remote-code` is explicitly supplied;
- requires `--output` to remain under the process cwd.

The save-format name `fp16` selects the ordinary dense merge path; the actual model dtype is independently selected by `--dtype`. The NNIS bridge therefore invokes exactly:

```text
soup merge \
  --adapter <materialized-adapter> \
  --output <temporary-merged-directory> \
  --dtype float32 \
  --save-format fp16 \
  --hub hf
```

It deliberately supplies neither `--base` nor `--trust-remote-code`.

## Why the capability is narrow

The capability is named:

```text
llm.merge.nnis@1.0.0
```

rather than publishing a generic Hub `llm.merge` surface. The fixed F32 policy exists only because the currently qualified NNIS direct local-HF path requires a dense F32 logical base graph. A future broader SOUP merge contract should be versioned separately rather than expanding this consumer-specific bridge in place.

Hub rejects unknown parameters. The only v1 parameter is optional `model_subpath`, used solely to disambiguate the adapter root inside the deterministic input bundle.

## Input contract

Input port:

```text
adapter_bundle: application/vnd.scirust-hub.soup-bundle.v1+tar
```

The existing Hub deterministic-bundle extractor is reused. It rejects unsafe paths, links, devices, FIFOs, duplicate members, overwrite attempts, excessive member counts, and excessive extracted payload.

After extraction, Hub resolves one model root and requires that root to contain a regular `adapter_config.json`. This is a boundary/type check, not a reimplementation of PEFT or SOUP merge semantics.

This matters for SOUP full fine-tuning. Current SOUP documents `lora.r: 0` as producing a complete dense model rather than an adapter. Such a bundle is not silently passed through this merge capability; it fails the adapter input contract. A future dense-model-to-NNIS edge can bypass merge and go directly to NNIS preflight if separately qualified.

## Output contract

Outputs:

```text
merged_model_bundle:
  application/vnd.scirust-hub.soup-bundle.v1+tar

report:
  application/vnd.scirust-hub.soup-nnis-merge-report.v1+json
```

After a successful SOUP process exit, Hub requires only that the requested merged output directory exists, then captures it with the existing deterministic bounded bundle implementation. Hub does not parse `config.json`, inspect Safetensors, infer actual tensor dtype, or assert NNIS compatibility.

The report records the fixed requested SOUP invocation, resolved adapter subpath, bundle size/count information, bounded stdout/stderr capture, and the reviewed SOUP source commit. It is orchestration provenance, not model-validation evidence.

## Failure semantics

No merged bundle or report is published when `soup merge` returns non-zero.

A successful merge does not imply successful NNIS admission. The output bundle must be passed to:

```text
inference.nnis.hf_preflight@1.0.0
```

which invokes NNIS's producer-owned `nnis-hf validate --json --output`. NNIS may still reject the model for architecture, config, tokenizer, Safetensors, tensor-name/shape/dtype, graph-completeness, or direct-execution-readiness reasons.

## Network and security boundary

The merge process is not declared network-free. SOUP may need to resolve the base model named by `adapter_config.json` from Hugging Face unless it is already available locally/cached or the adapter records a local base path.

Hub does not enable remote code. The v1 bridge supplies no `--trust-remote-code` flag and no `--base` override. This intentionally limits the bridge to the standard SOUP/Transformers path compatible with the current NNIS scope.

Process supervision is not an OS sandbox. Workers must apply their normal deployment and isolation policy.

## Relationship to `llm.train`

Hub's existing `llm.train@1.0.0` adapter bundles the exact SOUP training output directory after a successful run. For a compatible LoRA training run whose output root contains `adapter_config.json`, that immutable bundle can feed `llm.merge.nnis` directly.

This does not mean every `llm.train` configuration is mergeable:

- full fine-tuning (`lora.r: 0`) produces a dense model, not a LoRA adapter;
- other task/backend/output modes may have different artifact semantics;
- a merge may require access to the base model referenced by the adapter;
- downstream NNIS still decides whether the dense result is admissible.

The producer and consumer therefore fail closed at their respective boundaries rather than treating the shared tar transport type as semantic compatibility.

## Resource declaration

The capability publishes `hub.ml.resource-requirements@1.0.0` with:

- backend: `soup`;
- device: `cpu`;
- dtype: `float32`;
- accelerator: `none`;
- memory: `operation_defined`;
- placement enforcement: `component_preflight`.

This describes the operation, not HML1 resource-aware placement. A dense F32 merge can require substantial host RAM, and v1 does not claim an exact RAM estimator.

## Deployment

Install the adapter beside the other Hub process adapters:

```bash
sudo install -d -m 0755 /opt/scirust-hub/libexec
sudo install -m 0644 scripts/soup_hub_adapter.py \
  /opt/scirust-hub/libexec/soup_hub_adapter.py
sudo install -m 0644 scripts/soup_nnis_merge_hub_adapter.py \
  /opt/scirust-hub/libexec/soup_nnis_merge_hub_adapter.py
```

SOUP must be installed on the worker. The adapter resolves it from `SOUP_BIN` when set, otherwise from `soup` on `PATH`.

Register the component:

```bash
cargo run -p scirust-hub -- component register examples/soup-nnis-merge-component.json
```

Registration is metadata-only and does not execute a merge.

## Validation

Adapter contract tests use a fake `soup` executable. They verify that Hub:

- supplies exactly the fixed F32/ordinary-dense/HF merge flags;
- never supplies a base override or remote-code opt-in;
- bundles the exact directory written by the SOUP process;
- rejects a dense input bundle without `adapter_config.json`;
- rejects attempts to override the fixed dtype through Hub parameters;
- publishes no bundle/report after a SOUP failure.

Run them with:

```bash
python3 -m unittest scripts/test_soup_nnis_merge_hub_adapter.py
```

The Rust contract test pins the manifest and ownership boundary:

```bash
cargo test -p hub-core --test soup_nnis_merge_component_contract --locked
```

## Non-claims

This integration does not establish:

- general SOUP merge support in Hub;
- compatibility of every SOUP training output with this bridge;
- support for remote-code model families;
- offline/base-model availability;
- NNIS FP16, 4-bit, adapter-native, or general Hugging Face support;
- numerical/logit equivalence or model-quality evidence;
- serving-performance or physical-memory evidence;
- exact host-RAM requirements for the merge;
- HML1 resource-aware placement, HML2 distributed orchestration, or HML4 campaign maturity.
