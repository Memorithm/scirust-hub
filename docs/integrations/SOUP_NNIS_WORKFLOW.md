# SOUP → NNIS preflight workflow

This integration recipe composes existing versioned SciRust Hub capabilities without moving model-training, PEFT merge, or NNIS admission semantics into Hub.

The reference workflow is `examples/soup-nnis-preflight-workflow.json`.

## Graph

```text
existing config artifact ─┐
                          ├─> llm.train@1.0.0
existing dataset artifact ┘         │ model_bundle
                                    v
                         llm.merge.nnis@1.0.0
                                    │ merged_model_bundle
                                    v
                 inference.nnis.hf_preflight@1.0.0
                                    │ preflight
                                    v
                         nnis.hf-preflight@1
```

The workflow is intentionally sequential (`max_concurrency: 1`) because every downstream step consumes an immutable output from the preceding step. The data dependencies are expressed with Hub `from_step` bindings, so they also define the DAG ordering; no hidden shared filesystem contract is assumed.

## External inputs

The checked-in example uses placeholder artifact UUIDs for the two inputs that must already exist in Hub:

- `train.config`: a SOUP YAML template accepted by `llm.train@1.0.0`;
- `train.dataset`: the immutable dataset artifact referenced by that template.

Replace only those two placeholder artifact IDs when submitting a real workflow. Component IDs, capability names, output labels, and cross-step bindings are contract-pinned by the regression test and should not be edited casually.

## Step 1: SOUP training

Component `00000000-0000-0000-0000-000000000003` executes `llm.train@1.0.0`.

This step preserves the existing SOUP training contract. Its `model_bundle` is the deterministic bounded Hub bundle of the exact SOUP training output tree. The reference recipe is suitable only when that tree resolves to a LoRA adapter accepted by the next step.

A full-fine-tune SOUP run (`lora.r: 0`) produces a dense model rather than a LoRA adapter and therefore must not be routed through `llm.merge.nnis@1.0.0`.

## Step 2: SOUP dense F32 merge

Component `6df1e39d-b861-4eb9-a2a6-4d696c74bc75` executes `llm.merge.nnis@1.0.0`.

Hub materializes the `model_bundle` from the training step as `adapter_bundle`. The adapter invokes SOUP's own merge surface with the fixed v1 policy:

- `soup merge`;
- base model auto-detected from `adapter_config.json`;
- `--dtype float32`;
- `--save-format fp16`;
- `--hub hf`;
- no `--trust-remote-code`.

SOUP owns PEFT/model merge semantics. Hub only captures the resulting directory into the same deterministic bounded bundle format.

## Step 3: NNIS preflight

Component `9f13791c-0a54-4c2c-8abf-0d0644d33437` executes `inference.nnis.hf_preflight@1.0.0`.

Hub passes the merge step's `merged_model_bundle` unchanged as the preflight `model_bundle`. The adapter extracts the local Hugging Face-style tree and delegates admission to NNIS `nnis-hf validate`.

NNIS remains authoritative for `nnis.hf-preflight@1`. Hub validates only the known report schema boundary and preserves the exact producer bytes as an immutable output artifact.

## What this workflow proves

A successful Hub workflow proves only that:

1. all three registered process contracts executed successfully in dependency order;
2. their required immutable artifacts were materialized and preserved by Hub;
3. SOUP accepted and produced the fixed dense F32 merge output;
4. NNIS emitted a valid `nnis.hf-preflight@1` report for that exact merged bundle.

It does not by itself establish numerical equivalence, model quality, CUDA execution, serving performance, promotion eligibility, general model-family support, Jetson/ARM64 compatibility, HML1 resource-aware placement, HML2 distributed orchestration, or HML4 qualification-campaign maturity.

## Submission

Register the three published components, import the real config and dataset as Hub artifacts, replace the two placeholder artifact UUIDs in a copy of the reference JSON, then submit it with the existing workflow CLI:

```bash
cargo run -p scirust-hub -- workflow submit /path/to/soup-nnis-preflight-workflow.json
cargo run -p scirust-hub -- workflow run <workflow-uuid>
cargo run -p scirust-hub -- workflow inspect <workflow-uuid>
```

The workflow engine records each step as an ordinary Hub run with immutable artifact provenance. Reproduction and failure semantics therefore remain those of the existing workflow subsystem rather than a SOUP- or NNIS-specific scheduler path.
