# SOUP + SciRust symbolic reward v1

Status: versioned Hub component edge for deterministic symbolic-equivalence reward during SOUP GRPO.

Hub capability: `llm.train.scirust-symbolic@1.0.0`

SciRust source PR: `Memorithm/scirust#1361`

SciRust final source head: `58d850899cee1d62449cc02816b787b7f8a8a3de`

SciRust merge: `f6bdadb6234129e14e9ea4d69f46901c6dcecbd0`

Qualified SOUP revision: `05b646523727925990530667e7012ede50bd30b2` (v0.73.3 release line).

## What SciRust actually publishes

SciRust does not publish a generic SOUP training backend here. The qualified surface is deliberately narrow:

- executable: `scirust-reward`;
- transport: JSON Lines on stdin/stdout;
- `schema_version`: `1`;
- `kind`: `symbolic_equivalence`;
- candidate/reference symbolic expressions;
- score `1.0` only when `scirust-symbolic::prove_equal` establishes equivalence;
- score `0.0` for a candidate parse failure or when equivalence is not proven;
- malformed trusted references fail the process contract;
- a zero score is **not** a proof of inequality because the prover is sound but incomplete.

SciRust also publishes `integrations/soup/scirust_symbolic_reward.py`, a SOUP custom `reward_fn(completions, **kwargs)` bridge. It batches one `scirust-reward` subprocess per reward batch, consumes SOUP's `answer` dataset column, and extracts common `####` and `\\boxed{}` final-answer forms.

The SciRust exact source head passed the general CI, dedicated SOUP symbolic-reward workflow, workspace Rustdoc, and the additional contextual workflow before merge.

## Hub ownership boundary

Hub adds no symbolic algebra or reward semantics. This component only:

1. selects the already qualified GRPO integration edge;
2. requires fixed trusted deployment paths for the SciRust bridge and binary;
3. materializes exact template seams for `task: grpo` and the trusted `training.reward_fn` path;
4. sets `SCIRUST_REWARD_BIN` for the child SOUP process;
5. delegates training and artifact bundling to the existing SOUP Hub adapter;
6. records source/schema identity in the derived training report.

SOUP remains authoritative for training configuration and runtime behavior. SciRust remains authoritative for symbolic proof/reward behavior. Hub remains the orchestration/provenance layer.

The existing `llm.train@1.0.0` and `llm.train.elastic@1.0.0` capabilities are unchanged.

## Required template seams

A config for this capability must contain the existing Hub dataset/output placeholders plus exactly these two lines:

```yaml
task: ${SOUP_HUB_SCIRUST_SYMBOLIC_TASK}
```

and, directly under `training:` at two-space indentation:

```yaml
  ${SOUP_HUB_SCIRUST_SYMBOLIC_REWARD}
```

Example:

```yaml
base: HuggingFaceTB/SmolLM2-135M
task: ${SOUP_HUB_SCIRUST_SYMBOLIC_TASK}

data:
  train: ${SOUP_HUB_DATASET}

training:
  epochs: 1
  ${SOUP_HUB_SCIRUST_SYMBOLIC_REWARD}
  num_generations: 4
  lora:
    r: 8
    alpha: 16

output: ${SOUP_HUB_OUTPUT}
```

Hub materializes only:

```yaml
task: "grpo"
```

and:

```yaml
  reward_fn: "/opt/scirust-hub/libexec/scirust_symbolic_reward.py"
```

Any second root `task:` or second `reward_fn:` declaration fails closed. Hub intentionally does not provide a user-controlled reward-file parameter because SOUP custom reward files execute trusted Python code.

## Dataset contract

The SciRust bridge requires SOUP to supply an `answer` sequence for every completion batch. Consequently, datasets used with this capability must expose the `answer` column expected by the qualified SOUP GRPO/custom-reward seam.

Hub preserves the dataset artifact but does not reinterpret SOUP dataset semantics or precompute symbolic rewards. Missing or malformed trusted references are rejected by the SciRust/SOUP reward path.

## Deployment

Install the common SOUP adapter and this Hub wrapper:

```bash
sudo install -d -m 0755 /opt/scirust-hub/libexec
sudo install -m 0644 scripts/soup_hub_adapter.py \
  /opt/scirust-hub/libexec/soup_hub_adapter.py
sudo install -m 0644 scripts/soup_scirust_symbolic_hub_adapter.py \
  /opt/scirust-hub/libexec/soup_scirust_symbolic_hub_adapter.py
```

From SciRust merge `f6bdadb6234129e14e9ea4d69f46901c6dcecbd0`, install the exact reviewed reward bridge and a built `scirust-reward` executable at:

```text
/opt/scirust-hub/libexec/scirust_symbolic_reward.py
/opt/scirust-hub/libexec/scirust-reward
```

The binary must be executable; both paths must be regular non-symlink files. The Hub wrapper fails before launch otherwise.

Register the component independently:

```bash
cargo run -p scirust-hub -- component register \
  examples/soup-train-scirust-symbolic-component.json
```

Registration does not attest the deployed file bytes or hardware. Operators remain responsible for installing artifacts from the recorded SciRust revision. A stronger package/attestation layer can be added later without changing the symbolic reward semantics.

## Non-claims

This edge does not establish:

- SciRust as a PyTorch/Transformers replacement for SOUP training;
- correctness for expressions outside `scirust-symbolic`'s supported parser/prover scope;
- inequality when SciRust returns `not_proven`;
- HML1 dynamic worker/resource placement;
- CUDA, Jetson, ARM64, throughput, training-quality, or performance qualification;
- arbitrary custom-reward sandbox safety.

It is a deterministic symbolic-verification reward edge only, using the exact published SciRust process and bridge semantics.
