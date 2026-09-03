# SOUP + ElasticXxx resource-plan handoff v1

Status: versioned Hub component edge for pre-execution resource planning.

Hub capability: `llm.train.elastic@1.0.0`

Elastic contract: `elastic.soup.run-resource-plan@1.0.0`

Elastic media type: `application/vnd.elastic.soup.run-resource-plan.v1+json`

Elastic source merge: `6e0952e59842eb9c14f808266d6f3eb0b1f33014`

Qualified SOUP revision: `05b646523727925990530667e7012ede50bd30b2` (v0.73.3 release line).

## Ownership boundary

This edge composes three existing responsibilities without moving them:

- ElasticXxx chooses and validates the pre-execution resource plan;
- SciRust Hub stores the plan as an immutable input artifact, validates the known wire version, materializes the explicit template seams, launches the component, and records provenance;
- SOUP parses the final staged `soup.yaml` and remains authoritative for training and all deeper compatibility checks.

Hub does **not** optimize batch size, choose streaming policy, implement SOUP's trainer, or claim dynamic worker placement. `ml.placement_enforcement` remains `component_preflight`; HML1 resource-aware worker placement is a separate future control-plane gate.

The existing `llm.train@1.0.0` component remains unchanged. Elastic planning is opt-in through the separate `llm.train.elastic@1.0.0` capability.

## Inputs

The component accepts three immutable Hub inputs:

1. `config`: a SOUP YAML template;
2. `dataset`: the training dataset artifact;
3. `resource_plan`: the ElasticXxx v1 JSON envelope.

The resource plan is fail-closed on unknown fields, duplicate JSON keys, unknown contract identity, an unqualified SOUP revision, invalid batch/stream bounds, unknown enum values, and streaming tasks outside the Elastic v1 allowlist.

Hub revalidates these boundary facts because it is consuming an external artifact. It does not reproduce ElasticXxx planning logic.

## Required template seams

The config template must keep the existing Hub dataset/output placeholders and add exactly two Elastic-owned seams:

```yaml
base: HuggingFaceTB/SmolLM2-135M
task: ${SOUP_HUB_RESOURCE_TASK}

data:
  train: ${SOUP_HUB_DATASET}
  format: alpaca

training:
  epochs: 1
  ${SOUP_HUB_RESOURCE_PLAN}
  lora:
    r: 8
    alpha: 16
  quantization: none

output: ${SOUP_HUB_OUTPUT}
```

The task placeholder must occupy the exact root line:

```text
task: ${SOUP_HUB_RESOURCE_TASK}
```

The resource-plan placeholder must occupy the exact two-space-indented line:

```text
  ${SOUP_HUB_RESOURCE_PLAN}
```

This deliberately avoids a general-purpose YAML parser or arbitrary text mutation inside Hub. Every other config field remains byte-for-byte template-owned except for the pre-existing dataset/output materialization performed by the SOUP Hub adapter.

## Materialized knobs

For a resident plan, Hub emits only:

```yaml
  batch_size: <auto-or-positive-integer>
  auto_batch_size_strategy: <auto|static|probe>
  stream_layers: false
```

For a streaming plan it emits:

```yaml
  batch_size: <positive-integer>
  auto_batch_size_strategy: <auto|static|probe>
  stream_layers: true
  stream_source: <auto|ram|disk>
  stream_buffers: <2..8>
```

SOUP then revalidates its full config. For example, SOUP may reject combinations involving backend, modality, quantization, LoRA state, KTO batch constraints, or other trainer-specific rules that are intentionally not duplicated in Hub.

## Deployment

Install both process adapters on workers that advertise this component:

```bash
sudo install -d -m 0755 /opt/scirust-hub/libexec
sudo install -m 0644 scripts/soup_hub_adapter.py \
  /opt/scirust-hub/libexec/soup_hub_adapter.py
sudo install -m 0644 scripts/soup_elastic_hub_adapter.py \
  /opt/scirust-hub/libexec/soup_elastic_hub_adapter.py
```

Register the new component independently of legacy SOUP training:

```bash
cargo run -p scirust-hub -- component register examples/soup-train-elastic-component.json
```

Registration is not hardware qualification. No CUDA, Jetson, ARM64, throughput, VRAM-saving, or heterogeneous-placement claim follows from registering this component.

## Provenance

The resource plan is a first-class input artifact, so Hub run provenance retains its immutable identity alongside the config and dataset. The generated SOUP training report additionally records the accepted Elastic contract, media type, source merge, qualified SOUP commit, and resolved task for human inspection.

That report annotation is descriptive provenance only; it is not a replacement for the original input artifact and does not upgrade HML1 placement maturity.
