# SOUP → Forge → SciRust-Verify handoff

SciRust Hub publishes `llm.verify.forge_soup@1.0.0` as the verification edge for the already-published `llm.optimize.forge_soup@1.0.0` search workflow.

## Ownership

- SOUP owns training, dry-run validation, benchmark execution, model loading and metric semantics.
- Forge owns candidate search, verify-before-measure ordering and Pareto selection.
- SciRust-Verify owns evidence ingestion, dossier structure, integrity sealing, scope and verdict semantics.
- SciRust Hub only materializes immutable inputs, invokes the qualified Verify process and stores the resulting dossier artifact.

Hub does not recompute or reinterpret SciRust-Verify verdicts.

## Inputs and output

The component consumes two immutable artifacts emitted by the Forge/SOUP edge:

- `report`: `application/vnd.scirust-hub.forge-soup-campaign-report.v1+json`
- `evidence_bundle`: `application/vnd.scirust-hub.forge-soup-evidence.v1+tar`

It invokes the fixed deployment path:

```text
/opt/scirust-hub/libexec/scirust-verify-forge-soup
```

The only declared output is:

- `dossier`: `application/vnd.scirust-verify.dossier.v1+tar`

The output is ingested through Hub's normal bounded, content-addressed declared-output path and therefore becomes an immutable Hub artifact with run provenance.

## Qualified identities

The v1 component pins:

- SOUP commit `05b646523727925990530667e7012ede50bd30b2`
- Forge SOUP domain merge `1385c71a541419f15a558a5e94bc8a4a60567a4a`
- Forge SOUP runner merge `9e1f3fc568c176f401735c121780d9fbe6834f5d`
- Hub Forge/SOUP edge merge `074cf2c6e00a0b142fe46d1558c8b32df9228859`
- SciRust-Verify Forge/SOUP adapter merge `89f485633e842017781778eb1568b5306e0a5570`
- SciRust-Verify process head `71062d4b28ff1ed8bd26bbb76a643d15e07354cb`
- SciRust-Verify process merge `4c2db0832d8148f62261e67254f8bf00f80e808c`

Unknown or drifted evidence contracts fail in SciRust-Verify rather than being normalized by Hub.

## What a successful run means

A successful `llm.verify.forge_soup@1.0.0` run means the supplied report and evidence bundle satisfy the qualified v1 structural/identity contract and a sealed SciRust-Verify dossier was produced.

It does **not** independently establish:

- model quality;
- performance superiority;
- hardware portability;
- cross-host comparability;
- hostile-code isolation;
- source authenticity beyond the provenance identities supplied by the trusted Hub workflow.

Search scores, Pareto membership and SOUP dry-run success remain source observations; they do not self-authorize a broader verification verdict.

## Deployment

Install the SciRust-Verify process binary at the exact configured path, then register the component manifest:

```bash
sudo install -d -m 0755 /opt/scirust-hub/libexec
sudo install -m 0755 scirust-verify-forge-soup \
  /opt/scirust-hub/libexec/scirust-verify-forge-soup
cargo run -p scirust-hub -- component register examples/forge-soup-verify-component.json
```

A workflow can pass the `report` and `evidence_bundle` artifact outputs of `llm.optimize.forge_soup@1.0.0` directly into this capability. The resulting `dossier` artifact is the verification-layer handoff and should be preserved rather than reconstructed by Hub.
