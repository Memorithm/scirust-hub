# Forge → SOUP post-training search through SciRust Hub

Hub publishes `llm.optimize.forge_soup@1.0.0` as a narrow orchestration edge between the qualified Forge SOUP search domain and SOUP post-training execution.

## Ownership

- **Forge** owns candidate generation/mutation, verify-before-measure ordering, Pareto selection, holdout handling and campaign report semantics.
- **SOUP** owns training, model loading and benchmark semantics.
- **SciRust Hub** owns immutable input/output artifact flow, process wiring, boundary validation and orchestration provenance.

Hub does not reinterpret a successful process exit as model quality, does not invent VRAM numbers, does not promote a Forge winner automatically, and does not move Forge search logic or SOUP training logic into Hub.

## Qualified source identities

- Forge SOUP typed domain: PR #25, merge `1385c71a541419f15a558a5e94bc8a4a60567a4a`.
- Forge process runner: PR #26, exact green head `dc3189591436da6e27734b2953e94eaad7057f1e`, merge `9e1f3fc568c176f401735c121780d9fbe6834f5d`.
- SOUP qualified commit: `05b646523727925990530667e7012ede50bd30b2`.

## Inputs

The component consumes three immutable Hub inputs:

1. `campaign`: Forge `SoupCampaignSpecV1` JSON. The Hub v1 edge accepts only `MakazhanAlpamys/Soup` at the qualified commit above.
2. `config`: SOUP YAML template containing `${SOUP_HUB_DATASET}`, `${SOUP_HUB_OUTPUT}`, and one `${FORGE_SOUP:<dimension>}` token for every searchable dimension.
3. `dataset`: immutable dataset supplied to every candidate run.

Candidate dimension names on this edge are restricted to `[A-Za-z0-9_.-]{1,128}` and candidate values to `[A-Za-z0-9_./:+-]{1,256}`. This intentionally excludes multiline/raw-YAML candidate injection. A wider representation must be introduced as a new versioned contract rather than silently weakening v1.

## Verify and measure

`verify` is an executed SOUP gate: the evaluator materializes the candidate config and runs `soup train --dry-run --yes`. A non-zero dry-run result becomes `passed=false` evidence; it is not a measurement.

`measure` runs the materialized candidate through real `soup train`, then, when benchmark objectives are requested, runs real `soup eval benchmark`. The evaluator sets `SOUP_DB_PATH` to a fresh isolated SQLite database and reads scores from SOUP's own `eval_results` table. At the qualified SOUP commit, `soup eval benchmark` persists one numeric `score` per benchmark together with `details_json`; Hub therefore does not parse Rich terminal tables or infer a score from stdout.

Supported v1 objective names are:

- `benchmark:<lm-eval-task>` — value stored by SOUP for that benchmark;
- `train_wall_ms` — monotonic elapsed time around the executed training subprocess;
- `eval_wall_ms` — monotonic elapsed time around the executed benchmark subprocess; requires at least one benchmark objective;
- `total_wall_ms` — monotonic elapsed time around the complete measurement phase.

The executed metric set must exactly equal the Forge campaign objective set. Unsupported names, including synthetic VRAM objectives, fail closed.

## Evidence

Every Forge evaluator invocation writes one immutable JSON evidence record containing candidate/trial identity, materialized-config digest, executed metrics or dry-run verdict, environment fingerprint, bounded log excerpts and full log SHA-256 digests. After Forge completes, Hub packages the records into a deterministic tar output:

`application/vnd.scirust-hub.forge-soup-evidence.v1+tar`.

The Forge campaign report remains unmodified and is emitted separately as:

`application/vnd.scirust-hub.forge-soup-campaign-report.v1+json`.

The environment fingerprint records OS/kernel/architecture, Python version, observed `soup --version`, requested device, `CUDA_VISIBLE_DEVICES`, optional device-tree model, and optional `nvidia-smi` GPU/driver output. This improves evidence identity but is not a cryptographic attestation of the host or SOUP binary.

## Trust and isolation boundary

Forge PR #26 deliberately keeps its process runner local. This Hub v1 component likewise does not enable Forge distributed workers and never passes `--isolation-available`.

If a campaign declares `environment.isolation_required=true`, Hub refuses it. Local process supervision is not described as a hostile-code sandbox. FG1/FG2/FG6 and Hub HML1/HML2 remain separate work.

## Deployment

Install the Hub adapters and the Forge runner at the paths embedded in the component contract:

```bash
sudo install -d -m 0755 /opt/scirust-hub/libexec
sudo install -m 0644 scripts/forge_soup_hub_adapter.py \
  /opt/scirust-hub/libexec/forge_soup_hub_adapter.py
sudo install -m 0644 scripts/forge_soup_hub_evaluator.py \
  /opt/scirust-hub/libexec/forge_soup_hub_evaluator.py
sudo install -m 0755 /path/to/forge-soup-posttrain \
  /opt/scirust-hub/libexec/forge-soup-posttrain
cargo run -p scirust-hub -- component register examples/forge-soup-posttrain-component.json
```

`python3` is fixed as `/usr/bin/python3` when Forge spawns the evaluator because `ProcessSoupEvaluator` requires an absolute program path. `soup` itself remains deployment-resolved from `PATH`, and its observed `--version` is recorded in each evidence file. The component metadata is a qualification reference, not proof that an arbitrary installed `soup` binary exactly equals the qualified source commit.

## Non-claims

This contract does not establish distributed-worker trust, external hostile-code isolation, resource-aware worker placement, automatic candidate promotion, hardware performance portability, Jetson/ARM64 qualification, or Forge/SOUP 5/5 maturity. Those require separate evidence-backed phases.
