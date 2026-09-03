# SOUP integration

Status: first published integration slice, `llm.ship@1.0.0`.

This integration treats [SOUP](https://github.com/MakazhanAlpamys/Soup) as an independent
LLM post-training product. SciRust Hub owns only the outer component contract,
immutable artifact flow, execution lifecycle and provenance. It does not reimplement
SOUP training, evaluation, model loading, scoring or verdict semantics.

The first contract deliberately covers **offline ship-gate replay only**. Full SOUP
training/export is not yet a Hub contract because current Hub v1 declared outputs are
single bounded files, while trained adapters/checkpoints are naturally directory-shaped
and may be much larger than the current artifact limit. Publishing a training contract
before Hub can preserve that lineage would make the integration look more complete than
it is.

## Published capability

`examples/soup-ship-component.json` publishes:

- capability: `llm.ship`;
- contract version: `1.0.0`;
- immutable input: SOUP ship evidence JSON;
- required output: SOUP verdict JSON;
- execution mode: offline evidence replay.

The example component identifies upstream SOUP `v0.73.3` and records the upstream
`main` commit that was reviewed when this contract was authored. Those identifiers are
provenance, not a claim that every future SOUP release is wire-compatible.

## Why the adapter exists

SOUP's `ship` command has verdict-oriented exit codes:

- `0`: `SHIP`;
- `2`: `DON'T SHIP`;
- `1`: runtime error;
- `3`: usage/validation error.

A plain Hub process binding cannot preserve that distinction because Hub normally treats
a non-zero process exit as a failed run and ingests declared output files only after a
clean process exit. Directly binding `soup ship` would therefore discard a valid
`DON'T SHIP` verdict as if the evaluator itself had failed.

`scripts/soup_hub_adapter.py` is a deliberately narrow translation boundary. It invokes
SOUP without a shell and maps SOUP exits `0` and `2` to adapter success **only when a
regular, non-symlink verdict file was actually produced**. SOUP exits `1`, `3`, signals,
unknown statuses, or a missing verdict remain failures. The adapter does not inspect or
reinterpret the verdict payload.

## Runtime boundary

The component example expects the adapter at:

```text
/opt/scirust-hub/libexec/soup_hub_adapter.py
```

and executes it with `python3`. SOUP itself must be installed in the worker's `PATH`.
The example does not install or upgrade SOUP and does not enable network access.

For a source checkout, install the adapter explicitly:

```bash
sudo install -d -m 0755 /opt/scirust-hub/libexec
sudo install -m 0644 scripts/soup_hub_adapter.py \
  /opt/scirust-hub/libexec/soup_hub_adapter.py
```

Then register the component through the normal Hub registry path:

```bash
cargo run -p scirust-hub -- component register examples/soup-ship-component.json
```

Registration is metadata-only; it does not execute SOUP.

## Security and provenance properties

The v1 adapter:

- passes argv directly through `subprocess.run(..., shell=False)`;
- closes stdin;
- rejects a symlink or non-regular evidence input;
- accepts a semantic verdict only if the declared verdict exists as a regular,
  non-symlink file;
- leaves SOUP's verdict bytes untouched so Hub can content-address the exact output;
- keeps runtime/usage failures distinct from negative model-quality verdicts.

Hub still provides the outer per-run work directory, materializes the input artifact from
its content-addressed store, records the component/contract version and digests, captures
stdout/stderr, and ingests the required verdict artifact.

This is process supervision and provenance, not a sandbox. SOUP executes with the OS
identity and privileges of the selected Hub worker.

## Validation

The Rust integration test in `crates/hub-core/tests/soup_component_contract.rs` parses the
shipped manifest through Hub's real `ComponentManifest` validator and pins the published
capability, versioned ports and placeholders.

The dependency-free Python test suite exercises the semantic-exit adapter including a
real subprocess fixture that writes a `DON'T SHIP` verdict and exits `2`:

```bash
python3 -m unittest scripts/test_soup_hub_adapter.py
cargo test -p hub-core --test soup_component_contract --locked
```

Both commands are part of, or covered by, the repository CI path.

## Next integration slices

The following are intentionally **not** claimed by `llm.ship@1.0.0`:

1. **Large/directory artifact lineage.** Hub needs a versioned, bounded representation
   for model directories, adapters, checkpoints and tokenizer/config file sets before a
   trustworthy `llm.train` or `llm.export` contract is published.
2. **Resource-aware placement.** A SOUP training capability should declare accelerator,
   VRAM/RAM, dtype/backend and storage requirements only after Hub's ML capability and
   worker-resource schemas can validate them. Static worker URLs are not sufficient.
3. **SciRust acceleration.** No SciRust compute backend is enabled by this first slice.
   A future bridge must expose a measured, versioned operation that SOUP can call without
   making Hub or SciRust depend on PyTorch training semantics. Candidate areas must be
   selected from real SciRust APIs and benchmarked against SOUP's existing implementation
   before adoption.
4. **Forge optimization.** Forge may later search SOUP recipes/configurations, but the
   search semantics remain Forge-owned and the executed training/evaluation semantics
   remain SOUP-owned.

The integration should advance in that order so every new edge remains testable,
reproducible and attributable to the component that actually owns the result.
