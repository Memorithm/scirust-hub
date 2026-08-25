# Contributing to SciRust Hub

## Ground rules

1. **Observe before designing.** Do not invent contracts for other ecosystem
   projects (SciRust, SciCapsule, Forge, …). Read their repositories first;
   if a fact cannot be established, write "Not yet established" instead of
   guessing. See `docs/architecture/BOUNDARIES.md`.
2. **The Hub connects; it does not absorb.** No scientific kernels, no
   capsule formats, no search engines inside this repository.
3. **No false claims.** Every README/report statement about behavior must be
   backed by a command in this repo that demonstrates it.

## Validation gates (all must pass locally before pushing)

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo build --workspace --all-targets --locked
cargo test --workspace --all-features --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked
```

These are exactly what CI runs; results must match locally.

## Code conventions

- Edition 2021, MSRV 1.89. Avoid APIs newer than the MSRV.
- `unsafe` is forbidden workspace-wide.
- Library code never panics on user/system input: typed errors with context;
  panics are reserved for impossible invariants and tests.
- The domain layer (`hub-core`) stays synchronous, runtime-free and
  deterministic given an injected `Clock`. Transport/process concerns live in
  adapters.
- Determinism: sort explicitly, canonicalize serialized forms used for
  digests, never depend on HashMap iteration order.
- New dependencies require justification (maintenance, license, build cost)
  noted in the PR description; check whether SciRust already provides the
  capability first.

## Commits

Small, coherent commits following Conventional-Commit style prefixes
(`feat:`, `fix:`, `docs:`, `test:`, `ci:`, `chore:`). Never commit secrets,
caches or `target/`.

## Security-sensitive changes

Anything touching execution boundaries (argv handling, environment, paths,
input limits, blob storage paths) must state its threat model honestly:
resource control ≠ sandboxing, and subprocesses are not isolation.
