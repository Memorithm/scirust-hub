# Security policy

## Honest threat model of this foundation

SciRust Hub executes processes. The current foundation provides **resource
control and hygiene**, not isolation:

- Process execution takes a structured argv (no shell, no string splicing).
- Child environments are constructed from scratch; parent environment values
  do not leak unless explicitly listed. Only env *names* reach provenance.
- Output streams are capped per byte budget; oversized output is truncated
  and flagged, never buffered unbounded.
- Every execution has a wall-clock timeout and a cooperative cancellation
  token.
- Working directories are per-run, under the configured data dir.

**A subprocess is not a sandbox.** Children run with the daemon's OS
privileges: they can read whatever those privileges allow, open network
connections and consume CPU/IO beyond the caps we enforce on their *output*.
Container or remote executors are future backends and will not automatically
be security boundaries either — that depends entirely on how they are
deployed.

## Additional boundaries

- Registration is metadata-only; registering a manifest never runs its code.
- HTTP bodies are size-limited; manifests are validated against an explicit
  schema version with unknown-field tolerance on read.
- Input artifact names are validated path components; materialized paths live
  under the daemon's own data dir. Blob storage is content-addressed
  (`sha256`), written atomically via temp-file rename.
- No secrets belong in configuration today; nothing in the Hub reads or logs
  secret material. Env var *names* recorded in provenance only.

## Reporting

Report vulnerabilities privately to the maintainers via GitHub security
advisories for `Memorithm/scirust-hub` rather than public issues.
