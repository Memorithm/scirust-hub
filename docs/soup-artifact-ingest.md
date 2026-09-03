# SOUP and ML artifact ingestion boundary

SOUP exposed a generic Hub artifact-ingestion gap: declared outputs were read as whole files by the orchestrator and the storage port had no path-based ingestion contract. Model adapters and checkpoints also require a future large/multi-file artifact contract rather than an unbounded increase of the existing blob limit.

This slice adds `ArtifactStore::put_file` as the stable path-based boundary. The default implementation is intentionally conservative and remains bounded by the existing `Limits::max_artifact_bytes`: it rejects symbolic links and non-regular files, checks the pre-open size, enforces the same bound while reading, and returns the exact stored byte count. Backends may later override the method with true streaming storage without changing callers or artifact identity.

This slice does **not** raise the current 16 MiB artifact limit and does not claim that SOUP training checkpoints can be retained yet. A separate versioned model-bundle/chunked-artifact contract is required before publishing `llm.train` or `llm.export` in Hub.

The security rule is generic, not SOUP-specific: a component output must be a regular file whose bytes are the bytes Hub digests and stores. A symlink is never an acceptable substitute for a declared output artifact.
