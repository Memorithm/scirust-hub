# ADR-0005: Persistence

Status: accepted
Date: 2026-08-25

## Context

Components, runs and artifact metadata must outlive API calls, but the first
vertical slice must run in CI with zero infrastructure. The mission requires
a repository abstraction so business logic never embeds SQL.

## Decision

- Repository traits live in `hub-core`:
  `ComponentRepository`, `RunRepository`, `ArtifactMetadataRepository`.
- The first backend is `InMemory*` (mutex-guarded maps) inside `hub-core`,
  used by tests, CLI offline mode and as the daemon default for this PR.
- Content bytes go to a `FileSystemArtifactStore` (content-addressed blobs
  under a data directory), behind an `ArtifactStore` trait; metadata stays
  separate from blob storage.
- **SQLite persistence is deferred**, not forgotten: it will arrive as a new
  crate implementing the same traits with versioned migrations, transactions
  and constraints (see ADR-0002). Nothing in the domain references storage
  details, so the swap is additive.
- All persisted JSON uses explicit `schema_version`; readers tolerate unknown
  fields (additive evolution), writers always stamp the current version.

## Consequences

- Daemon state is not durable across restarts yet. This limitation is stated
  in README/report rather than hidden; restart durability lands with SQLite.
- Tests run hermetically.

## Amendment — 2026-08-25 (v0.2.0)

SQLite landed as `crates/hub-store-sqlite` implementing all three
repository traits over one embedded connection (WAL, synchronous FULL,
versioned forward-only migrations in a `schema_migrations` table, strictly
parameterized SQL). Domain code was untouched; only the daemon wiring
gained a `--store sqlite|memory` switch (sqlite default). The parity
requirement above is enforced by tests covering identical semantics
(idempotent registration, conflict digests, deterministic ordering,
restart durability including a hard-kill end-to-end proof).

## Alternatives considered

- SQLite immediately: adds `rusqlite` build surface and migration machinery
  before the data model has survived contact with real use; deferred one PR
  on purpose.
- File-based JSON journal: rejected, concurrency and querying become ad hoc.
