# ADR-0003: Component and capability model

Status: accepted
Date: 2026-08-25

## Context

The Hub must describe ecosystem components (crates, services, runtimes,
models, datasets, capsules, tools, artifacts) and what they can do, without
knowing their internals. Categories will grow; a closed enum would force
breaking changes. SciRust's `scirust-capsule-schema` demonstrates the
discipline this codebase follows: validate at construction, reject unknown
versions, keep the schema small.

## Decision

### Identity

Typed identifiers prevent accidental mixing (`ComponentId` vs `RunId`):

```rust
pub struct ComponentId(Uuid);  // logical identity, stable across versions
pub struct RunId(Uuid);
pub struct ArtifactId(Uuid);   // metadata handle
```

Distinct from identity, deliberately:

- `Version`: semver-shaped validated string (`MAJOR.MINOR.PATCH[-pre]`).
- `ContentDigest`: SHA-256 over bytes, domain-separated
  (`sha2`, hex interchange). Immutable content identity only; never used as
  a component version or location.
- Location lives in the artifact store mapping (`ArtifactId -> blob digest`),
  not inside the identifier.

### Capability

```rust
pub struct Capability {
    pub name: CapabilityName,        // "namespace.action", lowercase segments
    pub contract_version: Version,   // contract version, not component version
    pub inputs: Vec<Port>,           // ordered named slots
    pub outputs: Vec<Port>,
    pub properties: BTreeMap<String, String>,
}
```

`CapabilityName` validates `^[a-z][a-z0-9_]{0,63}(\.[a-z][a-z0-9_]{0,63})*$`.
Names are data, not Rust enums: the nomenclature of the ecosystem is not yet
stable, so the Hub indexes whatever is registered and queries by exact name
today (predicate search later).

### Component

```rust
pub struct ComponentManifest {
    pub id: ComponentId,
    pub name: ComponentName,
    pub version: Version,
    pub kind: ComponentKind(String),   // open newtype; well-known values documented
    pub capabilities: Vec<Capability>,
    pub execution: Option<ExecutionBinding>,
    pub source: Option<SourceInfo>,    // git url / commit / dirty when known
    pub metadata: BTreeMap<String, String>,
}
```

- `kind` is an open string newtype with recommended constants (`crate`,
  `service`, `runtime`, `model`, `dataset`, `capsule`, `tool`). Unknown kinds
  are valid; closed enums are rejected.
- `ExecutionBinding::Process { program, args, working_dir }` declares a fixed
  argv template with placeholders (`{params}`, `{input:<name>}`). Registration
  never executes anything; binding is only consulted at run time after
  validation.
- Manifests are content-digested on registration; re-registering byte-equal
  manifests is idempotent, conflicting content for an existing
  `(id, version)` is rejected.

## Consequences

- Forward evolution: new fields can be added to serialized forms; unknown
  fields are tolerated on read (ADR/protocol note) but validated strictly on
  construction.
- No capability inference from names: a component offers exactly what its
  manifest declares and nothing more.

## Alternatives considered

- Closed `enum ComponentKind`: rejected (mission §5).
- Deriving capabilities from crate names or directory layout: rejected,
  unverifiable claims.
