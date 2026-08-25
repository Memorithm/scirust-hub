//! # hub-core — SciRust Hub domain model
//!
//! The pure domain of the SciRust Hub control plane: typed identities,
//! content digests, capability declarations, component manifests, run
//! specifications with a controlled state machine, DAG primitives,
//! repository/artifact-store ports, in-memory backends and the orchestrator
//! that ties them together.
//!
//! Design rules (see `docs/` in the repository root):
//!
//! - No async runtimes, no process spawning, no HTTP: the domain is sync and
//!   deterministic given a controlled [`clock::Clock`].
//! - `#![forbid(unsafe_code)]` via workspace lints.
//! - Errors are typed ([`error::CoreError`]); no panics on user input.
//! - Registration is metadata-only; nothing executes without an explicit run.

pub mod artifact;
pub mod capability;
pub mod clock;
pub mod component;
pub mod dag;
pub mod digest;
pub mod error;
pub mod exec;
pub mod id;
pub mod limits;
pub mod memory;
pub mod orchestrator;
pub mod run;
pub mod store;
pub mod version;

pub use artifact::ArtifactMeta;
pub use capability::{Capability, CapabilityName, Port};
pub use clock::{Clock, ManualClock, SystemClock};
pub use component::{
    ComponentKind, ComponentManifest, ComponentName, ExecutionBinding, ProcessBinding,
    SourceInfo, MANIFEST_SCHEMA_VERSION,
};
pub use dag::{Dag, DagLimits};
pub use digest::ContentDigest;
pub use error::{CoreError, ExecutorFailure};
pub use exec::{CancelToken, ExecutionOutcome, ExecutionRequest, Executor};
pub use id::{ArtifactId, ComponentId, RunId};
pub use limits::Limits;
pub use memory::{FileSystemArtifactStore, InMemoryArtifactMeta, InMemoryComponents, InMemoryRuns};
pub use orchestrator::{Orchestrator, RegistrationStatus};
pub use run::{
    InputBinding, InputProvenance, OutputRef, RunOutcome, RunRecord, RunSpec,
    RunState, Transition,
};
pub use version::Version;
