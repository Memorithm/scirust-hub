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
pub mod event;
pub mod exec;
pub mod id;
pub mod limits;
pub mod memory;
pub mod orchestrator;
pub mod publication;
pub mod remoteops_host_snapshot;
pub mod remoteops_qualification;
pub mod run;
pub mod scicapsule;
pub mod store;
pub mod task;
pub mod task_lifecycle;
pub mod version;
pub mod workflow;

pub use artifact::ArtifactMeta;
pub use capability::{Capability, CapabilityName, Port};
pub use clock::{Clock, ManualClock, SystemClock};
pub use component::{
    ComponentKind, ComponentManifest, ComponentName, ExecutionBinding, ProcessBinding, SourceInfo,
    MANIFEST_SCHEMA_VERSION,
};
pub use dag::{Dag, DagLimits};
pub use digest::{ContentDigest, RawSha256};
pub use error::{CoreError, ExecutorFailure};
pub use event::{
    InMemoryLifecycleEvents, LifecycleEntityType, LifecycleEvent, LifecycleEventKind,
    LifecycleEventRepository, NewLifecycleEvent, DEFAULT_EVENT_PAGE, MAX_EVENT_PAGE,
};
pub use exec::{
    CancelToken, ExecutionOutcome, ExecutionReport, ExecutionRequest, Executor, TaskExecutionReport,
};
pub use id::{ArtifactId, AttemptId, ComponentId, RunId, TaskId, WorkflowId};
pub use limits::Limits;
pub use memory::{
    FileSystemArtifactStore, InMemoryArtifactMeta, InMemoryComponents, InMemoryHubStore,
    InMemoryRuns, InMemoryTasks, InMemoryWorkflows,
};
pub use orchestrator::{Orchestrator, RegistrationStatus};
pub use publication::{
    AuthoritativeStepPublication, InMemoryPublicationFences, PublicationCommit, PublicationFence,
    PublicationFenceRepository, MAX_PUBLICATION_OUTPUTS, PUBLICATION_FENCE_SCHEMA_VERSION,
};
pub use remoteops_host_snapshot::{
    CommandObservation, HostResourceObservationsV2, HostSandboxObservationsV1, HostSnapshotError,
    LimitObservation, RemoteOpsHostCapabilitySnapshotV1,
    MAX_REMOTEOPS_HOST_CAPABILITY_SNAPSHOT_BYTES, REMOTEOPS_HOST_CAPABILITY_SNAPSHOT_V1_SCHEMA,
};
pub use remoteops_qualification::{
    RemoteOpsBackendQualificationV2, RemoteOpsControlsV2, RemoteOpsIsolationV2,
    RemoteOpsQualificationError, MAX_REMOTEOPS_BACKEND_NAME_BYTES, MAX_REMOTEOPS_EVIDENCE_ID_BYTES,
    MAX_REMOTEOPS_QUALIFICATION_JSON_BYTES, REMOTEOPS_BACKEND_QUALIFICATION_V2_SCHEMA,
};
pub use run::{
    ComponentAdmissionPin, InputBinding, InputProvenance, OutputRef, RunOutcome, RunRecord,
    RunSpec, RunState, Transition,
};
pub use task::{
    CapabilitySet, IsolationLevel, MaterializedRepositoryEvidence, NetworkPolicy, ResourceBudget,
    ResourceCapacity, ResourceEnforcement, SandboxBackendDescriptor, SandboxRequirements,
    TaskIdentity, TaskSpec, WorkspaceMaterializationEvidence, WorkspaceRepository, WorkspaceSpec,
    TASK_SPEC_SCHEMA_VERSION, WORKSPACE_MATERIALIZATION_SCHEMA_VERSION,
};
pub use task_lifecycle::{
    validate_task_snapshot_update, TaskRecord, TaskState, TaskTransition,
    TASK_RECORD_SCHEMA_VERSION,
};
pub use version::Version;
pub use workflow::{
    AttemptFailureCategory, InputSource, RetryPolicy, Step, StepAttempt, StepResult,
    WorkflowAdmissionPins, WorkflowRecord, WorkflowSpec, WorkflowState, MAX_WORKFLOW_CONCURRENCY,
    WORKFLOW_ADMISSION_SCHEMA_VERSION, WORKFLOW_SCHEMA_VERSION,
};
