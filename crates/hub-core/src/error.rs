//! Typed errors for the Hub domain layer.
//!
//! Every variant carries the context needed to act on it; libraries in this
//! workspace never panic on user or system input.

use crate::id::{ArtifactId, ComponentId, RunId};
use crate::run::RunState;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CoreError {
    #[error("component {id} is already registered with a different manifest (registered digest {registered}, new digest {new})")]
    ComponentConflict {
        id: ComponentId,
        registered: String,
        new: String,
    },
    #[error("identical component manifest already registered for {id}")]
    ComponentAlreadyRegistered { id: ComponentId },
    #[error("component {0} not found")]
    ComponentNotFound(ComponentId),
    #[error("component {component} does not declare capability {capability}")]
    CapabilityNotDeclared {
        component: ComponentId,
        capability: String,
    },
    #[error("input {input_name:?} of capability {capability} has no matching entry in the run spec inputs")]
    MissingInputBinding {
        capability: String,
        input_name: String,
    },
    #[error("artifact {0} not found")]
    ArtifactNotFound(ArtifactId),
    #[error("blob {hex} not found in artifact store")]
    BlobNotFound { hex: String },
    #[error("artifact {artifact} exceeds the configured size limit ({size} > {limit} bytes)")]
    ArtifactTooLarge {
        artifact: ArtifactId,
        size: u64,
        limit: u64,
    },
    #[error("run {0} not found")]
    RunNotFound(RunId),
    #[error("invalid transition from {from} to {to}")]
    InvalidTransition { from: RunState, to: RunState },
    #[error("invalid workflow transition from {from:?} to {to:?}")]
    InvalidWorkflowTransition {
        from: crate::workflow::WorkflowState,
        to: crate::workflow::WorkflowState,
    },
    #[error("run {run} is not in an executable state (current state {current})")]
    RunNotExecutable { run: RunId, current: RunState },
    #[error("workflow {0} not found")]
    WorkflowNotFound(crate::id::WorkflowId),
    #[error("workflow {workflow} is not executable (current state {current:?})")]
    WorkflowNotExecutable {
        workflow: crate::id::WorkflowId,
        current: crate::workflow::WorkflowState,
    },
    #[error("run spec validation failed: {0}")]
    InvalidRunSpec(String),
    #[error("manifest validation failed: {0}")]
    InvalidManifest(String),
    #[error("validation failed: {0}")]
    Validation(String),
    #[error("executor failure during run {run}: {source}")]
    ExecutionFailed {
        run: RunId,
        #[source]
        source: ExecutorFailure,
    },
    #[error("storage failure: {0}")]
    Storage(String),
}

/// Failure reasons surfaced by the executor port, kept free of IO details so
/// the domain can match on them without depending on any backend.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ExecutorFailure {
    #[error("execution timed out after {timeout_ms} ms")]
    TimedOut { timeout_ms: u64 },
    #[error("execution was cancelled")]
    Cancelled,
    #[error("backend failure: {reason}")]
    Backend { reason: String },
}
