//! Workflow specifications and records: multiple steps executed in
//! dependency order.
//!
//! Scope honesty (see ADR-0006): this is **sequential, single-node**
//! orchestration. Steps run one at a time in topological order; a failed or
//! cancelled step fails the workflow immediately (fail-fast). No parallel
//! scheduling, retries or distribution yet.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::capability::CapabilityName;
use crate::dag::{Dag, DagLimits};
use crate::error::CoreError;
use crate::id::{ArtifactId, ComponentId, RunId};
use crate::run::RunSpec;
use crate::version::Version;

/// Current workflow schema version understood by this build.
pub const WORKFLOW_SCHEMA_VERSION: u16 = 1;

/// Version of the workflow model itself, stamped into every record so
/// stored provenance can be interpreted years later.
pub const WORKFLOW_MODEL_VERSION: &str = "1.0.0";

/// Where a step's input comes from.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputSource {
    /// An artifact that already exists in the store.
    Artifact { artifact: ArtifactId },
    /// An output of another step in the same workflow.
    FromStep { key: String, output: String },
}

impl std::fmt::Display for InputSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InputSource::Artifact { artifact } => write!(f, "artifact:{artifact}"),
            InputSource::FromStep { key, output } => write!(f, "step:{key}:{output}"),
        }
    }
}

/// One unit of work inside a workflow.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Step {
    /// Unique key inside the workflow; referenced by dependencies and
    /// `InputSource::FromStep`.
    pub key: String,
    pub component: ComponentId,
    pub capability: CapabilityName,
    #[serde(default)]
    pub parameters: BTreeMap<String, serde_json::Value>,
    /// Named inputs; names must match the capability's input ports (same
    /// rules as single runs).
    #[serde(default)]
    pub inputs: BTreeMap<String, InputSource>,
    pub timeout_ms: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub after: Vec<String>,
}

/// A validated multi-step specification.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowSpec {
    pub schema_version: u16,
    /// Human-readable label (not an identifier).
    pub name: String,
    pub steps: Vec<Step>,
}

impl WorkflowSpec {
    /// Validates structure: schema version, unique keys, key/timeout grammar
    /// reusing run rules, acyclic explicit dependencies, and `FromStep`
    /// references pointing at *other* declared keys.
    ///
    /// # Errors
    /// [`CoreError::InvalidRunSpec`] with the first violated rule.
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.schema_version != WORKFLOW_SCHEMA_VERSION {
            return Err(CoreError::InvalidRunSpec(format!(
                "unsupported workflow schema_version {}: expected {}",
                self.schema_version, WORKFLOW_SCHEMA_VERSION
            )));
        }
        if self.name.is_empty() || self.name.len() > 128 || self.name.contains('\0') {
            return Err(CoreError::InvalidRunSpec(
                "workflow name must be 1..=128 characters".into(),
            ));
        }
        let limits = DagLimits::default();
        if self.steps.len() > limits.max_nodes {
            return Err(CoreError::InvalidRunSpec(format!(
                "{} steps exceeds the maximum of {}",
                self.steps.len(),
                limits.max_nodes
            )));
        }
        if self.steps.is_empty() {
            return Err(CoreError::InvalidRunSpec(
                "workflow must contain at least one step".into(),
            ));
        }

        let mut dag: Dag<()> = Dag::new();
        let mut seen = BTreeSet::new();
        for step in &self.steps {
            validate_key(&step.key)?;
            if !seen.insert(step.key.as_str()) {
                return Err(CoreError::InvalidRunSpec(format!(
                    "duplicate step key {:?}",
                    step.key
                )));
            }
            if step.timeout_ms == 0 {
                return Err(CoreError::InvalidRunSpec(format!(
                    "step {:?} timeout_ms must be at least 1",
                    step.key
                )));
            }
            for dep in &step.after {
                if dep == &step.key {
                    return Err(CoreError::InvalidRunSpec(format!(
                        "step {:?} depends on itself",
                        step.key
                    )));
                }
            }
        }
        // Dependencies and FromStep refs must target declared keys.
        for step in &self.steps {
            for dep in &step.after {
                if !seen.contains(dep.as_str()) {
                    return Err(CoreError::InvalidRunSpec(format!(
                        "step {:?} depends on unknown step {dep:?}",
                        step.key
                    )));
                }
            }
            for (input_name, source) in &step.inputs {
                if let InputSource::FromStep { key, output } = source {
                    if key == &step.key {
                        return Err(CoreError::InvalidRunSpec(format!(
                            "step {:?} consumes its own output through input {input_name:?}",
                            step.key
                        )));
                    }
                    if !seen.contains(key.as_str()) {
                        return Err(CoreError::InvalidRunSpec(format!(
                            "input {input_name:?} of step {:?} references unknown step {key:?}",
                            step.key
                        )));
                    }
                    validate_output_ref_name(output)?;
                }
            }
        }
        // Build the DAG from data dependencies: step depends on every step
        // it consumes from plus its explicit `after` entries. Cycle checks
        // are delegated to the proven DAG primitive.
        for step in &self.steps {
            dag.add_node(step.key.clone(), (), &limits)?;
        }
        for step in &self.steps {
            let mut deps: BTreeSet<String> = step.after.iter().cloned().collect();
            for source in step.inputs.values() {
                if let InputSource::FromStep { key, .. } = source {
                    deps.insert(key.clone());
                }
            }
            for dep in deps {
                // Dag edges point from earlier to later: dependency first.
                dag.add_edge(&dep, &step.key, &limits)
                    .map_err(|e| CoreError::InvalidRunSpec(e.to_string()))?;
            }
        }
        dag.topological_order()
            .map(|_| ())
            .map_err(|e| CoreError::InvalidRunSpec(e.to_string()))
    }

    /// Deterministic execution order.
    ///
    /// # Errors
    /// See [`Self::validate`]; call it first.
    pub fn topo_keys(&self) -> Result<Vec<String>, CoreError> {
        let mut dag: Dag<()> = Dag::new();
        let limits = DagLimits::default();
        for step in &self.steps {
            dag.add_node(step.key.clone(), (), &limits)?;
        }
        for step in &self.steps {
            let mut deps: BTreeSet<String> = step.after.iter().cloned().collect();
            for source in step.inputs.values() {
                if let InputSource::FromStep { key, .. } = source {
                    deps.insert(key.clone());
                }
            }
            for dep in deps {
                // Dag edges point from earlier to later: dependency first.
                dag.add_edge(&dep, &step.key, &limits)
                    .map_err(|e| CoreError::InvalidRunSpec(e.to_string()))?;
            }
        }
        dag.topological_order()
            .map(|order| order.into_iter().map(|(k, _)| k).collect())
    }

    /// Converts a step into the single-run spec used by the orchestrator;
    /// input sources must already have been resolved to artifact ids.
    #[must_use]
    pub fn step_run_spec(step: &Step, resolved: &BTreeMap<String, ArtifactId>) -> RunSpec {
        RunSpec {
            component: step.component,
            capability: step.capability.clone(),
            parameters: step.parameters.clone(),
            inputs: resolved
                .iter()
                .map(|(name, artifact)| crate::run::InputBinding {
                    name: name.clone(),
                    artifact: *artifact,
                })
                .collect(),
            timeout_ms: step.timeout_ms,
        }
    }
}

fn validate_key(key: &str) -> Result<(), CoreError> {
    let valid = !key.is_empty()
        && key.len() <= 64
        && key.starts_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit())
        && key
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-');
    if valid {
        Ok(())
    } else {
        Err(CoreError::InvalidRunSpec(format!(
            "step key {key:?} must match [a-z0-9][a-z0-9_-]{{0,63}}"
        )))
    }
}

fn validate_output_ref_name(output: &str) -> Result<(), CoreError> {
    // Output labels are stream names ("stdout", "stderr") or file outputs
    // ("file:<name>"); bound them to printable short strings without
    // whitespace so they cannot smuggle anything else.
    let ok = !output.is_empty()
        && output.len() <= 128
        && output
            .chars()
            .all(|c| !c.is_whitespace() && !c.is_control());
    if ok {
        Ok(())
    } else {
        Err(CoreError::InvalidRunSpec(format!(
            "output reference {output:?} must be 1..=128 characters without whitespace"
        )))
    }
}

/// Outcome of one executed step.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepResult {
    pub key: String,
    pub run: RunId,
    pub state: crate::run::RunState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
}

/// Lifecycle of a workflow (mirrors run states but without queued phase:
/// submission validates, execution runs).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowState {
    Created,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl WorkflowState {
    /// Legal transitions: created → running → terminal; running may also go
    /// straight to failed/cancelled when a step fails before starting others.
    #[must_use]
    pub fn can_transition_to(self, next: Self) -> bool {
        use WorkflowState::*;
        matches!(
            (self, next),
            (Created, Running)
                | (Created, Cancelled)
                | (Running, Succeeded)
                | (Running, Failed)
                | (Running, Cancelled)
        )
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            WorkflowState::Succeeded | WorkflowState::Failed | WorkflowState::Cancelled
        )
    }
}

/// Provenance-bearing record of one workflow.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkflowRecord {
    pub id: crate::id::WorkflowId,
    pub spec: WorkflowSpec,
    pub state: WorkflowState,
    /// Contract version of the workflow model itself.
    pub model_version: Version,
    pub created_at: crate::clock::UnixMillis,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<crate::clock::UnixMillis>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<crate::clock::UnixMillis>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub steps: Vec<StepResult>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
}

impl WorkflowRecord {
    /// Creates a record in [`WorkflowState::Created`] after validating.
    ///
    /// # Errors
    /// [`CoreError::InvalidRunSpec`] from [`WorkflowSpec::validate`].
    pub fn create(
        spec: WorkflowSpec,
        model_version: Version,
        now: crate::clock::UnixMillis,
    ) -> Result<Self, CoreError> {
        spec.validate()?;
        Ok(Self {
            id: crate::id::WorkflowId::generate(),
            spec,
            state: WorkflowState::Created,
            model_version,
            created_at: now,
            started_at: None,
            finished_at: None,
            steps: Vec::new(),
            failure: None,
        })
    }

    /// Applies a controlled transition.
    ///
    /// # Errors
    /// [`CoreError::InvalidTransition`] for illegal moves.
    pub fn transition(
        &mut self,
        to: WorkflowState,
        now: crate::clock::UnixMillis,
    ) -> Result<(), CoreError> {
        let from = self.state;
        if !from.can_transition_to(to) {
            return Err(CoreError::InvalidWorkflowTransition { from, to });
        }
        self.state = to;
        if to == WorkflowState::Running {
            self.started_at.get_or_insert(now);
        }
        if to.is_terminal() {
            self.finished_at = Some(now);
        }
        Ok(())
    }
}
