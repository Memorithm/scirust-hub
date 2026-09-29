//! Agent-task execution contracts.
//!
//! These types deliberately separate *what a task is allowed to request* from
//! the executor/backend that may eventually satisfy the request. They are
//! inspired by task/workspace isolation patterns observed in external agent
//! runtimes, but are Memorithm-owned Rust contracts.
//!
//! A validated [`TaskSpec`] is metadata. Validation does not execute code,
//! authorize an effect, create a sandbox, or prove that a worker can enforce
//! the requested limits. Executors must independently prove compatibility
//! before dispatch.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::digest::{hash_bytes, ContentDigest, DOMAIN_TASK_WORKSPACE};
use crate::error::CoreError;
use crate::id::TaskId;

/// Current wire/domain schema version for [`TaskSpec`].
pub const TASK_SPEC_SCHEMA_VERSION: u32 = 1;
/// Current schema for retained workspace-materialization evidence.
pub const WORKSPACE_MATERIALIZATION_SCHEMA_VERSION: u32 = 1;

const MAX_PRINCIPAL_BYTES: usize = 256;
const MAX_REPOSITORIES: usize = 32;
const MAX_REPOSITORY_BYTES: usize = 512;
const MAX_CAPABILITIES: usize = 128;
const MAX_CAPABILITY_BYTES: usize = 256;
const MAX_ISOLATION_EVIDENCE_ID_BYTES: usize = 256;
const MAX_ENDPOINTS: usize = 64;
const MAX_ENDPOINT_BYTES: usize = 512;

/// Identity bound to one logical task.
///
/// `principal` is an opaque, non-secret workload identity string. A future
/// SPIFFE/SVID transport may carry this identity cryptographically; this type
/// intentionally does not claim that merely storing the string authenticates
/// the caller.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskIdentity {
    pub task_id: TaskId,
    pub principal: String,
}

impl TaskIdentity {
    fn validate(&self) -> Result<(), CoreError> {
        validate_bounded_token("task principal", &self.principal, MAX_PRINCIPAL_BYTES)
    }
}

/// One immutable repository input to a task workspace.
///
/// `revision` must be an exact hexadecimal Git object id (SHA-1 or SHA-256
/// width). Branches and tags are deliberately rejected here so "workspace
/// ready" can never silently mean "whatever the branch pointed to at setup
/// time".
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceRepository {
    pub repository: String,
    pub revision: String,
    #[serde(default)]
    pub read_only: bool,
}

impl WorkspaceRepository {
    fn validate(&self) -> Result<(), CoreError> {
        validate_bounded_token("repository", &self.repository, MAX_REPOSITORY_BYTES)?;
        if !is_exact_git_object_id(&self.revision) {
            return Err(CoreError::Validation(format!(
                "workspace repository {} revision must be an exact 40- or 64-digit lowercase hexadecimal Git object id",
                self.repository
            )));
        }
        Ok(())
    }
}

/// Declarative inputs required to construct a task workspace.
///
/// This contract records immutable repository identities plus optional named
/// MCP/skill dependencies. It does not itself clone repositories or launch
/// MCP servers.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceSpec {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub repositories: Vec<WorkspaceRepository>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mcp_servers: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skills: Vec<String>,
}

impl WorkspaceSpec {
    /// Validates bounded, unique workspace declarations.
    ///
    /// # Errors
    /// Validation errors for duplicate repositories/dependencies, floating Git
    /// revisions or oversized tokens.
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.repositories.len() > MAX_REPOSITORIES {
            return Err(CoreError::Validation(format!(
                "workspace has {} repositories; maximum is {MAX_REPOSITORIES}",
                self.repositories.len()
            )));
        }

        let mut seen = BTreeSet::new();
        for repository in &self.repositories {
            repository.validate()?;
            if !seen.insert(repository.repository.as_str()) {
                return Err(CoreError::Validation(format!(
                    "duplicate workspace repository {:?}",
                    repository.repository
                )));
            }
        }

        validate_unique_tokens("MCP server", &self.mcp_servers, MAX_CAPABILITY_BYTES)?;
        validate_unique_tokens("skill", &self.skills, MAX_CAPABILITY_BYTES)
    }
}

/// Verified observation of one repository materialized for a task.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaterializedRepositoryEvidence {
    pub repository: String,
    pub requested_revision: String,
    pub observed_revision: String,
    pub tree_id: String,
    pub content_digest: ContentDigest,
    pub read_only_requested: bool,
}

/// Retained proof that the repositories declared by a [`WorkspaceSpec`] were
/// materialized at their exact requested Git object IDs.
///
/// The digest also binds the complete declared workspace spec, including MCP
/// server and skill names. It is evidence of what was materialized; it is not
/// a claim that filesystem permissions or network isolation are enforced.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceMaterializationEvidence {
    pub schema_version: u32,
    pub repositories: Vec<MaterializedRepositoryEvidence>,
    pub workspace_digest: ContentDigest,
}

impl WorkspaceMaterializationEvidence {
    /// Constructs canonical evidence after validating exact repository identity.
    ///
    /// # Errors
    /// Validation errors for missing/extra repositories, revision drift,
    /// malformed Git object IDs or duplicate repository observations.
    pub fn new(
        spec: &WorkspaceSpec,
        mut repositories: Vec<MaterializedRepositoryEvidence>,
    ) -> Result<Self, CoreError> {
        spec.validate()?;
        repositories.sort_by(|left, right| left.repository.cmp(&right.repository));
        Self::validate_repository_evidence(spec, &repositories)?;
        let workspace_digest = Self::compute_digest(spec, &repositories)?;
        Ok(Self {
            schema_version: WORKSPACE_MATERIALIZATION_SCHEMA_VERSION,
            repositories,
            workspace_digest,
        })
    }

    /// Revalidates persisted evidence against the immutable workspace spec.
    ///
    /// # Errors
    /// Validation errors for schema drift, repository/revision mismatch or
    /// digest corruption.
    pub fn validate_against(&self, spec: &WorkspaceSpec) -> Result<(), CoreError> {
        if self.schema_version != WORKSPACE_MATERIALIZATION_SCHEMA_VERSION {
            return Err(CoreError::Validation(format!(
                "unsupported workspace materialization schema_version {}; expected {WORKSPACE_MATERIALIZATION_SCHEMA_VERSION}",
                self.schema_version
            )));
        }
        spec.validate()?;
        let mut repositories = self.repositories.clone();
        repositories.sort_by(|left, right| left.repository.cmp(&right.repository));
        if repositories != self.repositories {
            return Err(CoreError::Validation(
                "workspace materialization repositories must be canonically ordered".to_owned(),
            ));
        }
        Self::validate_repository_evidence(spec, &repositories)?;
        let expected = Self::compute_digest(spec, &repositories)?;
        if expected != self.workspace_digest {
            return Err(CoreError::Validation(
                "workspace materialization digest does not match retained evidence".to_owned(),
            ));
        }
        Ok(())
    }

    fn validate_repository_evidence(
        spec: &WorkspaceSpec,
        repositories: &[MaterializedRepositoryEvidence],
    ) -> Result<(), CoreError> {
        if repositories.len() != spec.repositories.len() {
            return Err(CoreError::Validation(format!(
                "workspace evidence contains {} repositories; spec declares {}",
                repositories.len(),
                spec.repositories.len()
            )));
        }

        let expected: std::collections::BTreeMap<_, _> = spec
            .repositories
            .iter()
            .map(|repository| (repository.repository.as_str(), repository))
            .collect();
        let mut seen = BTreeSet::new();

        for evidence in repositories {
            validate_bounded_token(
                "materialized repository",
                &evidence.repository,
                MAX_REPOSITORY_BYTES,
            )?;
            if !seen.insert(evidence.repository.as_str()) {
                return Err(CoreError::Validation(format!(
                    "duplicate materialized repository {:?}",
                    evidence.repository
                )));
            }
            let Some(declared) = expected.get(evidence.repository.as_str()) else {
                return Err(CoreError::Validation(format!(
                    "materialized repository {:?} is not declared by the workspace",
                    evidence.repository
                )));
            };
            if evidence.requested_revision != declared.revision {
                return Err(CoreError::Validation(format!(
                    "materialized repository {} requested revision does not match workspace spec",
                    evidence.repository
                )));
            }
            if evidence.observed_revision != declared.revision {
                return Err(CoreError::Validation(format!(
                    "materialized repository {} observed revision {} does not match requested {}",
                    evidence.repository, evidence.observed_revision, declared.revision
                )));
            }
            if !is_exact_git_object_id(&evidence.observed_revision)
                || !is_exact_git_object_id(&evidence.tree_id)
            {
                return Err(CoreError::Validation(format!(
                    "materialized repository {} must retain exact lowercase Git object IDs",
                    evidence.repository
                )));
            }
            if evidence.read_only_requested != declared.read_only {
                return Err(CoreError::Validation(format!(
                    "materialized repository {} read-only flag does not match workspace spec",
                    evidence.repository
                )));
            }
        }
        Ok(())
    }

    fn compute_digest(
        spec: &WorkspaceSpec,
        repositories: &[MaterializedRepositoryEvidence],
    ) -> Result<ContentDigest, CoreError> {
        #[derive(Serialize)]
        struct CanonicalWorkspaceEvidence<'a> {
            schema_version: u32,
            spec: &'a WorkspaceSpec,
            repositories: &'a [MaterializedRepositoryEvidence],
        }

        let bytes = serde_json::to_vec(&CanonicalWorkspaceEvidence {
            schema_version: WORKSPACE_MATERIALIZATION_SCHEMA_VERSION,
            spec,
            repositories,
        })
        .map_err(|error| {
            CoreError::Storage(format!(
                "serializing canonical workspace materialization evidence: {error}"
            ))
        })?;
        Ok(hash_bytes(DOMAIN_TASK_WORKSPACE, &bytes))
    }
}

/// Explicit capability grants requested by a task.
///
/// Strings are intentionally open so product-specific capabilities can evolve
/// without central enum churn. They are grants, not proof that a downstream
/// service will honor them.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CapabilitySet(pub Vec<String>);

impl CapabilitySet {
    fn validate(&self) -> Result<(), CoreError> {
        if self.0.len() > MAX_CAPABILITIES {
            return Err(CoreError::Validation(format!(
                "task has {} capabilities; maximum is {MAX_CAPABILITIES}",
                self.0.len()
            )));
        }
        validate_unique_tokens("capability grant", &self.0, MAX_CAPABILITY_BYTES)
    }

    /// Returns true when an exact grant is present.
    #[must_use]
    pub fn contains(&self, grant: &str) -> bool {
        self.0.iter().any(|candidate| candidate == grant)
    }
}

/// Bounded resources requested for one task.
///
/// Values are hard upper bounds requested from a compatible executor. `None`
/// means the dimension is unspecified, not unlimited. A production executor
/// must apply its own admission policy before treating an unspecified value as
/// admissible.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceBudget {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_millis: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wall_clock_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_devices: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_tokens: Option<u64>,
}

impl ResourceBudget {
    fn validate(&self) -> Result<(), CoreError> {
        for (name, value) in [
            ("cpu_millis", self.cpu_millis),
            ("memory_bytes", self.memory_bytes),
            ("wall_clock_ms", self.wall_clock_ms),
            ("model_tokens", self.model_tokens),
        ] {
            if value == Some(0) {
                return Err(CoreError::Validation(format!(
                    "resource budget {name} must be greater than zero when specified"
                )));
            }
        }
        Ok(())
    }
}

/// Observed capacity available from one concrete worker/backend.
///
/// Each dimension is optional because an adapter may not have a trustworthy
/// observation. An absent dimension is unknown, not unlimited; explicit
/// capacity-aware admission therefore fails closed when a task requests it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceCapacity {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_millis: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_devices: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_tokens: Option<u64>,
}

impl ResourceCapacity {
    /// Checks a budget against observed worker capacity.
    ///
    /// A zero GPU request is treated as no GPU demand, so a CPU-only worker
    /// may still satisfy it. Every positive requested dimension needs a
    /// measured capacity and must fit within that capacity.
    pub fn admit(&self, backend_id: &str, budget: &ResourceBudget) -> Result<(), CoreError> {
        if self.cpu_millis == Some(0) {
            return Err(CoreError::Validation(format!(
                "backend {backend_id} reported zero CPU capacity"
            )));
        }
        if self.memory_bytes == Some(0) {
            return Err(CoreError::Validation(format!(
                "backend {backend_id} reported zero memory capacity"
            )));
        }

        check_capacity_dimension(backend_id, "cpu_millis", budget.cpu_millis, self.cpu_millis)?;
        check_capacity_dimension(
            backend_id,
            "memory_bytes",
            budget.memory_bytes,
            self.memory_bytes,
        )?;
        check_capacity_dimension(
            backend_id,
            "gpu_devices",
            budget.gpu_devices,
            self.gpu_devices,
        )?;
        check_capacity_dimension(
            backend_id,
            "model_tokens",
            budget.model_tokens,
            self.model_tokens,
        )
    }
}

fn check_capacity_dimension<T>(
    backend_id: &str,
    dimension: &str,
    requested: Option<T>,
    available: Option<T>,
) -> Result<(), CoreError>
where
    T: Copy + Default + Ord + std::fmt::Display,
{
    let Some(requested) = requested else {
        return Ok(());
    };
    if requested == T::default() {
        return Ok(());
    }
    let Some(available) = available else {
        return Err(CoreError::Validation(format!(
            "backend {backend_id} has unknown capacity for requested resource dimension {dimension}"
        )));
    };
    if requested > available {
        return Err(CoreError::Validation(format!(
            "backend {backend_id} capacity {dimension} {available} is below requested {requested}"
        )));
    }
    Ok(())
}

/// Minimum isolation semantics requested by a task.
///
/// Ordering is intentional so backends can prove they meet or exceed a
/// requirement without conflating supervised processes with security
/// sandboxes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IsolationLevel {
    /// Process supervision only. Not a hostile-code security boundary.
    Process,
    /// Dedicated OS container boundary.
    Container,
    /// Userspace-kernel isolation such as gVisor.
    Gvisor,
    /// Hardware-virtualized microVM boundary.
    MicroVm,
}

/// Explicit task egress policy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkPolicy {
    /// When true, destinations not explicitly listed are denied.
    pub default_deny: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_endpoints: Vec<String>,
}

impl Default for NetworkPolicy {
    fn default() -> Self {
        Self {
            default_deny: true,
            allowed_endpoints: Vec::new(),
        }
    }
}

impl NetworkPolicy {
    fn validate(&self) -> Result<(), CoreError> {
        if self.allowed_endpoints.len() > MAX_ENDPOINTS {
            return Err(CoreError::Validation(format!(
                "network policy has {} endpoints; maximum is {MAX_ENDPOINTS}",
                self.allowed_endpoints.len()
            )));
        }
        validate_unique_tokens(
            "network endpoint",
            &self.allowed_endpoints,
            MAX_ENDPOINT_BYTES,
        )
    }
}

/// Isolation and egress requirements attached to a task.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxRequirements {
    pub minimum_isolation: IsolationLevel,
    #[serde(default)]
    pub network: NetworkPolicy,
    /// Whether the workspace itself may be modified by the task.
    #[serde(default)]
    pub writable_workspace: bool,
}

/// Canonical Memorithm task contract.
///
/// The contract is deliberately executor-neutral. A scheduler may persist and
/// inspect it before choosing a backend, but dispatch is legal only after a
/// backend proves it can enforce the declared isolation, capabilities and
/// resource requirements.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskSpec {
    pub schema_version: u32,
    pub id: TaskId,
    pub identity: TaskIdentity,
    #[serde(default)]
    pub workspace: WorkspaceSpec,
    #[serde(default)]
    pub capabilities: CapabilitySet,
    #[serde(default)]
    pub budget: ResourceBudget,
    pub sandbox: SandboxRequirements,
}

impl TaskSpec {
    /// Validates fail-closed structural invariants.
    ///
    /// # Errors
    /// Returns [`CoreError::Validation`] for unsupported schemas, mismatched
    /// identity, floating repository revisions, duplicate grants/dependencies,
    /// zero-valued resource bounds or malformed bounded tokens.
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.schema_version != TASK_SPEC_SCHEMA_VERSION {
            return Err(CoreError::Validation(format!(
                "unsupported task schema_version {}; expected {TASK_SPEC_SCHEMA_VERSION}",
                self.schema_version
            )));
        }
        if self.identity.task_id != self.id {
            return Err(CoreError::Validation(
                "task identity task_id must match task id".to_owned(),
            ));
        }
        self.identity.validate()?;
        self.workspace.validate()?;
        self.capabilities.validate()?;
        self.budget.validate()?;
        self.sandbox.network.validate()
    }
}

/// Resource dimensions that one backend can actually enforce.
///
/// Capability is per dimension. A timeout-capable process executor must not
/// claim memory/GPU/token enforcement merely because it can enforce wall time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceEnforcement {
    #[serde(default)]
    pub wall_clock_ms: bool,
    #[serde(default)]
    pub cpu_millis: bool,
    #[serde(default)]
    pub memory_bytes: bool,
    #[serde(default)]
    pub gpu_devices: bool,
    #[serde(default)]
    pub model_tokens: bool,
}

impl ResourceEnforcement {
    fn admit(&self, backend_id: &str, budget: &ResourceBudget) -> Result<(), CoreError> {
        for (requested, enforced, dimension) in [
            (
                budget.wall_clock_ms.is_some(),
                self.wall_clock_ms,
                "wall_clock_ms",
            ),
            (budget.cpu_millis.is_some(), self.cpu_millis, "cpu_millis"),
            (
                budget.memory_bytes.is_some(),
                self.memory_bytes,
                "memory_bytes",
            ),
            (
                budget.gpu_devices.is_some(),
                self.gpu_devices,
                "gpu_devices",
            ),
            (
                budget.model_tokens.is_some(),
                self.model_tokens,
                "model_tokens",
            ),
        ] {
            if requested && !enforced {
                return Err(CoreError::Validation(format!(
                    "sandbox backend {backend_id} cannot enforce requested resource dimension {dimension}"
                )));
            }
        }
        Ok(())
    }
}

/// Minimal executor-facing sandbox capability description.
///
/// This is a truthful capability contract, not an implementation of sandboxing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxBackendDescriptor {
    pub backend_id: String,
    pub isolation: IsolationLevel,
    /// Whether a stronger-than-process isolation boundary has been qualified.
    #[serde(default)]
    pub isolation_qualified: bool,
    /// Opaque reference to immutable backend qualification evidence.
    #[serde(default)]
    pub isolation_evidence_id: Option<String>,
    /// Whether this backend applies task network policy rules such as endpoint restrictions.
    #[serde(default)]
    pub enforces_network_policy: bool,
    /// Whether destinations outside an explicit allowlist are denied.
    ///
    /// Missing legacy values deserialize to false and fail closed for
    /// default-deny tasks.
    #[serde(default)]
    pub enforces_default_deny_network: bool,
    #[serde(default)]
    pub enforces_workspace_write_policy: bool,
    #[serde(default)]
    pub resources: ResourceEnforcement,
    #[serde(default)]
    pub capabilities: CapabilitySet,
}

impl SandboxBackendDescriptor {
    /// Checks whether this backend can truthfully satisfy a task's requested
    /// isolation/control envelope.
    ///
    /// # Errors
    /// [`CoreError::Validation`] when a required enforcement property is not
    /// provided by the backend.
    pub fn admit(&self, task: &TaskSpec) -> Result<(), CoreError> {
        task.validate()?;
        validate_bounded_token("sandbox backend id", &self.backend_id, MAX_CAPABILITY_BYTES)?;
        self.validate_qualification_claims()?;

        if self.isolation < task.sandbox.minimum_isolation {
            return Err(CoreError::Validation(format!(
                "sandbox backend {} isolation {:?} does not satisfy required {:?}",
                self.backend_id, self.isolation, task.sandbox.minimum_isolation
            )));
        }

        let network_policy_requires_enforcement =
            task.sandbox.network.default_deny || !task.sandbox.network.allowed_endpoints.is_empty();
        if network_policy_requires_enforcement && !self.enforces_network_policy {
            return Err(CoreError::Validation(format!(
                "sandbox backend {} cannot enforce the task network policy",
                self.backend_id
            )));
        }
        if task.sandbox.network.default_deny && !self.enforces_default_deny_network {
            return Err(CoreError::Validation(format!(
                "sandbox backend {} cannot enforce the task default-deny network policy",
                self.backend_id
            )));
        }

        if !task.sandbox.writable_workspace && !self.enforces_workspace_write_policy {
            return Err(CoreError::Validation(format!(
                "sandbox backend {} cannot enforce the task read-only workspace policy",
                self.backend_id
            )));
        }

        self.resources.admit(&self.backend_id, &task.budget)?;
        self.capabilities.validate()?;
        for capability in &task.capabilities.0 {
            if !self.capabilities.contains(capability) {
                return Err(CoreError::Validation(format!(
                    "sandbox backend {} cannot enforce requested capability {capability}",
                    self.backend_id
                )));
            }
        }
        Ok(())
    }

    fn validate_qualification_claims(&self) -> Result<(), CoreError> {
        if self.isolation > IsolationLevel::Process && !self.isolation_qualified {
            return Err(CoreError::Validation(
                "sandbox backend isolation lacks qualification evidence".to_owned(),
            ));
        }
        if self.isolation == IsolationLevel::Process && self.isolation_qualified {
            return Err(CoreError::Validation(
                "process supervision cannot claim qualified isolation".to_owned(),
            ));
        }

        if self.isolation_qualified {
            let evidence_id = self.isolation_evidence_id.as_deref().ok_or_else(|| {
                CoreError::Validation(
                    "qualified sandbox isolation requires an evidence reference".to_owned(),
                )
            })?;
            validate_bounded_token(
                "sandbox isolation evidence id",
                evidence_id,
                MAX_ISOLATION_EVIDENCE_ID_BYTES,
            )?;
        } else if self.isolation_evidence_id.is_some() {
            return Err(CoreError::Validation(
                "sandbox isolation evidence requires a qualified isolation claim".to_owned(),
            ));
        }
        Ok(())
    }

    /// Admits a task only after checking the backend's observed capacity.
    ///
    /// This stricter path is opt-in so existing descriptor-only callers do not
    /// mistake an absent inventory observation for unlimited capacity.
    pub fn admit_with_capacity(
        &self,
        task: &TaskSpec,
        capacity: &ResourceCapacity,
    ) -> Result<(), CoreError> {
        self.admit(task)?;
        capacity.admit(&self.backend_id, &task.budget)
    }
}

fn validate_unique_tokens(
    kind: &str,
    values: &[String],
    max_bytes: usize,
) -> Result<(), CoreError> {
    let mut seen = BTreeSet::new();
    for value in values {
        validate_bounded_token(kind, value, max_bytes)?;
        if !seen.insert(value.as_str()) {
            return Err(CoreError::Validation(format!("duplicate {kind} {value:?}")));
        }
    }
    Ok(())
}

fn validate_bounded_token(kind: &str, value: &str, max_bytes: usize) -> Result<(), CoreError> {
    let valid = !value.is_empty()
        && value.len() <= max_bytes
        && !value.chars().any(char::is_control)
        && value.trim() == value;
    if valid {
        Ok(())
    } else {
        Err(CoreError::Validation(format!(
            "{kind} must be non-empty, trimmed, control-free and at most {max_bytes} bytes"
        )))
    }
}

fn is_exact_git_object_id(value: &str) -> bool {
    matches!(value.len(), 40 | 64)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exact_revision() -> String {
        "0123456789abcdef0123456789abcdef01234567".to_owned()
    }

    fn base_task() -> TaskSpec {
        let id = TaskId::generate();
        TaskSpec {
            schema_version: TASK_SPEC_SCHEMA_VERSION,
            id,
            identity: TaskIdentity {
                task_id: id,
                principal: format!("task://memorithm/test/{id}"),
            },
            workspace: WorkspaceSpec {
                repositories: vec![WorkspaceRepository {
                    repository: "Memorithm/scirust-hub".to_owned(),
                    revision: exact_revision(),
                    read_only: true,
                }],
                mcp_servers: vec!["github.read".to_owned()],
                skills: vec!["rust.ci".to_owned()],
            },
            capabilities: CapabilitySet(vec!["github:read".to_owned()]),
            budget: ResourceBudget {
                cpu_millis: Some(2_000),
                memory_bytes: Some(512 * 1024 * 1024),
                wall_clock_ms: Some(60_000),
                gpu_devices: Some(0),
                model_tokens: Some(10_000),
            },
            sandbox: SandboxRequirements {
                minimum_isolation: IsolationLevel::Container,
                network: NetworkPolicy {
                    default_deny: true,
                    allowed_endpoints: vec!["github.com:443".to_owned()],
                },
                writable_workspace: false,
            },
        }
    }

    #[test]
    fn valid_task_is_admitted_by_stronger_backend() {
        let task = base_task();
        task.validate().expect("task valid");

        let backend = SandboxBackendDescriptor {
            backend_id: "microvm-v1".to_owned(),
            isolation: IsolationLevel::MicroVm,
            isolation_qualified: true,
            isolation_evidence_id: Some("qualification/microvm-v1".to_owned()),
            enforces_network_policy: true,
            enforces_default_deny_network: true,
            enforces_workspace_write_policy: true,
            resources: ResourceEnforcement {
                wall_clock_ms: true,
                cpu_millis: true,
                memory_bytes: true,
                gpu_devices: true,
                model_tokens: true,
            },
            capabilities: CapabilitySet(vec!["github:read".to_owned()]),
        };
        backend
            .admit_with_capacity(
                &task,
                &ResourceCapacity {
                    cpu_millis: Some(2_000),
                    memory_bytes: Some(512 * 1024 * 1024),
                    gpu_devices: Some(0),
                    model_tokens: Some(10_000),
                },
            )
            .expect("backend admits task");
    }

    #[test]
    fn capacity_admission_is_fail_closed_for_unknown_or_insufficient_dimensions() {
        let budget = ResourceBudget {
            cpu_millis: Some(2_000),
            memory_bytes: Some(512 * 1024 * 1024),
            gpu_devices: Some(0),
            model_tokens: Some(10_000),
            ..ResourceBudget::default()
        };
        let capacity = ResourceCapacity {
            cpu_millis: Some(1_000),
            memory_bytes: Some(1024 * 1024 * 1024),
            gpu_devices: Some(0),
            model_tokens: Some(20_000),
        };
        assert!(matches!(
            capacity.admit("worker-a", &budget),
            Err(CoreError::Validation(message)) if message.contains("cpu_millis")
        ));

        let mut capacity = capacity;
        capacity.cpu_millis = Some(2_000);
        capacity.memory_bytes = None;
        assert!(matches!(
            capacity.admit("worker-a", &budget),
            Err(CoreError::Validation(message)) if message.contains("unknown capacity")
        ));

        capacity.memory_bytes = Some(512 * 1024 * 1024);
        capacity
            .admit("worker-a", &budget)
            .expect("capacity admits task");
    }

    #[test]
    fn generic_remoteops_resource_claim_does_not_admit_cpu_or_memory_budgets() {
        use crate::remoteops_qualification::RemoteOpsBackendQualificationV2;

        const FIXTURE: &str =
            include_str!("../tests/fixtures/remoteops-backend-qualification-v2.json");

        let qualification = RemoteOpsBackendQualificationV2::parse(FIXTURE).expect("qualification");
        assert!(qualification.controls().resource_limits);
        let backend = qualification.to_sandbox_backend_descriptor();

        for (budget, dimension) in [
            (
                ResourceBudget {
                    cpu_millis: Some(1_000),
                    ..ResourceBudget::default()
                },
                "cpu_millis",
            ),
            (
                ResourceBudget {
                    memory_bytes: Some(1024),
                    ..ResourceBudget::default()
                },
                "memory_bytes",
            ),
        ] {
            let mut task = base_task();
            task.budget = budget;
            task.capabilities = CapabilitySet::default();
            task.sandbox.writable_workspace = true;

            assert!(matches!(
                backend.admit(&task),
                Err(CoreError::Validation(message)) if message.contains(dimension)
            ));
        }
    }

    #[test]
    fn rejects_floating_git_revision() {
        let mut task = base_task();
        task.workspace.repositories[0].revision = "main".to_owned();
        assert!(matches!(
            task.validate(),
            Err(CoreError::Validation(message)) if message.contains("exact 40- or 64-digit")
        ));
    }

    #[test]
    fn rejects_identity_mismatch() {
        let mut task = base_task();
        task.identity.task_id = TaskId::generate();
        assert!(matches!(
            task.validate(),
            Err(CoreError::Validation(message)) if message.contains("must match")
        ));
    }

    #[test]
    fn rejects_duplicate_grants() {
        let mut task = base_task();
        task.capabilities.0.push("github:read".to_owned());
        assert!(matches!(
            task.validate(),
            Err(CoreError::Validation(message)) if message.contains("duplicate capability grant")
        ));
    }

    #[test]
    fn process_supervision_cannot_claim_container_isolation() {
        let task = base_task();
        let backend = SandboxBackendDescriptor {
            backend_id: "process".to_owned(),
            isolation: IsolationLevel::Process,
            isolation_qualified: false,
            isolation_evidence_id: None,
            enforces_network_policy: false,
            enforces_default_deny_network: false,
            enforces_workspace_write_policy: false,
            resources: ResourceEnforcement::default(),
            capabilities: CapabilitySet::default(),
        };
        assert!(matches!(
            backend.admit(&task),
            Err(CoreError::Validation(message)) if message.contains("does not satisfy required")
        ));
    }

    #[test]
    fn stronger_isolation_requires_qualification_evidence() {
        let mut backend = SandboxBackendDescriptor {
            backend_id: "container-v1".to_owned(),
            isolation: IsolationLevel::Container,
            isolation_qualified: false,
            isolation_evidence_id: None,
            enforces_network_policy: false,
            enforces_default_deny_network: false,
            enforces_workspace_write_policy: false,
            resources: ResourceEnforcement::default(),
            capabilities: CapabilitySet::default(),
        };
        assert!(matches!(
            backend.validate_qualification_claims(),
            Err(CoreError::Validation(message)) if message.contains("lacks qualification evidence")
        ));

        backend.isolation_qualified = true;
        assert!(matches!(
            backend.validate_qualification_claims(),
            Err(CoreError::Validation(message)) if message.contains("requires an evidence reference")
        ));
        backend.isolation_evidence_id = Some("qualification/container-v1".to_owned());
        backend
            .validate_qualification_claims()
            .expect("qualified isolation has a bounded evidence reference");

        backend.isolation = IsolationLevel::Process;
        assert!(matches!(
            backend.validate_qualification_claims(),
            Err(CoreError::Validation(message)) if message.contains("cannot claim qualified isolation")
        ));
    }

    #[test]
    fn explicit_policy_requires_network_enforcement() {
        let mut task = base_task();
        task.sandbox.minimum_isolation = IsolationLevel::Process;
        let backend = SandboxBackendDescriptor {
            backend_id: "process".to_owned(),
            isolation: IsolationLevel::Process,
            isolation_qualified: false,
            isolation_evidence_id: None,
            enforces_network_policy: false,
            enforces_default_deny_network: false,
            enforces_workspace_write_policy: true,
            resources: ResourceEnforcement {
                wall_clock_ms: true,
                cpu_millis: true,
                memory_bytes: true,
                gpu_devices: true,
                model_tokens: true,
            },
            capabilities: CapabilitySet::default(),
        };
        assert!(matches!(
            backend.admit(&task),
            Err(CoreError::Validation(message)) if message.contains("network policy")
        ));
    }

    #[test]
    fn default_deny_requires_an_independent_backend_claim() {
        let mut task = base_task();
        task.sandbox.minimum_isolation = IsolationLevel::Process;
        task.sandbox.network.allowed_endpoints.clear();
        task.sandbox.writable_workspace = true;
        task.capabilities = CapabilitySet::default();
        task.budget = ResourceBudget::default();

        let mut backend = SandboxBackendDescriptor {
            backend_id: "network-filter".to_owned(),
            isolation: IsolationLevel::Process,
            isolation_qualified: false,
            isolation_evidence_id: None,
            enforces_network_policy: true,
            enforces_default_deny_network: false,
            enforces_workspace_write_policy: true,
            resources: ResourceEnforcement::default(),
            capabilities: CapabilitySet::default(),
        };
        assert!(matches!(
            backend.admit(&task),
            Err(CoreError::Validation(message)) if message.contains("default-deny")
        ));

        backend.enforces_default_deny_network = true;
        backend.admit(&task).expect("default-deny admitted");
    }

    #[test]
    fn legacy_backend_descriptor_defaults_default_deny_to_false() {
        let legacy =
            r#"{"backend_id":"legacy","isolation":"process","enforces_network_policy":true}"#;
        let backend: SandboxBackendDescriptor =
            serde_json::from_str(legacy).expect("legacy descriptor shape");
        assert!(!backend.enforces_default_deny_network);
        assert!(!backend.isolation_qualified);
        assert_eq!(backend.isolation_evidence_id, None);
    }

    #[test]
    fn resource_enforcement_is_dimension_specific() {
        let mut task = base_task();
        task.sandbox.minimum_isolation = IsolationLevel::Process;
        task.sandbox.network.default_deny = false;
        task.sandbox.network.allowed_endpoints.clear();
        task.sandbox.writable_workspace = true;
        task.budget = ResourceBudget {
            wall_clock_ms: Some(1_000),
            ..ResourceBudget::default()
        };
        let wall_clock_only = SandboxBackendDescriptor {
            backend_id: "process-timeout".to_owned(),
            isolation: IsolationLevel::Process,
            isolation_qualified: false,
            isolation_evidence_id: None,
            enforces_network_policy: false,
            enforces_default_deny_network: false,
            enforces_workspace_write_policy: false,
            resources: ResourceEnforcement {
                wall_clock_ms: true,
                ..ResourceEnforcement::default()
            },
            capabilities: CapabilitySet(vec!["github:read".to_owned()]),
        };
        wall_clock_only.admit(&task).expect("wall-clock admitted");

        task.budget.memory_bytes = Some(1024);
        assert!(matches!(
            wall_clock_only.admit(&task),
            Err(CoreError::Validation(message)) if message.contains("memory_bytes")
        ));
    }

    #[test]
    fn read_only_workspace_requires_enforcement() {
        let mut task = base_task();
        task.sandbox.minimum_isolation = IsolationLevel::Process;
        task.sandbox.network.default_deny = false;
        task.sandbox.network.allowed_endpoints.clear();
        task.sandbox.writable_workspace = false;
        task.budget = ResourceBudget::default();
        let backend = SandboxBackendDescriptor {
            backend_id: "plain-process".to_owned(),
            isolation: IsolationLevel::Process,
            isolation_qualified: false,
            isolation_evidence_id: None,
            enforces_network_policy: false,
            enforces_default_deny_network: false,
            enforces_workspace_write_policy: false,
            resources: ResourceEnforcement::default(),
            capabilities: CapabilitySet::default(),
        };
        assert!(matches!(
            backend.admit(&task),
            Err(CoreError::Validation(message)) if message.contains("read-only workspace")
        ));
    }

    #[test]
    fn unsupported_backend_capability_is_rejected() {
        let task = base_task();
        let backend = SandboxBackendDescriptor {
            backend_id: "microvm-v1".to_owned(),
            isolation: IsolationLevel::MicroVm,
            isolation_qualified: true,
            isolation_evidence_id: Some("qualification/microvm-v1".to_owned()),
            enforces_network_policy: true,
            enforces_default_deny_network: true,
            enforces_workspace_write_policy: true,
            resources: ResourceEnforcement {
                wall_clock_ms: true,
                cpu_millis: true,
                memory_bytes: true,
                gpu_devices: true,
                model_tokens: true,
            },
            capabilities: CapabilitySet::default(),
        };
        assert!(matches!(
            backend.admit(&task),
            Err(CoreError::Validation(message))
                if message.contains("cannot enforce requested capability github:read")
        ));
    }

    #[test]
    fn schema_is_fail_closed() {
        let mut task = base_task();
        task.schema_version = TASK_SPEC_SCHEMA_VERSION + 1;
        assert!(matches!(
            task.validate(),
            Err(CoreError::Validation(message)) if message.contains("unsupported task schema_version")
        ));
    }

    #[test]
    fn json_rejects_unknown_fields() {
        let task = base_task();
        let mut value = serde_json::to_value(&task).expect("serialize");
        value
            .as_object_mut()
            .expect("object")
            .insert("future".to_owned(), serde_json::json!(true));
        assert!(serde_json::from_value::<TaskSpec>(value).is_err());
    }
}
