//! Qualified gVisor execution boundary for hostile, bounded CPU workloads.
//!
//! The adapter invokes Docker only with the runsc runtime and a fixed
//! fail-closed control set. A dedicated workflow qualifies the real backend.

use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use crate::execution_guard::{execute_guarded_command, GuardedCommandOutput};
use hub_core::{IsolationLevel, ResourceEnforcement, SandboxBackendDescriptor};
use hub_store_sqlite::{SandboxAttempt, SandboxAttemptState};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const GVISOR_REQUEST_SCHEMA_V1: &str = "scirust-hub.gvisor-execution-request/v1";
pub const GVISOR_RECEIPT_SCHEMA_V1: &str = "scirust-hub.gvisor-execution-receipt/v1";
pub const GVISOR_QUALIFICATION_SCHEMA_V1: &str = "scirust-hub.gvisor-qualification-evidence/v1";

const MAX_TOKEN_BYTES: usize = 256;
const MAX_CAPTURE_BYTES: usize = 1024 * 1024;
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(3);
const CONTROL_CAPTURE_BYTES: usize = 4096;
const MAX_TASK_TIMEOUT_MS: u64 = 60 * 60 * 1000;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GvisorExecutionRequestV1 {
    pub schema: String,
    pub task_id: String,
    pub image: String,
    pub workspace: PathBuf,
    pub command: Vec<String>,
    #[serde(default)]
    pub environment: Vec<(String, String)>,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub pids_limit: u64,
    /// Optional stricter guest task ceiling; defaults to the host PID budget.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guest_pids_limit: Option<u64>,
    pub timeout_ms: u64,
    pub max_capture_bytes: usize,
    pub default_deny_network: bool,
    pub read_only_root: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GvisorExecutionReceiptV1 {
    pub schema: String,
    pub task_id: String,
    pub backend_id: String,
    pub isolation: String,
    pub isolation_evidence_id: Option<String>,
    pub exit_code: Option<i32>,
    pub timed_out: bool,
    pub stdout: String,
    pub stderr: String,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GvisorQualificationEvidenceV1 {
    pub schema: String,
    pub source_commit: String,
    pub workflow_run_id: u64,
    pub receipt_sha256: String,
}

impl GvisorQualificationEvidenceV1 {
    fn validate(&self) -> Result<(), String> {
        if self.schema != GVISOR_QUALIFICATION_SCHEMA_V1
            || !lower_hex(&self.source_commit, 40)
            || self.workflow_run_id == 0
            || !lower_hex(&self.receipt_sha256, 64)
        {
            return Err("invalid immutable gVisor qualification evidence".to_owned());
        }
        Ok(())
    }

    fn evidence_id(&self) -> String {
        format!(
            "github:Memorithm/scirust-hub@{}/actions/runs/{}#receipt-sha256:{}",
            self.source_commit, self.workflow_run_id, self.receipt_sha256
        )
    }
}

#[derive(Clone, Debug)]
pub struct GvisorDockerExecutor {
    docker: PathBuf,
    workspace_root: PathBuf,
    qualification: Option<GvisorQualificationEvidenceV1>,
}

impl GvisorDockerExecutor {
    #[must_use]
    pub fn new(workspace_root: PathBuf) -> Self {
        Self {
            docker: PathBuf::from("docker"),
            workspace_root,
            qualification: None,
        }
    }

    pub fn from_qualification_artifacts(
        workspace_root: PathBuf,
        manifest: &Path,
        receipt: &Path,
    ) -> Result<Self, String> {
        let build_source_commit = option_env!("SCIRUST_HUB_SOURCE_COMMIT").ok_or_else(|| {
            "qualified gVisor builds require SCIRUST_HUB_SOURCE_COMMIT at compile time".to_owned()
        })?;
        Self::from_qualification_artifacts_for_commit(
            workspace_root,
            manifest,
            receipt,
            build_source_commit,
        )
    }

    fn from_qualification_artifacts_for_commit(
        workspace_root: PathBuf,
        manifest: &Path,
        receipt: &Path,
        build_source_commit: &str,
    ) -> Result<Self, String> {
        if !lower_hex(build_source_commit, 40) {
            return Err("invalid compiled gVisor source commit".to_owned());
        }
        let manifest_bytes = std::fs::read(manifest)
            .map_err(|error| format!("qualification manifest unavailable: {error}"))?;
        let qualification: GvisorQualificationEvidenceV1 = serde_json::from_slice(&manifest_bytes)
            .map_err(|error| format!("invalid qualification manifest JSON: {error}"))?;
        qualification.validate()?;
        if qualification.source_commit != build_source_commit {
            return Err("qualification evidence does not match the compiled source".to_owned());
        }

        let receipt_bytes = std::fs::read(receipt)
            .map_err(|error| format!("qualification receipt unavailable: {error}"))?;
        let receipt_sha256 = format!("{:x}", Sha256::digest(&receipt_bytes));
        if receipt_sha256 != qualification.receipt_sha256 {
            return Err("qualification receipt digest does not match the manifest".to_owned());
        }
        let receipt: GvisorExecutionReceiptV1 = serde_json::from_slice(&receipt_bytes)
            .map_err(|error| format!("invalid qualification receipt JSON: {error}"))?;
        if receipt.schema != GVISOR_RECEIPT_SCHEMA_V1
            || receipt.backend_id != "docker-runsc"
            || receipt.isolation != "userspace_kernel"
            || receipt.isolation_evidence_id.is_some()
            || receipt.exit_code != Some(0)
            || receipt.timed_out
            || receipt.stdout_truncated
            || receipt.stderr_truncated
            || !receipt.stdout.contains("PAPERS_GVISOR_OK")
        {
            return Err("qualification receipt does not prove the gVisor probe".to_owned());
        }

        Ok(Self {
            docker: PathBuf::from("docker"),
            workspace_root,
            qualification: Some(qualification),
        })
    }

    #[cfg(test)]
    fn with_docker(docker: PathBuf, workspace_root: PathBuf) -> Self {
        Self {
            docker,
            workspace_root,
            qualification: None,
        }
    }

    #[must_use]
    pub fn backend_descriptor(&self) -> SandboxBackendDescriptor {
        SandboxBackendDescriptor {
            backend_id: "docker-runsc".to_owned(),
            isolation: IsolationLevel::Gvisor,
            isolation_qualified: self.qualification.is_some(),
            isolation_evidence_id: self
                .qualification
                .as_ref()
                .map(GvisorQualificationEvidenceV1::evidence_id),
            enforces_network_policy: false,
            enforces_default_deny_network: true,
            enforces_workspace_write_policy: false,
            resources: ResourceEnforcement {
                wall_clock_ms: true,
                cpu_millis: true,
                memory_bytes: true,
                gpu_devices: false,
                model_tokens: false,
            },
            capabilities: Default::default(),
        }
    }

    /// Execute one validated request and return bounded captured output.
    ///
    /// # Errors
    /// Fails before spawn on invalid input, or when Docker cannot be controlled.
    pub fn execute(
        &self,
        request: &GvisorExecutionRequestV1,
    ) -> Result<GvisorExecutionReceiptV1, String> {
        validate_request(request)?;
        let workspace_root = self
            .workspace_root
            .canonicalize()
            .map_err(|error| format!("workspace root unavailable: {error}"))?;
        let workspace = request
            .workspace
            .canonicalize()
            .map_err(|error| format!("workspace unavailable: {error}"))?;
        if !workspace.is_dir()
            || workspace == workspace_root
            || !workspace.starts_with(&workspace_root)
            || workspace.starts_with(workspace_root.join(".gvisor-control"))
        {
            return Err("workspace must be a task directory below the executor root".to_owned());
        }

        let metadata = workspace
            .metadata()
            .map_err(|error| format!("workspace metadata unavailable: {error}"))?;
        let container_user = nonroot_container_user(metadata.uid(), metadata.gid())?;
        // Validate the entire phase deadline before creating any OS resource.
        let deadline = Instant::now()
            .checked_add(Duration::from_millis(request.timeout_ms))
            .ok_or_else(|| "timeout overflow before container creation".to_owned())?;
        let (journal, directory) = crate::gvisor_journal::open(&workspace_root)?;
        let workspace_name = workspace.to_str().ok_or("workspace must be UTF-8")?;
        let _fences = crate::gvisor_journal::fence(&directory, &request.task_id, workspace_name)?;
        let request_digest = format!("{:x}", Sha256::digest(
            serde_json::to_vec(request).map_err(|e| e.to_string())?
        ));
        let attempt = journal.reserve_sandbox_attempt(
            &request.task_id, "docker-runsc", &request_digest, workspace_name,
        ).map_err(|e| e.to_string())?;
        let nonce = attempt.attempt_id.to_string();
        let name = format!("scirust-gvisor-{nonce}");
        let mut command = Command::new(&self.docker);
        command
            .args([
                "create",
                "--rm",
                "--runtime=runsc",
                "--network=none",
                "--cap-drop=ALL",
                "--security-opt=no-new-privileges",
                "--read-only",
                "--user",
                &container_user,
                "--name",
                &name,
                "--label",
                &format!("org.memorithm.hub.attempt={nonce}"),
                "--label",
                &format!("org.memorithm.hub.request={request_digest}"),
                "--cpus",
                &format_cpu(request.cpu_millis),
                "--memory",
                &request.memory_bytes.to_string(),
                "--memory-swap",
                &request.memory_bytes.to_string(),
                "--pids-limit",
                &request.pids_limit.to_string(),
                "--ulimit",
                &format!(
                    "nproc={0}:{0}",
                    request.guest_pids_limit.unwrap_or(request.pids_limit)
                ),
                "--ulimit",
                "fsize=16777216:16777216",
                "--tmpfs",
                "/tmp:rw,nosuid,nodev,size=67108864",
                "--mount",
            ])
            .arg(format!(
                "type=bind,src={},dst=/workspace",
                workspace.display()
            ))
            .args(["--workdir", "/workspace"]);
        for (key, value) in &request.environment {
            command.args(["--env", &format!("{key}={value}")]);
        }
        command.arg(&request.image).args(&request.command);
        let created = guarded_docker(&mut command, deadline, MAX_CAPTURE_BYTES)?;
        if created.timed_out
            || !created.status.success()
            || created.stdout.truncated
            || created.stderr.truncated
        {
            // A name is not an ownership token. Never remove a preexisting or
            // ambiguously created container merely because its name matches.
            return Err("container creation unconfirmed; no owned ID available for cleanup".into());
        }
        let container_id = String::from_utf8(created.stdout.bytes)
            .map_err(|_| "container identity is not UTF-8")?;
        let container_id = container_id.trim();
        if !lower_hex(container_id, 64) {
            return Err("container creation did not return an immutable owned ID".into());
        }
        // Never start unless the confirmed immutable handle survived a FULL
        // SQLite commit. A failed commit leaves a quarantined reservation.
        journal.sandbox_own(&request.task_id, attempt.attempt_id, container_id)
            .map_err(|e| format!("container ownership not durable; attempt quarantined: {e}"))?;
        let mut start = Command::new(&self.docker);
        start.args(["start", "--attach", container_id]);
        let executed = guarded_docker(&mut start, deadline, request.max_capture_bytes);
        // Always confirm the exact owned container's absence, even on client
        // errors. Killing a local group is not Docker-daemon termination proof.
        cleanup_owned_container(&self.docker, container_id).map_err(|error| {
            format!("owned Docker cleanup unconfirmed for ID {container_id}: {error}")
        })?;
        journal.sandbox_confirm_absent(&request.task_id, attempt.attempt_id, container_id)
            .map_err(|e| format!("cleanup observation not durable; attempt quarantined: {e}"))?;
        let executed = executed?;
        Ok(GvisorExecutionReceiptV1 {
            schema: GVISOR_RECEIPT_SCHEMA_V1.to_owned(),
            task_id: request.task_id.clone(),
            backend_id: "docker-runsc".to_owned(),
            isolation: "userspace_kernel".to_owned(),
            isolation_evidence_id: self
                .qualification
                .as_ref()
                .map(GvisorQualificationEvidenceV1::evidence_id),
            exit_code: executed.status.code(),
            timed_out: executed.timed_out,
            stdout: String::from_utf8_lossy(&executed.stdout.bytes).into_owned(),
            stderr: String::from_utf8_lossy(&executed.stderr.bytes).into_owned(),
            stdout_truncated: executed.stdout.truncated,
            stderr_truncated: executed.stderr.truncated,
        })
    }

    /// Inspect durable controller metadata, never a task-success receipt.
    pub fn inspect_attempt(&self, task_id: &str) -> Result<SandboxAttempt, String> {
        let root = self.workspace_root.canonicalize().map_err(|e| e.to_string())?;
        let (journal, _) = crate::gvisor_journal::open(&root)?;
        journal.sandbox_attempt(task_id).map_err(|e| e.to_string())?
            .ok_or_else(|| "no recorded sandbox attempt".into())
    }

    /// Read-only daemon reconciliation. Never start, replay or remove a container.
    /// A live resource remains quarantined for explicit operator cleanup.
    pub fn reconcile_attempt(&self, task_id: &str) -> Result<SandboxAttempt, String> {
        let root = self.workspace_root.canonicalize().map_err(|e| e.to_string())?;
        let (journal, directory) = crate::gvisor_journal::open(&root)?;
        let mut attempt = journal.sandbox_attempt(task_id).map_err(|e| e.to_string())?
            .ok_or("no recorded sandbox attempt")?;
        let _fences = crate::gvisor_journal::fence(&directory, task_id, &attempt.workspace)?;
        if attempt.state == SandboxAttemptState::Cleaned {
            return Ok(attempt);
        }
        if attempt.state == SandboxAttemptState::Reserved {
            let mut lookup = Command::new(&self.docker);
            lookup.args([
                "container", "ls", "--all", "--no-trunc", "--filter",
                &format!("label=org.memorithm.hub.attempt={}", attempt.attempt_id),
                "--filter", &format!("label=org.memorithm.hub.request={}", attempt.request_sha256),
                "--format", "{{.ID}}",
            ]);
            let observed = guarded_docker(&mut lookup, Instant::now() + CLEANUP_TIMEOUT, CONTROL_CAPTURE_BYTES)?;
            if observed.timed_out || !observed.status.success() || observed.stdout.truncated || observed.stderr.truncated {
                return Err("ownership lookup unavailable; reservation quarantined".into());
            }
            let output = String::from_utf8(observed.stdout.bytes).map_err(|e| e.to_string())?;
            let handles: Vec<_> = output.lines().filter(|s| !s.is_empty()).collect();
            if handles.len() != 1 || !lower_hex(handles[0], 64) {
                // Zero is not proof: a timed-out create may still complete later.
                return Err("unconfirmed creation remains quarantined; no unique ownership handle".into());
            }
            attempt = journal.sandbox_own(task_id, attempt.attempt_id, handles[0])
                .map_err(|e| e.to_string())?;
        }
        let handle = attempt.owned_handle.as_deref().ok_or("missing owned handle")?;
        if !lower_hex(handle, 64) {
            return Err("invalid immutable container handle".into());
        }
        confirm_owned_absence(&self.docker, handle)?;
        journal.sandbox_confirm_absent(task_id, attempt.attempt_id, handle).map_err(|e| e.to_string())
    }
}

fn validate_request(request: &GvisorExecutionRequestV1) -> Result<(), String> {
    if request.schema != GVISOR_REQUEST_SCHEMA_V1 {
        return Err("unsupported gVisor request schema".to_owned());
    }
    if !bounded_token(&request.task_id)
        || !request
            .task_id
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '-')
    {
        return Err("invalid task_id".to_owned());
    }
    let digest = request.image.split_once("@sha256:").map(|(_, value)| value);
    if request.image.len() > 512
        || digest.is_none_or(|value| {
            value.len() != 64 || !value.chars().all(|character| character.is_ascii_hexdigit())
        })
    {
        return Err("container image must use an immutable sha256 digest".to_owned());
    }
    if request.command.is_empty()
        || request.command.len() > 64
        || request.command.iter().any(|value| !bounded_text(value))
    {
        return Err("invalid bounded command".to_owned());
    }
    if request.environment.len() > 64
        || request
            .environment
            .iter()
            .any(|(key, value)| !bounded_token(key) || !bounded_text(value))
    {
        return Err("invalid bounded environment".to_owned());
    }
    if request.cpu_millis == 0
        || request.memory_bytes < 16 * 1024 * 1024
        || request.pids_limit == 0
        || request.pids_limit > 4096
        || request
            .guest_pids_limit
            .is_some_and(|guest| guest == 0 || guest > request.pids_limit)
        || request.timeout_ms == 0
        || request.timeout_ms > MAX_TASK_TIMEOUT_MS
        || request.max_capture_bytes == 0
        || request.max_capture_bytes > MAX_CAPTURE_BYTES
    {
        return Err("invalid resource bounds".to_owned());
    }
    if !request.default_deny_network || !request.read_only_root {
        return Err("gVisor requires default-deny network and read-only root".to_owned());
    }
    Ok(())
}

fn nonroot_container_user(uid: u32, gid: u32) -> Result<String, String> {
    if uid == 0 {
        return Err("guest process-count enforcement requires a nonroot workspace owner".into());
    }
    Ok(format!("{uid}:{gid}"))
}

fn lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .chars()
            .all(|character| character.is_ascii_digit() || ('a'..='f').contains(&character))
}

fn bounded_token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_TOKEN_BYTES
        && value
            .chars()
            .all(|character| !character.is_control() && !character.is_whitespace())
}

fn bounded_text(value: &str) -> bool {
    !value.is_empty() && value.len() <= 4096 && value.chars().all(|character| character != '\0')
}

fn format_cpu(cpu_millis: u64) -> String {
    format!("{}.{:03}", cpu_millis / 1000, cpu_millis % 1000)
}

fn guarded_docker(
    command: &mut Command,
    deadline: Instant,
    cap: usize,
) -> Result<GuardedCommandOutput, String> {
    execute_guarded_command(command, deadline, cap).map_err(|error| error.to_string())
}

fn cleanup_owned_container(docker: &Path, container_id: &str) -> Result<(), String> {
    let deadline = Instant::now()
        .checked_add(CLEANUP_TIMEOUT)
        .ok_or_else(|| "container cleanup deadline overflow".to_owned())?;
    let mut remove = Command::new(docker);
    remove.args(["rm", "--force", container_id]);
    let removed = guarded_docker(&mut remove, deadline, CONTROL_CAPTURE_BYTES)?;
    if removed.timed_out || removed.stdout.truncated || removed.stderr.truncated {
        return Err("owned container removal unconfirmed".into());
    }
    confirm_owned_absence_before(docker, container_id, deadline)
}

fn confirm_owned_absence(docker: &Path, container_id: &str) -> Result<(), String> {
    confirm_owned_absence_before(docker, container_id, Instant::now() + CLEANUP_TIMEOUT)
}

fn confirm_owned_absence_before(docker: &Path, container_id: &str, deadline: Instant) -> Result<(), String> {
    // rm may return nonzero if --rm already removed the container. Only a
    // successful exact-ID query with empty bounded output proves absence.
    let mut list = Command::new(docker);
    list.args([
        "container",
        "ls",
        "--all",
        "--filter",
        &format!("id={container_id}"),
        "--format",
        "{{.ID}}",
    ]);
    let absent = guarded_docker(&mut list, deadline, CONTROL_CAPTURE_BYTES)?;
    if absent.timed_out
        || !absent.status.success()
        || absent.stdout.truncated
        || absent.stderr.truncated
        || !absent.stdout.bytes.iter().all(u8::is_ascii_whitespace)
    {
        return Err("owned container absence unconfirmed".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);

    fn temporary() -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "hub-gvisor-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        path
    }

    fn task_workspace(root: &Path) -> PathBuf {
        let workspace = root.join("task");
        fs::create_dir(&workspace).unwrap();
        if fs::metadata(&workspace).unwrap().uid() == 0 {
            // Only this newly owned fixture directory is reassigned. The fake
            // Docker protocol still runs locally; no sandbox claim is made.
            std::os::unix::fs::chown(&workspace, Some(1001), Some(1001)).unwrap();
        }
        workspace
    }

    fn request(workspace: PathBuf) -> GvisorExecutionRequestV1 {
        GvisorExecutionRequestV1 {
            schema: GVISOR_REQUEST_SCHEMA_V1.to_owned(),
            task_id: "papers-01".to_owned(),
            image: "rust@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_owned(),
            workspace,
            command: vec!["true".to_owned()],
            environment: Vec::new(),
            cpu_millis: 1000,
            memory_bytes: 64 * 1024 * 1024,
            pids_limit: 32,
            guest_pids_limit: None,
            timeout_ms: 1000,
            max_capture_bytes: 1024,
            default_deny_network: true,
            read_only_root: true,
        }
    }

    #[test]
    fn descriptor_claims_only_qualified_enforcement() {
        let root = temporary();
        let descriptor = GvisorDockerExecutor::new(root.clone()).backend_descriptor();
        assert_eq!(descriptor.isolation, IsolationLevel::Gvisor);
        assert!(!descriptor.isolation_qualified);
        assert!(descriptor.isolation_evidence_id.is_none());
        assert!(!descriptor.enforces_network_policy);
        assert!(descriptor.enforces_default_deny_network);
        assert!(descriptor.resources.cpu_millis);
        assert!(descriptor.resources.memory_bytes);
        assert!(!descriptor.resources.gpu_devices);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn exact_docker_controls_and_receipt_are_typed() {
        let root = temporary();
        let workspace = task_workspace(&root);
        let log = root.join("args");
        let docker = root.join("docker");
        fs::write(
            &docker,
            format!(
                "#!/bin/sh\ncase \"$1\" in\ncreate) printf '%s\\n' \"$@\" > '{}'; printf '%s\\n' '{}' ;;\nstart) printf qualified ;;\nrm|container) exit 0 ;;\n*) exit 1 ;;\nesac\n",
                log.display(), "a".repeat(64)
            ),
        )
        .unwrap();
        fs::set_permissions(&docker, fs::Permissions::from_mode(0o755)).unwrap();
        let receipt = GvisorDockerExecutor::with_docker(docker, root.clone())
            .execute(&request(workspace))
            .unwrap();
        assert_eq!(receipt.schema, GVISOR_RECEIPT_SCHEMA_V1);
        assert_eq!(receipt.stdout, "qualified");
        let args = fs::read_to_string(log).unwrap();
        for required in [
            "--runtime=runsc",
            "--network=none",
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges",
            "--read-only",
            "--user",
            "--pids-limit",
            "nproc=32:32",
            "fsize=16777216:16777216",
            "--memory",
            "--memory-swap",
            "--cpus",
        ] {
            assert!(args.lines().any(|line| line == required), "{required}");
        }
        fs::remove_dir_all(root).unwrap();
    }

    // Docker protocol fixtures only, never sandbox qualification.
    fn fixture(root: &Path, create: &str, start: &str, remove: &str, list: &str) -> PathBuf {
        let docker = root.join("docker");
        fs::write(&docker, format!(
            "#!/bin/sh\ncase \"$1\" in\ncreate) {create} ;;\nstart) {start} ;;\nrm) {remove} ;;\ncontainer) {list} ;;\n*) exit 2 ;;\nesac\n"
        )).unwrap();
        fs::set_permissions(&docker, fs::Permissions::from_mode(0o755)).unwrap();
        docker
    }

    #[test]
    fn creation_failure_never_mutates_an_unowned_container() {
        let root = temporary();
        let workspace = task_workspace(&root);
        let touched = format!("touch '{}'; exit 0", root.join("mutated").display());
        let docker = fixture(&root, "exit 1", &touched, &touched, &touched);
        assert!(GvisorDockerExecutor::with_docker(docker, root.clone())
            .execute(&request(workspace))
            .is_err());
        assert!(!root.join("mutated").exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn cleanup_hang_returns_bounded_failure_not_a_receipt() {
        let root = temporary();
        let workspace = task_workspace(&root);
        let create = format!("printf '%s\\n' '{}'", "a".repeat(64));
        let docker = fixture(
            &root,
            &create,
            "printf completed",
            "exec sleep 30",
            "exit 0",
        );
        let started = Instant::now();
        let result =
            GvisorDockerExecutor::with_docker(docker, root.clone()).execute(&request(workspace));
        assert!(started.elapsed() < Duration::from_secs(6));
        assert!(result.unwrap_err().contains("removal unconfirmed"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn daemon_query_failure_never_proves_container_absence() {
        let root = temporary();
        let workspace = task_workspace(&root);
        let create = format!("printf '%s\\n' '{}'", "a".repeat(64));
        let docker = fixture(&root, &create, "printf completed", "exit 1", "exit 1");
        let result =
            GvisorDockerExecutor::with_docker(docker, root.clone()).execute(&request(workspace));
        assert!(result.unwrap_err().contains("absence unconfirmed"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn excessive_deadline_fails_before_creation() {
        let root = temporary();
        let workspace = task_workspace(&root);
        let touched = format!("touch '{}'; exit 0", root.join("created").display());
        let docker = fixture(&root, &touched, "exit 0", "exit 0", "exit 0");
        let mut candidate = request(workspace);
        candidate.timeout_ms = u64::MAX;
        assert!(GvisorDockerExecutor::with_docker(docker, root.clone())
            .execute(&candidate)
            .is_err());
        assert!(!root.join("created").exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn failed_creation_stays_quarantined_across_restart_and_empty_lookup() {
        let root = temporary();
        let workspace = task_workspace(&root);
        let docker = fixture(&root, "exit 1", "exit 0", "exit 0", "exit 0");
        let executor = GvisorDockerExecutor::with_docker(docker.clone(), root.clone());
        let candidate = request(workspace);
        assert!(executor.execute(&candidate).is_err());
        assert_eq!(executor.inspect_attempt(&candidate.task_id).unwrap().state, SandboxAttemptState::Reserved);
        drop(executor);
        let restarted = GvisorDockerExecutor::with_docker(docker, root.clone());
        assert!(restarted.reconcile_attempt(&candidate.task_id).is_err());
        assert_eq!(restarted.inspect_attempt(&candidate.task_id).unwrap().state, SandboxAttemptState::Reserved);
        assert!(restarted.execute(&candidate).is_err());
        let mut other = candidate;
        other.task_id = "papers-02".into();
        assert!(restarted.execute(&other).is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn durable_owned_handle_never_cleans_when_daemon_query_fails() {
        let root = temporary();
        let workspace = task_workspace(&root);
        let create = format!("printf '%s\\n' '{}'", "a".repeat(64));
        let docker = fixture(&root, &create, "printf completed", "exit 1", "exit 1");
        let executor = GvisorDockerExecutor::with_docker(docker, root.clone());
        let candidate = request(workspace);
        assert!(executor.execute(&candidate).is_err());
        let record = executor.inspect_attempt(&candidate.task_id).unwrap();
        assert_eq!(record.state, SandboxAttemptState::Owned);
        assert_eq!(record.owned_handle, Some("a".repeat(64)));
        assert!(executor.reconcile_attempt(&candidate.task_id).is_err());
        assert_eq!(executor.inspect_attempt(&candidate.task_id).unwrap().state, SandboxAttemptState::Owned);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn private_journal_is_not_an_admissible_guest_workspace() {
        let root = temporary();
        let (_journal, private) = crate::gvisor_journal::open(&root).unwrap();
        let docker = fixture(&root, "exit 1", "exit 1", "exit 1", "exit 1");
        assert!(GvisorDockerExecutor::with_docker(docker, root.clone()).execute(&request(private)).is_err());
        drop(_journal);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn root_workspace_identity_cannot_bypass_guest_process_limits() {
        assert!(nonroot_container_user(0, 0).is_err());
        assert_eq!(nonroot_container_user(1001, 1001).unwrap(), "1001:1001");
    }

    #[test]
    fn guest_pid_budget_is_bounded_before_creation() {
        let mut candidate = request(PathBuf::from("unused"));
        candidate.pids_limit = 4096;
        assert!(validate_request(&candidate).is_ok());
        for invalid in [0, 4097, u64::MAX] {
            candidate.pids_limit = invalid;
            assert!(validate_request(&candidate).is_err());
        }
    }

    #[test]
    fn independent_guest_budget_is_stricter_and_legacy_wire_is_preserved() {
        let mut candidate = request(PathBuf::from("unused"));
        let legacy = serde_json::to_value(&candidate).unwrap();
        assert!(legacy.get("guest_pids_limit").is_none());
        let decoded: GvisorExecutionRequestV1 = serde_json::from_value(legacy).unwrap();
        assert_eq!(decoded.guest_pids_limit, None);
        candidate.guest_pids_limit = Some(16);
        assert!(validate_request(&candidate).is_ok());
        let wire = serde_json::to_value(&candidate).unwrap();
        assert_eq!(wire["guest_pids_limit"], 16);
        for invalid in [0, 33, u64::MAX] {
            candidate.guest_pids_limit = Some(invalid);
            assert!(validate_request(&candidate).is_err());
        }
    }

    #[test]
    fn workspace_outside_executor_root_fails_closed() {
        let root = temporary();
        let workspace_root = root.join("owned");
        let outside = root.join("outside");
        fs::create_dir(&workspace_root).unwrap();
        fs::create_dir(&outside).unwrap();
        let docker = root.join("docker");
        fs::write(&docker, "#!/bin/sh\nexit 0\n").unwrap();
        fs::set_permissions(&docker, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(GvisorDockerExecutor::with_docker(docker, workspace_root)
            .execute(&request(outside))
            .is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn immutable_qualification_artifacts_control_claim() {
        let root = temporary();
        let manifest = root.join("qualification.json");
        let receipt_path = root.join("receipt.json");
        let receipt = serde_json::to_vec(&GvisorExecutionReceiptV1 {
            schema: GVISOR_RECEIPT_SCHEMA_V1.to_owned(),
            task_id: "papers-qualification".to_owned(),
            backend_id: "docker-runsc".to_owned(),
            isolation: "userspace_kernel".to_owned(),
            isolation_evidence_id: None,
            exit_code: Some(0),
            timed_out: false,
            stdout: "PAPERS_GVISOR_OK\n".to_owned(),
            stderr: String::new(),
            stdout_truncated: false,
            stderr_truncated: false,
        })
        .unwrap();
        fs::write(&receipt_path, &receipt).unwrap();
        let receipt_sha256 = format!("{:x}", Sha256::digest(&receipt));
        fs::write(
            &manifest,
            format!(
                "{{\"schema\":\"{}\",\"source_commit\":\"{}\",\"workflow_run_id\":7,\"receipt_sha256\":\"{}\"}}",
                GVISOR_QUALIFICATION_SCHEMA_V1,
                "a".repeat(40),
                receipt_sha256
            ),
        )
        .unwrap();
        let descriptor = GvisorDockerExecutor::from_qualification_artifacts_for_commit(
            root.clone(),
            &manifest,
            &receipt_path,
            &"a".repeat(40),
        )
        .unwrap()
        .backend_descriptor();
        assert!(descriptor.isolation_qualified);
        assert!(descriptor
            .isolation_evidence_id
            .as_deref()
            .unwrap()
            .contains("actions/runs/7#receipt-sha256:"));

        assert!(
            GvisorDockerExecutor::from_qualification_artifacts_for_commit(
                root.clone(),
                &manifest,
                &receipt_path,
                &"c".repeat(40),
            )
            .is_err()
        );
        fs::write(&receipt_path, b"{}").unwrap();
        assert!(
            GvisorDockerExecutor::from_qualification_artifacts_for_commit(
                root.clone(),
                &manifest,
                &receipt_path,
                &"a".repeat(40),
            )
            .is_err()
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn mutable_image_and_relaxed_boundaries_fail_closed() {
        let root = temporary();
        let mut candidate = request(root.clone());
        candidate.image = "rust:latest".to_owned();
        assert!(validate_request(&candidate).is_err());
        candidate = request(root.clone());
        candidate.default_deny_network = false;
        assert!(validate_request(&candidate).is_err());
        candidate = request(root.clone());
        candidate.read_only_root = false;
        assert!(validate_request(&candidate).is_err());
        fs::remove_dir_all(root).unwrap();
    }
}
