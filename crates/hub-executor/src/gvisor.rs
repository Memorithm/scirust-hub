//! Qualified gVisor execution boundary for hostile, bounded CPU workloads.
//!
//! The adapter invokes Docker only with the runsc runtime and a fixed
//! fail-closed control set. A dedicated workflow qualifies the real backend.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use hub_core::{IsolationLevel, ResourceEnforcement, SandboxBackendDescriptor};
use serde::{Deserialize, Serialize};

pub const GVISOR_REQUEST_SCHEMA_V1: &str = "scirust-hub.gvisor-execution-request/v1";
pub const GVISOR_RECEIPT_SCHEMA_V1: &str = "scirust-hub.gvisor-execution-receipt/v1";
pub const GVISOR_QUALIFICATION_SCHEMA_V1: &str = "scirust-hub.gvisor-qualification-evidence/v1";

const MAX_TOKEN_BYTES: usize = 256;
const MAX_CAPTURE_BYTES: usize = 1024 * 1024;
const POLL_MS: u64 = 10;

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

    pub fn from_qualification_manifest(
        workspace_root: PathBuf,
        manifest: &Path,
    ) -> Result<Self, String> {
        let bytes = std::fs::read(manifest)
            .map_err(|error| format!("qualification manifest unavailable: {error}"))?;
        let qualification: GvisorQualificationEvidenceV1 = serde_json::from_slice(&bytes)
            .map_err(|error| format!("invalid qualification manifest JSON: {error}"))?;
        qualification.validate()?;
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
        {
            return Err("workspace must be a task directory below the executor root".to_owned());
        }

        let name = format!("scirust-gvisor-{}", request.task_id);
        let mut command = Command::new(&self.docker);
        command
            .args([
                "run",
                "--rm",
                "--runtime=runsc",
                "--network=none",
                "--cap-drop=ALL",
                "--security-opt=no-new-privileges",
                "--read-only",
                "--name",
                &name,
                "--cpus",
                &format_cpu(request.cpu_millis),
                "--memory",
                &request.memory_bytes.to_string(),
                "--pids-limit",
                &request.pids_limit.to_string(),
                "--tmpfs",
                "/tmp:rw,nosuid,nodev,size=67108864",
                "--mount",
            ])
            .arg(format!(
                "type=bind,src={},dst=/workspace,rw",
                workspace.display()
            ))
            .args(["--workdir", "/workspace"]);
        for (key, value) in &request.environment {
            command.args(["--env", &format!("{key}={value}")]);
        }
        command.arg(&request.image).args(&request.command);
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let started = Instant::now();
        let mut child = command
            .spawn()
            .map_err(|error| format!("failed to start Docker/runsc: {error}"))?;
        let stdout = spawn_reader(child.stdout.take(), request.max_capture_bytes);
        let stderr = spawn_reader(child.stderr.take(), request.max_capture_bytes);
        let deadline = started
            .checked_add(Duration::from_millis(request.timeout_ms))
            .ok_or_else(|| "timeout overflow".to_owned())?;

        let mut timed_out = false;
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) if Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(POLL_MS));
                }
                Ok(None) => {
                    timed_out = true;
                    kill_container(&self.docker, &name);
                    let _ = child.kill();
                    break child
                        .wait()
                        .map_err(|error| format!("failed to reap Docker client: {error}"))?;
                }
                Err(error) => {
                    kill_container(&self.docker, &name);
                    return Err(format!("failed to poll Docker client: {error}"));
                }
            }
        };

        let (stdout, stdout_truncated) = join_reader(stdout)?;
        let (stderr, stderr_truncated) = join_reader(stderr)?;
        Ok(GvisorExecutionReceiptV1 {
            schema: GVISOR_RECEIPT_SCHEMA_V1.to_owned(),
            task_id: request.task_id.clone(),
            backend_id: "docker-runsc".to_owned(),
            isolation: "userspace_kernel".to_owned(),
            isolation_evidence_id: self
                .qualification
                .as_ref()
                .map(GvisorQualificationEvidenceV1::evidence_id),
            exit_code: status.code(),
            timed_out,
            stdout: String::from_utf8_lossy(&stdout).into_owned(),
            stderr: String::from_utf8_lossy(&stderr).into_owned(),
            stdout_truncated,
            stderr_truncated,
        })
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
        || request.timeout_ms == 0
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

fn kill_container(docker: &Path, name: &str) {
    let _ = Command::new(docker)
        .args(["kill", name])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

fn spawn_reader<R: Read + Send + 'static>(
    pipe: Option<R>,
    limit: usize,
) -> JoinHandle<Result<(Vec<u8>, bool), String>> {
    thread::spawn(move || {
        let mut pipe = pipe.ok_or_else(|| "capture pipe missing".to_owned())?;
        let mut captured = Vec::with_capacity(limit.min(8192));
        let mut buffer = [0_u8; 8192];
        let mut truncated = false;
        loop {
            let count = pipe
                .read(&mut buffer)
                .map_err(|error| format!("capture read failed: {error}"))?;
            if count == 0 {
                break;
            }
            let remaining = limit.saturating_sub(captured.len());
            let kept = remaining.min(count);
            captured.extend_from_slice(&buffer[..kept]);
            truncated |= kept < count;
        }
        Ok((captured, truncated))
    })
}

fn join_reader(
    reader: JoinHandle<Result<(Vec<u8>, bool), String>>,
) -> Result<(Vec<u8>, bool), String> {
    reader
        .join()
        .map_err(|_| "capture reader panicked".to_owned())?
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
        let workspace = root.join("task");
        fs::create_dir(&workspace).unwrap();
        let log = root.join("args");
        let docker = root.join("docker");
        fs::write(
            &docker,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > '{}'\nprintf qualified\n",
                log.display()
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
            "--pids-limit",
            "--memory",
            "--cpus",
        ] {
            assert!(args.lines().any(|line| line == required), "{required}");
        }
        fs::remove_dir_all(root).unwrap();
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
    fn immutable_qualification_manifest_controls_claim() {
        let root = temporary();
        let manifest = root.join("qualification.json");
        fs::write(
            &manifest,
            format!(
                "{{\"schema\":\"{}\",\"source_commit\":\"{}\",\"workflow_run_id\":7,\"receipt_sha256\":\"{}\"}}",
                GVISOR_QUALIFICATION_SCHEMA_V1,
                "a".repeat(40),
                "b".repeat(64)
            ),
        )
        .unwrap();
        let descriptor = GvisorDockerExecutor::from_qualification_manifest(root.clone(), &manifest)
            .unwrap()
            .backend_descriptor();
        assert!(descriptor.isolation_qualified);
        assert!(descriptor
            .isolation_evidence_id
            .as_deref()
            .unwrap()
            .contains("actions/runs/7#receipt-sha256:"));
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
