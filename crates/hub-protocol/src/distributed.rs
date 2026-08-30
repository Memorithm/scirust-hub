//! Versioned wire contract for the first remote process-execution substrate.
//!
//! The protocol deliberately transports immutable file bytes and workdir-
//! relative paths. It never assumes that Hub and a worker share a filesystem.

use std::collections::BTreeMap;

use hub_core::exec::ExecutionOutcome;
use serde::{Deserialize, Serialize};

/// Wire version for Hub <-> worker execution leases.
pub const WORKER_PROTOCOL_VERSION: u16 = 1;
/// Capability required by the v1 remote process executor.
pub const PROCESS_EXECUTION_CAPABILITY: &str = "process.execute.v1";
/// Token used inside argv/env values for paths rooted in the transported workdir.
pub const WORKDIR_TOKEN: &str = "{hub-workdir}";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerDescriptor {
    pub protocol_version: u16,
    pub worker_id: String,
    pub capabilities: Vec<String>,
    pub max_payload_bytes: u64,
    pub heartbeat_interval_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteFile {
    /// UTF-8, workdir-relative path. Receivers must reject absolute paths and
    /// parent traversal before materialization.
    pub path: String,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteExecutionRequest {
    pub program: String,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub timeout_ms: u64,
    pub max_capture_bytes_per_stream: usize,
    pub directories: Vec<String>,
    pub files: Vec<RemoteFile>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseCreateRequest {
    pub protocol_version: u16,
    /// Stable for retries of one executor invocation. Reusing this id with a
    /// different payload is a protocol conflict.
    pub attempt_id: String,
    pub execution: RemoteExecutionRequest,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseCreateResponse {
    pub lease_id: String,
    pub attempt_id: String,
    pub worker_id: String,
    pub state: LeaseState,
    pub expires_at_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeaseState {
    Queued,
    Running,
    Completed,
    Cancelled,
    Failed,
}

impl LeaseState {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Cancelled | Self::Failed)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteExecutionResult {
    pub outcome: ExecutionOutcome,
    /// Files present in the worker workdir after execution. The Hub-side
    /// remote executor materializes them back into its local run workdir;
    /// normal Hub output ingestion remains authoritative afterwards.
    pub files: Vec<RemoteFile>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseStatusResponse {
    pub lease_id: String,
    pub attempt_id: String,
    pub worker_id: String,
    pub state: LeaseState,
    pub last_heartbeat_ms: u64,
    pub expires_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<RemoteExecutionResult>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerErrorResponse {
    pub error: String,
}
