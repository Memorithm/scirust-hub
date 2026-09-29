//! Strict, diagnostic-only consumer for RemoteOps host capability snapshot v1.
//!
//! Parsing this unsigned observation never constructs backend qualification,
//! worker identity, or admission data.
use serde::{Deserialize, Deserializer};
use std::fmt;

pub const REMOTEOPS_HOST_CAPABILITY_SNAPSHOT_V1_SCHEMA: &str =
    "remoteops.host-capability-snapshot/v1";
pub const MAX_REMOTEOPS_HOST_CAPABILITY_SNAPSHOT_BYTES: usize = 16 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteOpsHostCapabilitySnapshotV1 {
    pub observed_at_unix_seconds: u64,
    pub host_sandbox_observations: HostSandboxObservationsV1,
    pub host_resource_observations: HostResourceObservationsV2,
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HostSandboxObservationsV1 {
    schema_version: u8,
    pub os: String,
    pub arch: String,
    pub cgroup_v2: bool,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub unprivileged_userns_clone: Option<bool>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub seccomp_mode: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub apparmor_enabled: Option<bool>,
    pub kvm_accessible: bool,
    pub bubblewrap: CommandObservation,
    pub podman: CommandObservation,
    pub docker: CommandObservation,
    pub runsc: CommandObservation,
    pub firecracker: CommandObservation,
    pub unshare: CommandObservation,
    pub systemd_run: CommandObservation,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CommandObservation {
    pub present: bool,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub executable: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub version: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HostResourceObservationsV2 {
    schema_version: u8,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub cpu_logical_count: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub memory_total_bytes: Option<u64>,
    pub cgroup_cpu_quota_millis: LimitObservation,
    pub cgroup_memory_limit_bytes: LimitObservation,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum LimitObservation {
    Unknown,
    Unbounded,
    Limited { value: u64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HostSnapshotError {
    DocumentTooLarge,
    InvalidJson,
    UnsupportedSchema,
    InvalidTrustStatus,
    UnsupportedSandboxSchema,
    UnsupportedResourceSchema,
    InvalidObservation,
}

impl fmt::Display for HostSnapshotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::DocumentTooLarge => "RemoteOps host snapshot exceeds the size limit",
            Self::InvalidJson => "RemoteOps host snapshot is invalid",
            Self::UnsupportedSchema => "unsupported RemoteOps host snapshot schema",
            Self::InvalidTrustStatus => "RemoteOps host snapshot is not an unsigned observation",
            Self::UnsupportedSandboxSchema => "unsupported host sandbox observation schema",
            Self::UnsupportedResourceSchema => "unsupported host resource observation schema",
            Self::InvalidObservation => "RemoteOps host snapshot contains an invalid observation",
        };
        f.write_str(message)
    }
}

impl std::error::Error for HostSnapshotError {}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotWire {
    schema: String,
    observed_at_unix_seconds: u64,
    trust_status: String,
    host_sandbox_observations: HostSandboxObservationsV1,
    host_resource_observations: HostResourceObservationsV2,
}

impl RemoteOpsHostCapabilitySnapshotV1 {
    /// Parse one bounded v1 observation. The result is diagnostic only.
    pub fn parse(json: &str) -> Result<Self, HostSnapshotError> {
        if json.len() > MAX_REMOTEOPS_HOST_CAPABILITY_SNAPSHOT_BYTES {
            return Err(HostSnapshotError::DocumentTooLarge);
        }
        let wire: SnapshotWire =
            serde_json::from_str(json).map_err(|_| HostSnapshotError::InvalidJson)?;
        if wire.schema != REMOTEOPS_HOST_CAPABILITY_SNAPSHOT_V1_SCHEMA {
            return Err(HostSnapshotError::UnsupportedSchema);
        }
        if wire.trust_status != "unsigned_observation" {
            return Err(HostSnapshotError::InvalidTrustStatus);
        }
        let sandbox = &wire.host_sandbox_observations;
        if sandbox.schema_version != 1 {
            return Err(HostSnapshotError::UnsupportedSandboxSchema);
        }
        let resources = &wire.host_resource_observations;
        if resources.schema_version != 2 {
            return Err(HostSnapshotError::UnsupportedResourceSchema);
        }
        if !valid_platform(&sandbox.os)
            || !valid_platform(&sandbox.arch)
            || resources.cpu_logical_count == Some(0)
            || resources.memory_total_bytes == Some(0)
            || [
                &sandbox.bubblewrap,
                &sandbox.podman,
                &sandbox.docker,
                &sandbox.runsc,
                &sandbox.firecracker,
                &sandbox.unshare,
                &sandbox.systemd_run,
            ]
            .iter()
            .any(|probe| !valid_probe(probe))
        {
            return Err(HostSnapshotError::InvalidObservation);
        }
        Ok(Self {
            observed_at_unix_seconds: wire.observed_at_unix_seconds,
            host_sandbox_observations: wire.host_sandbox_observations,
            host_resource_observations: wire.host_resource_observations,
        })
    }
}

fn valid_platform(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
}

fn valid_probe(probe: &CommandObservation) -> bool {
    if !probe.present && (probe.executable.is_some() || probe.version.is_some()) {
        return false;
    }
    probe
        .executable
        .as_ref()
        .is_none_or(|s| !s.is_empty() && s.len() <= 4096 && !s.chars().any(char::is_control))
        && probe
            .version
            .as_ref()
            .is_none_or(|s| !s.is_empty() && s.len() <= 512 && !s.chars().any(char::is_control))
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE: &str =
        include_str!("../tests/fixtures/remoteops-host-capability-snapshot-v1.json");

    #[test]
    fn parses_unsigned_fixture_as_observations_only() {
        let snapshot = RemoteOpsHostCapabilitySnapshotV1::parse(FIXTURE).expect("valid fixture");
        assert_eq!(snapshot.observed_at_unix_seconds, 1_798_000_000);
        assert_eq!(snapshot.host_sandbox_observations.os, "linux");
        assert_eq!(
            snapshot
                .host_resource_observations
                .cgroup_memory_limit_bytes,
            LimitObservation::Unknown
        );
    }

    #[test]
    fn rejects_unknown_fields_versions_and_trust_escalation() {
        let unknown = FIXTURE.trim_end().trim_end_matches('}');
        let unknown = format!("{unknown},\"future\":true}}");
        assert_eq!(
            RemoteOpsHostCapabilitySnapshotV1::parse(&unknown),
            Err(HostSnapshotError::InvalidJson)
        );
        let unsupported = FIXTURE.replace(
            REMOTEOPS_HOST_CAPABILITY_SNAPSHOT_V1_SCHEMA,
            "remoteops.host-capability-snapshot/v2",
        );
        assert_eq!(
            RemoteOpsHostCapabilitySnapshotV1::parse(&unsupported),
            Err(HostSnapshotError::UnsupportedSchema)
        );
        let trusted = FIXTURE.replace("unsigned_observation", "attested");
        assert_eq!(
            RemoteOpsHostCapabilitySnapshotV1::parse(&trusted),
            Err(HostSnapshotError::InvalidTrustStatus)
        );
    }

    #[test]
    fn rejects_invalid_nested_versions_and_observations() {
        let sandbox = FIXTURE.replace("\"schema_version\":1", "\"schema_version\":3");
        assert_eq!(
            RemoteOpsHostCapabilitySnapshotV1::parse(&sandbox),
            Err(HostSnapshotError::UnsupportedSandboxSchema)
        );
        let cpu = FIXTURE.replace("\"cpu_logical_count\":8", "\"cpu_logical_count\":0");
        assert_eq!(
            RemoteOpsHostCapabilitySnapshotV1::parse(&cpu),
            Err(HostSnapshotError::InvalidObservation)
        );
    }

    #[test]
    fn nullable_schema_fields_must_be_present_even_when_their_value_may_be_null() {
        let missing_kernel_observation =
            FIXTURE.replace("\"unprivileged_userns_clone\":false,", "");
        assert_eq!(
            RemoteOpsHostCapabilitySnapshotV1::parse(&missing_kernel_observation),
            Err(HostSnapshotError::InvalidJson)
        );

        let missing_nullable_command_field = FIXTURE.replace(
            "\"podman\":{\"present\":false,\"executable\":null,",
            "\"podman\":{\"present\":false,",
        );
        assert_eq!(
            RemoteOpsHostCapabilitySnapshotV1::parse(&missing_nullable_command_field),
            Err(HostSnapshotError::InvalidJson)
        );

        let missing_nullable_resource_field = FIXTURE.replace("\"cpu_logical_count\":8,", "");
        assert_eq!(
            RemoteOpsHostCapabilitySnapshotV1::parse(&missing_nullable_resource_field),
            Err(HostSnapshotError::InvalidJson)
        );
    }

    #[test]
    fn document_size_is_bounded() {
        assert_eq!(
            RemoteOpsHostCapabilitySnapshotV1::parse(
                &" ".repeat(MAX_REMOTEOPS_HOST_CAPABILITY_SNAPSHOT_BYTES + 1)
            ),
            Err(HostSnapshotError::DocumentTooLarge)
        );
    }
}
