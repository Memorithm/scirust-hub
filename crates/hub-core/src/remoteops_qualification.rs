//! Strict consumer for RemoteOps backend qualification v2.
//!
//! The importer preserves the source qualification record and projects only
//! controls Hub can represent precisely. The generic RemoteOps resource-limit
//! flag is retained on the imported record; it never becomes CPU, memory, or
//! other dimension-specific enforcement in Hub.

use serde::Deserialize;
use std::fmt;

use crate::task::{CapabilitySet, IsolationLevel, ResourceEnforcement, SandboxBackendDescriptor};

pub const REMOTEOPS_BACKEND_QUALIFICATION_V2_SCHEMA: &str = "remoteops.backend-qualification/v2";
pub const MAX_REMOTEOPS_QUALIFICATION_JSON_BYTES: usize = 16 * 1024;
pub const MAX_REMOTEOPS_BACKEND_NAME_BYTES: usize = 128;
pub const MAX_REMOTEOPS_EVIDENCE_ID_BYTES: usize = 256;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RemoteOpsIsolationV2 {
    SupervisedProcess,
    Container,
    UserspaceKernel,
    MicroVm,
}

impl RemoteOpsIsolationV2 {
    const fn to_hub(self) -> IsolationLevel {
        match self {
            Self::SupervisedProcess => IsolationLevel::Process,
            Self::Container => IsolationLevel::Container,
            Self::UserspaceKernel => IsolationLevel::Gvisor,
            Self::MicroVm => IsolationLevel::MicroVm,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RemoteOpsControlsV2 {
    pub network_egress: bool,
    pub default_deny_network: bool,
    pub resource_limits: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteOpsBackendQualificationV2 {
    backend: String,
    isolation: RemoteOpsIsolationV2,
    isolation_qualified: bool,
    controls: RemoteOpsControlsV2,
    evidence_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RemoteOpsBackendQualificationWireV2 {
    schema: String,
    backend: String,
    isolation: RemoteOpsIsolationV2,
    isolation_qualified: bool,
    controls: RemoteOpsControlsV2,
    evidence_id: serde_json::Value,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RemoteOpsQualificationError {
    DocumentTooLarge,
    InvalidJson,
    UnsupportedSchema,
    InvalidBackend,
    InvalidEvidence,
    MissingEvidence,
    SupervisedProcessCannotBeQualified,
}

impl fmt::Display for RemoteOpsQualificationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::DocumentTooLarge => "RemoteOps qualification document exceeds the size limit",
            Self::InvalidJson => "RemoteOps qualification document is invalid",
            Self::UnsupportedSchema => "unsupported RemoteOps qualification schema",
            Self::InvalidBackend => "RemoteOps backend name is invalid",
            Self::InvalidEvidence => "RemoteOps qualification evidence reference is invalid",
            Self::MissingEvidence => "RemoteOps qualification claims require an evidence reference",
            Self::SupervisedProcessCannotBeQualified => {
                "supervised process cannot claim qualified isolation"
            }
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for RemoteOpsQualificationError {}

impl RemoteOpsBackendQualificationV2 {
    /// Parse and validate one bounded RemoteOps qualification v2 document.
    pub fn parse(json: &str) -> Result<Self, RemoteOpsQualificationError> {
        if json.len() > MAX_REMOTEOPS_QUALIFICATION_JSON_BYTES {
            return Err(RemoteOpsQualificationError::DocumentTooLarge);
        }
        let wire: RemoteOpsBackendQualificationWireV2 =
            serde_json::from_str(json).map_err(|_| RemoteOpsQualificationError::InvalidJson)?;
        if wire.schema != REMOTEOPS_BACKEND_QUALIFICATION_V2_SCHEMA {
            return Err(RemoteOpsQualificationError::UnsupportedSchema);
        }
        let evidence_id = match wire.evidence_id {
            serde_json::Value::Null => None,
            serde_json::Value::String(value) => Some(value),
            _ => return Err(RemoteOpsQualificationError::InvalidJson),
        };
        validate_label(
            &wire.backend,
            MAX_REMOTEOPS_BACKEND_NAME_BYTES,
            RemoteOpsQualificationError::InvalidBackend,
        )?;
        if wire.isolation == RemoteOpsIsolationV2::SupervisedProcess && wire.isolation_qualified {
            return Err(RemoteOpsQualificationError::SupervisedProcessCannotBeQualified);
        }

        let claims_qualification = wire.isolation_qualified
            || wire.controls.network_egress
            || wire.controls.default_deny_network
            || wire.controls.resource_limits;
        match evidence_id.as_deref() {
            Some(evidence_id) => validate_label(
                evidence_id,
                MAX_REMOTEOPS_EVIDENCE_ID_BYTES,
                RemoteOpsQualificationError::InvalidEvidence,
            )?,
            None if claims_qualification => {
                return Err(RemoteOpsQualificationError::MissingEvidence);
            }
            None => {}
        }

        Ok(Self {
            backend: wire.backend,
            isolation: wire.isolation,
            isolation_qualified: wire.isolation_qualified,
            controls: wire.controls,
            evidence_id,
        })
    }

    #[must_use]
    pub fn backend(&self) -> &str {
        &self.backend
    }

    #[must_use]
    pub const fn isolation(&self) -> RemoteOpsIsolationV2 {
        self.isolation
    }

    #[must_use]
    pub const fn isolation_qualified(&self) -> bool {
        self.isolation_qualified
    }

    #[must_use]
    pub const fn controls(&self) -> RemoteOpsControlsV2 {
        self.controls
    }

    #[must_use]
    pub fn evidence_id(&self) -> Option<&str> {
        self.evidence_id.as_deref()
    }

    /// Build Hub's descriptor while retaining source claims Hub cannot map.
    /// The generic resource-limit flag does not identify individual dimensions.
    #[must_use]
    pub fn to_sandbox_backend_descriptor(&self) -> SandboxBackendDescriptor {
        SandboxBackendDescriptor {
            backend_id: self.backend.clone(),
            isolation: self.isolation.to_hub(),
            isolation_qualified: self.isolation_qualified,
            isolation_evidence_id: if self.isolation_qualified {
                self.evidence_id.clone()
            } else {
                None
            },
            enforces_network_policy: self.controls.network_egress,
            enforces_default_deny_network: self.controls.default_deny_network,
            enforces_workspace_write_policy: false,
            resources: ResourceEnforcement::default(),
            capabilities: CapabilitySet::default(),
        }
    }
}

fn validate_label(
    value: &str,
    maximum: usize,
    error: RemoteOpsQualificationError,
) -> Result<(), RemoteOpsQualificationError> {
    let lower = value.to_ascii_lowercase();
    let contains_secret_marker = ["token=", "api_key=", "apikey=", "secret=", "-----begin"]
        .iter()
        .any(|marker| lower.contains(marker));
    if value.is_empty()
        || value.len() > maximum
        || value.trim() != value
        || value.chars().any(char::is_control)
        || contains_secret_marker
    {
        return Err(error);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE: &str = include_str!("../tests/fixtures/remoteops-backend-qualification-v2.json");

    #[test]
    fn parses_v2_and_projects_only_precisely_representable_controls() {
        let qualification = RemoteOpsBackendQualificationV2::parse(FIXTURE).expect("qualification");
        assert_eq!(qualification.backend(), "runsc");
        assert_eq!(
            qualification.isolation(),
            RemoteOpsIsolationV2::UserspaceKernel
        );
        assert!(qualification.isolation_qualified());
        assert!(qualification.controls().resource_limits);
        assert_eq!(
            qualification.evidence_id(),
            Some("qualification/runsc/2026-09-27")
        );

        let descriptor = qualification.to_sandbox_backend_descriptor();
        assert_eq!(descriptor.backend_id, "runsc");
        assert_eq!(descriptor.isolation, IsolationLevel::Gvisor);
        assert!(descriptor.isolation_qualified);
        assert_eq!(
            descriptor.isolation_evidence_id.as_deref(),
            Some("qualification/runsc/2026-09-27")
        );
        assert!(descriptor.enforces_network_policy);
        assert!(descriptor.enforces_default_deny_network);
        assert!(!descriptor.resources.wall_clock_ms);
        assert!(!descriptor.resources.cpu_millis);
        assert!(!descriptor.resources.memory_bytes);
    }

    #[test]
    fn unknown_fields_and_schema_versions_fail_closed() {
        let unknown = format!(
            "{}{}",
            FIXTURE.trim_end().trim_end_matches('}'),
            r#","future":true}"#
        );
        assert_eq!(
            RemoteOpsBackendQualificationV2::parse(&unknown),
            Err(RemoteOpsQualificationError::InvalidJson)
        );

        let unsupported = FIXTURE.replace(
            REMOTEOPS_BACKEND_QUALIFICATION_V2_SCHEMA,
            "remoteops.backend-qualification/v3",
        );
        assert_eq!(
            RemoteOpsBackendQualificationV2::parse(&unsupported),
            Err(RemoteOpsQualificationError::UnsupportedSchema)
        );
    }

    #[test]
    fn affirmative_claims_need_evidence_and_process_is_not_a_sandbox() {
        let missing_evidence = r#"{"schema":"remoteops.backend-qualification/v2","backend":"bwrap","isolation":"container","isolation_qualified":true,"controls":{"network_egress":false,"default_deny_network":false,"resource_limits":false},"evidence_id":null}"#;
        assert_eq!(
            RemoteOpsBackendQualificationV2::parse(missing_evidence),
            Err(RemoteOpsQualificationError::MissingEvidence)
        );

        let omitted_evidence = r#"{"schema":"remoteops.backend-qualification/v2","backend":"bwrap","isolation":"container","isolation_qualified":false,"controls":{"network_egress":false,"default_deny_network":false,"resource_limits":false}}"#;
        assert_eq!(
            RemoteOpsBackendQualificationV2::parse(omitted_evidence),
            Err(RemoteOpsQualificationError::InvalidJson)
        );

        let process = FIXTURE.replace("userspace_kernel", "supervised_process");
        assert_eq!(
            RemoteOpsBackendQualificationV2::parse(&process),
            Err(RemoteOpsQualificationError::SupervisedProcessCannotBeQualified)
        );
    }

    #[test]
    fn document_and_identifiers_are_bounded_and_secret_free() {
        assert_eq!(
            RemoteOpsBackendQualificationV2::parse(
                &" ".repeat(MAX_REMOTEOPS_QUALIFICATION_JSON_BYTES + 1)
            ),
            Err(RemoteOpsQualificationError::DocumentTooLarge)
        );
        let padded = FIXTURE.replace(r#""backend":"runsc""#, r#""backend":" runsc""#);
        assert_eq!(
            RemoteOpsBackendQualificationV2::parse(&padded),
            Err(RemoteOpsQualificationError::InvalidBackend)
        );
        let secret = FIXTURE.replace(
            "qualification/runsc/2026-09-27",
            "qualification/secret=embedded",
        );
        assert_eq!(
            RemoteOpsBackendQualificationV2::parse(&secret),
            Err(RemoteOpsQualificationError::InvalidEvidence)
        );
    }
}
