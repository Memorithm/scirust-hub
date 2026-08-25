//! # hub-protocol — SciRust Hub wire protocol
//!
//! The stable, versioned contract spoken by the daemon API and CLI clients.
//! Domain types never travel directly: every payload here is a DTO with an
//! explicit conversion to/from [`hub_core`], so internal evolution does not
//! silently break clients.
//!
//! Versioning rules:
//!
//! - Every request body carries `schema_version`. This build understands
//!   [`PROTOCOL_VERSION`] (currently 1) and rejects others explicitly.
//! - Unknown fields are **tolerated on read** (additive evolution) and never
//!   emitted as errors; writers stamp the current version.
//! - Errors always use [`ErrorEnvelope`], never bare strings.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use hub_core::{
    ArtifactId, CapabilityName, ComponentId, ComponentKind, ComponentManifest,
    ComponentName, ContentDigest, ExecutionBinding, InputBinding, RunId, RunRecord,
    RunSpec, RunState, SourceInfo, Version,
};

/// Protocol version understood by this build.
pub const PROTOCOL_VERSION: u16 = 1;

/// Request-level protocol failures.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ProtocolError {
    #[error("unsupported schema_version {found}: this build speaks version {expected}")]
    UnsupportedSchemaVersion { found: u16, expected: u16 },
}

 /// Validates the `schema_version` of an incoming request body.
///
/// # Errors
/// [`ProtocolError::UnsupportedSchemaVersion`] for anything but
/// [`PROTOCOL_VERSION`].
pub fn check_schema_version(found: u16) -> Result<(), ProtocolError> {
    if found == PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(ProtocolError::UnsupportedSchemaVersion {
            found,
            expected: PROTOCOL_VERSION,
        })
    }
}

/// Machine-readable error codes for [`ErrorEnvelope`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorCode {
    BadRequest,
    UnsupportedSchemaVersion,
    Validation,
    NotFound,
    Conflict,
    Internal,
}

impl ErrorCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            ErrorCode::BadRequest => "bad_request",
            ErrorCode::UnsupportedSchemaVersion => "unsupported_schema_version",
            ErrorCode::Validation => "validation_failed",
            ErrorCode::NotFound => "not_found",
            ErrorCode::Conflict => "conflict",
            ErrorCode::Internal => "internal",
        }
    }
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Structured error response: `{"error": {"code": ..., "message": ..., ...}}`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorEnvelope {
    pub error: ErrorBody,
}

impl ErrorEnvelope {
    #[must_use]
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            error: ErrorBody {
                code,
                message: message.into(),
                details: BTreeMap::new(),
            },
        }
    }

    #[must_use]
    pub fn with_details(
        code: ErrorCode,
        message: impl Into<String>,
        details: BTreeMap<String, String>,
    ) -> Self {
        Self {
            error: ErrorBody { code, message: message.into(), details },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorBody {
    pub code: ErrorCode,
    pub message: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub details: BTreeMap<String, String>,
}

impl Serialize for ErrorCode {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ErrorCode {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        match raw.as_str() {
            "bad_request" => Ok(ErrorCode::BadRequest),
            "unsupported_schema_version" => Ok(ErrorCode::UnsupportedSchemaVersion),
            "validation_failed" => Ok(ErrorCode::Validation),
            "not_found" => Ok(ErrorCode::NotFound),
            "conflict" => Ok(ErrorCode::Conflict),
            "internal" => Ok(ErrorCode::Internal),
            other => Err(serde::de::Error::unknown_variant(
                other,
                &[
                    "bad_request",
                    "unsupported_schema_version",
                    "validation_failed",
                    "not_found",
                    "conflict",
                    "internal",
                ],
            )),
        }
    }
}

// ----------------------------------------------------------------------
// Health
// ----------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub protocol_version: u16,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadyResponse {
    pub ready: bool,
    pub components_registered: u64,
    pub runs_recorded: u64,
    /// Backend identifiers currently wired into the orchestrator.
    pub executor_backend: String,
}

// ----------------------------------------------------------------------
// Components
// ----------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RegisterComponentRequest {
    pub schema_version: u16,
    pub manifest: ComponentManifestDto,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RegisterComponentResponse {
    /// `created` or `already_registered` (idempotent replay).
    pub status: String,
    pub component: ComponentDto,
    pub manifest_digest: String,
}

/// Wire form of a component manifest. Mirrors
/// [`ComponentManifest`](hub_core::ComponentManifest) field-for-field;
/// conversion validates through the domain constructor.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ComponentManifestDto {
    pub id: ComponentId,
    pub name: ComponentName,
    pub version: Version,
    pub kind: ComponentKind,
    #[serde(default)]
    pub capabilities: Vec<CapabilityDto>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution: Option<ExecutionBinding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<SourceInfo>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityDto {
    pub name: CapabilityName,
    pub contract_version: Version,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inputs: Vec<PortDto>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outputs: Vec<PortDto>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub properties: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortDto {
    pub name: String,
    #[serde(default)]
    pub description: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ComponentDto {
    pub id: ComponentId,
    pub name: ComponentName,
    pub version: Version,
    pub kind: ComponentKind,
    pub capabilities: Vec<CapabilityDto>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution: Option<ExecutionBinding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<SourceInfo>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
    pub manifest_digest: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ComponentListResponse {
    pub components: Vec<ComponentDto>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityListResponse {
    /// Distinct capability names across registered components.
    pub capabilities: Vec<CapabilitySummaryDto>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilitySummaryDto {
    pub name: CapabilityName,
    /// How many latest component manifests declare it.
    pub declared_by: u64,
    pub contract_version: Version,
}

// ----------------------------------------------------------------------
// Runs
// ----------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SubmitRunRequest {
    pub schema_version: u16,
    pub run_spec: RunSpecDto,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SubmitRunResponse {
    pub run: RunDto,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunSpecDto {
    pub component: ComponentId,
    pub capability: CapabilityName,
    #[serde(default)]
    pub parameters: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    pub inputs: Vec<InputBinding>,
    pub timeout_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RunDto {
    pub id: RunId,
    pub state: RunState,
    pub spec: RunSpecDto,
    pub component_name: String,
    pub component_version: Version,
    pub contract_version: Version,
    pub created_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transitions: Vec<TransitionDto>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<RunOutcomeDto>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransitionDto {
    pub from: RunState,
    pub to: RunState,
    pub at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunOutcomeDto {
    pub exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal: Option<i32>,
    pub timed_out: bool,
    pub cancelled: bool,
    pub executor_backend: String,
    pub duration_ms: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outputs: Vec<OutputRefDto>,
    pub env_keys: Vec<String>,
    pub params_digest: ContentDigest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputRefDto {
    pub name: String,
    pub artifact: ArtifactId,
    pub digest: ContentDigest,
    pub size: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RunListResponse {
    pub runs: Vec<RunDto>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelRunRequest {
    pub schema_version: u16,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelRunResponse {
    pub run_id: RunId,
    /// True when an active execution was signalled; false when a queued run
    /// was cancelled in place (or was already terminal).
    pub signalled_active_execution: bool,
}

// ----------------------------------------------------------------------
// Artifacts
// ----------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactDto {
    pub id: ArtifactId,
    pub name: String,
    pub media_type: String,
    pub digest: ContentDigest,
    pub size: u64,
    pub created_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub produced_by_run: Option<RunId>,
    /// Present when the caller asked for content (`?include=content`) and it
    /// fits the inline limit; text payloads only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_text: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactListResponse {
    pub artifacts: Vec<ArtifactDto>,
}

// ----------------------------------------------------------------------
// Conversions domain <-> wire
// ----------------------------------------------------------------------

mod convert {
    use super::*;

    impl From<hub_core::Capability> for CapabilityDto {
        fn from(c: hub_core::Capability) -> Self {
            Self {
                name: c.name,
                contract_version: c.contract_version,
                inputs: c
                    .inputs
                    .into_iter()
                    .map(|p| PortDto { name: p.name, description: p.description })
                    .collect(),
                outputs: c
                    .outputs
                    .into_iter()
                    .map(|p| PortDto { name: p.name, description: p.description })
                    .collect(),
                properties: c.properties,
            }
        }
    }

    impl From<CapabilityDto> for hub_core::Capability {
        fn from(d: CapabilityDto) -> Self {
            hub_core::Capability {
                name: d.name,
                contract_version: d.contract_version,
                inputs: d
                    .inputs
                    .into_iter()
                    .map(|p| hub_core::Port { name: p.name, description: p.description })
                    .collect(),
                outputs: d
                    .outputs
                    .into_iter()
                    .map(|p| hub_core::Port { name: p.name, description: p.description })
                    .collect(),
                properties: d.properties,
            }
        }
    }

    impl From<ComponentManifest> for ComponentManifestDto {
        fn from(m: ComponentManifest) -> Self {
            Self {
                id: m.id,
                name: m.name,
                version: m.version,
                kind: m.kind,
                capabilities: m.capabilities.into_iter().map(Into::into).collect(),
                execution: m.execution,
                source: m.source,
                metadata: m.metadata,
            }
        }
    }

    impl From<ComponentManifestDto> for ComponentManifest {
        fn from(d: ComponentManifestDto) -> Self {
            // schema_version is stamped by the domain constructor; unknown
            // fields were already tolerated by serde at this layer.
            ComponentManifest {
                schema_version: PROTOCOL_VERSION,
                id: d.id,
                name: d.name,
                version: d.version,
                kind: d.kind,
                capabilities: d.capabilities.into_iter().map(Into::into).collect(),
                execution: d.execution,
                source: d.source,
                metadata: d.metadata,
            }
        }
    }

    impl From<&ComponentManifest> for ComponentDto {
        fn from(m: &ComponentManifest) -> Self {
            Self {
                id: m.id,
                name: m.name.clone(),
                version: m.version.clone(),
                kind: m.kind.clone(),
                capabilities: m.capabilities.iter().map(|c| c.clone().into()).collect(),
                execution: m.execution.clone(),
                source: m.source.clone(),
                metadata: m.metadata.clone(),
                manifest_digest: m
                    .content_digest()
                    .map(|d| d.to_string())
                    .unwrap_or_default(),
            }
        }
    }

    impl From<&RunSpec> for RunSpecDto {
        fn from(s: &RunSpec) -> Self {
            Self {
                component: s.component,
                capability: s.capability.clone(),
                parameters: s.parameters.clone(),
                inputs: s.inputs.clone(),
                timeout_ms: s.timeout_ms,
            }
        }
    }

    impl From<RunSpecDto> for RunSpec {
        fn from(d: RunSpecDto) -> Self {
            RunSpec {
                component: d.component,
                capability: d.capability,
                parameters: d.parameters,
                inputs: d.inputs,
                timeout_ms: d.timeout_ms,
            }
        }
    }

    impl From<&RunRecord> for RunDto {
        fn from(r: &RunRecord) -> Self {
            Self {
                id: r.id,
                state: r.state,
                spec: RunSpecDto::from(&r.spec),
                component_name: r.component_name.clone(),
                component_version: r.component_version.clone(),
                contract_version: r.contract_version.clone(),
                created_at: r.created_at,
                started_at: r.started_at,
                finished_at: r.finished_at,
                transitions: r
                    .transitions
                    .iter()
                    .map(|t| TransitionDto { from: t.from, to: t.to, at: t.at })
                    .collect(),
                outcome: r.outcome.as_ref().map(|o| RunOutcomeDto {
                    exit_code: o.exit_code,
                    signal: o.signal,
                    timed_out: o.timed_out,
                    cancelled: o.cancelled,
                    executor_backend: o.executor_backend.clone(),
                    duration_ms: o.duration_ms,
                    outputs: o
                        .outputs
                        .iter()
                        .map(|out| OutputRefDto {
                            name: out.name.clone(),
                            artifact: out.artifact,
                            digest: out.digest,
                            size: out.size,
                        })
                        .collect(),
                    env_keys: o.env_keys.clone(),
                    params_digest: o.params_digest,
                    failure: o.failure.clone(),
                }),
            }
        }
    }

    impl From<&hub_core::ArtifactMeta> for ArtifactDto {
        fn from(m: &hub_core::ArtifactMeta) -> Self {
            Self {
                id: m.id,
                name: m.name.clone(),
                media_type: m.media_type.clone(),
                digest: m.digest,
                size: m.size,
                created_at: m.created_at,
                produced_by_run: m.produced_by_run,
                content_text: None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hub_core::Capability;

    fn sample_manifest() -> ComponentManifest {
        ComponentManifest::new_v1(
            ComponentId::generate(),
            ComponentName::parse("demo-echo").expect("n"),
            Version::parse("0.1.0").expect("v"),
            ComponentKind::parse("tool").expect("k"),
            vec![Capability {
                name: CapabilityName::parse("demo.echo").expect("c"),
                contract_version: Version::parse("1.0.0").expect("cv"),
                inputs: Vec::new(),
                outputs: Vec::new(),
                properties: BTreeMap::new(),
            }],
            Some(ExecutionBinding::Process(hub_core::ProcessBinding {
                program: "/bin/echo".into(),
                args: vec!["{params}".into()],
                working_dir: None,
            })),
            None,
            BTreeMap::new(),
        )
        .expect("m")
    }

    #[test]
    fn manifest_dto_round_trips_into_valid_domain() {
        let domain = sample_manifest();
        let dto = ComponentManifestDto::from(domain.clone());
        let json = serde_json::to_string(&dto).expect("encode");
        let decoded: ComponentManifestDto = serde_json::from_str(&json).expect("decode");
        let back: ComponentManifest = decoded.into();
        assert_eq!(back, domain);
    }

    #[test]
    fn unknown_fields_are_tolerated_on_read() {
        // Forward compatibility: a newer client may add fields.
        let manifest = sample_manifest();
        let mut dto = ComponentManifestDto::from(manifest);
        dto.metadata.insert("future_hint".into(), "ignored".into());
        let json = serde_json::to_string(&dto).expect("encode");
        let with_unknown = json.replace(
            "{\"id\":",
            "{\"a_future_field\": 42, \"id\":",
        );
        let decoded: ComponentManifestDto =
            serde_json::from_str(&with_unknown).expect("tolerant decode");
        let back: ComponentManifest = decoded.into();
        assert!(back.validate().is_ok());
    }

    #[test]
    fn error_envelope_shape_is_stable() {
        let envelope = ErrorEnvelope::with_details(
            ErrorCode::Conflict,
            "component already registered with different content",
            BTreeMap::from([("component_id".into(), "abc".into())]),
        );
        let json = serde_json::to_value(&envelope).expect("json");
        assert_eq!(
            json["error"]["code"],
            serde_json::Value::String("conflict".into())
        );
        let decoded: ErrorEnvelope =
            serde_json::from_value(json).expect("decode");
        assert_eq!(decoded, envelope);
    }

    #[test]
    fn schema_version_gate_rejects_other_versions() {
        assert!(check_schema_version(PROTOCOL_VERSION).is_ok());
        match check_schema_version(99) {
            Err(ProtocolError::UnsupportedSchemaVersion { found, expected }) => {
                assert_eq!(found, 99);
                assert_eq!(expected, PROTOCOL_VERSION);
            },
            Ok(()) => panic!("should reject unknown versions"),
        }
    }

    #[test]
    fn run_record_maps_to_wire_and_back_to_equivalent_spec() {
        use hub_core::{InputBinding, Limits, RunRecord};
        let spec = RunSpec {
            component: ComponentId::generate(),
            capability: CapabilityName::parse("demo.echo").expect("cap"),
            parameters: BTreeMap::from([("k".into(), serde_json::json!(7))]),
            inputs: Vec::<InputBinding>::new(),
            timeout_ms: 1_000,
        };
        let record = RunRecord::create(
            spec.clone(),
            "demo-echo".into(),
            Version::parse("1.0.0").expect("v"),
            Version::parse("1.0.0").expect("v"),
            5,
            &Limits::default(),
        )
        .expect("record");
        let dto = RunDto::from(&record);
        let json = serde_json::to_string(&dto).expect("encode");
        let decoded: RunDto = serde_json::from_str(&json).expect("decode");
        assert_eq!(decoded.state, RunState::Created);
        assert_eq!(RunSpec::from(decoded.spec), spec);
    }
}
