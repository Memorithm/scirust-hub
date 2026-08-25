//! Component manifests: the registered description of an ecosystem member.
//!
//! Registration is metadata-only. Declaring an execution binding never
//! triggers anything; bindings are consulted solely when a validated run is
//! actually executed.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::capability::Capability;
use crate::digest::{hash_bytes, ContentDigest, DOMAIN_COMPONENT_MANIFEST};
use crate::error::CoreError;
use crate::id::ComponentId;
use crate::version::Version;

/// Current manifest schema version understood by this Hub build.
pub const MANIFEST_SCHEMA_VERSION: u16 = 1;

/// Open component category. Well-known values exist as constants, but any
/// non-empty lowercase string is valid; closed enums are rejected by design.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ComponentKind(String);

impl ComponentKind {
    pub const MAX_LEN: usize = 64;

    pub const CRATE: &str = "crate";
    pub const SERVICE: &str = "service";
    pub const RUNTIME: &str = "runtime";
    pub const MODEL: &str = "model";
    pub const DATASET: &str = "dataset";
    pub const CAPSULE: &str = "capsule";
    pub const TOOL: &str = "tool";

    /// # Errors
    /// [`CoreError::InvalidManifest`] for empty or malformed kinds.
    pub fn parse(raw: &str) -> Result<Self, CoreError> {
        let valid = !raw.is_empty()
            && raw.len() <= Self::MAX_LEN
            && raw.starts_with(|c: char| c.is_ascii_lowercase())
            && raw
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-');
        if valid {
            Ok(Self(raw.to_owned()))
        } else {
            Err(CoreError::InvalidManifest(format!(
                "component kind {raw:?} must be 1..={} chars matching [a-z][a-z0-_-]*",
                Self::MAX_LEN
            )))
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Component display name. Constrained but human-readable.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ComponentName(String);

impl ComponentName {
    pub const MAX_LEN: usize = 128;

    /// # Errors
    /// [`CoreError::InvalidManifest`] outside 1..=MAX_LEN printable chars.
    pub fn parse(raw: &str) -> Result<Self, CoreError> {
        if raw.is_empty() || raw.len() > Self::MAX_LEN || !raw.chars().all(|c| !c.is_control()) {
            return Err(CoreError::InvalidManifest(format!(
                "component name must be 1..={} characters without control chars",
                Self::MAX_LEN
            )));
        }
        Ok(Self(raw.to_owned()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Source code identity, recorded only when actually known (e.g. provided by
/// the registrant from a real VCS checkout). Never inferred.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceInfo {
    /// Commit hash as provided; format is not verified beyond basic shape.
    pub commit: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repository: Option<String>,
    /// True only when the source tree had uncommitted changes at build time.
    #[serde(default)]
    pub dirty: bool,
}

/// A validated fixed argv binding.
///
/// Placeholders allowed in `args` (never in `program`):
/// - `{params}` — compact JSON of the run parameters;
/// - `{input:<name>}` — absolute path of the materialized input artifact.
///
/// The argv is passed directly to the OS (`Command`), never through a shell.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessBinding {
    pub program: String,
    pub args: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub working_dir: Option<String>,
}

/// How the Hub may execute this component. Extensible enum: new variants are
/// additive and unknown ones are rejected explicitly rather than silently
/// dropped.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExecutionBinding {
    Process(ProcessBinding),
}

impl ExecutionBinding {
    /// Validates structural constraints shared by all variants.
    ///
    /// # Errors
    /// [`CoreError::InvalidManifest`] with a precise reason.
    pub fn validate(&self) -> Result<(), CoreError> {
        match self {
            ExecutionBinding::Process(p) => validate_process_binding(p),
        }
    }
}

fn validate_process_binding(p: &ProcessBinding) -> Result<(), CoreError> {
    let program = p.program.as_str();
    if program.is_empty() || program.len() > 4096 {
        return Err(CoreError::InvalidManifest(
            "binding program must be 1..=4096 characters".into(),
        ));
    }
    if program.contains('\0') {
        return Err(CoreError::InvalidManifest(
            "binding program must not contain NUL bytes".into(),
        ));
    }
    if p.args.len() > crate::limits::Limits::default().max_args {
        return Err(CoreError::InvalidManifest(format!(
            "binding declares {} args, more than the allowed {}",
            p.args.len(),
            crate::limits::Limits::default().max_args
        )));
    }
    for arg in &p.args {
        if arg.contains('\0') {
            return Err(CoreError::InvalidManifest(
                "binding args must not contain NUL bytes".into(),
            ));
        }
        if arg.len() > 4096 {
            return Err(CoreError::InvalidManifest(
                "each binding arg must be at most 4096 characters".into(),
            ));
        }
    }
    Ok(())
}

/// A complete, validated component registration payload.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComponentManifest {
    pub schema_version: u16,
    pub id: ComponentId,
    pub name: ComponentName,
    pub version: Version,
    pub kind: ComponentKind,
    #[serde(default)]
    pub capabilities: Vec<Capability>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution: Option<ExecutionBinding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<SourceInfo>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
}

impl ComponentManifest {
    /// Builds a v1 manifest and validates every invariant.
    ///
    /// # Errors
    /// [`CoreError::InvalidManifest`] describing the first violated rule.
    #[allow(clippy::too_many_arguments)] // manifest fields are independent by design
    pub fn new_v1(
        id: ComponentId,
        name: ComponentName,
        version: Version,
        kind: ComponentKind,
        capabilities: Vec<Capability>,
        execution: Option<ExecutionBinding>,
        source: Option<SourceInfo>,
        metadata: BTreeMap<String, String>,
    ) -> Result<Self, CoreError> {
        let manifest = Self {
            schema_version: MANIFEST_SCHEMA_VERSION,
            id,
            name,
            version,
            kind,
            capabilities,
            execution,
            source,
            metadata,
        };
        manifest.validate()?;
        Ok(manifest)
    }

    /// Validates schema version, uniqueness of capability names, per-
    /// capability invariants and the execution binding.
    ///
    /// # Errors
    /// [`CoreError::InvalidManifest`] with the first violated rule.
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.schema_version != MANIFEST_SCHEMA_VERSION {
            return Err(CoreError::InvalidManifest(format!(
                "unsupported manifest schema_version {}: this build understands {}",
                self.schema_version, MANIFEST_SCHEMA_VERSION
            )));
        }
        let mut names = std::collections::BTreeSet::new();
        for cap in &self.capabilities {
            cap.validate()?;
            if !names.insert(cap.name.clone()) {
                return Err(CoreError::InvalidManifest(format!(
                    "duplicate capability {}",
                    cap.name
                )));
            }
        }
        if let Some(binding) = &self.execution {
            binding.validate()?;
        }
        if let Some(source) = &self.source {
            if source.commit.is_empty() || source.commit.len() > 128 {
                return Err(CoreError::InvalidManifest(
                    "source commit must be 1..=128 characters".into(),
                ));
            }
        }
        Ok(())
    }

    /// Canonical JSON serialization used for the content digest. Field order
    /// is fixed by struct declaration via `serde_json::to_vec`, which is
    /// deterministic for structs (maps use BTreeMap).
    ///
    /// # Errors
    /// Serialization of this type cannot fail in practice; errors propagate.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, CoreError> {
        serde_json::to_vec(self).map_err(|e| CoreError::Storage(e.to_string()))
    }

    /// Domain-separated digest over [`Self::canonical_bytes`].
    ///
    /// # Errors
    /// See [`Self::canonical_bytes`].
    pub fn content_digest(&self) -> Result<ContentDigest, CoreError> {
        Ok(hash_bytes(
            DOMAIN_COMPONENT_MANIFEST,
            &self.canonical_bytes()?,
        ))
    }

    /// Looks up a declared capability by exact name.
    #[must_use]
    pub fn capability(&self, name: &crate::capability::CapabilityName) -> Option<&Capability> {
        self.capabilities.iter().find(|c| &c.name == name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capability::{Capability, CapabilityName, Port};
    use std::collections::BTreeMap;

    fn sample_manifest() -> ComponentManifest {
        let echo_cap = Capability {
            name: CapabilityName::parse("demo.echo").expect("valid"),
            contract_version: Version::parse("1.0.0").expect("valid"),
            inputs: vec![],
            outputs: vec![Port {
                name: "stdout".into(),
                description: String::new(),
            }],
            properties: BTreeMap::new(),
        };
        ComponentManifest::new_v1(
            ComponentId::generate(),
            ComponentName::parse("demo-echo").expect("valid"),
            Version::parse("0.1.0").expect("valid"),
            ComponentKind::parse(ComponentKind::TOOL).expect("valid"),
            vec![echo_cap],
            Some(ExecutionBinding::Process(ProcessBinding {
                program: "/bin/echo".into(),
                args: vec!["{params}".into()],
                working_dir: None,
            })),
            None,
            BTreeMap::new(),
        )
        .expect("valid manifest")
    }

    #[test]
    fn manifest_round_trip_is_stable() {
        let m = sample_manifest();
        let bytes = m.canonical_bytes().expect("serialize");
        let decoded: ComponentManifest = serde_json::from_slice(&bytes).expect("deserialize");
        assert_eq!(decoded, m);
        assert_eq!(
            decoded.content_digest().expect("digest"),
            m.content_digest().expect("digest")
        );
    }

    #[test]
    fn duplicate_capabilities_rejected() {
        let mut m = sample_manifest();
        let dup = Capability {
            name: CapabilityName::parse("demo.echo").expect("valid"),
            contract_version: Version::parse("1.0.0").expect("valid"),
            inputs: vec![],
            outputs: vec![],
            properties: BTreeMap::new(),
        };
        m.capabilities.push(dup);
        assert!(m.validate().is_err());
    }

    #[test]
    fn unsupported_schema_version_rejected() {
        let mut m = sample_manifest();
        m.schema_version = 99;
        assert!(matches!(
            m.validate(),
            Err(CoreError::InvalidManifest(msg)) if msg.contains("schema_version")
        ));
    }

    #[test]
    fn nul_byte_in_program_rejected() {
        let bad = ExecutionBinding::Process(ProcessBinding {
            program: "/bin/echo\0".into(),
            args: vec![],
            working_dir: None,
        });
        assert!(bad.validate().is_err());
    }

    #[test]
    fn placeholder_in_program_position_not_special_but_args_validated() {
        // Program placeholders are simply literal text; they will fail at
        // start time, not bypass validation.
        let ok = ExecutionBinding::Process(ProcessBinding {
            program: "/bin/echo".into(),
            args: vec!["{input:missing}".into()],
            working_dir: None,
        });
        assert!(ok.validate().is_ok());
    }

    #[test]
    fn open_kind_accepts_unknown_values() {
        assert!(ComponentKind::parse("quantum_backend").is_ok());
        assert!(ComponentKind::parse("").is_err());
        assert!(ComponentKind::parse("Bad Kind").is_err());
    }
}
