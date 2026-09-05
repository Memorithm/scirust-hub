//! SciCapsule Hub adapter contract guard.
//!
//! This module deliberately knows only SciCapsule's published Hub process
//! contracts. It does not decode, verify, extract, authorize or otherwise
//! interpret `.scicap`; those responsibilities remain in SciCapsule / SciRust.

use crate::capability::Capability;
use crate::component::{ComponentManifest, ExecutionBinding};
use crate::error::CoreError;

pub const CAPABILITY: &str = "capsule.execute";
pub const CONTRACT_VERSION_V1: &str = "1.0.0";
pub const CONTRACT_VERSION_V2: &str = "2.0.0";
pub const CONTRACT_METADATA_V1: &str = "scicapsule-hub-v1";
pub const CONTRACT_METADATA_V2: &str = "scicapsule-hub-v2";
pub const CAPSULE_MEDIA_TYPE: &str = "application/vnd.scirust.scicap";
pub const POLICY_MEDIA_TYPE: &str = "application/vnd.scicapsule.trust-policy.v1+json";
pub const REQUEST_MEDIA_TYPE: &str = "application/vnd.scicapsule.hub-run-request.v1+json";
pub const RESULT_MEDIA_TYPE_V1: &str = "application/vnd.scicapsule.hub-run-result.v1+json";
pub const RESULT_MEDIA_TYPE_V2: &str = "application/vnd.scicapsule.hub-run-result.v2+json";
pub const V2_SOURCE_HEAD: &str = "bb79eea787f0d9562585b27dd38f5f57fa5b5ea9";
pub const V2_SOURCE_MERGE: &str = "31e4a825c8a45837ce4f8ff69f936b46e53d3b82";

fn unsupported(reason: impl Into<String>) -> CoreError {
    CoreError::Validation(format!(
        "unsupported SciCapsule Hub execution contract: {}",
        reason.into()
    ))
}

/// Validates one known public SciCapsule Hub process adapter before Hub
/// executes a `capsule.execute` run.
///
/// Registration remains open: future contract versions may be indexed and
/// discovered. Execution fails closed until Hub explicitly supports them.
pub fn validate_execution_contract(
    manifest: &ComponentManifest,
    capability: &Capability,
) -> Result<(), CoreError> {
    if capability.name.as_str() != CAPABILITY {
        return Err(unsupported("unexpected capability name"));
    }
    match capability.contract_version.as_str() {
        CONTRACT_VERSION_V1 => validate_v1(manifest, capability),
        CONTRACT_VERSION_V2 => validate_v2(manifest, capability),
        version => Err(unsupported(format!(
            "contract version {version}; supported versions are {CONTRACT_VERSION_V1} and {CONTRACT_VERSION_V2}"
        ))),
    }
}

fn validate_common(
    manifest: &ComponentManifest,
    capability: &Capability,
    expected_output_media_type: &str,
) -> Result<(), CoreError> {
    if manifest
        .metadata
        .get("canonical_capsule_owner")
        .map(String::as_str)
        != Some("scirust")
    {
        return Err(unsupported(
            "canonical_capsule_owner metadata must be scirust",
        ));
    }

    let inputs = capability
        .inputs
        .iter()
        .map(|port| (port.name.as_str(), port.description.as_str()))
        .collect::<Vec<_>>();
    let expected_inputs = vec![
        ("capsule", CAPSULE_MEDIA_TYPE),
        ("policy", POLICY_MEDIA_TYPE),
        ("request", REQUEST_MEDIA_TYPE),
    ];
    if inputs != expected_inputs {
        return Err(unsupported(
            "input ports do not match SciCapsule Hub contract",
        ));
    }
    let outputs = capability
        .outputs
        .iter()
        .map(|port| (port.name.as_str(), port.description.as_str()))
        .collect::<Vec<_>>();
    if outputs != vec![("result", expected_output_media_type)] {
        return Err(unsupported(
            "result output media type does not match contract",
        ));
    }

    for (key, expected) in [
        ("authorization", "local_trust_policy"),
        ("request_media_type", REQUEST_MEDIA_TYPE),
        ("result_media_type", expected_output_media_type),
        ("sandbox", "none"),
    ] {
        if capability.properties.get(key).map(String::as_str) != Some(expected) {
            return Err(unsupported(format!(
                "capability property {key:?} must be {expected:?}"
            )));
        }
    }
    Ok(())
}

fn validate_v1(manifest: &ComponentManifest, capability: &Capability) -> Result<(), CoreError> {
    validate_common(manifest, capability, RESULT_MEDIA_TYPE_V1)?;
    if manifest.metadata.get("contract").map(String::as_str) != Some(CONTRACT_METADATA_V1) {
        return Err(unsupported(format!(
            "contract metadata must be {CONTRACT_METADATA_V1}"
        )));
    }

    let execution = manifest
        .execution
        .as_ref()
        .ok_or_else(|| unsupported("missing process execution binding"))?;
    let ExecutionBinding::Process(process) = execution;
    if !std::path::Path::new(&process.program).is_absolute() {
        return Err(unsupported("SciCapsule executable path must be absolute"));
    }
    if process.working_dir.is_some() {
        return Err(unsupported(
            "SciCapsule Hub v1 does not declare a working_dir override",
        ));
    }
    let expected_args = [
        "hub-run",
        "--capsule",
        "{input:capsule}",
        "--policy",
        "{input:policy}",
        "--request",
        "{input:request}",
        "--result",
        "{output:result}",
    ];
    if process.args.iter().map(String::as_str).collect::<Vec<_>>() != expected_args {
        return Err(unsupported("process argv does not match SciCapsule Hub v1"));
    }
    validate_single_output(
        process,
        "outputs/scicapsule-result.json",
        RESULT_MEDIA_TYPE_V1,
    )
}

fn validate_v2(manifest: &ComponentManifest, capability: &Capability) -> Result<(), CoreError> {
    validate_common(manifest, capability, RESULT_MEDIA_TYPE_V2)?;
    if manifest.metadata.get("contract").map(String::as_str) != Some(CONTRACT_METADATA_V2) {
        return Err(unsupported(format!(
            "contract metadata must be {CONTRACT_METADATA_V2}"
        )));
    }
    for (key, expected) in [
        ("execution_mode", "bounded_process_unix"),
        ("trust_decision_owner", "SciCapsule"),
        ("trust_is_scientific_verdict", "false"),
        ("scicapsule.source_head", V2_SOURCE_HEAD),
        ("scicapsule.source_merge", V2_SOURCE_MERGE),
    ] {
        if capability.properties.get(key).map(String::as_str) != Some(expected) {
            return Err(unsupported(format!(
                "v2 capability property {key:?} must be {expected:?}"
            )));
        }
    }

    let execution = manifest
        .execution
        .as_ref()
        .ok_or_else(|| unsupported("missing process execution binding"))?;
    let ExecutionBinding::Process(process) = execution;
    if !std::path::Path::new(&process.program).is_absolute() {
        return Err(unsupported("SciCapsule v2 launcher path must be absolute"));
    }
    if process.working_dir.is_some() {
        return Err(unsupported(
            "SciCapsule Hub v2 does not declare a working_dir override",
        ));
    }
    let expected_args = [
        "--capsule",
        "{input:capsule}",
        "--policy",
        "{input:policy}",
        "--request",
        "{input:request}",
        "--result",
        "{output:result}",
        "--scicapsule-program",
        "/opt/scicapsule/bin/scicapsule",
    ];
    if process.args.iter().map(String::as_str).collect::<Vec<_>>() != expected_args {
        return Err(unsupported("process argv does not match SciCapsule Hub v2"));
    }
    validate_single_output(
        process,
        "outputs/scicapsule-result-v2.json",
        RESULT_MEDIA_TYPE_V2,
    )
}

fn validate_single_output(
    process: &crate::component::ProcessBinding,
    expected_path: &str,
    expected_media_type: &str,
) -> Result<(), CoreError> {
    if process.outputs.len() != 1 {
        return Err(unsupported(
            "SciCapsule execution requires exactly one result output",
        ));
    }
    let output = &process.outputs[0];
    if output.name != "result"
        || output.path != expected_path
        || output.media_type.as_deref() != Some(expected_media_type)
        || !output.required
    {
        return Err(unsupported(
            "result output does not match SciCapsule execution contract",
        ));
    }
    Ok(())
}
