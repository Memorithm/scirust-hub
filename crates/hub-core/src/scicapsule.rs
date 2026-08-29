//! SciCapsule Hub adapter contract guard.
//!
//! This module deliberately knows only SciCapsule's published Hub process
//! contract. It does not decode, verify, extract, or otherwise interpret
//! `.scicap`; those responsibilities remain in SciCapsule / SciRust.

use crate::capability::Capability;
use crate::component::{ComponentManifest, ExecutionBinding};
use crate::error::CoreError;

pub const CAPABILITY: &str = "capsule.execute";
pub const CONTRACT_VERSION: &str = "1.0.0";
pub const CONTRACT_METADATA: &str = "scicapsule-hub-v1";
pub const CAPSULE_MEDIA_TYPE: &str = "application/vnd.scirust.scicap";
pub const POLICY_MEDIA_TYPE: &str = "application/vnd.scicapsule.trust-policy.v1+json";
pub const REQUEST_MEDIA_TYPE: &str = "application/vnd.scicapsule.hub-run-request.v1+json";
pub const RESULT_MEDIA_TYPE: &str = "application/vnd.scicapsule.hub-run-result.v1+json";

fn unsupported(reason: impl Into<String>) -> CoreError {
    CoreError::Validation(format!(
        "unsupported SciCapsule Hub execution contract: {}",
        reason.into()
    ))
}

/// Validates the exact public SciCapsule Hub v1 process adapter before Hub
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
    if capability.contract_version.as_str() != CONTRACT_VERSION {
        return Err(unsupported(format!(
            "contract version {}; supported version is {CONTRACT_VERSION}",
            capability.contract_version
        )));
    }
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
    if manifest.metadata.get("contract").map(String::as_str) != Some(CONTRACT_METADATA) {
        return Err(unsupported(format!(
            "contract metadata must be {CONTRACT_METADATA}"
        )));
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
        return Err(unsupported("input ports do not match SciCapsule Hub v1"));
    }
    let outputs = capability
        .outputs
        .iter()
        .map(|port| (port.name.as_str(), port.description.as_str()))
        .collect::<Vec<_>>();
    if outputs != vec![("result", RESULT_MEDIA_TYPE)] {
        return Err(unsupported("output ports do not match SciCapsule Hub v1"));
    }

    for (key, expected) in [
        ("authorization", "local_trust_policy"),
        ("request_media_type", REQUEST_MEDIA_TYPE),
        ("result_media_type", RESULT_MEDIA_TYPE),
        ("sandbox", "none"),
    ] {
        if capability.properties.get(key).map(String::as_str) != Some(expected) {
            return Err(unsupported(format!(
                "capability property {key:?} must be {expected:?}"
            )));
        }
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
    if process.outputs.len() != 1 {
        return Err(unsupported(
            "SciCapsule Hub v1 requires exactly one result output",
        ));
    }
    let output = &process.outputs[0];
    if output.name != "result"
        || output.path != "outputs/scicapsule-result.json"
        || output.media_type.as_deref() != Some(RESULT_MEDIA_TYPE)
        || !output.required
    {
        return Err(unsupported(
            "result output does not match SciCapsule Hub v1",
        ));
    }
    Ok(())
}
