use hub_core::{CapabilityName, ComponentManifest, ExecutionBinding};

const VERIFY_MANIFEST: &str = include_str!("../../../examples/scicapsule-verify-component.json");

#[test]
fn scicapsule_verify_component_preserves_source_and_verdict_ownership() {
    let manifest: ComponentManifest =
        serde_json::from_str(VERIFY_MANIFEST).expect("parse SciCapsule Verify manifest");
    manifest.validate().expect("validate manifest");

    let name = CapabilityName::parse("capsule.verify.scicapsule").expect("capability name");
    let capability = manifest.capability(&name).expect("Verify capability");
    assert_eq!(capability.contract_version.as_str(), "1.0.0");
    assert_eq!(
        capability.inputs[0].description,
        "application/vnd.scicapsule.hub-run-result.v2+json; immutable SciCapsule capsule.execute@2.0.0 execution evidence"
    );
    assert_eq!(
        capability.outputs[0].description,
        "application/vnd.scirust-verify.dossier.v1+tar; integrity-sealed SciRust-Verify evidence dossier"
    );
    assert_eq!(
        capability
            .properties
            .get("verify.process_merge")
            .map(String::as_str),
        Some("0d319ef157922635932a7b1591b6c364f46b9106")
    );
    assert_eq!(
        capability
            .properties
            .get("verify.process_final_exact_head")
            .map(String::as_str),
        Some("bfca2aa4eca00d9a41a369284d95a07a38841f48")
    );
    assert_eq!(
        capability
            .properties
            .get("scicapsule.source_merge")
            .map(String::as_str),
        Some("31e4a825c8a45837ce4f8ff69f936b46e53d3b82")
    );
    assert_eq!(
        capability
            .properties
            .get("scicapsule.trust_decision_owner")
            .map(String::as_str),
        Some("SciCapsule")
    );
    assert_eq!(
        capability
            .properties
            .get("scicapsule.trust_is_scientific_verdict")
            .map(String::as_str),
        Some("false")
    );
    assert_eq!(
        capability
            .properties
            .get("hub.policy_interpretation")
            .map(String::as_str),
        Some("forbidden")
    );

    let execution = manifest.execution.as_ref().expect("execution binding");
    let ExecutionBinding::Process(process) = execution;
    assert_eq!(
        process.program,
        "/opt/scirust-hub/libexec/scirust-verify-scicapsule"
    );
    assert_eq!(
        process.args,
        vec![
            "--evidence",
            "{input:evidence}",
            "--output",
            "{output:dossier}",
        ]
    );
    assert_eq!(process.outputs.len(), 1);
    assert_eq!(
        process.outputs[0].path,
        "outputs/scicapsule-verification-dossier.tar"
    );
    assert_eq!(
        process.outputs[0].media_type.as_deref(),
        Some("application/vnd.scirust-verify.dossier.v1+tar")
    );
    assert!(process.outputs[0].required);
}
