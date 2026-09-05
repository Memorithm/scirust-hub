use hub_core::{CapabilityName, ComponentManifest, ExecutionBinding};

const VERIFY_MANIFEST: &str = include_str!("../../../examples/nnis-parity-verify-component.json");

#[test]
fn nnis_parity_verify_component_preserves_binding_scope_and_ownership() {
    let manifest: ComponentManifest =
        serde_json::from_str(VERIFY_MANIFEST).expect("parse NNIS Verify manifest");
    manifest.validate().expect("validate manifest");

    let name = CapabilityName::parse("inference.nnis.parity_verify").expect("capability name");
    let capability = manifest.capability(&name).expect("NNIS Verify capability");
    assert_eq!(capability.contract_version.as_str(), "1.0.0");
    assert_eq!(capability.inputs.len(), 2);
    assert_eq!(capability.inputs[0].name, "parity_evidence");
    assert_eq!(capability.inputs[1].name, "validation");
    assert!(capability.inputs[0]
        .description
        .starts_with("application/vnd.nnis.nnml1.parity-evidence.v1+json"));
    assert!(capability.inputs[1]
        .description
        .starts_with("application/vnd.nnis.nnml1.parity-validation.v1+json"));
    assert_eq!(capability.outputs.len(), 1);
    assert_eq!(capability.outputs[0].name, "dossier");
    assert!(capability.outputs[0]
        .description
        .starts_with("application/vnd.scirust-verify.dossier.v1+tar"));

    for (key, expected) in [
        (
            "verify.contract",
            "scirust-verify.nnis-parity-dossier@1.0.0",
        ),
        (
            "verify.process_merge",
            "593692aea76b90e50899e733344ffa4cf61ba380",
        ),
        (
            "verify.process_final_exact_head",
            "a120c54ed05b058d9347691d0036eaaaa831df41",
        ),
        (
            "verify.contract_verdict_scope",
            "exact_byte_binding_and_validation_envelope_only",
        ),
        ("verify.model_quality_claim", "not_established"),
        ("verify.serving_performance_claim", "not_established"),
        (
            "verify.general_model_family_support_claim",
            "not_established",
        ),
        ("verify.promotion_authorization_claim", "not_established"),
        ("nnis.contract", "nnis.nnml1.parity-validation@1.0.0"),
        (
            "nnis.source_merge",
            "0ae4b0d4659c8de9b8a8322ed6ab7f8e110b53f2",
        ),
        ("nnis.semantics_owner", "NNIS"),
        ("nnis.promotion_authorized", "false"),
        ("nnis.serving_performance_verified", "false"),
        ("nnis.general_model_family_support_verified", "false"),
        ("hub.policy_interpretation", "forbidden"),
        (
            "dossier.media_type",
            "application/vnd.scirust-verify.dossier.v1+tar",
        ),
    ] {
        assert_eq!(
            capability.properties.get(key).map(String::as_str),
            Some(expected),
            "property {key}"
        );
    }

    let execution = manifest.execution.as_ref().expect("execution binding");
    let ExecutionBinding::Process(process) = execution;
    assert_eq!(
        process.program,
        "/opt/scirust-hub/libexec/scirust-verify-nnis-parity"
    );
    assert_eq!(
        process.args,
        vec![
            "--parity-evidence",
            "{input:parity_evidence}",
            "--validation",
            "{input:validation}",
            "--output",
            "{output:dossier}",
        ]
    );
    assert_eq!(process.outputs.len(), 1);
    assert_eq!(
        process.outputs[0].path,
        "outputs/nnis-parity-verification-dossier.tar"
    );
    assert_eq!(
        process.outputs[0].media_type.as_deref(),
        Some("application/vnd.scirust-verify.dossier.v1+tar")
    );
    assert!(process.outputs[0].required);
}
