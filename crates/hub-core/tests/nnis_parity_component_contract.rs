use hub_core::{CapabilityName, ComponentManifest, ExecutionBinding};

const NNIS_MANIFEST: &str = include_str!("../../../examples/nnis-parity-validation-component.json");

#[test]
fn nnis_parity_validation_component_preserves_nnis_semantics_and_non_claims() {
    let manifest: ComponentManifest =
        serde_json::from_str(NNIS_MANIFEST).expect("parse NNIS parity manifest");
    manifest.validate().expect("validate manifest");

    let name = CapabilityName::parse("inference.nnis.parity_validate").expect("capability name");
    let capability = manifest.capability(&name).expect("NNIS parity capability");
    assert_eq!(capability.contract_version.as_str(), "1.0.0");
    assert_eq!(capability.inputs.len(), 1);
    assert_eq!(capability.inputs[0].name, "parity_evidence");
    assert!(capability.inputs[0]
        .description
        .starts_with("application/vnd.nnis.nnml1.parity-evidence.v1+json"));
    assert_eq!(capability.outputs.len(), 1);
    assert_eq!(capability.outputs[0].name, "validation");
    assert!(capability.outputs[0]
        .description
        .starts_with("application/vnd.nnis.nnml1.parity-validation.v1+json"));

    for (key, expected) in [
        ("nnis.contract", "nnis.nnml1.parity-validation@1.0.0"),
        (
            "nnis.source_head",
            "c74b6b04c45e320c86cdd973b31f49f43c720681",
        ),
        (
            "nnis.source_merge",
            "0ae4b0d4659c8de9b8a8322ed6ab7f8e110b53f2",
        ),
        ("nnis.semantics_owner", "NNIS"),
        ("nnis.promotion_authorized", "false"),
        ("nnis.serving_performance_verified", "false"),
        ("nnis.general_model_family_support_verified", "false"),
        ("nnis.new_physical_evidence_created", "false"),
        ("hub.policy_interpretation", "forbidden"),
        ("sandbox", "none"),
    ] {
        assert_eq!(
            capability.properties.get(key).map(String::as_str),
            Some(expected),
            "property {key}"
        );
    }

    let execution = manifest.execution.as_ref().expect("execution binding");
    let ExecutionBinding::Process(process) = execution;
    assert_eq!(process.program, "/usr/bin/python3");
    assert_eq!(
        process.args,
        vec![
            "/opt/nnis/libexec/nnis_hub_nnml1_parity_validate.py",
            "--evidence",
            "{input:parity_evidence}",
            "--result",
            "{output:validation}",
        ]
    );
    assert_eq!(process.outputs.len(), 1);
    assert_eq!(
        process.outputs[0].path,
        "outputs/nnis-nnml1-parity-validation.json"
    );
    assert_eq!(
        process.outputs[0].media_type.as_deref(),
        Some("application/vnd.nnis.nnml1.parity-validation.v1+json")
    );
    assert!(process.outputs[0].required);
}
