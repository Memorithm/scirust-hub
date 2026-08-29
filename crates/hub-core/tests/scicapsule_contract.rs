use hub_core::{CapabilityName, ComponentKind, ComponentManifest, ExecutionBinding};

const SCICAPSULE_MANIFEST: &str = include_str!("../../../examples/scicapsule-component.json");

#[test]
fn scicapsule_generated_manifest_is_a_valid_hub_v1_process_contract() {
    let manifest: ComponentManifest =
        serde_json::from_str(SCICAPSULE_MANIFEST).expect("parse SciCapsule manifest fixture");
    manifest.validate().expect("validate Hub v1 manifest");

    assert_eq!(manifest.schema_version, hub_core::MANIFEST_SCHEMA_VERSION);
    assert_eq!(manifest.kind.as_str(), ComponentKind::TOOL);
    assert_eq!(manifest.name.as_str(), "SciCapsule");
    assert_eq!(manifest.version.as_str(), "0.1.0");
    assert_eq!(
        manifest
            .metadata
            .get("canonical_capsule_owner")
            .map(String::as_str),
        Some("scirust")
    );
    assert_eq!(
        manifest.metadata.get("contract").map(String::as_str),
        Some("scicapsule-hub-v1")
    );

    let capability_name = CapabilityName::parse("capsule.execute").expect("capability name");
    let capability = manifest
        .capability(&capability_name)
        .expect("capsule.execute capability");
    assert_eq!(capability.contract_version.as_str(), "1.0.0");
    assert_eq!(
        capability
            .inputs
            .iter()
            .map(|port| (port.name.as_str(), port.description.as_str()))
            .collect::<Vec<_>>(),
        vec![
            ("capsule", "application/vnd.scirust.scicap"),
            ("policy", "application/vnd.scicapsule.trust-policy.v1+json"),
            (
                "request",
                "application/vnd.scicapsule.hub-run-request.v1+json"
            ),
        ]
    );
    assert_eq!(
        capability
            .outputs
            .iter()
            .map(|port| (port.name.as_str(), port.description.as_str()))
            .collect::<Vec<_>>(),
        vec![(
            "result",
            "application/vnd.scicapsule.hub-run-result.v1+json"
        )]
    );
    assert_eq!(
        capability
            .properties
            .get("authorization")
            .map(String::as_str),
        Some("local_trust_policy")
    );
    assert_eq!(
        capability.properties.get("sandbox").map(String::as_str),
        Some("none")
    );

    let execution = manifest.execution.as_ref().expect("execution binding");
    let ExecutionBinding::Process(process) = execution;
    assert_eq!(process.program, "/opt/scicapsule/bin/scicapsule");
    assert_eq!(
        process.args,
        vec![
            "hub-run",
            "--capsule",
            "{input:capsule}",
            "--policy",
            "{input:policy}",
            "--request",
            "{input:request}",
            "--result",
            "{output:result}",
        ]
    );
    assert_eq!(process.outputs.len(), 1);
    assert_eq!(process.outputs[0].name, "result");
    assert_eq!(process.outputs[0].path, "outputs/scicapsule-result.json");
    assert_eq!(
        process.outputs[0].media_type.as_deref(),
        Some("application/vnd.scicapsule.hub-run-result.v1+json")
    );
    assert!(process.outputs[0].required);

    let canonical = manifest.canonical_bytes().expect("canonical manifest JSON");
    let reparsed: ComponentManifest =
        serde_json::from_slice(&canonical).expect("reparse canonical manifest JSON");
    assert_eq!(reparsed, manifest);
}

#[test]
fn scicapsule_execution_contract_rejects_future_version_until_supported() {
    let mut manifest: ComponentManifest = serde_json::from_str(SCICAPSULE_MANIFEST).unwrap();
    let name = CapabilityName::parse("capsule.execute").unwrap();
    let capability = manifest
        .capabilities
        .iter_mut()
        .find(|capability| capability.name == name)
        .unwrap();
    capability.contract_version = hub_core::Version::parse("2.0.0").unwrap();
    let capability = manifest.capability(&name).unwrap();
    let error = hub_core::scicapsule::validate_execution_contract(&manifest, capability)
        .expect_err("future contract must fail closed");
    assert!(error.to_string().contains("supported version is 1.0.0"));
}

#[test]
fn scicapsule_execution_contract_rejects_drifted_process_binding() {
    let mut manifest: ComponentManifest = serde_json::from_str(SCICAPSULE_MANIFEST).unwrap();
    let Some(ExecutionBinding::Process(process)) = manifest.execution.as_mut() else {
        panic!("process binding expected");
    };
    process.args.push("--unexpected".into());
    let name = CapabilityName::parse("capsule.execute").unwrap();
    let capability = manifest.capability(&name).unwrap();
    assert!(hub_core::scicapsule::validate_execution_contract(&manifest, capability).is_err());
}
