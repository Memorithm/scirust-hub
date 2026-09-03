use hub_core::capability::CapabilityName;
use hub_core::component::{ComponentManifest, ExecutionBinding};

#[test]
fn soup_ship_component_contract_is_valid_and_versioned() {
    let raw = include_str!("../../../examples/soup-ship-component.json");
    let manifest: ComponentManifest =
        serde_json::from_str(raw).expect("SOUP component manifest must deserialize");
    manifest
        .validate()
        .expect("SOUP component manifest must satisfy Hub v1 validation");

    let capability_name = CapabilityName::parse("llm.ship").expect("capability name");
    let capability = manifest
        .capability(&capability_name)
        .expect("SOUP manifest must publish llm.ship");
    assert_eq!(capability.contract_version.to_string(), "1.0.0");
    assert_eq!(capability.inputs.len(), 1);
    assert_eq!(capability.inputs[0].name, "evidence");
    assert_eq!(capability.outputs.len(), 1);
    assert_eq!(capability.outputs[0].name, "verdict");

    let ExecutionBinding::Process(binding) =
        manifest.execution.as_ref().expect("execution binding");
    assert_eq!(binding.program, "python3");
    assert!(binding.args.iter().any(|arg| arg == "{input:evidence}"));
    assert!(binding.args.iter().any(|arg| arg == "{output:verdict}"));
    assert_eq!(binding.outputs.len(), 1);
    assert!(binding.outputs[0].required);
}
