use hub_core::capability::CapabilityName;
use hub_core::component::{ComponentManifest, ExecutionBinding};

const ML_RESOURCE_CONTRACT: &str = "hub.ml.resource-requirements@1.0.0";

struct Case {
    raw: &'static str,
    capability: &'static str,
    inputs: &'static [&'static str],
    outputs: &'static [&'static str],
    operation: &'static str,
    device_resolution: &'static str,
    dtype_resolution: &'static str,
    accelerator_resolution: &'static str,
    memory_resolution: &'static str,
}

#[test]
fn soup_ml_component_contracts_are_valid_and_versioned() {
    let cases = [
        Case {
            raw: include_str!("../../../examples/soup-train-component.json"),
            capability: "llm.train",
            inputs: &["config", "dataset"],
            outputs: &["model_bundle", "report"],
            operation: "train",
            device_resolution: "runtime_configured",
            dtype_resolution: "runtime_configured",
            accelerator_resolution: "runtime_configured",
            memory_resolution: "runtime_preflight",
        },
        Case {
            raw: include_str!("../../../examples/soup-eval-component.json"),
            capability: "llm.eval",
            inputs: &["model_bundle"],
            outputs: &["result"],
            operation: "eval",
            device_resolution: "parameter:device",
            dtype_resolution: "model_defined",
            accelerator_resolution: "optional",
            memory_resolution: "runtime_preflight",
        },
        Case {
            raw: include_str!("../../../examples/soup-export-component.json"),
            capability: "llm.export",
            inputs: &["model_bundle"],
            outputs: &["export_bundle", "report"],
            operation: "export",
            device_resolution: "operation_defined",
            dtype_resolution: "operation_defined",
            accelerator_resolution: "operation_defined",
            memory_resolution: "operation_defined",
        },
    ];

    for case in cases {
        let manifest: ComponentManifest =
            serde_json::from_str(case.raw).expect("SOUP ML component manifest must deserialize");
        manifest
            .validate()
            .expect("SOUP ML component manifest must satisfy Hub v1 validation");

        let capability_name = CapabilityName::parse(case.capability).expect("capability name");
        let capability = manifest
            .capability(&capability_name)
            .expect("manifest must publish requested capability");
        assert_eq!(capability.contract_version.to_string(), "1.0.0");
        assert_eq!(
            capability
                .inputs
                .iter()
                .map(|port| port.name.as_str())
                .collect::<Vec<_>>(),
            case.inputs
        );
        assert_eq!(
            capability
                .outputs
                .iter()
                .map(|port| port.name.as_str())
                .collect::<Vec<_>>(),
            case.outputs
        );
        assert_eq!(
            capability
                .properties
                .get("ml.resource_contract")
                .map(String::as_str),
            Some(ML_RESOURCE_CONTRACT)
        );
        assert_eq!(
            capability.properties.get("ml.backend").map(String::as_str),
            Some("soup")
        );
        assert_eq!(
            capability.properties.get("ml.device").map(String::as_str),
            Some(case.device_resolution)
        );
        assert_eq!(
            capability.properties.get("ml.dtype").map(String::as_str),
            Some(case.dtype_resolution)
        );
        assert_eq!(
            capability
                .properties
                .get("ml.accelerator")
                .map(String::as_str),
            Some(case.accelerator_resolution)
        );
        assert_eq!(
            capability.properties.get("ml.memory").map(String::as_str),
            Some(case.memory_resolution)
        );
        assert_eq!(
            capability
                .properties
                .get("ml.placement_enforcement")
                .map(String::as_str),
            Some("component_preflight")
        );
        assert_eq!(
            manifest
                .metadata
                .get("upstream_qualified_commit")
                .map(String::as_str),
            Some("05b646523727925990530667e7012ede50bd30b2")
        );

        let ExecutionBinding::Process(binding) =
            manifest.execution.as_ref().expect("execution binding");
        assert_eq!(binding.program, "python3");
        assert!(binding.args.iter().any(|arg| arg == case.operation));
        assert!(binding.args.iter().any(|arg| arg == "{params}"));
        for input in case.inputs {
            assert!(
                binding
                    .args
                    .iter()
                    .any(|arg| arg == &format!("{{input:{input}}}")),
                "missing input placeholder for {input}"
            );
        }
        for output in case.outputs {
            assert!(
                binding
                    .args
                    .iter()
                    .any(|arg| arg == &format!("{{output:{output}}}")),
                "missing output placeholder for {output}"
            );
            let spec = binding
                .outputs
                .iter()
                .find(|spec| spec.name == *output)
                .expect("declared capability output must have process OutputSpec");
            assert!(spec.required);
        }
    }
}
