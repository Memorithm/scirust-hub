use hub_core::capability::CapabilityName;
use hub_core::component::{ComponentManifest, ExecutionBinding};

struct Case {
    raw: &'static str,
    capability: &'static str,
    inputs: &'static [&'static str],
    outputs: &'static [&'static str],
    operation: &'static str,
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
        },
        Case {
            raw: include_str!("../../../examples/soup-eval-component.json"),
            capability: "llm.eval",
            inputs: &["model_bundle"],
            outputs: &["result"],
            operation: "eval",
        },
        Case {
            raw: include_str!("../../../examples/soup-export-component.json"),
            capability: "llm.export",
            inputs: &["model_bundle"],
            outputs: &["export_bundle", "report"],
            operation: "export",
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
            manifest.metadata.get("upstream_qualified_commit").map(String::as_str),
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
