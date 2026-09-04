use hub_core::capability::CapabilityName;
use hub_core::component::{ComponentManifest, ExecutionBinding};

const SCIRUST_SOURCE_HEAD: &str = "58d850899cee1d62449cc02816b787b7f8a8a3de";
const SCIRUST_SOURCE_MERGE: &str = "f6bdadb6234129e14e9ea4d69f46901c6dcecbd0";
const SOUP_QUALIFIED_COMMIT: &str = "05b646523727925990530667e7012ede50bd30b2";

#[test]
fn soup_scirust_symbolic_training_contract_is_explicit_and_versioned() {
    let manifest: ComponentManifest = serde_json::from_str(include_str!(
        "../../../examples/soup-train-scirust-symbolic-component.json"
    ))
    .expect("SOUP SciRust symbolic manifest must deserialize");
    manifest
        .validate()
        .expect("SOUP SciRust symbolic manifest must satisfy Hub v1 validation");

    let capability_name =
        CapabilityName::parse("llm.train.scirust-symbolic").expect("capability name");
    let capability = manifest
        .capability(&capability_name)
        .expect("manifest must publish llm.train.scirust-symbolic");
    assert_eq!(capability.contract_version.to_string(), "1.0.0");
    assert_eq!(
        capability
            .inputs
            .iter()
            .map(|port| port.name.as_str())
            .collect::<Vec<_>>(),
        ["config", "dataset"]
    );
    assert_eq!(
        capability
            .outputs
            .iter()
            .map(|port| port.name.as_str())
            .collect::<Vec<_>>(),
        ["model_bundle", "report"]
    );
    assert_eq!(
        capability
            .properties
            .get("scirust.source_head")
            .map(String::as_str),
        Some(SCIRUST_SOURCE_HEAD)
    );
    assert_eq!(
        capability
            .properties
            .get("scirust.source_merge")
            .map(String::as_str),
        Some(SCIRUST_SOURCE_MERGE)
    );
    assert_eq!(
        capability
            .properties
            .get("scirust.reward_schema_version")
            .map(String::as_str),
        Some("1")
    );
    assert_eq!(
        capability
            .properties
            .get("scirust.reward_kind")
            .map(String::as_str),
        Some("symbolic_equivalence")
    );
    assert_eq!(
        capability.properties.get("soup.task").map(String::as_str),
        Some("grpo")
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
        Some(SOUP_QUALIFIED_COMMIT)
    );
    assert_eq!(
        manifest
            .metadata
            .get("scirust_source_merge")
            .map(String::as_str),
        Some(SCIRUST_SOURCE_MERGE)
    );

    let ExecutionBinding::Process(binding) =
        manifest.execution.as_ref().expect("execution binding");
    assert_eq!(binding.program, "python3");
    assert!(binding
        .args
        .iter()
        .any(|arg| arg == "/opt/scirust-hub/libexec/soup_scirust_symbolic_hub_adapter.py"));
    for expected in [
        "{input:config}",
        "{input:dataset}",
        "{output:model_bundle}",
        "{output:report}",
    ] {
        assert!(
            binding.args.iter().any(|arg| arg == expected),
            "missing execution placeholder {expected}"
        );
    }
}

#[test]
fn existing_soup_training_contracts_remain_separate() {
    let legacy: ComponentManifest =
        serde_json::from_str(include_str!("../../../examples/soup-train-component.json"))
            .expect("legacy SOUP train manifest");
    let elastic: ComponentManifest = serde_json::from_str(include_str!(
        "../../../examples/soup-train-elastic-component.json"
    ))
    .expect("SOUP Elastic train manifest");

    for (manifest, name) in [(legacy, "llm.train"), (elastic, "llm.train.elastic")] {
        manifest
            .validate()
            .expect("existing manifest remains valid");
        let capability_name = CapabilityName::parse(name).expect("capability name");
        let capability = manifest
            .capability(&capability_name)
            .expect("existing capability remains published");
        assert!(!capability.properties.contains_key("scirust.reward_kind"));
    }
}
