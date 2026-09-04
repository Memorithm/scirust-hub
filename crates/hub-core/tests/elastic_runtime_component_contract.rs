use hub_core::capability::CapabilityName;
use hub_core::component::{ComponentManifest, ExecutionBinding};

const ELASTIC_HEAD: &str = "571d0deb8921df54502fbb35909dd8830cbf4fb4";
const ELASTIC_MERGE: &str = "9e51879b96e54c812b6a265fe5901e960bbe6250";

#[test]
fn elastic_runtime_contract_is_explicit_and_versioned() {
    let manifest: ComponentManifest = serde_json::from_str(include_str!(
        "../../../examples/elastic-runtime-component.json"
    ))
    .expect("Elastic runtime component manifest must deserialize");
    manifest
        .validate()
        .expect("Elastic runtime component manifest must satisfy Hub v1 validation");

    let name = CapabilityName::parse("resource.elastic.run").expect("capability name");
    let capability = manifest
        .capability(&name)
        .expect("manifest must publish resource.elastic.run");
    assert_eq!(capability.contract_version.to_string(), "1.0.0");
    assert_eq!(
        capability
            .inputs
            .iter()
            .map(|port| port.name.as_str())
            .collect::<Vec<_>>(),
        ["config"]
    );
    assert_eq!(
        capability
            .outputs
            .iter()
            .map(|port| port.name.as_str())
            .collect::<Vec<_>>(),
        ["evidence"]
    );
    assert_eq!(
        capability
            .properties
            .get("elastic.source_merge")
            .map(String::as_str),
        Some(ELASTIC_MERGE)
    );
    assert_eq!(
        capability
            .properties
            .get("elastic.source_final_exact_head")
            .map(String::as_str),
        Some(ELASTIC_HEAD)
    );
    assert_eq!(
        capability
            .properties
            .get("elastic.evidence_schema")
            .map(String::as_str),
        Some("elastic-runtime-evidence-v1")
    );
    assert_eq!(
        capability
            .properties
            .get("hub.policy_interpretation")
            .map(String::as_str),
        Some("forbidden")
    );

    let ExecutionBinding::Process(binding) =
        manifest.execution.as_ref().expect("execution binding");
    assert_eq!(binding.program, "/opt/scirust-hub/libexec/elastic");
    assert_eq!(
        binding.args,
        [
            "hub-run",
            "--config",
            "{input:config}",
            "--evidence-output",
            "{output:evidence}",
        ]
    );
    assert_eq!(binding.outputs.len(), 1);
    assert_eq!(binding.outputs[0].name, "evidence");
    assert_eq!(
        binding.outputs[0].media_type.as_deref(),
        Some("application/vnd.elastic.runtime-evidence.v1+json")
    );
    assert!(binding.outputs[0].required);
}

#[test]
fn elastic_runtime_edge_does_not_absorb_soup_preexecution_contract() {
    let runtime_manifest: ComponentManifest = serde_json::from_str(include_str!(
        "../../../examples/elastic-runtime-component.json"
    ))
    .expect("Elastic runtime manifest");
    let soup_manifest: ComponentManifest = serde_json::from_str(include_str!(
        "../../../examples/soup-train-elastic-component.json"
    ))
    .expect("SOUP Elastic manifest");

    runtime_manifest.validate().expect("runtime manifest valid");
    soup_manifest.validate().expect("SOUP manifest valid");

    let runtime = CapabilityName::parse("resource.elastic.run").expect("runtime capability");
    let soup = CapabilityName::parse("llm.train.elastic").expect("SOUP capability");
    assert!(runtime_manifest.capability(&runtime).is_some());
    assert!(runtime_manifest.capability(&soup).is_none());
    assert!(soup_manifest.capability(&soup).is_some());
    assert!(soup_manifest.capability(&runtime).is_none());
}
