# Hub ML resource requirements profile v1

Contract identifier: `hub.ml.resource-requirements@1.0.0`

This profile is the HML0 declaration boundary for ML component capabilities. It makes resource semantics discoverable without moving tensor, training, inference, or adaptive scheduling policy into SciRust Hub.

A capability using this profile publishes the following string properties:

- `ml.resource_contract`: exactly `hub.ml.resource-requirements@1.0.0`;
- `ml.backend`: the execution/runtime owner whose semantics remain authoritative;
- `ml.device`: how the concrete device requirement is resolved (`runtime_configured`, `parameter:<name>`, or `operation_defined`);
- `ml.dtype`: how the dtype requirement is resolved (`runtime_configured`, `model_defined`, or `operation_defined`);
- `ml.accelerator`: whether accelerator use is `none`, `optional`, `required`, `runtime_configured`, or `operation_defined`;
- `ml.memory`: how host/device memory fit is established (`runtime_preflight`, `operation_defined`, or a future versioned fixed requirement form);
- `ml.placement_enforcement`: which layer currently enforces compatibility. In v1 the accepted value for the SOUP contracts is `component_preflight`; Hub resource-aware placement is HML1 and must not be inferred from this declaration alone.

The values are deliberately resolution modes rather than fabricated static capacities. A SOUP config can select different model sizes, precision, batching, devices, and streaming policies, so one component manifest cannot truthfully claim one fixed VRAM or RAM requirement. The component's own validated preflight remains authoritative until HML1 introduces worker capability descriptors and run-specific placement requirements.

This profile therefore satisfies HML0's separation of capability discovery from execution success: registration can expose how a capability resolves resources, but registration never proves that a particular worker can execute a particular run.

Changing the meaning or vocabulary of these keys requires a new contract version. HML1 may consume this profile, but must not silently reinterpret v1 values.
