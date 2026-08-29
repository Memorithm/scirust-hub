from pathlib import Path

p = Path("crates/hub-core/tests/workflow_retry_cancel.rs")
text = p.read_text()
old = "use hub_core::store::WorkflowRepository as _;"
new = "use hub_core::store::{ComponentRepository as _, WorkflowRepository as _};"
if text.count(old) != 1:
    raise SystemExit("expected workflow test trait import")
p.write_text(text.replace(old, new, 1))

p = Path("crates/hub-core/src/orchestrator.rs")
text = p.read_text()
old = """            timeout_ms: 1_000,\n            after,\n        };"""
new = """            timeout_ms: 1_000,\n            after,\n            retry: None,\n        };"""
if text.count(old) != 1:
    raise SystemExit(f"expected one mk_step literal, found {text.count(old)}")
p.write_text(text.replace(old, new, 1))

print("PR1 compile fixes applied")
