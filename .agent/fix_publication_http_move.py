from pathlib import Path

p = Path("crates/hub-api/src/lib.rs")
s = p.read_text()
old = '''    let orch = state.orchestrator.clone();
    match joined(
        tokio::task::spawn_blocking(move || {
            orch.authoritative_step_publication(parsed, &step_key)
        })
        .await,
    ) {'''
new = '''    let orch = state.orchestrator.clone();
    let lookup_step_key = step_key.clone();
    match joined(
        tokio::task::spawn_blocking(move || {
            orch.authoritative_step_publication(parsed, &lookup_step_key)
        })
        .await,
    ) {'''
if s.count(old) != 1:
    raise SystemExit(f"expected one publication handler match, found {s.count(old)}")
p.write_text(s.replace(old, new, 1))
