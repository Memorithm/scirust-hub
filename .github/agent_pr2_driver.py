from pathlib import Path
import re
import runpy

path = Path('.github/agent_pr2.py')
text = path.read_text()

old = '''replace_once(
    "crates/hub-core/src/orchestrator.rs",
    "use std::collections::BTreeMap;",
    "use std::collections::{BTreeMap, BTreeSet};",
)'''
new = '''replace_once(
    "crates/hub-core/src/orchestrator.rs",
    "use std::collections::BTreeMap;\\nuse std::path::PathBuf;",
    "use std::collections::{BTreeMap, BTreeSet};\\nuse std::path::PathBuf;",
)'''
if text.count(old) != 1:
    raise SystemExit(f'expected one import patch stanza, found {text.count(old)}')
text = text.replace(old, new, 1)

old = '''    \'\'\'    let recovered_cancellations = orchestrator.recover_workflow_cancellations()?;
    if recovered_cancellations > 0 {
        tracing::info!(recovered_cancellations, "reconciled workflow cancellations after restart");
    }

    tracing::info!(\'\'\','''
new = '''    \'\'\'    let recovered_cancellations = orchestrator.recover_workflow_cancellations()?;
    if recovered_cancellations > 0 {
        tracing::info!(
            recovered_cancellations,
            "reconciled workflow cancellations after restart"
        );
    }

    tracing::info!(\'\'\','''
if text.count(old) != 1:
    raise SystemExit(f'expected one daemon patch marker, found {text.count(old)}')
text = text.replace(old, new, 1)
path.write_text(text)

runpy.run_path(str(path), run_name='__main__')

p = Path('crates/hub-core/src/orchestrator.rs')
generated = p.read_text()
old = '''#[cfg(test)]
mod tests {#[cfg(test)]
mod tests {'''
new = '''#[cfg(test)]
mod tests {'''
if generated.count(old) != 1:
    raise SystemExit(f'expected one duplicated test module marker, found {generated.count(old)}')
generated = generated.replace(old, new, 1)

# Keep cancellation classification simple and unambiguous.
old = '''            if self.workflow_cancel_requested(workflow_id)?
                || (token.is_cancelled() && finished.state == RunState::Cancelled)
                || finished.state == RunState::Cancelled
            {'''
new = '''            if self.workflow_cancel_requested(workflow_id)?
                || finished.state == RunState::Cancelled
            {'''
if generated.count(old) != 1:
    raise SystemExit(f'expected one cancellation condition, found {generated.count(old)}')
generated = generated.replace(old, new, 1)
p.write_text(generated)

# Worker start order is OS-scheduled. Ready-set selection is lexical, but tests
# must not confuse that policy with thread execution timing.
p = Path('crates/hub-core/tests/parallel_dag.rs')
generated = p.read_text()
old = '''    assert_eq!(
        *executor.starts.lock().expect("starts"),
        vec!["a".to_owned(), "b".to_owned()]
    );'''
new = '''    let mut starts = executor.starts.lock().expect("starts").clone();
    starts.sort();
    assert_eq!(starts, vec!["a".to_owned(), "b".to_owned()]);'''
if generated.count(old) != 2:
    raise SystemExit(f'expected two start-order assertions, found {generated.count(old)}')
p.write_text(generated.replace(old, new))

# WorkflowSpec gained an additive max_concurrency field. Cover every existing
# Rust literal that predates the field, not only the new integration tests.
def add_default_concurrency(path: Path) -> int:
    source = path.read_text()
    cursor = 0
    changed = 0
    while True:
        start = source.find('WorkflowSpec {', cursor)
        if start < 0:
            break
        steps = re.search(r'\n(?P<indent>[ \t]*)steps:', source[start:])
        if steps is None:
            cursor = start + len('WorkflowSpec {')
            continue
        steps_pos = start + steps.start()
        header = source[start:steps_pos]
        if 'max_concurrency:' not in header:
            names = list(re.finditer(r'(?m)^(?P<indent>[ \t]*)name:[^\n]*$', header))
            if not names:
                raise SystemExit(f'{path}: WorkflowSpec literal without name before steps')
            name = names[-1]
            insert_at = start + name.end()
            indent = name.group('indent')
            source = (
                source[:insert_at]
                + f'\n{indent}max_concurrency: 1,'
                + source[insert_at:]
            )
            changed += 1
            cursor = insert_at + len(indent) + 24
        else:
            cursor = steps_pos + 1
    if changed:
        path.write_text(source)
    return changed

patched_literals = 0
for root in (Path('crates'), Path('apps')):
    for rust_file in root.rglob('*.rs'):
        patched_literals += add_default_concurrency(rust_file)
print(f'patched {patched_literals} legacy WorkflowSpec literal(s)')
