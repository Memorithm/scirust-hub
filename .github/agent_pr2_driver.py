from pathlib import Path
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

old = '''    ''' + '"""    let recovered_cancellations = orchestrator.recover_workflow_cancellations()?;\\n    if recovered_cancellations > 0 {\\n        tracing::info!(recovered_cancellations, \\\"reconciled workflow cancellations after restart\\\");\\n    }\\n\\n    tracing::info!("""' + ''','''
new = '''    ''' + '"""    let recovered_cancellations = orchestrator.recover_workflow_cancellations()?;\\n    if recovered_cancellations > 0 {\\n        tracing::info!(\\n            recovered_cancellations,\\n            \\\"reconciled workflow cancellations after restart\\\"\\n        );\\n    }\\n\\n    tracing::info!("""' + ''','''
if text.count(old) != 1:
    raise SystemExit(f'expected one daemon patch marker, found {text.count(old)}')
text = text.replace(old, new, 1)
path.write_text(text)

runpy.run_path(str(path), run_name='__main__')

# Concurrent worker start order is OS-scheduled, even though ready-set
# selection itself is lexical. Compare the admitted set instead of asserting
# a thread scheduling order.
p = Path('crates/hub-core/tests/parallel_dag.rs')
generated = p.read_text()
old = '''    assert_eq!(
        *executor.starts.lock().expect("starts"),
        vec!["a".to_owned(), "b".to_owned()]
    );'''
new = '''    let mut starts = executor.starts.lock().expect("starts").clone();
    starts.sort();
    assert_eq!(starts, vec!["a".to_owned(), "b".to_owned()]);'''
if generated.count(old) != 1:
    raise SystemExit(f'expected one parallel start-order assertion, found {generated.count(old)}')
generated = generated.replace(old, new, 1)
p.write_text(generated)

# Keep cancellation classification simple and unambiguous.
p = Path('crates/hub-core/src/orchestrator.rs')
generated = p.read_text()
old = '''            if self.workflow_cancel_requested(workflow_id)?
                || (token.is_cancelled() && finished.state == RunState::Cancelled)
                || finished.state == RunState::Cancelled
            {'''
new = '''            if self.workflow_cancel_requested(workflow_id)?
                || finished.state == RunState::Cancelled
            {'''
if generated.count(old) != 1:
    raise SystemExit(f'expected one cancellation condition, found {generated.count(old)}')
p.write_text(generated.replace(old, new, 1))
