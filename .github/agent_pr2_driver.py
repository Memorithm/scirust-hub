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
path.write_text(text.replace(old, new, 1))
runpy.run_path(str(path), run_name='__main__')
