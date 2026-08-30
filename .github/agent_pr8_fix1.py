from pathlib import Path

path = Path(".github/agent_pr8.py")
text = path.read_text()
old = '''replace_once(
    "CHANGELOG.md",
    ''' + "'''### Added\\n\\n'''" + ''',
    ''' + "'''### Added\\n\\n- Configured multi-worker remote placement: repeating `--remote-worker-url`\\n  (or comma-separating `SCIRUST_HUB_REMOTE_WORKER_URL`) discovers compatible\\n  worker descriptors before dispatch and deterministically selects the lowest\\n  Hub-local in-flight target. Duplicate worker identities fail closed; once a\\n  target is selected there is no ambiguous post-dispatch failover. Per-run\\n  provenance records the concrete selected worker target while single-worker\\n  remote mode remains compatible.\\n\\n'''" + ''',
)
'''
new = '''replace_once(
    "CHANGELOG.md",
    ''' + "'''## [Unreleased]\\n\\n### Added\\n\\n'''" + ''',
    ''' + "'''## [Unreleased]\\n\\n### Added\\n\\n- Configured multi-worker remote placement: repeating `--remote-worker-url`\\n  (or comma-separating `SCIRUST_HUB_REMOTE_WORKER_URL`) discovers compatible\\n  worker descriptors before dispatch and deterministically selects the lowest\\n  Hub-local in-flight target. Duplicate worker identities fail closed; once a\\n  target is selected there is no ambiguous post-dispatch failover. Per-run\\n  provenance records the concrete selected worker target while single-worker\\n  remote mode remains compatible.\\n\\n'''" + ''',
)
'''
if text.count(old) != 1:
    raise SystemExit(f"expected one generator CHANGELOG block, found {text.count(old)}")
path.write_text(text.replace(old, new, 1))
print("generator changelog target fixed")
