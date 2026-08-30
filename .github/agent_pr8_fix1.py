from pathlib import Path

path = Path(".github/agent_pr8.py")
text = path.read_text()
old = '''    "CHANGELOG.md",
    \'\'\'### Added

\'\'\',
    \'\'\'### Added

- Configured multi-worker remote placement:'''
new = '''    "CHANGELOG.md",
    \'\'\'## [Unreleased]

### Added

\'\'\',
    \'\'\'## [Unreleased]

### Added

- Configured multi-worker remote placement:'''
count = text.count(old)
if count != 1:
    raise SystemExit(f"expected one generator CHANGELOG prefix, found {count}")
path.write_text(text.replace(old, new, 1))
print("generator changelog target fixed")
