from pathlib import Path

p = Path('crates/hub-core/src/lib.rs')
text = p.read_text()
old = '''pub use event::{
    LifecycleEntityType, LifecycleEvent, LifecycleEventKind, LifecycleEventRepository,
    NewLifecycleEvent, DEFAULT_EVENT_PAGE, MAX_EVENT_PAGE,
};'''
new = '''pub use event::{
    InMemoryLifecycleEvents, LifecycleEntityType, LifecycleEvent, LifecycleEventKind,
    LifecycleEventRepository, NewLifecycleEvent, DEFAULT_EVENT_PAGE, MAX_EVENT_PAGE,
};'''
if text.count(old) != 1:
    raise SystemExit('expected lifecycle event export block exactly once')
p.write_text(text.replace(old, new, 1))
