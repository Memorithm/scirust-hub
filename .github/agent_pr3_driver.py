from pathlib import Path
import runpy

path = Path('.github/agent_pr3.py')
text = path.read_text()
old = 'assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);'
new = 'assert_eq!(response.status(), StatusCode::NOT_ACCEPTABLE);'
if text.count(old) != 1:
    raise SystemExit(f'expected one binary-content status assertion, found {text.count(old)}')
path.write_text(text.replace(old, new, 1))
runpy.run_path(str(path), run_name='__main__')
