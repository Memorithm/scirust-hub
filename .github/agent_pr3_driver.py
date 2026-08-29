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

# `component register` historically accepted the API request wrapper used by
# the CLI E2E fixture. SciCapsule's public `hub-manifest` intentionally emits a
# raw Hub component manifest. Accept both forms and wrap raw manifests at the
# thin-client boundary before POSTing to the versioned API.
p = Path('apps/scirust-hub/src/main.rs')
source = p.read_text()
old = '''        Command::Component(ComponentCommand::Register { path }) => {
            let body = read_manifest(path)?;
            let response = send_json(ureq::post(&url_of(args, "/api/v1/components")), body)?;'''
new = '''        Command::Component(ComponentCommand::Register { path }) => {
            let manifest_or_request = read_manifest(path)?;
            let body = if manifest_or_request.get("manifest").is_some() {
                manifest_or_request
            } else {
                serde_json::json!({
                    "schema_version": 1,
                    "manifest": manifest_or_request,
                })
            };
            let response = send_json(ureq::post(&url_of(args, "/api/v1/components")), body)?;'''
if source.count(old) != 1:
    raise SystemExit(f'expected one component register dispatch, found {source.count(old)}')
p.write_text(source.replace(old, new, 1))
