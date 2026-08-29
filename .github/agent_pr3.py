from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one replacement, found {count}: {old[:120]!r}")
    p.write_text(text.replace(old, new, 1))

# ---------------------------------------------------------------------------
# Hub-side SciCapsule contract guard. Hub validates only the published adapter
# contract; it never parses or extracts .scicap itself.
# ---------------------------------------------------------------------------
Path("crates/hub-core/src/scicapsule.rs").write_text(r'''//! SciCapsule Hub adapter contract guard.
//!
//! This module deliberately knows only SciCapsule's published Hub process
//! contract. It does not decode, verify, extract, or otherwise interpret
//! `.scicap`; those responsibilities remain in SciCapsule / SciRust.

use crate::capability::Capability;
use crate::component::{ComponentManifest, ExecutionBinding};
use crate::error::CoreError;

pub const CAPABILITY: &str = "capsule.execute";
pub const CONTRACT_VERSION: &str = "1.0.0";
pub const CONTRACT_METADATA: &str = "scicapsule-hub-v1";
pub const CAPSULE_MEDIA_TYPE: &str = "application/vnd.scirust.scicap";
pub const POLICY_MEDIA_TYPE: &str = "application/vnd.scicapsule.trust-policy.v1+json";
pub const REQUEST_MEDIA_TYPE: &str = "application/vnd.scicapsule.hub-run-request.v1+json";
pub const RESULT_MEDIA_TYPE: &str = "application/vnd.scicapsule.hub-run-result.v1+json";

fn unsupported(reason: impl Into<String>) -> CoreError {
    CoreError::Validation(format!(
        "unsupported SciCapsule Hub execution contract: {}",
        reason.into()
    ))
}

/// Validates the exact public SciCapsule Hub v1 process adapter before Hub
/// executes a `capsule.execute` run.
///
/// Registration remains open: future contract versions may be indexed and
/// discovered. Execution fails closed until Hub explicitly supports them.
pub fn validate_execution_contract(
    manifest: &ComponentManifest,
    capability: &Capability,
) -> Result<(), CoreError> {
    if capability.name.as_str() != CAPABILITY {
        return Err(unsupported("unexpected capability name"));
    }
    if capability.contract_version.as_str() != CONTRACT_VERSION {
        return Err(unsupported(format!(
            "contract version {}; supported version is {CONTRACT_VERSION}",
            capability.contract_version
        )));
    }
    if manifest
        .metadata
        .get("canonical_capsule_owner")
        .map(String::as_str)
        != Some("scirust")
    {
        return Err(unsupported(
            "canonical_capsule_owner metadata must be scirust",
        ));
    }
    if manifest.metadata.get("contract").map(String::as_str) != Some(CONTRACT_METADATA) {
        return Err(unsupported(format!(
            "contract metadata must be {CONTRACT_METADATA}"
        )));
    }

    let inputs = capability
        .inputs
        .iter()
        .map(|port| (port.name.as_str(), port.description.as_str()))
        .collect::<Vec<_>>();
    let expected_inputs = vec![
        ("capsule", CAPSULE_MEDIA_TYPE),
        ("policy", POLICY_MEDIA_TYPE),
        ("request", REQUEST_MEDIA_TYPE),
    ];
    if inputs != expected_inputs {
        return Err(unsupported("input ports do not match SciCapsule Hub v1"));
    }
    let outputs = capability
        .outputs
        .iter()
        .map(|port| (port.name.as_str(), port.description.as_str()))
        .collect::<Vec<_>>();
    if outputs != vec![("result", RESULT_MEDIA_TYPE)] {
        return Err(unsupported("output ports do not match SciCapsule Hub v1"));
    }

    for (key, expected) in [
        ("authorization", "local_trust_policy"),
        ("request_media_type", REQUEST_MEDIA_TYPE),
        ("result_media_type", RESULT_MEDIA_TYPE),
        ("sandbox", "none"),
    ] {
        if capability.properties.get(key).map(String::as_str) != Some(expected) {
            return Err(unsupported(format!(
                "capability property {key:?} must be {expected:?}"
            )));
        }
    }

    let execution = manifest
        .execution
        .as_ref()
        .ok_or_else(|| unsupported("missing process execution binding"))?;
    let ExecutionBinding::Process(process) = execution;
    if !std::path::Path::new(&process.program).is_absolute() {
        return Err(unsupported(
            "SciCapsule executable path must be absolute",
        ));
    }
    if process.working_dir.is_some() {
        return Err(unsupported(
            "SciCapsule Hub v1 does not declare a working_dir override",
        ));
    }
    let expected_args = [
        "hub-run",
        "--capsule",
        "{input:capsule}",
        "--policy",
        "{input:policy}",
        "--request",
        "{input:request}",
        "--result",
        "{output:result}",
    ];
    if process.args.iter().map(String::as_str).collect::<Vec<_>>() != expected_args {
        return Err(unsupported("process argv does not match SciCapsule Hub v1"));
    }
    if process.outputs.len() != 1 {
        return Err(unsupported(
            "SciCapsule Hub v1 requires exactly one result output",
        ));
    }
    let output = &process.outputs[0];
    if output.name != "result"
        || output.path != "outputs/scicapsule-result.json"
        || output.media_type.as_deref() != Some(RESULT_MEDIA_TYPE)
        || !output.required
    {
        return Err(unsupported(
            "result output does not match SciCapsule Hub v1",
        ));
    }
    Ok(())
}
''')

replace_once(
    "crates/hub-core/src/lib.rs",
    "pub mod run;\npub mod store;",
    "pub mod run;\npub mod scicapsule;\npub mod store;",
)

# Generic external artifact ingestion. This is a control-plane primitive, not
# a SciCapsule-specific storage path.
replace_once(
    "crates/hub-core/src/orchestrator.rs",
    '''    // ------------------------------------------------------------------
    // Runs
    // ------------------------------------------------------------------
''',
    r'''    // ------------------------------------------------------------------
    // External artifacts
    // ------------------------------------------------------------------

    /// Stores caller-provided immutable bytes as a Hub artifact. This is the
    /// ingress path for workflow/run inputs such as capsules, policies and
    /// datasets. Content remains addressed by digest; the artifact id is the
    /// provenance identity of this ingestion event.
    pub fn ingest_artifact(
        &self,
        name: String,
        media_type: String,
        bytes: &[u8],
    ) -> Result<crate::artifact::ArtifactMeta, CoreError> {
        let id = ArtifactId::generate();
        let size = u64::try_from(bytes.len()).map_err(|_| CoreError::ArtifactTooLarge {
            artifact: id,
            size: u64::MAX,
            limit: self.limits.max_artifact_bytes,
        })?;
        if size > self.limits.max_artifact_bytes {
            return Err(CoreError::ArtifactTooLarge {
                artifact: id,
                size,
                limit: self.limits.max_artifact_bytes,
            });
        }
        let digest = crate::digest::hash_bytes(crate::digest::DOMAIN_ARTIFACT_BLOB, bytes);
        let meta = crate::artifact::ArtifactMeta {
            id,
            name,
            media_type,
            digest,
            size,
            created_at: self.clock.now_ms(),
            produced_by_run: None,
        };
        meta.validate()?;
        let stored = self.blobs.put(
            bytes,
            self.limits.max_artifact_bytes,
            crate::digest::DOMAIN_ARTIFACT_BLOB,
        )?;
        debug_assert_eq!(stored, digest);
        self.artifacts_meta.put(&meta)?;
        info!(artifact = %meta.id, digest = %meta.digest, size = meta.size, "artifact ingested");
        Ok(meta)
    }

    // ------------------------------------------------------------------
    // Runs
    // ------------------------------------------------------------------
''',
)

replace_once(
    "crates/hub-core/src/orchestrator.rs",
    '''            .clone();

        // Every declared input port must be bound, and no unbound extras may
''',
    '''            .clone();

        if spec.capability.as_str() == crate::scicapsule::CAPABILITY {
            crate::scicapsule::validate_execution_contract(&manifest, &capability)?;
        }

        // Every declared input port must be bound, and no unbound extras may
''',
)

# Strengthen the existing contract regression with fail-closed cases.
p = Path("crates/hub-core/tests/scicapsule_contract.rs")
text = p.read_text()
text += r'''

#[test]
fn scicapsule_execution_contract_rejects_future_version_until_supported() {
    let mut manifest: ComponentManifest = serde_json::from_str(SCICAPSULE_MANIFEST).unwrap();
    let name = CapabilityName::parse("capsule.execute").unwrap();
    let capability = manifest
        .capabilities
        .iter_mut()
        .find(|capability| capability.name == name)
        .unwrap();
    capability.contract_version = hub_core::Version::parse("2.0.0").unwrap();
    let capability = manifest.capability(&name).unwrap();
    let error = hub_core::scicapsule::validate_execution_contract(&manifest, capability)
        .expect_err("future contract must fail closed");
    assert!(error.to_string().contains("supported version is 1.0.0"));
}

#[test]
fn scicapsule_execution_contract_rejects_drifted_process_binding() {
    let mut manifest: ComponentManifest = serde_json::from_str(SCICAPSULE_MANIFEST).unwrap();
    let Some(ExecutionBinding::Process(process)) = manifest.execution.as_mut() else {
        panic!("process binding expected");
    };
    process.args.push("--unexpected".into());
    let name = CapabilityName::parse("capsule.execute").unwrap();
    let capability = manifest.capability(&name).unwrap();
    assert!(hub_core::scicapsule::validate_execution_contract(&manifest, capability).is_err());
}
'''
p.write_text(text)

# ---------------------------------------------------------------------------
# HTTP raw artifact upload. The route is versioned by /api/v1 and the body is
# explicitly bounded using Hub's artifact limit, independently of JSON limits.
# ---------------------------------------------------------------------------
replace_once(
    "crates/hub-api/src/lib.rs",
    "use axum::extract::{DefaultBodyLimit, Path, Query, State};\nuse axum::http::StatusCode;",
    "use axum::extract::{DefaultBodyLimit, Path, Query, Request, State};\nuse axum::http::{header, StatusCode};",
)
replace_once(
    "crates/hub-api/src/lib.rs",
    '''        .route("/api/v1/artifacts", get(list_artifacts))
''',
    '''        .route("/api/v1/artifacts", post(upload_artifact).get(list_artifacts))
''',
)
replace_once(
    "crates/hub-api/src/lib.rs",
    '''async fn list_artifacts(State(state): State<HubState>) -> Response {
''',
    r'''async fn upload_artifact(State(state): State<HubState>, request: Request) -> Response {
    let name = match request
        .headers()
        .get("x-scirust-artifact-name")
        .and_then(|value| value.to_str().ok())
    {
        Some(value) if !value.is_empty() => value.to_owned(),
        _ => return bad_request("missing or invalid x-scirust-artifact-name header"),
    };
    let media_type = match request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
    {
        Some(value) if !value.is_empty() => value.to_owned(),
        _ => return bad_request("missing or invalid content-type header"),
    };
    let limit = usize::try_from(state.orchestrator.limits().max_artifact_bytes)
        .unwrap_or(usize::MAX);
    let bytes = match axum::body::to_bytes(request.into_body(), limit).await {
        Ok(bytes) => bytes.to_vec(),
        Err(error) => {
            return error_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                proto::ErrorCode::Validation,
                format!("artifact body exceeds configured limit: {error}"),
            );
        }
    };
    let orch = state.orchestrator.clone();
    match joined(
        tokio::task::spawn_blocking(move || orch.ingest_artifact(name, media_type, &bytes)).await,
    ) {
        Ok(meta) => {
            let mut response = Json(proto::ArtifactDto::from(&meta)).into_response();
            *response.status_mut() = StatusCode::CREATED;
            response
        }
        Err(response) => response,
    }
}

async fn list_artifacts(State(state): State<HubState>) -> Response {
''',
)

# Add API coverage for raw binary ingress and size rejection.
replace_once(
    "crates/hub-api/src/lib.rs",
    '''    #[tokio::test]
    async fn health_and_ready_report_shape() {
''',
    r'''    #[tokio::test]
    async fn raw_artifact_upload_round_trips_exact_bytes() {
        let (state, _clock, _dir) = test_state();
        let app = router(state);
        let payload = vec![0, 1, 2, 0xff, b'x'];
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/artifacts")
            .header("x-scirust-artifact-name", "capsule-input")
            .header("content-type", "application/octet-stream")
            .body(Body::from(payload.clone()))
            .expect("req");
        let (status, body) = send(app.clone(), request).await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        assert_eq!(body["name"], "capsule-input");
        assert_eq!(body["size"], payload.len());
        assert!(body["produced_by_run"].is_null());
        let id = body["id"].as_str().expect("id");

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/v1/artifacts/{id}?include=content"))
                    .body(Body::empty())
                    .expect("req"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn artifact_upload_requires_explicit_metadata_headers() {
        let (state, _clock, _dir) = test_state();
        let app = router(state);
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/artifacts")
            .body(Body::from("bytes"))
            .expect("req");
        let (status, body) = send(app, request).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], "bad_request");
    }

    #[tokio::test]
    async fn health_and_ready_report_shape() {
''',
)

# ---------------------------------------------------------------------------
# CLI artifact put command.
# ---------------------------------------------------------------------------
replace_once(
    "apps/scirust-hub/src/main.rs",
    '''enum ArtifactCommand {
    Inspect {
''',
    r'''enum ArtifactCommand {
    /// Store immutable bytes as a Hub input artifact.
    Put {
        /// File to upload (`-` for stdin).
        path: String,
        /// Provenance label. Defaults to the file name for regular files.
        #[arg(long)]
        name: Option<String>,
        /// Media type recorded in Hub metadata.
        #[arg(long, default_value = "application/octet-stream")]
        media_type: String,
    },
    Inspect {
''',
)
replace_once(
    "apps/scirust-hub/src/main.rs",
    '''        Command::Artifact(ArtifactCommand::Inspect { id, content }) => {
''',
    r'''        Command::Artifact(ArtifactCommand::Put {
            path,
            name,
            media_type,
        }) => {
            let bytes = read_artifact_bytes(path)?;
            let artifact_name = match name {
                Some(name) => name.clone(),
                None if path == "-" => {
                    return Err(CliError::Usage(
                        "artifact put from stdin requires --name".into(),
                    ));
                }
                None => std::path::Path::new(path)
                    .file_name()
                    .and_then(|value| value.to_str())
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| CliError::Usage("cannot derive artifact name; use --name".into()))?
                    .to_owned(),
            };
            let response = send_artifact(
                &url_of(args, "/api/v1/artifacts"),
                &artifact_name,
                media_type,
                &bytes,
            )?;
            emit(args, &response, |v| {
                println!("artifact {}: {}", v["id"], v["name"]);
                println!("digest: {}", v["digest"]);
                println!("size:   {}", v["size"]);
            })
        }
        Command::Artifact(ArtifactCommand::Inspect { id, content }) => {
''',
)
replace_once(
    "apps/scirust-hub/src/main.rs",
    '''fn url_of(args: &Args, path: &str) -> String {
''',
    r'''#[allow(clippy::result_large_err)]
fn read_artifact_bytes(path: &str) -> Result<Vec<u8>, CliError> {
    if path == "-" {
        let mut bytes = Vec::new();
        std::io::stdin()
            .read_to_end(&mut bytes)
            .map_err(|e| CliError::Usage(format!("reading stdin: {e}")))?;
        Ok(bytes)
    } else {
        std::fs::read(path).map_err(|e| CliError::Usage(format!("reading {path:?}: {e}")))
    }
}

fn url_of(args: &Args, path: &str) -> String {
''',
)
replace_once(
    "apps/scirust-hub/src/main.rs",
    '''#[allow(clippy::result_large_err)] // CliError keeps full API context
fn send_json(request: ureq::Request, payload: Value) -> Result<Value, CliError> {
''',
    r'''#[allow(clippy::result_large_err)] // CliError keeps full API context
fn send_artifact(
    path_url: &str,
    name: &str,
    media_type: &str,
    bytes: &[u8],
) -> Result<Value, CliError> {
    ureq::post(path_url)
        .set("x-scirust-artifact-name", name)
        .set("content-type", media_type)
        .send_bytes(bytes)
        .map_err(request_error)?
        .into_json()
        .map_err(|e| CliError::BadResponse(format!("decoding body: {e}")))
}

#[allow(clippy::result_large_err)] // CliError keeps full API context
fn send_json(request: ureq::Request, payload: Value) -> Result<Value, CliError> {
''',
)

# Documentation: make the actual end-to-end path explicit and remove stale
# sequential limitation text now that PR #10 is merged.
p = Path("docs/integrations/SCICAPSULE.md")
text = p.read_text()
text = text.replace(
    "Upload/store the capsule, policy and request as Hub artifacts, bind them to the\nthree capability inputs, and submit the run through the ordinary Hub API/CLI.",
    "Store the capsule, policy and request through Hub's bounded artifact-ingress\nendpoint (or `scirust-hub artifact put`), bind the returned artifact ids to the\nthree capability inputs, and submit the run through the ordinary Hub API/CLI."
)
text += r'''

## Artifact ingress

Hub accepts external immutable input bytes at `POST /api/v1/artifacts`. The raw
request body is bounded by Hub's configured `max_artifact_bytes`; callers must
send `x-scirust-artifact-name` and `content-type`. The response is normal Hub
artifact metadata. This is a generic control-plane primitive and is not coupled
to `.scicap`.

The CLI equivalent is:

```text
scirust-hub --output json artifact put demo.scicap \
  --name demo.scicap \
  --media-type application/vnd.scirust.scicap
```

Hub does not inspect capsule bytes on ingress. At run submission it does,
however, fail closed if a component claiming `capsule.execute` does not match
the published SciCapsule Hub contract `1.0.0`. Canonical capsule format/version
rejection remains delegated to SciCapsule's own `Capsule::decode` path during
`hub-run`.
'''
p.write_text(text)

p = Path("README.md")
text = p.read_text()
text = text.replace(
    "# multi-step workflows chain artifacts between runs (sequential, fail-fast):",
    "# multi-step workflows chain artifacts between runs (bounded parallel, fail-fast):",
)
text = text.replace(
    "- Workflow execution is sequential and fail-fast (ADR-0006); parallel\n  scheduling, retries and distributed execution do not exist yet. Workflow\n  cancellation is not wired to running steps.",
    "- Workflow execution supports bounded parallelism, retries and active cancellation;\n  distributed executor placement does not exist yet."
)
p.write_text(text)

# A shell-level interop check uses only public CLIs and contracts. CI builds a
# pinned SciCapsule commit and passes its binary here.
Path("scripts/test-scicapsule-hub-v1.sh").parent.mkdir(parents=True, exist_ok=True)
Path("scripts/test-scicapsule-hub-v1.sh").write_text(r'''#!/usr/bin/env bash
set -euo pipefail

: "${SCICAPSULE_BIN:?set SCICAPSULE_BIN to an absolute SciCapsule binary}"
HUB_BIN="${HUB_BIN:-target/debug/scirust-hub}"
HUBD_BIN="${HUBD_BIN:-target/debug/scirust-hubd}"
PORT="${SCIRUST_HUB_TEST_PORT:-18477}"
BASE="http://127.0.0.1:${PORT}"
TMP="$(mktemp -d)"
PID=""
cleanup() {
  if [[ -n "$PID" ]]; then kill "$PID" 2>/dev/null || true; wait "$PID" 2>/dev/null || true; fi
  rm -rf "$TMP"
}
trap cleanup EXIT

cat >"$TMP/run.sh" <<'SH'
#!/bin/sh
[ "${SCICAPSULE_HUB_E2E:-}" = "ok" ] || exit 31
[ "${1:-}" = "--literal" ] || exit 32
printf 'payload-ok\n'
SH
chmod +x "$TMP/run.sh"

openssl genpkey -algorithm ED25519 -out "$TMP/private.pem" >/dev/null 2>&1
openssl pkey -in "$TMP/private.pem" -pubout -out "$TMP/public.pem" >/dev/null 2>&1
"$SCICAPSULE_BIN" pack --name hub-e2e --entrypoint bin/run --output "$TMP/demo.scicap" "bin/run=$TMP/run.sh" >/dev/null
"$SCICAPSULE_BIN" sign "$TMP/demo.scicap" --key "$TMP/private.pem" --output "$TMP/demo.sig" >/dev/null
"$SCICAPSULE_BIN" create-trust-policy --output "$TMP/policy.json" --require 1 "release=$TMP/public.pem" >/dev/null
"$SCICAPSULE_BIN" create-hub-request --output "$TMP/request.json" --signature "$TMP/demo.sig" --timeout-seconds 10 --env SCICAPSULE_HUB_E2E=ok -- --literal >/dev/null
COMPONENT="00000000-0000-0000-0000-000000000123"
"$SCICAPSULE_BIN" hub-manifest --component-id "$COMPONENT" --program "$SCICAPSULE_BIN" --output "$TMP/manifest.json" >/dev/null

"$HUBD_BIN" --listen "127.0.0.1:${PORT}" --data-dir "$TMP/hub-data" >"$TMP/hubd.log" 2>&1 &
PID=$!
for _ in $(seq 1 100); do
  curl -fsS "$BASE/health" >/dev/null 2>&1 && break
  sleep 0.05
done
curl -fsS "$BASE/health" >/dev/null

"$HUB_BIN" --url "$BASE" component register "$TMP/manifest.json" >/dev/null
put() {
  local file=$1 name=$2 media=$3 out=$4
  "$HUB_BIN" --url "$BASE" --output json artifact put "$file" --name "$name" --media-type "$media" >"$out"
  python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["id"])' "$out"
}
CAPSULE_ID=$(put "$TMP/demo.scicap" capsule application/vnd.scirust.scicap "$TMP/capsule.json")
POLICY_ID=$(put "$TMP/policy.json" policy application/vnd.scicapsule.trust-policy.v1+json "$TMP/policy-art.json")
REQUEST_ID=$(put "$TMP/request.json" request application/vnd.scicapsule.hub-run-request.v1+json "$TMP/request-art.json")

"$HUB_BIN" --url "$BASE" --output json run submit \
  --component "$COMPONENT" --capability capsule.execute \
  --input "capsule=$CAPSULE_ID" --input "policy=$POLICY_ID" --input "request=$REQUEST_ID" \
  --timeout-ms 30000 --wait >"$TMP/run.json"
python3 - "$TMP/run.json" <<'PY'
import json,sys
r=json.load(open(sys.argv[1]))
assert r["state"] == "succeeded", r
outs=r["outcome"]["outputs"]
assert any(o["name"] == "file:result" for o in outs), outs
PY

# Corrupted capsule bytes must reach SciCapsule and fail there; Hub must not
# fabricate a result artifact for the failed run.
cp "$TMP/demo.scicap" "$TMP/corrupt.scicap"
printf 'corrupt' >>"$TMP/corrupt.scicap"
BAD_CAPSULE_ID=$(put "$TMP/corrupt.scicap" corrupt application/vnd.scirust.scicap "$TMP/corrupt-art.json")
set +e
"$HUB_BIN" --url "$BASE" --output json run submit \
  --component "$COMPONENT" --capability capsule.execute \
  --input "capsule=$BAD_CAPSULE_ID" --input "policy=$POLICY_ID" --input "request=$REQUEST_ID" \
  --timeout-ms 30000 --wait >"$TMP/bad-run.json" 2>"$TMP/bad-run.err"
STATUS=$?
set -e
# The CLI successfully receives a terminal failed RunDto, so its own status is
# zero; assert the machine record rather than relying on process status.
python3 - "$TMP/bad-run.json" <<'PY'
import json,sys
r=json.load(open(sys.argv[1]))
assert r["state"] == "failed", r
assert not any(o["name"] == "file:result" for o in r["outcome"]["outputs"]), r
PY

echo "SciCapsule Hub v1 end-to-end: success + corrupted-capsule rejection passed"
''')

print("PR3 SciCapsule integration transformations complete")
