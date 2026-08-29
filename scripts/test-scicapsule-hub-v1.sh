#!/usr/bin/env bash
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
