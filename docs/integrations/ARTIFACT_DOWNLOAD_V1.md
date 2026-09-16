# Verified bounded artifact downloads

`GET /api/v1/artifacts/{id}/content` returns a binary snapshot of at most
16 MiB. It uses the existing `inspect` authorization permission. The store
verifies the exact returned buffer against immutable metadata size and either
the Hub artifact or historical capture digest. It rejects oversized metadata
before opening the blob and limits reads even if the file grows. Corruption
is an error, never a successful partial download.

```bash
curl --fail --header "Authorization: Bearer ${SCIRUST_HUB_TOKEN}" \
  http://127.0.0.1:8477/api/v1/artifacts/ARTIFACT_UUID/content \
  --output selected-artifact.bin
```

Replace `ARTIFACT_UUID` with a registered artifact ID. A portable client should
also fetch `/portable-digest` and compare ordinary SHA-256 and byte count with
its expected descriptor. Hub's domain-separated digest is not ordinary SHA-256.
The raw endpoint always serves `application/octet-stream`, `attachment`,
`nosniff`, and `no-store`, including when producer metadata declares HTML.

The text-only `?include=content` path retains its 64 KiB limit and now enforces
that limit before reading. It uses the same verified snapshot primitive.
Objects above the download budget require a separately qualified streaming
transport; this endpoint does not claim large-model streaming or aggregate
memory admission. Storage directories remain administrator-owned. Existing
Hub authorization is deployment-wide, not per-artifact scientific clearance;
restricted research data requires a separate appropriately authorized Hub.

Validation: `cargo test --locked -p hub-core -p hub-api`. Tests cover both
digest domains, same-size corruption, growth, truncation, symlinks, binary
bytes, authentication, safe response headers, and size rejection before I/O.
