# ElasticXxx runtime evidence → SciRust-Verify

Hub publishes `resource.elastic.verify@1.0.0` as a separate process component over the already-published ElasticXxx runtime evidence boundary.

## Inputs and output

Input `evidence` uses `application/vnd.elastic.runtime-evidence.v1+json` and must come from the qualified `elastic.hub.run@1.0.0` / Hub `resource.elastic.run@1.0.0` path. Output `dossier` uses `application/vnd.scirust-verify.dossier.v1+tar`.

The process binding invokes `/opt/scirust-hub/libexec/scirust-verify-elastic --evidence {input:evidence} --output {output:dossier}`. Hub owns immutable artifact flow and execution provenance. SciRust-Verify owns evidence validation, integrity sealing, scope and verdict semantics.

## Qualified identities

- ElasticXxx process PR: #54
- ElasticXxx final exact head: `571d0deb8921df54502fbb35909dd8830cbf4fb4`
- ElasticXxx merge: `9e51879b96e54c812b6a265fe5901e960bbe6250`
- Hub Elastic runtime merge: `c946ca413c145128dacb22c964fb1f87b39bfc61`
- SciRust-Verify process PR: #31
- SciRust-Verify final exact head: `c685e3d596ab7f5e77106e001792a0c3b5d59837`
- SciRust-Verify merge: `d65c924699d1b1061d0359669884ca9742a3baa6`

## Semantic boundary

The Verify dossier establishes only conformance of the supplied artifact to the qualified runtime-evidence v1 contract. ElasticXxx remains authoritative for runtime planning, actuation, verification and COMMIT/ROLLBACK decisions. Those decisions are preserved as source observations; neither Hub nor SciRust-Verify promotes them into a generic policy-success, optimality, model-quality or performance verdict.

This process is local process execution and is not an OS sandbox. The integration does not establish cross-host comparability or hardware portability.
