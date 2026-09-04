# ElasticXxx runtime orchestration

SciRust Hub publishes `resource.elastic.run@1.0.0` as a narrow process contract over the ElasticXxx runtime.

## Data flow

The component consumes one immutable ElasticXxx `OperatorConfig` v1 artifact and invokes:

```text
/opt/scirust-hub/libexec/elastic hub-run \
  --config {input:config} \
  --evidence-output {output:evidence}
```

The declared output is `application/vnd.elastic.runtime-evidence.v1+json`, carrying the library-owned `elastic-runtime-evidence-v1` contract.

Hub materializes the immutable input, supervises the process, ingests the declared evidence output through the ordinary content-addressed artifact path, and records normal run provenance.

## Ownership boundary

ElasticXxx remains authoritative for observation, forecasting, planning, validation, actuation, post-actuation verification, COMMIT/ROLLBACK, stop reason and final resource state. Hub does not inspect an Elastic runtime record and decide whether the adaptation should have committed or rolled back.

The existing SOUP pre-execution capability `llm.train.elastic@1.0.0` remains separate. That capability consumes the narrower `elastic.soup.run-resource-plan@1.0.0` handoff. `resource.elastic.run@1.0.0` executes a generic ElasticXxx operator configuration and is not a SOUP-specific runtime.

## Deployment

Install the exact ElasticXxx binary built from merge `9e51879b96e54c812b6a265fe5901e960bbe6250` at:

```text
/opt/scirust-hub/libexec/elastic
```

The source process was validated on exact PR head `571d0deb8921df54502fbb35909dd8830cbf4fb4` by ElasticXxx CI #211 on the repository's trusted ARM64 runner.

## Failure and retry semantics

An Elastic run may physically actuate resources. A failed or ambiguous post-dispatch attempt must therefore follow Hub's side-effecting-work rules and must not be blindly replayed on another worker merely because an outer transport result is uncertain.

The evidence output itself uses create-new semantics inside ElasticXxx. Existing output paths are not replaced.

## Non-claims

This component does not establish OS sandboxing, dynamic resource-aware Hub placement, multi-host coordination, cross-resource atomicity, hardware portability, or ML 5/5 maturity. It only establishes a versioned process and immutable evidence boundary between Hub and the existing ElasticXxx runtime.
