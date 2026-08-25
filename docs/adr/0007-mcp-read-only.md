# ADR-0007: MCP adapter is read-only

Status: accepted
Date: 2026-08-25

## Context

Agents are first-class ecosystem actors, and SciRust already ships an MCP
server for its own tools (`scirust-mcp`, JSON-RPC 2.0 over stdio, one
request per line). The Hub should be introspectable by the same agents —
but MCP has no built-in authentication, and `tools/call` that could trigger
executions would let anything that can spawn a local process run arbitrary
registered components on the host.

## Decision

`hub-mcp` exposes **read-only introspection only**: status, components,
runs, workflows, artifacts. The tool catalog contains no submission or
execution entry points, and the underlying `HubFetcher` port used by the
adapter offers GET semantics only (`post_json` exists on the port for
symmetry but no tool calls it). Submissions and executions stay behind the
HTTP API / CLI, where an authorization story can land deliberately.

Protocol conformance mirrors `scirust-mcp`: protocol version `2025-06-18`,
NDJSON framing, notifications answered with silence, unknown tools surfaced
as tool-level errors (`isError: true`) while unknown methods are RPC-level
`-32601`.

## Consequences

- An agent connected over MCP can observe everything but change nothing.
- Execution-via-MCP becomes possible later as additive tools gated on
  whatever auth mechanism the HTTP surface grows; nothing in this design
  blocks it.
- The adapter talks to a running daemon over HTTP rather than embedding the
  domain: one process boundary, same trust model as the CLI.

## Alternatives considered

- Full read-write MCP from day one: rejected without auth (mission §42:
  registration must never imply execution permission).
- Embedding domain directly in-process: rejected to keep the adapter a thin,
  restartable boundary process.
