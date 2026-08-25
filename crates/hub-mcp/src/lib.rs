//! # hub-mcp — read-only MCP adapter for SciRust Hub
//!
//! Exposes Hub *introspection* (components, runs, workflows, artifacts,
//! status) as MCP tools so agents can discover ecosystem state without
//! bespoke glue. Protocol shape mirrors the established `scirust-mcp`
//! discipline: JSON-RPC 2.0 over stdio, one request per line, protocol
//! version `2025-06-18`.
//!
//! Boundary decision (ADR-0007): the adapter is **read-only**. Triggering
//! executions through MCP is deliberately out of scope until an explicit
//! authorization story exists; submissions stay with the HTTP API and CLI.

use std::sync::Arc;

use serde_json::{json, Value};

pub const JSONRPC_VERSION: &str = "2.0";
pub const MCP_PROTOCOL_VERSION: &str = "2025-06-18";
pub const PARSE_ERROR: i64 = -32700;
const METHOD_NOT_FOUND: i64 = -32601;
const INVALID_PARAMS: i64 = -32602;

/// How the adapter reaches a running Hub daemon.
pub trait HubFetcher: Send + Sync {
    /// # Errors
    /// Transport or non-2xx status as a string.
    fn get_json(&self, path: &str) -> Result<Value, String>;

    /// # Errors
    /// Transport or non-2xx status as a string.
    #[allow(dead_code)] // symmetric with get_json; used by future tools
    fn post_json(&self, path: &str, body: &Value) -> Result<Value, String>;
}

/// HTTP implementation against the daemon's `/api/v1`.
pub struct HttpHub {
    base_url: String,
}

impl HttpHub {
    #[must_use]
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
        }
    }
}

impl HubFetcher for HttpHub {
    fn get_json(&self, path: &str) -> Result<Value, String> {
        ureq::get(&format!("{}{path}", self.base_url))
            .call()
            .map_err(|e| format!("hub request failed: {e}"))?
            .into_json::<Value>()
            .map_err(|e| format!("hub response was not JSON: {e}"))
    }

    fn post_json(&self, path: &str, body: &Value) -> Result<Value, String> {
        ureq::post(&format!("{}{path}", self.base_url))
            .send_json(body.clone())
            .map_err(|e| format!("hub request failed: {e}"))?
            .into_json::<Value>()
            .map_err(|e| format!("hub response was not JSON: {e}"))
    }
}

/// The MCP tool surface. Stateless besides the hub connection.
pub struct McpAdapter {
    hub: Arc<dyn HubFetcher>,
    server_name: &'static str,
}

impl McpAdapter {
    #[must_use]
    pub fn new(hub: Arc<dyn HubFetcher>) -> Self {
        Self {
            hub,
            server_name: "scirust-hub-mcp",
        }
    }

    /// Handles one deserialized JSON-RPC request. Returns `None` for
    /// notifications (the protocol expects no reply).
    #[must_use]
    pub fn handle_request(&self, request: &Value) -> Option<Value> {
        let Some(method) = request.get("method").and_then(Value::as_str) else {
            return Some(self.error_response(
                request.get("id"),
                INVALID_PARAMS,
                "missing method".to_owned(),
            ));
        };
        let id = request.get("id").cloned();
        let is_notification = id.is_none();

        let result: Result<Value, (i64, String)> = match method {
            "initialize" => Ok(json!({
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "serverInfo": {
                    "name": self.server_name,
                    "version": env!("CARGO_PKG_VERSION"),
                },
                "capabilities": { "tools": {} },
            })),
            "notifications/initialized" | "notifications/cancelled" => return None,
            "ping" => Ok(json!({})),
            "tools/list" => Ok(json!({ "tools": Self::tool_descriptions() })),
            "resources/list" => Ok(json!({ "resources": [] })),
            "prompts/list" => Ok(json!({ "prompts": [] })),
            "tools/call" => self.handle_tool_call(request.get("params")),
            other => Err((METHOD_NOT_FOUND, format!("unknown method: {other}"))),
        };

        if is_notification {
            return None;
        }
        Some(match result {
            Ok(value) => json!({
                "jsonrpc": JSONRPC_VERSION,
                "id": id,
                "result": value,
            }),
            Err((code, message)) => self.error_response(id.as_ref(), code, message),
        })
    }

    /// Convenience wrapper: parse one NDJSON line, handle it.
    ///
    /// # Errors
    /// [`ParseLineError`] when the line is not valid JSON (a protocol-level
    /// parse error response is produced instead of a panic).
    pub fn handle_line(&self, line: &str) -> Result<Option<String>, ParseLineError> {
        match serde_json::from_str::<Value>(line) {
            Ok(request) => Ok(self
                .handle_request(&request)
                .map(|response| response.to_string())),
            Err(_) => Err(ParseLineError),
        }
    }

    fn error_response(&self, id: Option<&Value>, code: i64, message: String) -> Value {
        json!({
            "jsonrpc": JSONRPC_VERSION,
            "id": id,
            "error": { "code": code, "message": message },
        })
    }

    fn handle_tool_call(&self, params: Option<&Value>) -> Result<Value, (i64, String)> {
        let Some(params) = params else {
            return Err((INVALID_PARAMS, "tools/call requires params".into()));
        };
        let name = params
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let arg = |key: &str| arguments.get(key).and_then(Value::as_str);

        let fetch: Result<Value, String> = match name {
            "hub.status" => self.hub.get_json("/ready"),
            "hub.list_components" => match arg("capability") {
                Some(capability) => self.hub.get_json(&format!(
                    "/api/v1/components?capability={}",
                    urlencoded(capability)
                )),
                None => self.hub.get_json("/api/v1/components"),
            },
            "hub.get_component" => require_id(arg("id"))
                .map_err(String::from)
                .and_then(|id| self.hub.get_json(&format!("/api/v1/components/{id}"))),
            "hub.list_runs" => self.hub.get_json("/api/v1/runs"),
            "hub.get_run" => require_id(arg("id"))
                .map_err(String::from)
                .and_then(|id| self.hub.get_json(&format!("/api/v1/runs/{id}"))),
            "hub.list_workflows" => self.hub.get_json("/api/v1/workflows"),
            "hub.get_workflow" => require_id(arg("id"))
                .map_err(String::from)
                .and_then(|id| self.hub.get_json(&format!("/api/v1/workflows/{id}"))),
            "hub.list_artifacts" => self.hub.get_json("/api/v1/artifacts"),
            "hub.get_artifact" => {
                let suffix = if arguments
                    .get("include_content")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                {
                    "?include=content"
                } else {
                    ""
                };
                require_id(arg("id")).map_err(String::from).and_then(|id| {
                    self.hub
                        .get_json(&format!("/api/v1/artifacts/{id}{suffix}"))
                })
            }
            other => {
                return Ok(json!({
                    "content": [{ "type": "text", "text": format!("unknown tool: {other}") }],
                    "isError": true,
                }));
            }
        };

        match fetch {
            Ok(value) => Ok(json!({
                "content": [{
                    "type": "text",
                    "text": serde_json::to_string_pretty(&value)
                        .unwrap_or_else(|_| value.to_string()),
                }],
            })),
            Err(message) => Ok(json!({
                "content": [{ "type": "text", "text": message }],
                "isError": true,
            })),
        }
    }

    /// Static tool catalog: names, descriptions and input schemas.
    fn tool_descriptions() -> Vec<Value> {
        let schema = |properties: Value, required: Vec<&str>| {
            let mut schema = json!({ "type": "object", "properties": properties });
            if !required.is_empty() {
                schema["required"] = json!(required);
            }
            schema
        };
        vec![
            json!({
                "name": "hub.status",
                "description": "Hub readiness: registered components, recorded runs, executor backend",
                "inputSchema": schema(json!({}), vec![]),
            }),
            json!({
                "name": "hub.list_components",
                "description": "List registered components, optionally filtered by declared capability",
                "inputSchema": schema(
                    json!({ "capability": { "type": "string", "description": "capability name filter such as demo.echo" } }),
                    vec![],
                ),
            }),
            json!({
                "name": "hub.get_component",
                "description": "Fetch one component manifest by UUID",
                "inputSchema": schema(json!({ "id": { "type": "string" } }), vec!["id"]),
            }),
            json!({
                "name": "hub.list_runs",
                "description": "List recorded runs with lifecycle states",
                "inputSchema": schema(json!({}), vec![]),
            }),
            json!({
                "name": "hub.get_run",
                "description": "Fetch one run record including full provenance",
                "inputSchema": schema(json!({ "id": { "type": "string" } }), vec!["id"]),
            }),
            json!({
                "name": "hub.list_workflows",
                "description": "List recorded multi-step workflows",
                "inputSchema": schema(json!({}), vec![]),
            }),
            json!({
                "name": "hub.get_workflow",
                "description": "Fetch one workflow record with per-step results",
                "inputSchema": schema(json!({ "id": { "type": "string" } }), vec!["id"]),
            }),
            json!({
                "name": "hub.list_artifacts",
                "description": "List artifact metadata (digests, sizes)",
                "inputSchema": schema(json!({}), vec![]),
            }),
            json!({
                "name": "hub.get_artifact",
                "description": "Fetch artifact metadata; optionally inline text content",
                "inputSchema": schema(
                    json!({
                        "id": { "type": "string" },
                        "include_content": { "type": "boolean" },
                    }),
                    vec!["id"],
                ),
            }),
        ]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("request line is not valid JSON")]
pub struct ParseLineError;

fn require_id(raw: Option<&str>) -> Result<String, &'static str> {
    raw.map(str::to_owned)
        .filter(|id| !id.is_empty())
        .ok_or("missing required string argument: id")
}

/// Minimal percent-encoding for query values (space + non-query-safe bytes).
fn urlencoded(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    /// Canned hub responses; records requested paths for assertions.
    struct FakeHub {
        routes: BTreeMap<String, Value>,
        seen: Mutex<Vec<String>>,
    }

    impl FakeHub {
        fn with(path: &str, value: Value) -> (Self, Arc<Self>) {
            let mut routes = BTreeMap::new();
            routes.insert(path.to_owned(), value);
            let hub = Arc::new(Self {
                routes,
                seen: Mutex::new(Vec::new()),
            });
            let bare = Self {
                routes: BTreeMap::new(),
                seen: Mutex::new(Vec::new()),
            };
            // Return both: the shared one for the adapter, a throwaway to
            // satisfy tuple symmetry in call sites.
            (bare, hub)
        }
    }

    impl HubFetcher for FakeHub {
        fn get_json(&self, path: &str) -> Result<Value, String> {
            self.seen.lock().expect("lock").push(path.to_owned());
            self.routes
                .get(path)
                .cloned()
                .ok_or_else(|| format!("hub request failed: unexpected path {path:?}"))
        }

        fn post_json(&self, _path: &str, _body: &Value) -> Result<Value, String> {
            Err("read-only adapter".into())
        }
    }

    fn rpc(method: &str) -> Value {
        json!({ "jsonrpc": "2.0", "id": 1, "method": method })
    }

    fn ready_body() -> Value {
        json!({
            "ready": true,
            "components_registered": 3,
            "runs_recorded": 7,
            "executor_backend": "process",
        })
    }

    #[test]
    fn initialize_matches_protocol_shape() {
        let (_bare, hub) = FakeHub::with("/ready", ready_body());
        let adapter = McpAdapter::new(hub.clone());
        let response = adapter.handle_request(&rpc("initialize")).expect("reply");
        assert_eq!(response["jsonrpc"], "2.0");
        assert_eq!(response["id"], 1);
        assert_eq!(response["result"]["protocolVersion"], MCP_PROTOCOL_VERSION);
        assert_eq!(response["result"]["capabilities"]["tools"], json!({}));
        assert!(response["result"]["serverInfo"]["name"]
            .as_str()
            .expect("name")
            .starts_with("scirust-hub-mcp"));
    }

    #[test]
    fn notifications_and_ping_follow_the_protocol() {
        let (_bare, hub) = FakeHub::with("/ready", ready_body());
        let adapter = McpAdapter::new(hub);
        assert!(adapter
            .handle_request(&json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
            .is_none());
        let pong = adapter.handle_request(&rpc("ping")).expect("pong");
        assert_eq!(pong["result"], json!({}));
    }

    #[test]
    fn tools_list_advertises_the_read_only_catalog() {
        let (_bare, hub) = FakeHub::with("/ready", ready_body());
        let adapter = McpAdapter::new(hub);
        let response = adapter.handle_request(&rpc("tools/list")).expect("reply");
        let names: Vec<&str> = response["result"]["tools"]
            .as_array()
            .expect("tools")
            .iter()
            .map(|t| t["name"].as_str().expect("name"))
            .collect();
        for expected in [
            "hub.status",
            "hub.list_components",
            "hub.get_component",
            "hub.list_runs",
            "hub.get_run",
            "hub.list_workflows",
            "hub.get_workflow",
            "hub.list_artifacts",
            "hub.get_artifact",
        ] {
            assert!(names.contains(&expected), "missing {expected}: {names:?}");
        }
        // Every tool declares an object input schema.
        for tool in response["result"]["tools"].as_array().expect("tools") {
            assert_eq!(tool["inputSchema"]["type"], "object");
        }
    }

    #[test]
    fn status_tool_calls_hub_and_wraps_text_content() {
        let (_bare, hub) = FakeHub::with("/ready", ready_body());
        let adapter = McpAdapter::new(hub.clone());
        let response = adapter
            .handle_request(&json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": { "name": "hub.status", "arguments": {} },
            }))
            .expect("reply");
        let text = response["result"]["content"][0]["text"]
            .as_str()
            .expect("text content");
        let parsed: Value = serde_json::from_str(text).expect("embedded JSON");
        assert_eq!(parsed["ready"], true);
        assert_eq!(parsed["components_registered"], 3);
        assert!(response["result"].get("isError").is_none());
        assert_eq!(*hub.seen.lock().expect("seen"), vec!["/ready".to_owned()]);
    }

    #[test]
    fn component_filter_is_forwarded_url_encoded() {
        let (_bare, hub) = FakeHub::with(
            "/api/v1/components?capability=demo.echo",
            json!({ "components": [] }),
        );
        let adapter = McpAdapter::new(hub.clone());
        let response = adapter
            .handle_request(&json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "hub.list_components",
                    "arguments": { "capability": "demo echo" },
                },
            }))
            .expect("reply");
        assert!(response["error"].is_null(), "{response}");
        // Space percent-encoded, path recorded exactly.
        assert_eq!(
            *hub.seen.lock().expect("seen"),
            vec!["/api/v1/components?capability=demo%20echo".to_owned()]
        );
    }

    #[test]
    fn missing_required_argument_yields_tool_error_not_rpc_error() {
        let (_bare, hub) = FakeHub::with("/api/v1/runs", json!({}));
        let adapter = McpAdapter::new(hub);
        let response = adapter
            .handle_request(&json!({
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": { "name": "hub.get_run", "arguments": {} },
            }))
            .expect("reply");
        assert_eq!(response["result"]["isError"], true);
        assert!(response["result"]["content"][0]["text"]
            .as_str()
            .expect("text")
            .contains("missing required string argument: id"));
    }

    #[test]
    fn unknown_tools_and_methods_are_distinguished() {
        let (_bare, hub) = FakeHub::with("/ready", ready_body());
        let adapter = McpAdapter::new(hub);

        let unknown_tool = adapter
            .handle_request(&json!({
                "jsonrpc": "2.0",
                "id": 5,
                "method": "tools/call",
                "params": { "name": "hub.launch_missiles", "arguments": {} },
            }))
            .expect("reply");
        assert_eq!(unknown_tool["result"]["isError"], true);
        assert!(unknown_tool["error"].is_null());

        let unknown_method = adapter
            .handle_request(&rpc("resources/read"))
            .expect("reply");
        assert_eq!(unknown_method["error"]["code"], -32601);
    }

    #[test]
    fn malformed_line_produces_parse_error_response() {
        let (_bare, hub) = FakeHub::with("/ready", ready_body());
        let adapter = McpAdapter::new(hub);
        match adapter.handle_line("{not json") {
            Err(ParseLineError) => {}
            other => panic!("expected parse error, got {other:?}"),
        }
    }
}
