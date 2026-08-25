//! # scirust-hub-mcp — stdio MCP server exposing Hub introspection.
//!
//! Reads one JSON-RPC request per line from stdin, writes one response per
//! line to stdout (notifications produce no output). All state lives in the
//! Hub daemon reached over HTTP.

use std::io::{BufRead, Write};
use std::sync::Arc;

use clap::Parser;
use hub_mcp::{HttpHub, McpAdapter, ParseLineError, PARSE_ERROR};

#[derive(Debug, clap::Parser)]
#[command(
    name = "scirust-hub-mcp",
    about = "Read-only MCP adapter for a running SciRust Hub daemon",
    version
)]
struct Args {
    /// Base URL of the hub daemon.
    #[arg(long, env = "SCIRUST_HUB_URL", default_value = "http://127.0.0.1:8477")]
    url: String,
}

fn main() {
    let args = Args::parse();
    let adapter = McpAdapter::new(Arc::new(HttpHub::new(args.url)));

    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    for line in stdin.lock().lines() {
        match line {
            Ok(line) if line.trim().is_empty() => continue,
            Ok(line) => match adapter.handle_line(&line) {
                Ok(Some(response)) => {
                    if writeln!(stdout, "{response}").is_err() {
                        break; // stdout closed: agent went away
                    }
                    let _ = stdout.flush();
                }
                Ok(None) => {} // notification
                Err(ParseLineError) => {
                    let error = serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": null,
                        "error": {
                            "code": PARSE_ERROR,
                            "message": "parse error: line is not valid JSON",
                        },
                    });
                    let _ = writeln!(stdout, "{error}");
                    let _ = stdout.flush();
                }
            },
            Err(error) => {
                eprintln!("scirust-hub-mcp: stdin read failed: {error}");
                break;
            }
        }
    }
}
