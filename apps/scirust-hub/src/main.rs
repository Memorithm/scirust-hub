//! # scirust-hub — CLI client for a running SciRust Hub daemon.
//!
//! The CLI is a thin, scriptable HTTP client: all state lives in the daemon.
//! `--output json` emits the daemon's response bodies verbatim for piping.

use std::io::Read as _;

use clap::{Parser as ClapParser, Subcommand};
use serde_json::Value;

#[derive(Debug, ClapParser)]
#[command(
    name = "scirust-hub",
    about = "SciRust Hub control plane client",
    version
)]
struct Args {
    /// Base URL of the hub daemon.
    #[arg(long, env = "SCIRUST_HUB_URL", default_value = "http://127.0.0.1:8477")]
    url: String,
    /// Output format: human-readable or raw JSON.
    #[arg(long, value_enum, default_value_t = Output::Human)]
    output: Output,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum Output {
    Human,
    Json,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Daemon health and readiness.
    Status,
    #[command(subcommand)]
    Component(ComponentCommand),
    /// List capability summaries across registered components.
    Capabilities,
    #[command(subcommand)]
    Run(RunCommand),
    #[command(subcommand)]
    Artifact(ArtifactCommand),
    #[command(subcommand)]
    Workflow(WorkflowCommand),
}

#[derive(Debug, Subcommand)]
enum WorkflowCommand {
    /// Register a workflow from a spec JSON file.
    Submit {
        /// Path to the workflow spec (`-` for stdin).
        path: String,
    },
    /// Execute a created workflow sequentially and wait.
    Run {
        id: String,
    },
    List,
    Inspect {
        id: String,
    },
}

#[derive(Debug, Subcommand)]
enum ComponentCommand {
    /// Register a component from a manifest JSON file.
    Register {
        /// Path to the manifest file (`-` for stdin).
        path: String,
    },
    List,
    Inspect {
        id: String,
    },
}

#[derive(Debug, Subcommand)]
enum RunCommand {
    /// Submit a run spec.
    Submit {
        #[arg(long)]
        component: String,
        #[arg(long)]
        capability: String,
        /// Parameters as a JSON object string (default `{}`).
        #[arg(long, default_value = "{}")]
        params: String,
        /// Input binding `name=artifact-id`; repeatable.
        #[arg(long = "input")]
        inputs: Vec<String>,
        /// Execution budget in milliseconds.
        #[arg(long, default_value_t = 30_000)]
        timeout_ms: u64,
        /// Execute immediately and wait for completion.
        #[arg(long)]
        wait: bool,
    },
    List,
    Inspect {
        id: String,
    },
    Cancel {
        id: String,
    },
}

#[derive(Debug, Subcommand)]
enum ArtifactCommand {
    Inspect {
        id: String,
        /// Also fetch inline text content when available.
        #[arg(long)]
        content: bool,
    },
}

#[derive(Debug, thiserror::Error)]
#[allow(clippy::large_enum_variant)] // ureq::Error is only carried transiently
enum CliError {
    #[error("cannot reach hub at {url}: {source}")]
    Connect { url: String, source: ureq::Error },
    #[error("hub rejected the request ({status}): {body}")]
    ApiStatus { status: u16, body: String },
    #[error("invalid response from hub: {0}")]
    BadResponse(String),
    #[error("{0}")]
    Usage(String),
}

fn main() {
    let args = Args::parse();
    match dispatch(&args) {
        Ok(()) => {}
        Err(error) => {
            eprintln!("scirust-hub: {error}");
            std::process::exit(1);
        }
    }
}

#[allow(clippy::result_large_err)] // CliError keeps full API context for stderr
fn dispatch(args: &Args) -> Result<(), CliError> {
    match &args.command {
        Command::Status => {
            let health = get(url_of(args, "/health"))?;
            let ready = get(url_of(args, "/ready"))?;
            emit(args, &health, |v| {
                println!(
                    "hub: {} (protocol v{})",
                    v["status"].as_str().unwrap_or("?"),
                    v["protocol_version"]
                );
            })?;
            emit(args, &ready, |v| {
                println!("ready: {}", v["ready"]);
                println!(
                    "components: {}, runs: {}, executor: {}",
                    v["components_registered"],
                    v["runs_recorded"],
                    v["executor_backend"].as_str().unwrap_or("?")
                );
            })
        }
        Command::Component(ComponentCommand::Register { path }) => {
            let body = read_manifest(path)?;
            let response = send_json(ureq::post(&url_of(args, "/api/v1/components")), body)?;
            emit(args, &response, |v| {
                println!("component {}: {}", v["component"]["id"], v["status"]);
                println!("manifest digest: {}", v["manifest_digest"]);
            })
        }
        Command::Component(ComponentCommand::List) => {
            let response = get(url_of(args, "/api/v1/components"))?;
            emit(args, &response, |v| {
                let components = v["components"].as_array().cloned().unwrap_or_default();
                if components.is_empty() {
                    println!("no components registered");
                }
                for c in components {
                    println!(
                        "{}  {:<24} v{}  [{}] caps={}",
                        c["id"],
                        c["name"],
                        c["version"],
                        c["kind"],
                        c["capabilities"].as_array().map(Vec::len).unwrap_or(0)
                    );
                }
            })
        }
        Command::Component(ComponentCommand::Inspect { id }) => {
            let response = get(url_of(args, &format!("/api/v1/components/{id}")))?;
            emit(args, &response, |v| {
                println!("id:       {}", v["id"]);
                println!("name:     {}", v["name"]);
                println!("kind:     {}", v["kind"]);
                println!("version:  {}", v["version"]);
                println!("digest:   {}", v["manifest_digest"]);
                for cap in v["capabilities"].as_array().cloned().unwrap_or_default() {
                    println!(
                        "capability: {} (contract {})",
                        cap["name"], cap["contract_version"]
                    );
                }
            })
        }
        Command::Capabilities => {
            let response = get(url_of(args, "/api/v1/capabilities"))?;
            emit(args, &response, |v| {
                for cap in v["capabilities"].as_array().cloned().unwrap_or_default() {
                    println!(
                        "{:<40} declared_by={} contract={}",
                        cap["name"], cap["declared_by"], cap["contract_version"]
                    );
                }
            })
        }
        Command::Run(RunCommand::Submit {
            component,
            capability,
            params,
            inputs,
            timeout_ms,
            wait,
        }) => {
            // Server-side validation returns structured errors for bad ids;
            // the CLI stays a thin client.
            let component_id = component.clone();
            let parameters: Value = serde_json::from_str(params)
                .map_err(|e| CliError::Usage(format!("--params is not valid JSON: {e}")))?;
            let input_bindings = parse_inputs(inputs)?;
            let payload = serde_json::json!({
                "schema_version": 1,
                "run_spec": {
                    "component": component_id,
                    "capability": capability,
                    "parameters": parameters,
                    "inputs": input_bindings,
                    "timeout_ms": timeout_ms,
                }
            });
            let submitted = send_json(ureq::post(&url_of(args, "/api/v1/runs")), payload)?;
            if !*wait {
                return emit(args, &submitted, |v| {
                    println!("run {}: {}", v["run"]["id"], v["run"]["state"]);
                });
            }
            let run_id = submitted["run"]["id"]
                .as_str()
                .ok_or_else(|| CliError::BadResponse("missing run id".into()))?
                .to_owned();
            let executed = send_json(
                ureq::post(&url_of(args, "/api/v1/executions")),
                serde_json::json!(run_id),
            )?;
            emit(args, &executed, |v| {
                println!("run {}: {}", v["id"], v["state"]);
                if let Some(outcome) = v["outcome"].as_object() {
                    println!(
                        "exit: {} backend: {} duration: {}ms",
                        outcome["exit_code"]
                            .as_i64()
                            .map(|c| c.to_string())
                            .unwrap_or_else(|| "?".into()),
                        outcome["executor_backend"].as_str().unwrap_or("?"),
                        outcome["duration_ms"]
                    );
                    for out in outcome["outputs"].as_array().cloned().unwrap_or_default() {
                        println!(
                            "artifact {}: {} ({} bytes)",
                            out["name"], out["artifact"], out["size"]
                        );
                    }
                    if let Some(failure) = outcome["failure"].as_str() {
                        println!("failure: {failure}");
                    }
                }
            })
        }
        Command::Run(RunCommand::List) => {
            let response = get(url_of(args, "/api/v1/runs"))?;
            emit(args, &response, |v| {
                let runs = v["runs"].as_array().cloned().unwrap_or_default();
                if runs.is_empty() {
                    println!("no runs recorded");
                }
                for r in runs {
                    println!(
                        "{}  {}  {}@{}  created {}",
                        r["id"],
                        r["state"],
                        r["spec"]["capability"],
                        r["component_name"],
                        r["created_at"]
                    );
                }
            })
        }
        Command::Run(RunCommand::Inspect { id }) => {
            let response = get(url_of(args, &format!("/api/v1/runs/{id}")))?;
            emit(args, &response, |v| {
                println!("id:          {}", v["id"]);
                println!("state:       {}", v["state"]);
                println!(
                    "component:   {} v{}",
                    v["component_name"], v["component_version"]
                );
                println!("capability:  {}", v["spec"]["capability"]);
                println!("params:      {}", v["spec"]["parameters"]);
                for t in v["transitions"].as_array().cloned().unwrap_or_default() {
                    println!("transition:  {} -> {} at {}", t["from"], t["to"], t["at"]);
                }
                if !v["outcome"].is_null() {
                    let o = &v["outcome"];
                    println!("exit_code:   {}", o["exit_code"]);
                    println!("backend:     {}", o["executor_backend"]);
                    println!("duration_ms: {}", o["duration_ms"]);
                    if let Some(f) = o["failure"].as_str() {
                        println!("failure:     {f}");
                    }
                    for out in o["outputs"].as_array().cloned().unwrap_or_default() {
                        println!(
                            "output:      {} artifact={} digest={} size={}",
                            out["name"], out["artifact"], out["digest"], out["size"]
                        );
                    }
                }
            })
        }
        Command::Run(RunCommand::Cancel { id }) => {
            let response = post_empty(url_of(args, &format!("/api/v1/runs/{id}/cancel")))?;
            emit(args, &response, |v| {
                println!(
                    "run {}: signalled_active_execution={}",
                    v["run_id"], v["signalled_active_execution"]
                );
            })
        }
        Command::Workflow(WorkflowCommand::Submit { path }) => {
            let body = read_manifest(path)?;
            let response = send_json(ureq::post(&url_of(args, "/api/v1/workflows")), body)?;
            emit(args, &response, |v| {
                println!(
                    "workflow {}: {}",
                    v["workflow"]["id"], v["workflow"]["state"]
                );
                for (i, step) in v["workflow"]["steps"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default()
                    .iter()
                    .enumerate()
                {
                    println!("  step {}: {:?}", i, step["key"]);
                }
            })
        }
        Command::Workflow(WorkflowCommand::Run { id }) => {
            let response = post_empty(url_of(args, &format!("/api/v1/workflows/{id}/executions")))?;
            emit(args, &response, |v| {
                println!("workflow {}: {}", v["id"], v["state"]);
                for step in v["steps"].as_array().cloned().unwrap_or_default() {
                    println!(
                        "  step {:?}: run {} {}",
                        step["key"], step["run"], step["state"]
                    );
                }
                if let Some(f) = v["failure"].as_str() {
                    println!("failure: {f}");
                }
            })
        }
        Command::Workflow(WorkflowCommand::List) => {
            let response = get(url_of(args, "/api/v1/workflows"))?;
            emit(args, &response, |v| {
                let items = v["workflows"].as_array().cloned().unwrap_or_default();
                if items.is_empty() {
                    println!("no workflows recorded");
                }
                for w in items {
                    println!("{}  {}  {}", w["id"], w["name"], w["state"]);
                }
            })
        }
        Command::Workflow(WorkflowCommand::Inspect { id }) => {
            let response = get(url_of(args, &format!("/api/v1/workflows/{id}")))?;
            emit(args, &response, |v| {
                println!("id:       {}", v["id"]);
                println!("name:     {}", v["name"]);
                println!("state:    {}", v["state"]);
                println!("model:    {}", v["model_version"]);
                for step in v["steps"].as_array().cloned().unwrap_or_default() {
                    println!(
                        "step {:?}: run {} {}{}",
                        step["key"],
                        step["run"],
                        step["state"],
                        step["failure"]
                            .as_str()
                            .map(|f| format!(" ({f})"))
                            .unwrap_or_default()
                    );
                }
            })
        }
        Command::Artifact(ArtifactCommand::Inspect { id, content }) => {
            let suffix = if *content { "?include=content" } else { "" };
            let response = get(url_of(args, &format!("/api/v1/artifacts/{id}{suffix}")))?;
            emit(args, &response, |v| {
                println!("id:         {}", v["id"]);
                println!("name:       {}", v["name"]);
                println!("media_type: {}", v["media_type"]);
                println!("digest:     {}", v["digest"]);
                println!("size:       {}", v["size"]);
                if let Some(text) = v["content_text"].as_str() {
                    println!("--- content ---");
                    println!("{text}");
                }
            })
        }
    }
}

#[allow(clippy::result_large_err)] // CliError keeps full API context
fn parse_inputs(inputs: &[String]) -> Result<Vec<serde_json::Value>, CliError> {
    inputs
        .iter()
        .map(|raw| {
            let (name, artifact) = raw.split_once('=').ok_or_else(|| {
                CliError::Usage(format!("--input expects name=artifact-id, got {raw:?}"))
            })?;
            Ok(serde_json::json!({ "name": name, "artifact": artifact }))
        })
        .collect()
}

#[allow(clippy::result_large_err)] // CliError keeps full API context
fn read_manifest(path: &str) -> Result<Value, CliError> {
    let text = if path == "-" {
        let mut buf = String::new();
        std::io::stdin()
            .read_to_string(&mut buf)
            .map_err(|e| CliError::Usage(format!("reading stdin: {e}")))?;
        buf
    } else {
        std::fs::read_to_string(path)
            .map_err(|e| CliError::Usage(format!("reading {path:?}: {e}")))?
    };
    serde_json::from_str(&text)
        .map_err(|e| CliError::Usage(format!("manifest is not valid JSON: {e}")))
}

fn url_of(args: &Args, path: &str) -> String {
    format!("{}{path}", args.url)
}

/// Distinguishes API rejections (structured envelope body kept) from
/// transport failures.
fn request_error(e: ureq::Error) -> CliError {
    match e {
        ureq::Error::Status(status, response) => {
            let mut body = String::new();
            let _ = response
                .into_reader()
                .take(1_000_000)
                .read_to_string(&mut body);
            CliError::ApiStatus { status, body }
        }
        other => CliError::Connect {
            url: String::from("hub"),
            source: other,
        },
    }
}

#[allow(clippy::result_large_err)] // CliError keeps full API context
fn get(path_url: String) -> Result<Value, CliError> {
    ureq::get(&path_url)
        .call()
        .map_err(request_error)?
        .into_json()
        .map_err(|e| CliError::BadResponse(format!("decoding body: {e}")))
}

#[allow(clippy::result_large_err)] // CliError keeps full API context
fn post_empty(path_url: String) -> Result<Value, CliError> {
    ureq::post(&path_url)
        .call()
        .map_err(request_error)?
        .into_json()
        .map_err(|e| CliError::BadResponse(format!("decoding body: {e}")))
}

#[allow(clippy::result_large_err)] // CliError keeps full API context
fn send_json(request: ureq::Request, payload: Value) -> Result<Value, CliError> {
    request
        .send_json(payload)
        .map_err(request_error)?
        .into_json()
        .map_err(|e| CliError::BadResponse(format!("decoding body: {e}")))
}

#[allow(clippy::result_large_err)] // CliError keeps full API context
fn emit(args: &Args, value: &Value, human: impl FnOnce(&Value)) -> Result<(), CliError> {
    match args.output {
        Output::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(value)
                    .map_err(|e| CliError::BadResponse(format!("re-encoding: {e}")))?
            )
        }
        Output::Human => human(value),
    }
    Ok(())
}
