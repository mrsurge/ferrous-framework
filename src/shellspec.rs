use anyhow::{Context, Result, anyhow, bail};
use serde_json::{Map, Value};
use std::{collections::HashMap, net::TcpListener, path::PathBuf};

#[derive(Clone, Debug, Default)]
pub struct ShellspecRenderInput {
    pub ctx: HashMap<String, String>,
    pub env: HashMap<String, String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RenderedShellSpec {
    pub id: String,
    pub backend: String,
    pub command: Vec<String>,
    pub cwd: Option<PathBuf>,
    pub env: HashMap<String, String>,
    pub subgroups: Vec<String>,
    pub pty_mode: String,
    pub autostart: bool,
    pub readiness: Option<RenderedReadinessProbe>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RenderedReadinessProbe {
    pub probe_type: String,
    pub timeout_seconds: f64,
    pub pattern: Option<String>,
    pub host: String,
    pub port: Option<u16>,
    pub url: Option<String>,
    pub status_codes: Vec<u16>,
}

#[derive(Clone, Debug, Default)]
struct RenderState {
    free_port: Option<u16>,
}

pub fn render_shellspec_value(value: &Value, input: &ShellspecRenderInput) -> Result<Value> {
    let mut state = RenderState::default();
    render_value(value, input, &mut state)
}

pub fn render_shellspec_entry(
    document: &Value,
    entry: &str,
    input: &ShellspecRenderInput,
) -> Result<RenderedShellSpec> {
    let rendered = render_shellspec_value(document, input)?;
    parse_shellspec_entry(&rendered, entry)
}

pub fn parse_shellspec_entry(document: &Value, entry: &str) -> Result<RenderedShellSpec> {
    let shells = document
        .get("shells")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("shellspec document missing shells object"))?;
    let shell = shells
        .get(entry)
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("shellspec id '{entry}' not found"))?;
    let id = shell
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or(entry)
        .to_owned();
    let backend = shell
        .get("backend")
        .and_then(Value::as_str)
        .unwrap_or("proc")
        .to_ascii_lowercase();
    let command = parse_command_value(shell.get("command"), &id)?;
    let cwd = shell
        .get("cwd")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from);
    let env = parse_string_map(shell.get("env"));
    let subgroups = parse_string_list(shell.get("subgroups"));
    let pty_mode = shell
        .get("pty_mode")
        .and_then(Value::as_str)
        .unwrap_or("raw")
        .to_owned();
    let autostart = shell
        .get("autostart")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let readiness = parse_readiness_value(shell.get("readiness"), &id)?;
    Ok(RenderedShellSpec {
        id,
        backend,
        command,
        cwd,
        env,
        subgroups,
        pty_mode,
        autostart,
        readiness,
    })
}

fn render_value(
    value: &Value,
    input: &ShellspecRenderInput,
    state: &mut RenderState,
) -> Result<Value> {
    match value {
        Value::String(value) => Ok(Value::String(render_string(value, input, state)?)),
        Value::Array(values) => values
            .iter()
            .map(|item| render_value(item, input, state))
            .collect::<Result<Vec<_>>>()
            .map(Value::Array),
        Value::Object(values) => values
            .iter()
            .map(|(key, item)| Ok((key.clone(), render_value(item, input, state)?)))
            .collect::<Result<Map<String, Value>>>()
            .map(Value::Object),
        _ => Ok(value.clone()),
    }
}

fn parse_command_value(value: Option<&Value>, id: &str) -> Result<Vec<String>> {
    match value {
        Some(Value::Array(items)) => {
            let command = items
                .iter()
                .map(|item| match item {
                    Value::String(value) => Ok(value.clone()),
                    _ => bail!("shellspec '{id}' command array must contain only strings"),
                })
                .collect::<Result<Vec<_>>>()?;
            if command.is_empty() {
                bail!("shellspec '{id}' command cannot be empty");
            }
            Ok(command)
        }
        Some(Value::String(command)) => {
            let command = shlex::split(command)
                .ok_or_else(|| anyhow!("shellspec '{id}' command string has invalid quoting"))?;
            if command.is_empty() {
                bail!("shellspec '{id}' command cannot be empty");
            }
            Ok(command)
        }
        Some(_) => bail!("shellspec '{id}' command must be a string or string array"),
        None => bail!("shellspec '{id}' command is required"),
    }
}

fn parse_string_map(value: Option<&Value>) -> HashMap<String, String> {
    value
        .and_then(Value::as_object)
        .map(|object| {
            object
                .iter()
                .map(|(key, value)| {
                    (
                        key.clone(),
                        value
                            .as_str()
                            .map(str::to_owned)
                            .unwrap_or_else(|| value.to_string()),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

fn parse_string_list(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

fn parse_readiness_value(
    value: Option<&Value>,
    id: &str,
) -> Result<Option<RenderedReadinessProbe>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let Some(object) = value.as_object() else {
        bail!("shellspec '{id}' readiness must be an object");
    };
    let probe_type = object
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    if probe_type.is_empty() {
        bail!("shellspec '{id}' readiness.type is required");
    }
    Ok(Some(RenderedReadinessProbe {
        probe_type,
        timeout_seconds: parse_f64(object.get("timeout")).unwrap_or(30.0),
        pattern: object
            .get("pattern")
            .and_then(Value::as_str)
            .map(str::to_owned),
        host: object
            .get("host")
            .and_then(Value::as_str)
            .unwrap_or("127.0.0.1")
            .to_owned(),
        port: parse_u16(object.get("port")),
        url: object.get("url").and_then(Value::as_str).map(str::to_owned),
        status_codes: object
            .get("status_codes")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| parse_u16(Some(item)))
                    .collect()
            })
            .unwrap_or_else(|| vec![200]),
    }))
}

fn parse_f64(value: Option<&Value>) -> Option<f64> {
    match value {
        Some(Value::Number(number)) => number.as_f64(),
        Some(Value::String(raw)) => raw.parse::<f64>().ok(),
        _ => None,
    }
}

fn parse_u16(value: Option<&Value>) -> Option<u16> {
    match value {
        Some(Value::Number(number)) => number.as_u64().and_then(|raw| u16::try_from(raw).ok()),
        Some(Value::String(raw)) => raw.trim().parse::<u16>().ok(),
        _ => None,
    }
}

fn render_string(
    value: &str,
    input: &ShellspecRenderInput,
    state: &mut RenderState,
) -> Result<String> {
    let mut out = String::with_capacity(value.len());
    let mut rest = value;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after_start = &rest[start + 2..];
        let Some(end) = after_start.find('}') else {
            out.push_str(&rest[start..]);
            return Ok(out);
        };
        let key = after_start[..end].trim();
        out.push_str(&resolve_template_key(key, input, state)?);
        rest = &after_start[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

fn resolve_template_key(
    key: &str,
    input: &ShellspecRenderInput,
    state: &mut RenderState,
) -> Result<String> {
    if key.is_empty() {
        return Ok(String::new());
    }
    if key == "free_port" {
        if state.free_port.is_none() {
            state.free_port = Some(find_free_port()?);
        }
        return Ok(state.free_port.expect("free_port set").to_string());
    }
    if let Some(name) = key.strip_prefix("env:") {
        return Ok(input.env.get(name).cloned().unwrap_or_default());
    }
    if let Some(name) = key.strip_prefix("ctx:") {
        return Ok(input.ctx.get(name).cloned().unwrap_or_default());
    }
    if let Some(value) = input.ctx.get(key) {
        return Ok(value.clone());
    }
    Ok(input.env.get(key).cloned().unwrap_or_default())
}

fn find_free_port() -> Result<u16> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).context("failed to reserve free port")?;
    let port = listener
        .local_addr()
        .map_err(|err| anyhow!("failed to read free port: {err}"))?
        .port();
    Ok(port)
}
