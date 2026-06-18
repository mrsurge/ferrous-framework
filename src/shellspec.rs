use anyhow::{Context, Result, anyhow};
use serde_json::{Map, Value};
use std::{collections::HashMap, net::TcpListener};

#[derive(Clone, Debug, Default)]
pub struct ShellspecRenderInput {
    pub ctx: HashMap<String, String>,
    pub env: HashMap<String, String>,
}

#[derive(Clone, Debug, Default)]
struct RenderState {
    free_port: Option<u16>,
}

pub fn render_shellspec_value(value: &Value, input: &ShellspecRenderInput) -> Result<Value> {
    let mut state = RenderState::default();
    render_value(value, input, &mut state)
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
