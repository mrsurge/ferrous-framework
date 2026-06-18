use anyhow::{Result, anyhow};
use serde_json::Value;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FerrousShutdownStats {
    pub total: u64,
    pub terminated: u64,
    pub clean_exits: u64,
    pub force_killed: u64,
    pub errors: Vec<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FerrousShutdownResult {
    pub ok: bool,
    pub kind: String,
    pub target: String,
    pub started_at_ms: u64,
    pub ended_at_ms: u64,
    pub elapsed_ms: u64,
    pub root_pids: Vec<i64>,
    pub stats: FerrousShutdownStats,
    pub events: Vec<String>,
    pub note: Option<String>,
}

pub fn parse_shutdown_result(value: &Value) -> Result<FerrousShutdownResult> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("shutdown result must be an object"))?;
    let stats_value = object
        .get("stats")
        .ok_or_else(|| anyhow!("shutdown result missing stats"))?;
    Ok(FerrousShutdownResult {
        ok: object.get("ok").and_then(Value::as_bool).unwrap_or(false),
        kind: string_field(value, "kind")?,
        target: string_field(value, "target")?,
        started_at_ms: u64_field(value, "started_at_ms")?,
        ended_at_ms: u64_field(value, "ended_at_ms")?,
        elapsed_ms: u64_field(value, "elapsed_ms")?,
        root_pids: i64_vec_field(value, "root_pids")?,
        stats: parse_stats(stats_value)?,
        events: string_vec_field(value, "events")?,
        note: object
            .get("note")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
    })
}

fn parse_stats(value: &Value) -> Result<FerrousShutdownStats> {
    Ok(FerrousShutdownStats {
        total: u64_field(value, "total")?,
        terminated: u64_field(value, "terminated")?,
        clean_exits: u64_field(value, "clean_exits")?,
        force_killed: u64_field(value, "force_killed")?,
        errors: string_vec_field(value, "errors")?,
    })
}

fn string_field(value: &Value, key: &str) -> Result<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("shutdown result missing string field {key}"))
}

fn u64_field(value: &Value, key: &str) -> Result<u64> {
    value
        .get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("shutdown result missing u64 field {key}"))
}

fn i64_vec_field(value: &Value, key: &str) -> Result<Vec<i64>> {
    let values = value
        .get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("shutdown result missing array field {key}"))?;
    values
        .iter()
        .map(|item| {
            item.as_i64()
                .ok_or_else(|| anyhow!("shutdown result field {key} contains non-i64 value"))
        })
        .collect()
}

fn string_vec_field(value: &Value, key: &str) -> Result<Vec<String>> {
    let values = value
        .get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("shutdown result missing array field {key}"))?;
    values
        .iter()
        .map(|item| {
            item.as_str()
                .map(ToOwned::to_owned)
                .ok_or_else(|| anyhow!("shutdown result field {key} contains non-string value"))
        })
        .collect()
}
