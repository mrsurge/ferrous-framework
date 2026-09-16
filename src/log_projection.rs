//! Protocol-neutral projection primitives. Raw references always address source bytes.
use anyhow::{Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const RECORD_BYTES: usize = 8192;
pub const PARSE_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Codec {
    Text,
    Json,
    Messagepack,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum WindowAction {
    Tail,
    Older,
    Newer,
    Current,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RawReference {
    pub generation: String,
    pub byte_start: u64,
    pub byte_end: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct Omission {
    pub pointer: String,
    pub original_type: String,
    pub serialized_bytes: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct RecordProjection {
    pub text: String,
    pub raw: RawReference,
    pub omissions: Vec<Omission>,
    pub diagnostic: Option<String>,
}

pub fn window_start(
    total: usize,
    current: usize,
    count: usize,
    shift: usize,
    action: WindowAction,
) -> Result<usize> {
    ensure!(
        count > 0 && shift > 0 && shift <= count,
        "invalid window bounds"
    );
    let maximum = total.saturating_sub(count);
    let start = current.min(maximum);
    Ok(match action {
        WindowAction::Tail => maximum,
        WindowAction::Older => start.saturating_sub(shift),
        WindowAction::Newer => start.saturating_add(shift).min(maximum),
        WindowAction::Current => start,
    })
}

fn kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn candidates(value: &Value, path: &str, out: &mut Vec<(usize, String, String)>) -> Result<()> {
    if let Value::Object(object) = value {
        for (key, item) in object {
            if (path.is_empty() && ["jsonrpc", "id", "method"].contains(&key.as_str()))
                || (path == "/error" && key == "code")
            {
                continue;
            }
            let pointer = format!("{path}/{}", key.replace('~', "~0").replace('/', "~1"));
            if item.as_object().is_some_and(|object| !object.is_empty()) {
                candidates(item, &pointer, out)?;
            } else {
                out.push((
                    serde_json::to_vec(item)?.len(),
                    pointer,
                    kind(item).to_owned(),
                ));
            }
        }
    }
    Ok(())
}

pub fn project_record(
    data: &[u8],
    raw: RawReference,
    codec: Codec,
    max_bytes: usize,
    parse_bytes: usize,
) -> Result<RecordProjection> {
    ensure!(
        max_bytes >= 32 && parse_bytes >= max_bytes,
        "invalid projection budgets"
    );
    ensure!(
        raw.byte_end.checked_sub(raw.byte_start) == Some(data.len() as u64),
        "raw reference does not match record bytes"
    );
    if matches!(codec, Codec::Messagepack) {
        bail!("MessagePack requires the frame decoder");
    }
    let text =
        String::from_utf8_lossy(&data[..data.len().min(max_bytes.saturating_add(4))]).into_owned();
    let preview = |reason: &str| {
        let mut end = text.len().min(max_bytes);
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        RecordProjection {
            text: text[..end].to_owned(),
            raw: raw.clone(),
            omissions: vec![],
            diagnostic: Some(reason.to_owned()),
        }
    };
    if data.len() <= max_bytes && text.len() <= max_bytes {
        return Ok(RecordProjection {
            text,
            raw,
            omissions: vec![],
            diagnostic: None,
        });
    }
    if matches!(codec, Codec::Text) {
        return Ok(preview("record_too_large"));
    }
    if data.len() > parse_bytes {
        return Ok(preview("parse_budget_exceeded"));
    }
    let Ok(mut value) = serde_json::from_slice::<Value>(data) else {
        return Ok(preview("invalid_json"));
    };
    let compact = serde_json::to_string(&value)?;
    if compact.len() <= max_bytes {
        return Ok(RecordProjection {
            text: compact,
            raw,
            omissions: vec![],
            diagnostic: None,
        });
    }
    if !value.is_object() {
        return Ok(preview("structured_summary_required"));
    }
    let mut items = Vec::new();
    candidates(&value, "", &mut items)?;
    items.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    let mut omissions = Vec::new();
    for (size, pointer, original_type) in items {
        let (parent, escaped) = pointer.rsplit_once('/').expect("field pointer");
        let key = escaped.replace("~1", "/").replace("~0", "~");
        value
            .pointer_mut(parent)
            .and_then(Value::as_object_mut)
            .expect("field parent")
            .remove(&key);
        omissions.push(Omission {
            pointer,
            original_type,
            serialized_bytes: size,
        });
        let compact = serde_json::to_string(&value)?;
        if compact.len() <= max_bytes {
            return Ok(RecordProjection {
                text: compact,
                raw,
                omissions,
                diagnostic: Some("values_omitted".to_owned()),
            });
        }
    }
    Ok(preview("structured_summary_required"))
}
