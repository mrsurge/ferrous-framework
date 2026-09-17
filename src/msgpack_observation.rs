//! Bounded single-frame observation decoding; callers retain incomplete bytes.
use crate::log_projection::PARSE_BYTES;
use anyhow::{Result, bail, ensure};
use rmpv::ValueRef;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Debug, Serialize)]
pub struct DecodedFrame {
    pub value: Value,
    pub consumed: usize,
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn normalize(value: ValueRef<'_>, depth: usize) -> Result<Value> {
    ensure!(depth <= 64, "MessagePack nesting limit exceeded");
    Ok(match value {
        ValueRef::Nil => Value::Null,
        ValueRef::Boolean(value) => json!(value),
        ValueRef::Integer(value) => {
            if let Some(n) = value.as_i64() {
                if (-9007199254740991..=9007199254740991).contains(&n) {
                    json!(n)
                } else {
                    json!({"$fws":"integer","decimal":n.to_string()})
                }
            } else {
                let n = value.as_u64().expect("unsigned MessagePack integer");
                if n <= 9007199254740991 {
                    json!(n)
                } else {
                    json!({"$fws":"integer","decimal":n.to_string()})
                }
            }
        }
        ValueRef::F32(value) => normalize(ValueRef::F64(f64::from(value)), depth)?,
        ValueRef::F64(value) => {
            if value.is_finite() {
                json!(value)
            } else {
                json!({"$fws":"float64","hex":hex(&value.to_be_bytes())})
            }
        }
        ValueRef::String(value) => json!(
            value
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("invalid UTF-8"))?
        ),
        ValueRef::Binary(value) => json!({"$fws":"binary","hex":hex(value)}),
        ValueRef::Ext(-1, value) => {
            let (seconds, nanoseconds) = match value.len() {
                4 => (u32::from_be_bytes(value.try_into()?) as i64, 0),
                8 => {
                    let packed = u64::from_be_bytes(value.try_into()?);
                    ((packed & 0x3ffffffff) as i64, (packed >> 34) as u32)
                }
                12 => (
                    i64::from_be_bytes(value[4..].try_into()?),
                    u32::from_be_bytes(value[..4].try_into()?),
                ),
                _ => bail!("invalid timestamp"),
            };
            ensure!(nanoseconds < 1_000_000_000, "invalid timestamp");
            json!({"$fws":"timestamp","seconds":seconds.to_string(),"nanoseconds":nanoseconds})
        }
        ValueRef::Ext(code, value) => json!({"$fws":"extension","code":code,"hex":hex(value)}),
        ValueRef::Array(values) => Value::Array(
            values
                .into_iter()
                .map(|v| normalize(v, depth + 1))
                .collect::<Result<Vec<_>>>()?,
        ),
        ValueRef::Map(values) => {
            let mut keys = std::collections::HashSet::new();
            let plain = values.iter().all(|(key, _)| match key {
                ValueRef::String(key) => key
                    .as_str()
                    .is_some_and(|key| key != "$fws" && keys.insert(key.to_owned())),
                _ => false,
            });
            if plain {
                let mut object = serde_json::Map::new();
                for (key, value) in values {
                    let ValueRef::String(key) = key else {
                        unreachable!("validated string key");
                    };
                    object.insert(
                        key.as_str().expect("string key").to_owned(),
                        normalize(value, depth + 1)?,
                    );
                }
                Value::Object(object)
            } else {
                let mut entries = Vec::new();
                for (key, value) in values {
                    entries.push(json!([
                        normalize(key, depth + 1)?,
                        normalize(value, depth + 1)?
                    ]));
                }
                json!({"$fws":"map","entries":entries})
            }
        }
    })
}

pub fn decode_frame(data: &[u8], max_bytes: usize) -> Result<Option<DecodedFrame>> {
    ensure!(
        (1..=PARSE_BYTES).contains(&max_bytes),
        "invalid MessagePack budget"
    );
    let prefix = &data[..data.len().min(max_bytes)];
    let mut remaining = prefix;
    let value = match rmpv::decode::read_value_ref_with_max_depth(&mut remaining, 66) {
        Ok(value) => value,
        Err(error) => match error {
            rmpv::decode::Error::InvalidMarkerRead(ref e)
            | rmpv::decode::Error::InvalidDataRead(ref e)
                if e.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                if data.len() >= max_bytes {
                    bail!("MessagePack frame exceeds byte budget");
                }
                return Ok(None);
            }
            _ => return Err(error.into()),
        },
    };
    let consumed = prefix.len() - remaining.len();
    // rmpv accepts the reserved marker as nil. Validate strictly before exposing
    // the decoded value, including markers nested within arrays and maps.
    let mut validator = rmp_serde::Deserializer::from_read_ref(&prefix[..consumed]);
    validator.set_max_depth(66);
    let _ = serde::de::IgnoredAny::deserialize(&mut validator)?;
    Ok(Some(DecodedFrame {
        consumed,
        value: normalize(value, 0)?,
    }))
}
