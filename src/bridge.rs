use anyhow::{Result, anyhow};
use serde_json::Value;

pub const REQUIRED_BRIDGE_API: u64 = 1;
pub const REQUIRED_BRIDGE_SUPPORTS: &[&str] = &["ctx", "host", "free_port", "shellspec_parity"];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BridgeInfo {
    pub bridge_api: u64,
    pub framework_shells_version: String,
    pub supports: Vec<String>,
}

pub fn validate_bridge_info(info: &Value) -> Result<BridgeInfo> {
    let bridge_api = info
        .get("bridge_api")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("ferrous Python bridge missing numeric bridge_api"))?;
    if bridge_api < REQUIRED_BRIDGE_API {
        return Err(anyhow!(
            "ferrous Python bridge API {bridge_api} is too old; need {REQUIRED_BRIDGE_API}"
        ));
    }

    let framework_shells_version = info
        .get("framework_shells_version")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_owned();

    let supports_obj = info
        .get("supports")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("ferrous Python bridge missing supports map"))?;

    let mut supports = Vec::new();
    for required in REQUIRED_BRIDGE_SUPPORTS {
        if supports_obj.get(*required).and_then(Value::as_bool) != Some(true) {
            return Err(anyhow!(
                "ferrous Python bridge from framework-shells {framework_shells_version} does not support required feature {required}"
            ));
        }
        supports.push((*required).to_owned());
    }

    Ok(BridgeInfo {
        bridge_api,
        framework_shells_version,
        supports,
    })
}
