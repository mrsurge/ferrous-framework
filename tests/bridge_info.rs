use ferrous_framework::bridge::{REQUIRED_BRIDGE_API, validate_bridge_info};
use serde_json::json;

#[test]
fn accepts_required_bridge_info() {
    let info = json!({
        "bridge_api": REQUIRED_BRIDGE_API,
        "framework_shells_version": "0.0.57",
        "supports": {
            "ctx": true,
            "host": true,
            "free_port": true,
            "shellspec_parity": true
        }
    });
    let validated = validate_bridge_info(&info).expect("valid bridge info");
    assert_eq!(validated.bridge_api, REQUIRED_BRIDGE_API);
    assert_eq!(validated.framework_shells_version, "0.0.57");
}

#[test]
fn rejects_missing_required_support() {
    let info = json!({
        "bridge_api": REQUIRED_BRIDGE_API,
        "framework_shells_version": "0.0.57",
        "supports": {
            "ctx": true,
            "host": true,
            "free_port": true
        }
    });
    let err = validate_bridge_info(&info).expect_err("missing support must fail");
    assert!(err.to_string().contains("shellspec_parity"));
}

#[test]
fn rejects_old_bridge_api() {
    let info = json!({
        "bridge_api": 0,
        "framework_shells_version": "0.0.1",
        "supports": {
            "ctx": true,
            "host": true,
            "free_port": true,
            "shellspec_parity": true
        }
    });
    let err = validate_bridge_info(&info).expect_err("old bridge must fail");
    assert!(err.to_string().contains("too old"));
}
