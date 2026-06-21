use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const FWS_SOCKETIO_NAMESPACE: &str = "/fws";
pub const FWS_SOCKETIO_SOCKET_PATH: &str = "/fws_ws/socket.io";

pub const FWS_BROWSER_ROLE: &str = "browser";
pub const FWS_PEER_ROLE: &str = "peer";
pub const FWS_DASHBOARD_ROOM: &str = "fws:dashboard";
pub const FWS_PEER_ROOM: &str = "fws:peers";

pub const FWS_REQUEST_EVENT: &str = "fws_request";
pub const FWS_NOTIFICATION_EVENT: &str = "fws_notification";
pub const FWS_PEER_SUBSCRIPTIONS_EVENT: &str = "fws_peer_subscriptions";
pub const FWS_PEER_REQUEST_EVENT: &str = "fws_peer_request";
pub const FWS_PEER_NOTIFICATION_EVENT: &str = "fws_peer_notification";

pub const FWS_DASHBOARD_OPEN_METHOD: &str = "fws.dashboard.open";
pub const FWS_DASHBOARD_REFRESH_METHOD: &str = "fws.dashboard.refresh";
pub const FWS_LOGS_OPEN_METHOD: &str = "fws.logs.open";
pub const FWS_LOGS_CLOSE_METHOD: &str = "fws.logs.close";
pub const FWS_LOGS_INITIAL_METHOD: &str = "fws.logs.initial";
pub const FWS_SHELL_INPUT_METHOD: &str = "fws.shell.input";
pub const FWS_LOGS_CHUNK_METHOD: &str = "fws.logs.chunk";
pub const FWS_LOGS_IO_METADATA_METHOD: &str = "fws.logs.io_metadata";
pub const FWS_LOGS_RESET_METHOD: &str = "fws.logs.reset";
pub const FWS_ERROR_METHOD: &str = "fws.error";

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct FwsPeerAuth {
    pub role: String,
    pub api_token: String,
    pub runtime_id: String,
    pub pid: String,
}

impl FwsPeerAuth {
    pub fn new(
        api_token: impl Into<String>,
        runtime_id: impl Into<String>,
        pid: impl Into<String>,
    ) -> Self {
        Self {
            role: FWS_PEER_ROLE.to_owned(),
            api_token: api_token.into(),
            runtime_id: runtime_id.into(),
            pid: pid.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct FwsPeerSubscriptions {
    pub shell_ids: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct FwsPeerShellInputParams {
    pub shell_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub data: String,
    #[serde(default)]
    pub append_newline: bool,
    #[serde(default)]
    pub eof: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub source: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct FwsPeerShellInputRequest {
    pub method: String,
    pub params: FwsPeerShellInputParams,
}

impl FwsPeerShellInputRequest {
    pub fn new(
        shell_id: impl Into<String>,
        data: impl Into<String>,
        append_newline: bool,
        eof: bool,
        source: impl Into<String>,
    ) -> Self {
        Self {
            method: FWS_SHELL_INPUT_METHOD.to_owned(),
            params: FwsPeerShellInputParams {
                shell_id: shell_id.into(),
                data: data.into(),
                append_newline,
                eof,
                source: source.into(),
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct FwsPeerSuccessResponse {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl FwsPeerSuccessResponse {
    pub fn new(data: Option<Value>) -> Self {
        Self { ok: true, data }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct FwsPeerErrorResponse {
    pub ok: bool,
    pub code: String,
    pub error: String,
}

impl FwsPeerErrorResponse {
    pub fn new(code: impl Into<String>, error: impl Into<String>) -> Self {
        Self {
            ok: false,
            code: code.into(),
            error: error.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum FwsPeerResponse {
    Success(FwsPeerSuccessResponse),
    Error(FwsPeerErrorResponse),
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct FwsJsonRpcNotification {
    pub jsonrpc: String,
    pub method: String,
    pub params: Value,
}

pub fn shell_room(shell_id: &str) -> String {
    format!("shell:{shell_id}")
}

pub fn peer_notification_requires_subscription(method: &str) -> bool {
    matches!(
        method,
        FWS_LOGS_CHUNK_METHOD | FWS_LOGS_RESET_METHOD | FWS_ERROR_METHOD
    )
}

pub fn notification_shell_id(notification: &FwsJsonRpcNotification) -> Option<String> {
    notification
        .params
        .as_object()
        .and_then(|params| params.get("shell_id"))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn peer_auth_matches_python_shape() {
        let auth = FwsPeerAuth::new("token", "runtime", "42");
        assert_eq!(
            serde_json::to_value(auth).unwrap(),
            json!({
                "role": "peer",
                "api_token": "token",
                "runtime_id": "runtime",
                "pid": "42"
            })
        );
    }

    #[test]
    fn peer_shell_input_request_matches_python_shape() {
        let request = FwsPeerShellInputRequest::new("fs_1", "hello", true, false, "dashboard");
        assert_eq!(
            serde_json::to_value(request).unwrap(),
            json!({
                "method": "fws.shell.input",
                "params": {
                    "shell_id": "fs_1",
                    "data": "hello",
                    "append_newline": true,
                    "eof": false,
                    "source": "dashboard"
                }
            })
        );
    }

    #[test]
    fn peer_ack_responses_match_python_shape() {
        let success =
            FwsPeerResponse::Success(FwsPeerSuccessResponse::new(Some(json!({"accepted": true}))));
        assert_eq!(
            serde_json::to_value(success).unwrap(),
            json!({"ok": true, "data": {"accepted": true}})
        );

        let error = FwsPeerResponse::Error(FwsPeerErrorResponse::new("not_owner", "not mine"));
        assert_eq!(
            serde_json::to_value(error).unwrap(),
            json!({"ok": false, "code": "not_owner", "error": "not mine"})
        );
    }

    #[test]
    fn peer_subscription_filter_is_limited_to_log_like_notifications() {
        assert!(peer_notification_requires_subscription("fws.logs.chunk"));
        assert!(peer_notification_requires_subscription("fws.logs.reset"));
        assert!(peer_notification_requires_subscription("fws.error"));
        assert!(!peer_notification_requires_subscription(
            "fws.shell.updated"
        ));
    }
}
